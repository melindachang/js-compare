#!/usr/bin/env python3
"""Database inspection and analysis suite for CST/AST code similarity."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import difflib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any

from dotenv import load_dotenv

from db_compare import get_db_connection

load_dotenv()

# ANSI Color Codes for terminal diff output
COLOR_RED = "\033[31m"
COLOR_GREEN = "\033[32m"
COLOR_CYAN = "\033[36m"
COLOR_YELLOW = "\033[33m"
COLOR_BOLD = "\033[1m"
COLOR_RESET = "\033[0m"


@dataclass
class PairCandidate:  # pylint: disable=too-many-instance-attributes
    file1_sha: str
    file2_sha: str
    similarity: float
    shared_weight: int
    nodes1: int
    nodes2: int
    content1: str
    content2: str


class QueryTimer:
    """Live background status ticker while database queries are executing."""

    def __init__(self, message: str = "Executing query in database"):
        self.message = message
        self.start_time = time.time()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def _tick(self) -> None:
        while not self.stop_event.wait(1.0):
            elapsed = time.time() - self.start_time
            sys.stderr.write(f"\r{COLOR_CYAN}[{self.message}... {elapsed:.1f}s elapsed]{COLOR_RESET} ")
            sys.stderr.flush()

    def __enter__(self) -> "QueryTimer":
        self.start_time = time.time()
        self.thread = threading.Thread(target=self._tick, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=0.5)
        elapsed = time.time() - self.start_time
        # Clear the ticker line
        sys.stderr.write(f"\r\033[K{COLOR_GREEN}[Completed in {elapsed:.2f}s]{COLOR_RESET}\n")
        sys.stderr.flush()


def decode_content(content: bytes | str | memoryview | None) -> str:
    """Safely decode PostgreSQL content into a UTF-8 string."""
    if content is None:
        return ""
    if isinstance(content, memoryview):
        content = bytes(content)
    if isinstance(content, bytes):
        return content.decode("utf-8", errors="replace")
    return str(content)


def build_candidate_query(sample_files: int | None = None, max_digest_freq: int | None = None) -> tuple[str, list[Any]]:
    """Return SQL query to retrieve candidate file pairs and their contents with optional filters."""
    params: list[Any] = []

    # If sampling files to avoid joining the entire database
    sample_cte = ""
    file_join = ""
    if sample_files and sample_files > 0:
        sample_cte = """
            sampled_files AS (
                SELECT file_sha FROM telegram.file_indexed LIMIT %s
            ),
        """
        file_join = """
            JOIN sampled_files sf1 ON a.file_sha = sf1.file_sha
            JOIN sampled_files sf2 ON b.file_sha = sf2.file_sha
        """
        params.append(sample_files)

    freq_filter = ""
    if max_digest_freq and max_digest_freq > 0:
        freq_filter = """
            AND a.digest NOT IN (
                SELECT digest FROM telegram.file_subtrees
                GROUP BY digest HAVING count(DISTINCT file_sha) > %s
            )
        """

    query = f"""
        WITH {sample_cte}
        candidate_pairs AS (
            SELECT
                a.file_sha AS file1,
                b.file_sha AS file2,
                SUM(LEAST(a.count, b.count) * a.weight) AS shared_weight
            FROM telegram.file_subtrees a
            JOIN telegram.file_subtrees b
                ON a.digest = b.digest
                AND a.file_sha < b.file_sha
            {file_join}
            WHERE a.weight >= %s
              AND b.weight >= %s
              {freq_filter}
            GROUP BY a.file_sha, b.file_sha
            HAVING SUM(LEAST(a.count, b.count) * a.weight) >= %s
        ),
        ranked_pairs AS (
            SELECT
                c.file1,
                c.file2,
                c.shared_weight,
                t1.total_nodes AS nodes1,
                t2.total_nodes AS nodes2,
                LEAST(1.0, (2.0 * c.shared_weight / (t1.total_nodes + t2.total_nodes))) AS similarity
            FROM candidate_pairs c
            JOIN telegram.file_indexed t1 ON c.file1 = t1.file_sha
            JOIN telegram.file_indexed t2 ON c.file2 = t2.file_sha
            WHERE (2.0 * c.shared_weight / (t1.total_nodes + t2.total_nodes)) >= %s
              AND (2.0 * c.shared_weight / (t1.total_nodes + t2.total_nodes)) <= %s
            ORDER BY similarity DESC
            LIMIT %s
        )
        SELECT
            r.file1,
            r.file2,
            r.similarity,
            r.shared_weight,
            r.nodes1,
            r.nodes2,
            f1.content AS content1,
            f2.content AS content2
        FROM ranked_pairs r
        JOIN telegram.githubcodeapi_file f1 ON r.file1 = f1.sha
        JOIN telegram.githubcodeapi_file f2 ON r.file2 = f2.sha;
    """
    return query, params


def fetch_candidate_pairs(
    conn: Any,
    min_sim: float,
    max_sim: float,
    min_weight: int,
    min_shared: int,
    limit: int,
    sample_files: int | None = None,
    max_digest_freq: int | None = None,
    timeout: int = 45,
    verbose: bool = False,
) -> list[PairCandidate]:
    """Execute candidate query with timing, timeout guard, and progress logging."""
    # Fast path for exact 1.0 clones if min_similarity == 1.0
    if min_sim >= 0.9999 and max_sim >= 1.0:
        if verbose:
            print("[Verbose] Using fast indexed query on root_digest for exact 1.0 clones...")
        exact_query = """
            WITH exact_pairs AS (
                SELECT
                    a.file_sha AS file1,
                    b.file_sha AS file2,
                    1.0::float AS similarity,
                    a.total_nodes AS shared_weight,
                    a.total_nodes AS nodes1,
                    b.total_nodes AS nodes2
                FROM telegram.file_indexed a
                JOIN telegram.file_indexed b
                    ON a.root_digest = b.root_digest
                    AND a.file_sha < b.file_sha
                LIMIT %s
            )
            SELECT
                p.file1, p.file2, p.similarity, p.shared_weight, p.nodes1, p.nodes2,
                f1.content AS content1,
                f2.content AS content2
            FROM exact_pairs p
            JOIN telegram.githubcodeapi_file f1 ON p.file1 = f1.sha
            JOIN telegram.githubcodeapi_file f2 ON p.file2 = f2.sha;
        """
        with conn.cursor() as cur:
            cur.execute(exact_query, (limit,))
            rows = cur.fetchall()
            return [
                PairCandidate(
                    file1_sha=f1,
                    file2_sha=f2,
                    similarity=float(sim),
                    shared_weight=int(shared),
                    nodes1=int(n1),
                    nodes2=int(n2),
                    content1=decode_content(c1),
                    content2=decode_content(c2),
                )
                for f1, f2, sim, shared, n1, n2, c1, c2 in rows
            ]

    query, base_params = build_candidate_query(sample_files=sample_files, max_digest_freq=max_digest_freq)
    params = list(base_params) + [min_weight, min_weight]
    if max_digest_freq and max_digest_freq > 0:
        params.append(max_digest_freq)
    params.extend([min_shared, min_sim, max_sim, limit])

    with conn.cursor() as cur:
        if timeout > 0:
            if verbose:
                print(f"[Verbose] Setting statement_timeout to {timeout}s")
            cur.execute(f"SET statement_timeout = {int(timeout * 1000)};")

        if verbose:
            print(f"[Verbose] Executing query with parameters: {params}")

        with QueryTimer("Scanning PostgreSQL for candidate pairs"):
            cur.execute(query, params)
            rows = cur.fetchall()

    if verbose:
        print(f"[Verbose] Retrieved {len(rows)} matching pair record(s).")

    candidates: list[PairCandidate] = []
    for f1, f2, sim, shared, n1, n2, c1, c2 in rows:
        candidates.append(
            PairCandidate(
                file1_sha=f1,
                file2_sha=f2,
                similarity=float(sim),
                shared_weight=int(shared),
                nodes1=int(n1),
                nodes2=int(n2),
                content1=decode_content(c1),
                content2=decode_content(c2),
            )
        )
    return candidates


def run_diagnostics(conn: Any, args: argparse.Namespace) -> None:
    """Print performance diagnosis of candidate pair matching."""
    print(f"\n{COLOR_BOLD}=== Candidate Query Performance Diagnostics ==={COLOR_RESET}")
    with conn.cursor() as cur:
        # Check table sizes
        cur.execute("SELECT count(*) FROM telegram.file_indexed;")
        total_files = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM telegram.file_subtrees;")
        total_subtrees = cur.fetchone()[0]
        cur.execute("SELECT count(DISTINCT digest) FROM telegram.file_subtrees;")
        unique_digests = cur.fetchone()[0]

        print(f"Total files indexed:     {total_files:,}")
        print(f"Total subtree rows:      {total_subtrees:,}")
        print(f"Unique subtree digests:   {unique_digests:,}")

        # Check for ubiquitous subtrees (the hub/combinatorial explosion problem)
        print(f"\n{COLOR_YELLOW}Top 10 Most Common Subtrees (weight >= {args.min_weight}):{COLOR_RESET}")
        print(f"Subtrees appearing in hundreds of files cause O(N^2) join row explosions.")
        cur.execute("""
            SELECT digest, label, weight, count(DISTINCT file_sha) AS file_count
            FROM telegram.file_subtrees
            WHERE weight >= %s
            GROUP BY digest, label, weight
            ORDER BY file_count DESC
            LIMIT 10;
        """, (args.min_weight,))
        rows = cur.fetchall()

        print(f"{'Digest':<18} {'Label':<24} {'Weight':<8} {'Files':<8} {'Join Combinations':<18}")
        print("-" * 80)
        total_potential_combos = 0
        for d, l, w, cnt in rows:
            combos = cnt * (cnt - 1) // 2
            total_potential_combos += combos
            print(f"{d[:16]:<18} {l[:22]:<24} {w:<8} {cnt:<8,} ~{combos:<18,}")
        print("-" * 80)
        print(f"Just these top 10 subtrees alone generate ~{total_potential_combos:,} intermediate join rows!")

        # Run EXPLAIN on the candidate query
        print(f"\n{COLOR_BOLD}PostgreSQL Query Execution Plan (EXPLAIN):{COLOR_RESET}")
        query, base_params = build_candidate_query(sample_files=args.sample_files, max_digest_freq=args.max_digest_freq)
        params = list(base_params) + [args.min_weight, args.min_weight]
        if args.max_digest_freq and args.max_digest_freq > 0:
            params.append(args.max_digest_freq)
        params.extend([args.min_shared_weight, args.min_similarity, args.max_similarity, args.limit])

        cur.execute("EXPLAIN " + query, params)
        plan_lines = cur.fetchall()
        for (line,) in plan_lines:
            print(f"  {line}")


def colorized_diff(
    text1: str,
    text2: str,
    label1: str,
    label2: str,
    use_color: bool = True,
) -> str:
    """Generate a unified diff with optional ANSI colors."""
    lines1 = text1.splitlines(keepends=True)
    lines2 = text2.splitlines(keepends=True)

    diff = list(
        difflib.unified_diff(
            lines1,
            lines2,
            fromfile=label1,
            tofile=label2,
            n=3,
        )
    )
    if not diff:
        return "(File contents are textually identical)\n"

    if not use_color:
        return "".join(diff)

    colored_lines: list[str] = []
    for line in diff:
        if line.startswith("+++") or line.startswith("---"):
            colored_lines.append(f"{COLOR_BOLD}{line}{COLOR_RESET}")
        elif line.startswith("+"):
            colored_lines.append(f"{COLOR_GREEN}{line}{COLOR_RESET}")
        elif line.startswith("-"):
            colored_lines.append(f"{COLOR_RED}{line}{COLOR_RESET}")
        elif line.startswith("@@"):
            colored_lines.append(f"{COLOR_CYAN}{line}{COLOR_RESET}")
        else:
            colored_lines.append(line)

    return "".join(colored_lines)


def export_pair(pair: PairCandidate, target_dir: Path) -> Path:
    """Save pair contents to disk."""
    target_dir.mkdir(parents=True, exist_ok=True)
    file1_path = target_dir / f"{pair.file1_sha[:12]}.js"
    file2_path = target_dir / f"{pair.file2_sha[:12]}.js"
    file1_path.write_text(pair.content1, encoding="utf-8")
    file2_path.write_text(pair.content2, encoding="utf-8")
    return target_dir


def launch_external_diff(pair: PairCandidate, custom_diff_tool: str | None = None) -> None:
    """Open pair files in an external visual diff tool."""
    with tempfile.TemporaryDirectory(prefix="js_diff_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        f1 = tmp_path / f"A_{pair.file1_sha[:10]}.js"
        f2 = tmp_path / f"B_{pair.file2_sha[:10]}.js"
        f1.write_text(pair.content1, encoding="utf-8")
        f2.write_text(pair.content2, encoding="utf-8")

        if custom_diff_tool:
            cmd = custom_diff_tool.format(file1=str(f1), file2=str(f2))
            subprocess.run(cmd, shell=True, check=False)
            return

        diff_prog = os.getenv("DIFFPROG")
        if diff_prog:
            subprocess.run([diff_prog, str(f1), str(f2)], check=False)
            return

        # Try VS Code diff if 'code' is on PATH
        if shutil.which("code"):
            subprocess.run(["code", "--diff", str(f1), str(f2)], check=False)
            return

        # Try Meld if installed
        if shutil.which("meld"):
            subprocess.run(["meld", str(f1), str(f2)], check=False)
            return

        # Fall back to git diff --no-index
        subprocess.run(["git", "diff", "--no-index", "--color", str(f1), str(f2)], check=False)


def generate_html_report(pairs: list[PairCandidate], output_path: Path) -> None:
    """Generate a self-contained HTML side-by-side comparison report."""
    differ = difflib.HtmlDiff(wrapcolumn=90)
    css = (
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; "
        "margin: 20px; }\n"
        ".pair-card { border: 1px solid #ddd; border-radius: 6px; margin-bottom: 24px; "
        "padding: 16px; background: #fafafa; }\n"
        ".pair-header { font-size: 1.1em; font-weight: bold; margin-bottom: 10px; }\n"
        "table.diff { font-family: monospace; font-size: 12px; width: 100%; "
        "border-collapse: collapse; background: #fff; }\n"
        ".diff_header { background-color: #e0e0e0; }\n"
        "td.diff_header { text-align: right; }\n"
        ".diff_next { background-color: #c0c0c0; }\n"
        ".diff_add { background-color: #aaffaa; }\n"
        ".diff_chg { background-color: #ffff77; }\n"
        ".diff_sub { background-color: #ffaaaa; }"
    )
    sections: list[str] = [
        "<!DOCTYPE html>",
        "<html><head><meta charset='utf-8'><title>Pairwise Similarity Report</title>",
        f"<style>{css}</style></head><body>",
        "<h1>Pairwise Similarity Inspection Report</h1>",
        f"<p>Showing {len(pairs)} candidate pairs.</p>",
    ]

    for i, pair in enumerate(pairs, 1):
        diff_table = differ.make_table(
            pair.content1.splitlines(keepends=True),
            pair.content2.splitlines(keepends=True),
            fromdesc=f"File 1: {pair.file1_sha} ({pair.nodes1} nodes)",
            todesc=f"File 2: {pair.file2_sha} ({pair.nodes2} nodes)",
            context=True,
            numlines=3,
        )
        sections.append(
            f"<div class='pair-card'>"
            f"<div class='pair-header'>Pair #{i} &mdash; Similarity: {pair.similarity:.4f} "
            f"(Shared AST Weight: {pair.shared_weight})</div>"
            f"<div><code>{pair.file1_sha}</code> &harr; <code>{pair.file2_sha}</code></div>"
            f"<details open style='margin-top: 10px;'><summary><b>Side-by-side Diff</b>"
            f"</summary>{diff_table}</details></div>"
        )

    sections.append("</body></html>")
    output_path.write_text("\n".join(sections), encoding="utf-8")


def interactive_loop(pairs: list[PairCandidate], custom_diff_tool: str | None = None) -> None:
    """Prompt user to inspect each pair interactively."""
    total = len(pairs)
    use_color = sys.stdout.isatty()

    for idx, pair in enumerate(pairs, 1):
        lines1 = pair.content1.count("\n") + 1
        lines2 = pair.content2.count("\n") + 1
        print("=" * 80)
        print(
            f"[{idx}/{total}] Similarity: {pair.similarity:.4f} | "
            f"Shared Weight: {pair.shared_weight} nodes | "
            f"Nodes: ({pair.nodes1} vs {pair.nodes2})"
        )
        print(f"  File 1: {pair.file1_sha} ({lines1} lines)")
        print(f"  File 2: {pair.file2_sha} ({lines2} lines)")
        print("-" * 80)

        while True:
            try:
                choice = input(
                    "Action: [d]iff terminal, [v]iew in diff tool, [e]xport files, "
                    "[n]ext pair, [q]uit: "
                ).strip().lower()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting.")
                return

            if choice in ("", "d"):
                diff = colorized_diff(
                    pair.content1,
                    pair.content2,
                    f"a/{pair.file1_sha[:10]}.js",
                    f"b/{pair.file2_sha[:10]}.js",
                    use_color=use_color,
                )
                print(diff)
            elif choice == "v":
                print("Opening visual diff...")
                launch_external_diff(pair, custom_diff_tool)
            elif choice == "e":
                export_dir = Path("inspected_pairs") / f"pair_{idx:03d}_sim_{pair.similarity:.3f}"
                export_pair(pair, export_dir)
                print(f"Exported pair to: {export_dir.resolve()}")
            elif choice in ("n", "s"):
                break
            elif choice == "q":
                print("Quitting inspection.")
                return
            else:
                print("Unknown action. Enter 'd', 'v', 'e', 'n', or 'q'.")


def handle_pairs_command(args: argparse.Namespace) -> None:
    """Handle the 'pairs' subcommand execution."""
    if args.verbose:
        print(f"[Verbose] Connecting to PostgreSQL database...")

    try:
        with get_db_connection() as conn:
            if args.diagnose or args.explain:
                run_diagnostics(conn, args)
                return

            print(
                f"Querying candidate pairs in similarity window "
                f"[{args.min_similarity:.4f}, {args.max_similarity:.4f}] "
                f"(min_weight: {args.min_weight}, min_shared: {args.min_shared_weight}, limit: {args.limit})..."
            )
            pairs = fetch_candidate_pairs(
                conn,
                min_sim=args.min_similarity,
                max_sim=args.max_similarity,
                min_weight=args.min_weight,
                min_shared=args.min_shared_weight,
                limit=args.limit,
                sample_files=args.sample_files,
                max_digest_freq=args.max_digest_freq,
                timeout=args.timeout,
                verbose=args.verbose,
            )
    except Exception as exc:  # pylint: disable=broad-except
        err_msg = str(exc)
        print(f"\n{COLOR_RED}Database query error: {err_msg}{COLOR_RESET}", file=sys.stderr)
        if "canceling statement due to statement timeout" in err_msg or "timeout" in err_msg:
            print(f"\n{COLOR_YELLOW}[Diagnosis & Suggested Fixes]{COLOR_RESET}")
            print("The self-join on telegram.file_subtrees timed out. This occurs when common subtrees")
            print("create a massive combinatorial explosion of pair comparisons across the database.")
            print("\nRecommended solutions:")
            print(f"  1. Use --sample-files (e.g. --sample-files 2000):")
            print("     Restricts candidate search to a random/initial slice of files, running in seconds.")
            print(f"  2. Increase --min-weight (e.g. --min-weight 10 or 15):")
            print("     Filters out tiny 5-node subtrees (common 1-line statements) that cause millions of joins.")
            print(f"  3. Increase --min-shared-weight (e.g. --min-shared-weight 50).")
            print(f"  4. Run with --diagnose or --explain to inspect query plan and common subtrees:")
            print("     ./inspect_db.py pairs --diagnose")
        sys.exit(1)

    if not pairs:
        print(
            f"No candidate pairs found with similarity between "
            f"{args.min_similarity:.4f} and {args.max_similarity:.4f}."
        )
        return

    print(f"Found {len(pairs)} candidate pair(s).\n")

    # Mode 1: Export all pairs
    if args.export_all:
        args.export_all.mkdir(parents=True, exist_ok=True)
        for i, pair in enumerate(pairs, 1):
            pair_dir = args.export_all / f"pair_{i:03d}_{pair.similarity:.3f}"
            export_pair(pair, pair_dir)
        print(f"Successfully exported {len(pairs)} pair(s) to: {args.export_all.resolve()}")
        return

    # Mode 2: Generate HTML report
    if args.html_report:
        generate_html_report(pairs, args.html_report)
        print(f"HTML report successfully written to: {args.html_report.resolve()}")
        return

    # Mode 3: Print all diffs non-interactively
    if args.diff_all:
        for i, pair in enumerate(pairs, 1):
            print(f"\n# Pair {i}/{len(pairs)} | Similarity: {pair.similarity:.4f}")
            print(f"# {pair.file1_sha} <-> {pair.file2_sha}")
            print(
                colorized_diff(
                    pair.content1,
                    pair.content2,
                    f"a/{pair.file1_sha[:10]}.js",
                    f"b/{pair.file2_sha[:10]}.js",
                    use_color=sys.stdout.isatty(),
                )
            )
        return

    # Mode 4: Interactive loop (default)
    interactive_loop(pairs, custom_diff_tool=args.diff_tool)


def build_parser() -> argparse.ArgumentParser:
    """Construct argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        prog="inspect_db",
        description="Database inspection and analysis suite for CST/AST code similarity.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable detailed progress logging, execution times, and query inspection",
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available subcommands")

    # Subcommand: pairs
    pairs_parser = subparsers.add_parser(
        "pairs",
        help="Filter and manually inspect file pairs within a similarity threshold window.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    pairs_parser.add_argument(
        "--min-similarity",
        type=float,
        default=0.80,
        help="Minimum similarity score threshold (0.0 to 1.0)",
    )
    pairs_parser.add_argument(
        "--max-similarity",
        type=float,
        default=1.00,
        help="Maximum similarity score (e.g. 0.9999 to exclude exact 1.0 AST duplicates)",
    )
    pairs_parser.add_argument(
        "--min-shared-weight",
        type=int,
        default=20,
        help="Minimum shared node weight between candidate pairs",
    )
    pairs_parser.add_argument(
        "--min-weight",
        type=int,
        default=5,
        help="Minimum subtree node weight threshold (increase to 10+ to avoid boilerplate noise)",
    )
    pairs_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum candidate pairs to retrieve",
    )
    pairs_parser.add_argument(
        "--sample-files",
        type=int,
        default=None,
        metavar="N",
        help="Accelerate search by sampling an initial N files (e.g. 2000) instead of joining the entire DB",
    )
    pairs_parser.add_argument(
        "--max-digest-freq",
        type=int,
        default=None,
        metavar="N",
        help="Exclude subtrees shared across more than N files (filters out common boilerplate hub nodes)",
    )
    pairs_parser.add_argument(
        "--timeout",
        type=int,
        default=45,
        help="PostgreSQL query statement timeout in seconds (0 to disable)",
    )
    pairs_parser.add_argument(
        "--diagnose",
        action="store_true",
        help="Run database diagnostics: table sizes, top common subtrees, and query plan (EXPLAIN)",
    )
    pairs_parser.add_argument(
        "--explain",
        action="store_true",
        help="Alias for --diagnose",
    )
    pairs_parser.add_argument(
        "--export-all",
        type=Path,
        metavar="DIR",
        default=None,
        help="Non-interactive: export all matching pairs into subdirectories under DIR",
    )
    pairs_parser.add_argument(
        "--html-report",
        type=Path,
        metavar="FILE",
        default=None,
        help="Non-interactive: generate a side-by-side HTML comparison report",
    )
    pairs_parser.add_argument(
        "--diff-all",
        action="store_true",
        help="Non-interactive: print unified diffs for all matching pairs directly to stdout",
    )
    pairs_parser.add_argument(
        "--diff-tool",
        type=str,
        default=None,
        help="Custom diff command template (e.g. 'code --diff {file1} {file2}')",
    )
    pairs_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable detailed progress logging, execution times, and query inspection",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "pairs":
        handle_pairs_command(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
