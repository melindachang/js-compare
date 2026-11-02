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
from typing import Any

from dotenv import load_dotenv

from db_compare import get_db_connection

load_dotenv()

# ANSI Color Codes for terminal diff output
COLOR_RED = "\033[31m"
COLOR_GREEN = "\033[32m"
COLOR_CYAN = "\033[36m"
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


def decode_content(content: bytes | str | memoryview | None) -> str:
    """Safely decode PostgreSQL content into a UTF-8 string."""
    if content is None:
        return ""
    if isinstance(content, memoryview):
        content = bytes(content)
    if isinstance(content, bytes):
        return content.decode("utf-8", errors="replace")
    return str(content)


def build_candidate_query() -> str:
    """Return SQL query to retrieve candidate file pairs and their contents."""
    return """
        WITH candidate_pairs AS (
            SELECT
                a.file_sha AS file1,
                b.file_sha AS file2,
                SUM(LEAST(a.count, b.count) * a.weight) AS shared_weight
            FROM telegram.file_subtrees a
            JOIN telegram.file_subtrees b
                ON a.digest = b.digest
                AND a.file_sha < b.file_sha
            WHERE a.weight >= %s
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


# pylint: disable=too-many-positional-arguments,too-many-locals
def fetch_candidate_pairs(
    conn: Any,
    min_sim: float,
    max_sim: float,
    min_weight: int,
    min_shared: int,
    limit: int,
) -> list[PairCandidate]:
    """Execute candidate query and return candidate pair models."""
    query = build_candidate_query()
    with conn.cursor() as cur:
        cur.execute(query, (min_weight, min_shared, min_sim, max_sim, limit))
        rows = cur.fetchall()

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
    print(
        f"Connecting to database and querying pairs with similarity in "
        f"[{args.min_similarity:.4f}, {args.max_similarity:.4f}] (limit {args.limit})..."
    )
    try:
        with get_db_connection() as conn:
            pairs = fetch_candidate_pairs(
                conn,
                min_sim=args.min_similarity,
                max_sim=args.max_similarity,
                min_weight=args.min_weight,
                min_shared=args.min_shared_weight,
                limit=args.limit,
            )
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Database error: {exc}", file=sys.stderr)
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
        help="Minimum subtree node weight threshold",
    )
    pairs_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum candidate pairs to retrieve",
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
