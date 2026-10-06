#!/usr/bin/env python3

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import os
from pathlib import Path
import sys
import time
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv
import psycopg

from js_compare.ast_tree import ASTTree
from js_compare.parse import source_to_graph
from js_compare.types import cst_node_types

if TYPE_CHECKING:
    from js_compare.types import CstNodeType

load_dotenv()

LOOSE_OPTIONS: list[CstNodeType] = [
    "Programs",
    "Functions",
    "Declarations",
    "Statements",
]


@dataclass
class SubtreeRow:
    file_sha: str
    digest: str
    weight: int
    label: str
    is_root: bool
    count: int


def get_db_connection() -> psycopg.Connection[Any]:
    conn_params: dict[str, Any] = {}

    if host := os.getenv("PGHOST"):
        conn_params["host"] = host
    if port := os.getenv("PGPORT"):
        conn_params["port"] = int(port)
    if dbname := os.getenv("PGDATABASE"):
        conn_params["dbname"] = dbname
    if user := os.getenv("PGUSER"):
        conn_params["user"] = user
    if password := os.getenv("PGPASSWORD"):
        conn_params["password"] = password

    # If password is not explicitly set, libpq checks ~/.pgpass automatically.
    # We can also pass passfile explicitly if PGPASSFILE is set or ~/.pgpass exists.
    if "password" not in conn_params:
        if passfile := os.getenv("PGPASSFILE"):
            conn_params["passfile"] = os.path.expanduser(passfile)
        else:
            default_pgpass = Path.home() / ".pgpass"
            if default_pgpass.exists():
                conn_params["passfile"] = str(default_pgpass)

    return psycopg.connect(**conn_params)


def init_db(conn: psycopg.Connection[Any]) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS telegram;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS telegram.file_indexed (
                file_sha TEXT PRIMARY KEY,
                total_nodes INTEGER NOT NULL,
                root_digest CHAR(64) NOT NULL,
                indexed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_indexed_root
            ON telegram.file_indexed (root_digest);
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS telegram.file_subtrees (
                file_sha TEXT NOT NULL,
                digest CHAR(64) NOT NULL,
                weight INTEGER NOT NULL,
                label TEXT NOT NULL,
                is_root BOOLEAN NOT NULL DEFAULT FALSE,
                count INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (file_sha, digest)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_subtrees_digest_weight
            ON telegram.file_subtrees (digest, weight DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_subtrees_root
            ON telegram.file_subtrees (digest) WHERE is_root = TRUE;
        """)
        # Drop redundant index (PRIMARY KEY (file_sha, digest) already indexes file_sha as leading column)
        cur.execute("""
            DROP INDEX IF EXISTS telegram.idx_file_subtrees_file_sha;
        """)
        # Backfill file_indexed from existing file_subtrees if present
        cur.execute("""
            INSERT INTO telegram.file_indexed (file_sha, total_nodes, root_digest)
            SELECT file_sha, weight, digest
            FROM telegram.file_subtrees
            WHERE is_root = TRUE
            ON CONFLICT (file_sha) DO NOTHING;
        """)
    conn.commit()


def process_file_ast(
    file_sha: str,
    content: bytes | str | None,
    node_types: list[CstNodeType] | None,
    min_weight: int,
    tolerant: bool = False,
) -> tuple[str, list[SubtreeRow], str | None]:
    if content is None:
        return file_sha, [], "File content is NULL in database"
    if (isinstance(content, str) and not content.strip()) or (isinstance(content, bytes) and not content.strip()):
        return file_sha, [], "File content is empty or whitespace"

    try:
        graph = source_to_graph(content, node_types, tolerant=tolerant)
        tree = ASTTree(graph)
        root_digest = tree.attrs_for_node(tree.root).digest

        # Group duplicate subtrees within the same file
        subtrees: dict[str, SubtreeRow] = {}
        for node_attrs in tree.nodes_sorted("weight", reverse=True):
            if node_attrs.weight < min_weight and node_attrs.node != tree.root:
                continue

            d = node_attrs.digest
            is_root = (d == root_digest)
            if d not in subtrees:
                subtrees[d] = SubtreeRow(
                    file_sha=file_sha,
                    digest=d,
                    weight=node_attrs.weight,
                    label=node_attrs.label,
                    is_root=is_root,
                    count=1,
                )
            else:
                subtrees[d].count += 1

        return file_sha, list(subtrees.values()), None
    except Exception as exc:  # pylint: disable=broad-except
        return file_sha, [], str(exc)


def resolve_node_types(type_arg: str) -> list[CstNodeType] | None:
    if type_arg == "all":
        return None
    if type_arg == "loose":
        return LOOSE_OPTIONS
    types = [t.strip() for t in type_arg.split(",") if t.strip()]
    return types  # type: ignore[return-value]


def populate_subtrees(args: argparse.Namespace) -> None:
    node_types = resolve_node_types(args.types)
    batch_size: int = args.batch_size
    min_weight: int = args.min_weight
    max_workers: int = args.workers

    print("Connecting to PostgreSQL database...")
    with get_db_connection() as read_conn, get_db_connection() as write_conn:
        init_db(write_conn)

        # Base query to fetch files
        query = """
            SELECT a.sha, a.content
            FROM telegram.githubcodeapi_file a
            JOIN telegram.githubcodeapi_file_printed b ON a.sha = b.file_sha
            WHERE b.level_2 = true
        """
        if args.resume:
            query += " AND NOT EXISTS (SELECT 1 FROM telegram.file_indexed s WHERE s.file_sha = a.sha)"
        if args.limit:
            query += f" LIMIT {args.limit}"

        tolerant: bool = getattr(args, "tolerant", True)

        print(f"Beginning file processing with {max_workers} worker processes...")
        total_files = 0
        total_subtrees = 0
        failed_files = 0
        start_time = time.time()

        # Use streaming server-side cursor without withhold=True so PostgreSQL
        # lazily fetches batches without materializing hundreds of thousands of files up front.
        with read_conn.cursor(name="file_stream") as stream_cur:
            stream_cur.itersize = batch_size
            stream_cur.execute(query)

            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                while True:
                    rows = stream_cur.fetchmany(batch_size)
                    if not rows:
                        break

                    # Submit batch to worker pool
                    futures = [
                        executor.submit(
                            process_file_ast,
                            sha,
                            content,
                            node_types,
                            min_weight,
                            tolerant,
                        )
                        for sha, content in rows
                    ]

                    indexed_rows: list[tuple[str, int, str]] = []
                    batch_rows: list[tuple[str, str, int, str, bool, int]] = []
                    for fut in futures:
                        sha, subtrees, err = fut.result()
                        if err:
                            failed_files += 1
                        else:
                            total_files += 1
                            root_row = next((r for r in subtrees if r.is_root), None)
                            if root_row:
                                indexed_rows.append((sha, root_row.weight, root_row.digest))
                            for row in subtrees:
                                batch_rows.append(
                                    (row.file_sha, row.digest, row.weight, row.label, row.is_root, row.count)
                                )

                    # Insert batch of subtrees and indexed file metadata
                    if batch_rows or indexed_rows:
                        with write_conn.cursor() as write_cur:
                            if batch_rows:
                                write_cur.executemany(
                                    """
                                    INSERT INTO telegram.file_subtrees
                                        (file_sha, digest, weight, label, is_root, count)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (file_sha, digest) DO UPDATE
                                        SET count = telegram.file_subtrees.count + EXCLUDED.count
                                    """,
                                    batch_rows,
                                )
                            if indexed_rows:
                                write_cur.executemany(
                                    """
                                    INSERT INTO telegram.file_indexed
                                        (file_sha, total_nodes, root_digest)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (file_sha) DO UPDATE
                                        SET total_nodes = EXCLUDED.total_nodes,
                                            root_digest = EXCLUDED.root_digest
                                    """,
                                    indexed_rows,
                                )
                        write_conn.commit()
                        total_subtrees += len(batch_rows)

                    elapsed = time.time() - start_time
                    rate = total_files / elapsed if elapsed > 0 else 0
                    print(
                        f"Processed {total_files:,} files | "
                        f"{total_subtrees:,} subtrees saved | "
                        f"{failed_files:,} failed | "
                        f"Rate: {rate:.1f} files/s",
                        end="\r",
                        flush=True,
                    )

    print()
    print(
        f"Completed: {total_files:,} files parsed, {total_subtrees:,} subtrees inserted, "
        f"{failed_files:,} parse failures in {time.time() - start_time:.1f}s."
    )


def show_corpus_stats(_args: argparse.Namespace) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            print("=== Overview ===")
            cur.execute("SELECT count(*) FROM telegram.file_indexed;")
            total_files = cur.fetchone()[0]  # type: ignore[index]
            cur.execute("SELECT count(*) FROM telegram.file_subtrees;")
            total_subtrees = cur.fetchone()[0]  # type: ignore[index]
            cur.execute("SELECT count(DISTINCT digest) FROM telegram.file_subtrees;")
            unique_digests = cur.fetchone()[0]  # type: ignore[index]

            print(f"Total files indexed:     {total_files:,}")
            print(f"Total subtree records:   {total_subtrees:,}")
            print(f"Unique subtree digests:  {unique_digests:,}")
            if total_files > 0:
                print(f"Avg subtrees per file:   {total_subtrees / total_files:.1f}")

            print("\n=== Groups of duplicate CSTs ===")
            cur.execute("""
                SELECT count(*), coalesce(sum(file_count), 0)
                FROM (
                    SELECT count(*) as file_count
                    FROM telegram.file_indexed
                    GROUP BY root_digest
                    HAVING count(*) > 1
                ) sub;
            """)
            row = cur.fetchone()
            dup_groups = row[0] if row else 0
            total_dup_files = row[1] if row else 0

            print(f"Total duplicate groups:               {dup_groups:,}")
            print(f"Total unique files across all groups: {total_dup_files:,}")

            if dup_groups == 0:
                print("No identical duplicate files found.")
            else:
                cur.execute("""
                    SELECT root_digest, count(*) as file_count, min(total_nodes) as nodes
                    FROM telegram.file_indexed
                    GROUP BY root_digest
                    HAVING count(*) > 1
                    ORDER BY file_count DESC
                    LIMIT 10;
                """)
                dups = cur.fetchall()
                for digest, count, nodes in dups:
                    print(f"Root: {digest[:16]}... | Files: {count:,} | Nodes: {nodes:,}")

            print("\n=== Top shared subtrees ===")
            cur.execute("""
                SELECT digest, label, weight, count(DISTINCT file_sha) as file_count
                FROM telegram.file_subtrees
                WHERE is_root = FALSE
                GROUP BY digest, label, weight
                ORDER BY file_count DESC, weight DESC
                LIMIT 15;
            """)
            for digest, label, weight, count in cur.fetchall():
                print(f"Digest: {digest[:16]}... | {label:<22} | Weight: {weight:>4} nodes | Files: {count:>6,}")


def cluster_candidates(args: argparse.Namespace) -> None:
    """Finds clusters of similar files based on shared subtrees and Sørensen-Dice similarity."""
    min_shared = args.min_shared_weight
    min_sim = args.min_similarity
    limit = args.limit or 1000

    print(f"Querying candidate file pairs with shared weight >= {min_shared}...")
    query = f"""
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
        )
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
        ORDER BY similarity DESC
        LIMIT %s;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (args.min_weight, min_shared, min_sim, limit))
            pairs = cur.fetchall()

            print(f"\nTop {len(pairs)} Similar Pairs (Similarity >= {min_sim:.2f}):")
            print(f"{'File 1 SHA':<20} {'File 2 SHA':<20} {'Overlap':<10} {'Sim':<8}")
            print("-" * 62)
            for f1, f2, shared, n1, n2, sim in pairs:
                print(f"{f1[:18]:<20} {f2[:18]:<20} {shared:>7} n {sim:>7.3f}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Database AST subtree extractor and clustering engine for JavaScript/TypeScript files."
    )
    subparsers = parser.add_subparsers(dest="command", required=False)

    # Subcommand: populate
    pop_p = subparsers.add_parser("populate", help="Extract and persist AST subtrees into PostgreSQL.")
    pop_p.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "500")), help="Batch size")
    pop_p.add_argument("--workers", type=int, default=int(os.getenv("NUM_WORKERS", "4")), help="Worker processes")
    pop_p.add_argument(
        "--types",
        default=os.getenv("NODE_TYPES", "all"),
        help="CST node types: 'all', 'loose', or comma-separated list",
    )
    pop_p.add_argument(
        "--min-weight",
        type=int,
        default=int(os.getenv("MIN_WEIGHT", "5")),
        help="Minimum subtree node count to persist (default: 5; >=5 filters leaf noise)",
    )
    pop_p.add_argument(
        "--tolerant",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("TOLERANT", "true").lower() in ("true", "1", "yes"),
        help="Allow tree-sitter error recovery on syntax errors (default: True)",
    )
    pop_p.add_argument("--limit", type=int, default=None, help="Limit number of source files to process")
    pop_p.add_argument("--no-resume", dest="resume", action="store_false", help="Reprocess already indexed files")
    pop_p.set_defaults(resume=True)

    # Subcommand: stats
    subparsers.add_parser("stats", help="Show corpus figures and statistics.")

    # Subcommand: cluster
    clust_p = subparsers.add_parser("cluster", help="Compute file similarity clusters.")
    clust_p.add_argument("--min-similarity", type=float, default=0.7, help="Minimum Sørensen-Dice similarity (0-1)")
    clust_p.add_argument("--min-shared-weight", type=int, default=20, help="Minimum shared node weight for pairs")
    clust_p.add_argument("--min-weight", type=int, default=5, help="Subtree weight threshold for candidate matching")
    clust_p.add_argument("--limit", type=int, default=500, help="Max candidate pairs to display")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    command = args.command or "populate"
    if command == "populate":
        populate_subtrees(args)
    elif command == "stats":
        show_corpus_stats(args)
    elif command == "cluster":
        cluster_candidates(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
