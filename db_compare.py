#!/usr/bin/env python3

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import json
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

            cur.execute("""
                WITH duplicate_files AS (
                    SELECT file_sha
                    FROM telegram.file_indexed
                    WHERE root_digest IN (
                        SELECT root_digest
                        FROM telegram.file_indexed
                        GROUP BY root_digest
                        HAVING count(*) > 1
                    )
                )
                SELECT
                    count(*),
                    count(DISTINCT a.repo_id)
                FROM telegram.githubcodeapi_file a
                JOIN telegram.githubcodeapi_file_printed b ON a.sha = b.file_sha
                WHERE b.level_2 = true
                  AND NOT EXISTS (
                      SELECT 1
                      FROM duplicate_files d
                      WHERE d.file_sha = a.sha
                  );
            """)
            corpus_row = cur.fetchone()
            corpus_files = corpus_row[0] if corpus_row else 0
            corpus_repos = corpus_row[1] if corpus_row else 0
            distinct_asts = corpus_files + dup_groups

            print(f"Total duplicate groups:               {dup_groups:,}")
            print(
                f"Total duplicate files: {total_dup_files:,} "
                f"(distinct ASTs: {distinct_asts:,} across {corpus_repos:,} distinct repos)"
            )

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


def select_cluster_representative(
    members: set[str],
    root_digests: dict[str, str],
    node_counts: dict[str, int],
    similarity_matrix: dict[tuple[str, str], float],
) -> tuple[str, float]:
    """Select the medoid representative of a cluster (highest avg similarity to others)."""
    if len(members) == 1:
        return next(iter(members)), 1.0

    best_rep = ""
    best_score: tuple[float, int, str] = (-1.0, -1, "")
    best_avg_sim = 0.0

    for u in members:
        total_sim = 0.0
        u_root = root_digests.get(u)
        for v in members:
            if u == v:
                continue
            if u_root and u_root == root_digests.get(v):
                total_sim += 1.0
            else:
                pair_key = (min(u, v), max(u, v))
                total_sim += similarity_matrix.get(pair_key, 0.0)

        avg_sim = total_sim / (len(members) - 1)
        score = (avg_sim, node_counts.get(u, 0), u)
        if score > best_score:
            best_score = score
            best_rep = u
            best_avg_sim = avg_sim

    return best_rep, best_avg_sim


def cluster_corpus(args: argparse.Namespace) -> None:  # pylint: disable=too-many-locals,too-many-statements,too-many-branches
    """Cluster files by pairwise AST similarity down to a target cluster count.

    Uses constrained complete-linkage agglomerative clustering to prevent chaining:
    1. Pre-groups exact AST duplicates (same root_digest).
    2. Merges pairs in descending similarity order only if all cross-pairs satisfy min_similarity
       and the merged cluster size does not exceed max_cluster_size.
    3. Selects the medoid (highest average similarity) as the representative for each cluster.
    """
    target_clusters: int = args.target_clusters
    min_sim: float = args.min_similarity
    max_size: int = args.max_cluster_size
    min_shared: int = args.min_shared_weight
    min_weight: int = args.min_weight
    pair_limit: int | None = args.pair_limit

    start_time = time.time()
    print("Connecting to PostgreSQL database...")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            print("Fetching indexed files from telegram.file_indexed...")
            cur.execute("SELECT file_sha, total_nodes, root_digest FROM telegram.file_indexed;")
            rows = cur.fetchall()

            if not rows:
                print("No indexed files found in telegram.file_indexed. Run 'populate' first.")
                return

            root_digests: dict[str, str] = {}
            node_counts: dict[str, int] = {}
            digest_to_shas: dict[str, list[str]] = defaultdict(list)

            for sha, total_nodes, root_digest in rows:
                root_digests[sha] = root_digest
                node_counts[sha] = total_nodes
                digest_to_shas[root_digest].append(sha)

            total_files = len(rows)

            # Step 1: Pre-group exact AST duplicates (root_digest matches)
            clusters: dict[int, set[str]] = {}
            file_to_cluster: dict[str, int] = {}
            cluster_id_counter = 0

            for shas in digest_to_shas.values():
                if max_size > 0 and len(shas) > max_size:
                    for i in range(0, len(shas), max_size):
                        chunk = set(shas[i : i + max_size])
                        cid = cluster_id_counter
                        cluster_id_counter += 1
                        clusters[cid] = chunk
                        for s in chunk:
                            file_to_cluster[s] = cid
                else:
                    chunk = set(shas)
                    cid = cluster_id_counter
                    cluster_id_counter += 1
                    clusters[cid] = chunk
                    for s in chunk:
                        file_to_cluster[s] = cid

            initial_clusters = len(clusters)
            collapsed_dups = total_files - initial_clusters
            print(
                f"Loaded {total_files:,} files | Pre-grouped into {initial_clusters:,} clusters "
                f"({collapsed_dups:,} exact AST duplicates collapsed)."
            )

            if initial_clusters <= target_clusters:
                print(
                    f"Cluster count ({initial_clusters:,}) is already at or below target "
                    f"({target_clusters:,}). No near-duplicate merges required."
                )
                pair_rows = []
                similarity_matrix: dict[tuple[str, str], float] = {}
            else:
                # Step 2: Fetch candidate pairs sorted descending by similarity
                print(
                    f"Querying candidate pairs (min_sim >= {min_sim:.2f}, "
                    f"shared_weight >= {min_shared}, weight >= {min_weight})..."
                )
                query = """
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
                        ORDER BY similarity DESC
                """
                if pair_limit:
                    query += f" LIMIT {pair_limit}"
                query += "\n) SELECT file1, file2, similarity FROM ranked_pairs;"

                cur.execute(query, (min_weight, min_shared, min_sim))
                pair_rows = cur.fetchall()
                print(f"Retrieved {len(pair_rows):,} candidate pairs.")

                # Populate edge lookup
                similarity_matrix = {
                    (min(f1, f2), max(f1, f2)): float(sim)
                    for f1, f2, sim in pair_rows
                }

                # Step 3: Constrained agglomerative clustering loop
                print(
                    f"Running constrained complete-linkage clustering towards target {target_clusters:,} "
                    f"(max_size={max_size or 'unlimited'}, min_sim={min_sim:.2f})..."
                )
                merges_performed = 0
                rejections_size = 0
                rejections_sim = 0

                for f1, f2, _ in pair_rows:
                    if len(clusters) <= target_clusters:
                        print(f"\nReached target cluster count ({len(clusters):,}).")
                        break

                    c1 = file_to_cluster.get(f1)
                    c2 = file_to_cluster.get(f2)
                    if c1 is None or c2 is None or c1 == c2:
                        continue

                    members1 = clusters[c1]
                    members2 = clusters[c2]

                    # Constraint 2: Maximum cluster size
                    if max_size > 0 and (len(members1) + len(members2) > max_size):
                        rejections_size += 1
                        continue

                    # Constraint 1: Complete linkage (all cross-pairs >= min_similarity)
                    can_merge = True
                    for m1 in members1:
                        for m2 in members2:
                            if root_digests.get(m1) == root_digests.get(m2):
                                continue
                            pair_key = (min(m1, m2), max(m1, m2))
                            if similarity_matrix.get(pair_key, 0.0) < min_sim:
                                can_merge = False
                                break
                        if not can_merge:
                            break

                    if not can_merge:
                        rejections_sim += 1
                        continue

                    # Merge smaller cluster into larger cluster
                    if len(members1) < len(members2):
                        c1, c2 = c2, c1
                        members1, members2 = members2, members1

                    members1.update(members2)
                    for m in members2:
                        file_to_cluster[m] = c1
                    del clusters[c2]
                    merges_performed += 1

                print(
                    f"\nClustering complete: {merges_performed:,} merges performed "
                    f"(rejected: {rejections_size:,} by size cap, {rejections_sim:,} by min_similarity)."
                )

        # Step 4: Compute representatives and cluster stats
        print("Selecting medoid representatives for each cluster...")
        cluster_results: list[tuple[int, str, float, set[str]]] = []
        for cid, members in clusters.items():
            rep, avg_sim = select_cluster_representative(
                members, root_digests, node_counts, similarity_matrix
            )
            cluster_results.append((cid, rep, avg_sim, members))

        # Sort clusters by size descending
        cluster_results.sort(key=lambda x: len(x[3]), reverse=True)

        size_counts = Counter(len(m) for _, _, _, m in cluster_results)
        singleton_count = size_counts.get(1, 0)
        multi_count = len(cluster_results) - singleton_count

        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print("CLUSTER SUMMARY")
        print("=" * 60)
        print(f"Total files:              {total_files:,}")
        print(f"Final distinct clusters:  {len(cluster_results):,} (Target: {target_clusters:,})")
        print(f"Singletons (size 1):      {singleton_count:,}")
        print(f"Multi-file clusters:      {multi_count:,}")
        print(f"Largest cluster size:     {len(cluster_results[0][3]) if cluster_results else 0}")
        print(f"Time elapsed:             {elapsed:.1f}s")

        print("\nCluster Size Distribution:")
        print(f"  Size 1:      {size_counts.get(1, 0):>6,}")
        print(f"  Size 2:      {size_counts.get(2, 0):>6,}")
        print(f"  Size 3 - 5:  {sum(c for s, c in size_counts.items() if 3 <= s <= 5):>6,}")
        print(f"  Size 6 - 10: {sum(c for s, c in size_counts.items() if 6 <= s <= 10):>6,}")
        print(f"  Size 11+:    {sum(c for s, c in size_counts.items() if s >= 11):>6,}")

        print("\nTop 10 Largest Clusters:")
        print(f"{'Cluster':<9} {'Size':<6} {'Avg Sim':<9} {'Representative SHA':<20} {'Sample Members'}")
        print("-" * 80)
        for i, (cid, rep, avg_sim, members) in enumerate(cluster_results[:10], 1):
            sample_members = ", ".join(m[:8] for m in list(members) if m != rep)[:35]
            if len(members) > 1 and not sample_members:
                sample_members = "(duplicates)"
            print(f"#{i:<8} {len(members):<6} {avg_sim:<9.3f} {rep[:18]:<20} {sample_members}")

        # Step 5: Save to database if requested
        if args.save_to_db:
            print("\nSaving cluster assignments into telegram.file_clusters...")
            with get_db_connection() as write_conn:
                with write_conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS telegram.file_clusters (
                            cluster_id INTEGER NOT NULL,
                            file_sha TEXT NOT NULL PRIMARY KEY,
                            representative_sha TEXT NOT NULL,
                            is_representative BOOLEAN NOT NULL DEFAULT FALSE,
                            cluster_size INTEGER NOT NULL,
                            avg_similarity_to_rep REAL
                        );
                        CREATE INDEX IF NOT EXISTS idx_file_clusters_rep
                        ON telegram.file_clusters (representative_sha);
                        TRUNCATE TABLE telegram.file_clusters;
                    """)
                    insert_rows = []
                    for cid_norm, (_, rep, _, members) in enumerate(cluster_results):
                        c_size = len(members)
                        for m in members:
                            is_rep = (m == rep)
                            if is_rep:
                                sim_to_rep = 1.0
                            elif root_digests.get(m) == root_digests.get(rep):
                                sim_to_rep = 1.0
                            else:
                                sim_to_rep = similarity_matrix.get((min(m, rep), max(m, rep)), 0.0)
                            insert_rows.append((cid_norm, m, rep, is_rep, c_size, sim_to_rep))

                    cur.executemany("""
                        INSERT INTO telegram.file_clusters
                            (cluster_id, file_sha, representative_sha, is_representative, cluster_size, avg_similarity_to_rep)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, insert_rows)
                write_conn.commit()
            print(f"Saved {len(insert_rows):,} cluster assignments into telegram.file_clusters.")

        # Step 6: Export to JSON if requested
        if args.output:
            export_data = {
                "summary": {
                    "total_files": total_files,
                    "final_distinct_clusters": len(cluster_results),
                    "target_clusters": target_clusters,
                    "min_similarity": min_sim,
                    "max_cluster_size": max_size,
                    "singletons": singleton_count,
                    "multi_file_clusters": multi_count,
                },
                "clusters": [
                    {
                        "cluster_id": cid_norm,
                        "representative": rep,
                        "cluster_size": len(members),
                        "avg_similarity": round(avg_sim, 4),
                        "members": sorted(list(members)),
                    }
                    for cid_norm, (_, rep, avg_sim, members) in enumerate(cluster_results)
                ],
            }
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(export_data, indent=2), encoding="utf-8")
            print(f"Exported cluster results to: {args.output.resolve()}")


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
    clust_p = subparsers.add_parser(
        "cluster",
        help="Perform constrained agglomerative clustering to reduce files down to target cluster budget.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    clust_p.add_argument(
        "--target-clusters",
        type=int,
        default=int(os.getenv("TARGET_CLUSTERS", "30000")),
        help="Target number of distinct clusters to stop merging at",
    )
    clust_p.add_argument(
        "--min-similarity",
        type=float,
        default=float(os.getenv("MIN_SIMILARITY", "0.90")),
        help="Minimum pairwise similarity required across ALL cluster members (prevents chaining)",
    )
    clust_p.add_argument(
        "--max-cluster-size",
        type=int,
        default=int(os.getenv("MAX_CLUSTER_SIZE", "20")),
        help="Maximum files allowed in a single cluster (0 for unlimited)",
    )
    clust_p.add_argument(
        "--min-shared-weight",
        type=int,
        default=int(os.getenv("MIN_SHARED_WEIGHT", "20")),
        help="Minimum shared node weight for candidate pairs",
    )
    clust_p.add_argument(
        "--min-weight",
        type=int,
        default=int(os.getenv("MIN_WEIGHT", "5")),
        help="Subtree weight threshold for candidate matching",
    )
    clust_p.add_argument(
        "--pair-limit",
        type=int,
        default=None,
        help="Optional limit on candidate pairs retrieved from database",
    )
    clust_p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional file path to export clusters (JSON format)",
    )
    clust_p.add_argument(
        "--save-to-db",
        action="store_true",
        help="Save cluster assignments and representatives into telegram.file_clusters table",
    )

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
        cluster_corpus(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
