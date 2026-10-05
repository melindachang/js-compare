"""Unit tests for js-compare in-memory parsing, database extraction pipeline,
and clustering utilities.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from js_compare.ast_tree import ASTTree
from js_compare.parse import code_to_graph, source_to_graph
from db_compare import (
    LOOSE_OPTIONS,
    SubtreeRow,
    build_parser,
    cluster_candidates,
    init_db,
    process_file_ast,
    resolve_node_types,
    show_corpus_stats,
)


class TestSourceToGraph(unittest.TestCase):
    def test_parse_js_string(self) -> None:
        code = "function add(a, b) { return a + b; }"
        g = source_to_graph(code)
        self.assertGreater(len(g), 0)
        labels = [g.nodes[n]["label"] for n in g.nodes]
        self.assertIn("program", labels)
        self.assertIn("function_declaration", labels)

    def test_parse_js_bytes(self) -> None:
        code_bytes = b"const x = 42;\nconsole.log(x);"
        g = source_to_graph(code_bytes)
        self.assertGreater(len(g), 0)

    def test_parse_typescript(self) -> None:
        ts_code = """
        interface User {
            id: number;
            name: string;
        }
        function greet(u: User): string {
            return `Hello, ${u.name}!`;
        }
        """
        g = source_to_graph(ts_code)
        self.assertGreater(len(g), 0)

    def test_parse_jsx_tsx(self) -> None:
        tsx_code = """
        const Button = ({ label }: { label: string }) => (
            <button className="primary">{label}</button>
        );
        """
        g = source_to_graph(tsx_code)
        self.assertGreater(len(g), 0)

    def test_code_to_graph_backwards_compatibility(self) -> None:
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as tmp:
            tmp.write("let answer = 42;")
            tmp_path = Path(tmp.name)

        try:
            g = code_to_graph(tmp_path)
            self.assertGreater(len(g), 0)
        finally:
            tmp_path.unlink()

    def test_category_filtering(self) -> None:
        code = "function compute() { let a = 1; let b = 2; return a + b; }"
        g_all = source_to_graph(code)
        g_loose = source_to_graph(code, node_types=["loose"])
        # 'loose' excludes fine-grained leaf nodes (identifiers, literals, operators)
        self.assertLess(len(g_loose), len(g_all))


class TestDbPipelineHelpers(unittest.TestCase):
    def test_resolve_node_types(self) -> None:
        self.assertIsNone(resolve_node_types("all"))
        self.assertEqual(resolve_node_types("loose"), LOOSE_OPTIONS)
        custom = resolve_node_types("Functions, Declarations")
        self.assertEqual(custom, ["Functions", "Declarations"])

    def test_process_file_ast_valid(self) -> None:
        code = """
        function hello() {
            return 'world';
        }
        """
        sha, rows, err = process_file_ast("sha_test_123", code, node_types=None, min_weight=1)
        self.assertIsNone(err)
        self.assertEqual(sha, "sha_test_123")
        self.assertGreater(len(rows), 0)

        # Verify exactly one root row exists and matches total weight
        root_rows = [r for r in rows if r.is_root]
        self.assertEqual(len(root_rows), 1)
        self.assertEqual(root_rows[0].label, "program")
        self.assertGreaterEqual(root_rows[0].weight, max(r.weight for r in rows))

    def test_process_file_ast_min_weight_filtering(self) -> None:
        code = """
        function hello() {
            return 'world';
        }
        """
        _, rows_all, _ = process_file_ast("sha_1", code, node_types=None, min_weight=1)
        _, rows_filtered, _ = process_file_ast("sha_1", code, node_types=None, min_weight=4)

        # Filtered rows should only contain subtrees with weight >= 4 or root
        self.assertLess(len(rows_filtered), len(rows_all))
        for r in rows_filtered:
            if not r.is_root:
                self.assertGreaterEqual(r.weight, 4)

    def test_process_file_ast_duplicate_subtrees_within_file(self) -> None:
        # Code containing identical statement blocks
        code = """
        function run() {
            return 1;
            return 1;
        }
        """
        _, rows, _ = process_file_ast("sha_dup", code, node_types=None, min_weight=1)
        return_rows = [r for r in rows if r.label == "return_statement"]
        self.assertEqual(len(return_rows), 1)
        self.assertEqual(return_rows[0].count, 2)

    def test_process_file_ast_syntax_error_resilience(self) -> None:
        # Invalid / unparseable code should return err without raising an unhandled exception
        bad_code = "??!@#$%^&*()}{`~ invalid syntax error"
        sha, rows, err = process_file_ast("sha_bad", bad_code, node_types=None, min_weight=1)
        self.assertIsNotNone(err)
        self.assertEqual(rows, [])
        self.assertEqual(sha, "sha_bad")


class TestCliParser(unittest.TestCase):
    def test_cli_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([])
        self.assertIsNone(args.command)

    def test_cli_parser_populate(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "populate",
            "--batch-size", "250",
            "--workers", "2",
            "--types", "loose",
            "--min-weight", "5",
            "--limit", "100",
            "--no-resume",
            "--no-tolerant",
        ])
        self.assertEqual(args.command, "populate")
        self.assertEqual(args.batch_size, 250)
        self.assertEqual(args.workers, 2)
        self.assertEqual(args.types, "loose")
        self.assertEqual(args.min_weight, 5)
        self.assertEqual(args.limit, 100)
        self.assertFalse(args.resume)
        self.assertFalse(args.tolerant)

    def test_cli_parser_cluster(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "cluster",
            "--min-similarity", "0.8",
            "--min-shared-weight", "50",
            "--limit", "200",
        ])
        self.assertEqual(args.command, "cluster")
        self.assertEqual(args.min_similarity, 0.8)
        self.assertEqual(args.min_shared_weight, 50)
        self.assertEqual(args.limit, 200)


class TestDatabaseOperationsMocked(unittest.TestCase):
    @patch("db_compare.psycopg.connect")
    def test_init_db_creates_schema_and_indexes(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        init_db(mock_conn)

        executed_sqls = " ".join(call[0][0] for call in mock_cur.execute.call_args_list)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS telegram", executed_sqls)
        self.assertIn("CREATE TABLE IF NOT EXISTS telegram.file_indexed", executed_sqls)
        self.assertIn("CREATE TABLE IF NOT EXISTS telegram.file_subtrees", executed_sqls)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_file_subtrees_digest_weight", executed_sqls)
        self.assertIn("DROP INDEX IF EXISTS telegram.idx_file_subtrees_file_sha", executed_sqls)
        mock_conn.commit.assert_called_once()

    @patch("db_compare.get_db_connection")
    def test_show_corpus_stats(self, mock_get_conn: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_conn.return_value = mock_conn

        # Mock fetchone and fetchall responses for stats queries
        mock_cur.fetchone.side_effect = [
            (100,),   # total_files
            (1500,),  # total_subtrees
            (400,),   # unique_digests
        ]
        mock_cur.fetchall.side_effect = [
            [("digest_root_1", 5, 20)],  # duplicate roots
            [("digest_sub_1", "function_declaration", 15, 30)],  # top shared
        ]

        parser = build_parser()
        args = parser.parse_args(["stats"])
        # Should execute queries without raising errors
        show_corpus_stats(args)
        self.assertGreaterEqual(mock_cur.execute.call_count, 3)

    @patch("db_compare.get_db_connection")
    def test_cluster_candidates(self, mock_get_conn: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_conn.return_value = mock_conn

        mock_cur.fetchall.return_value = [
            ("sha_a", "sha_b", 80, 100, 100, 0.8),
        ]

        parser = build_parser()
        args = parser.parse_args(["cluster", "--min-similarity", "0.75"])
        cluster_candidates(args)
        self.assertEqual(mock_cur.execute.call_count, 1)

    @patch("db_compare.get_db_connection")
    def test_populate_subtrees_mocked(self, mock_get_conn: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_stream_cur = MagicMock()
        mock_write_cur = MagicMock()

        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.side_effect = [
            # init_db cursor
            MagicMock(),
            # stream_cur cursor
            mock_stream_cur,
            # write_cur cursor
            mock_write_cur,
        ]
        # First call to fetchmany returns sample files, second call returns [] to terminate
        mock_stream_cur.__enter__.return_value = mock_stream_cur
        mock_stream_cur.fetchmany.side_effect = [
            [("sha_1", "function a() { return 1; }"), ("sha_2", "function b() { return 2; }")],
            [],
        ]
        mock_write_cur.__enter__.return_value = mock_write_cur

        mock_get_conn.return_value = mock_conn

        from db_compare import populate_subtrees
        parser = build_parser()
        args = parser.parse_args(["populate", "--workers", "1", "--batch-size", "10"])
        populate_subtrees(args)

        self.assertTrue(mock_write_cur.executemany.called)
        # Check that both file_subtrees and file_indexed insertions occurred
        executed_sqls = " ".join(call[0][0] for call in mock_write_cur.executemany.call_args_list)
        self.assertIn("telegram.file_subtrees", executed_sqls)
        self.assertIn("telegram.file_indexed", executed_sqls)
        self.assertGreaterEqual(mock_conn.commit.call_count, 1)

        # Check resume query uses telegram.file_indexed
        stream_query = mock_stream_cur.execute.call_args[0][0]
        self.assertIn("telegram.file_indexed", stream_query)
        self.assertNotIn("withhold", str(mock_conn.cursor.call_args))


class TestRealExampleFiles(unittest.TestCase):
    def test_example_files_overlap(self) -> None:
        f1 = Path("examples/obsidian_bot.ts")
        f2 = Path("examples/twitter_bot.ts")
        if not f1.exists() or not f2.exists():
            self.skipTest("Example files not found")

        _, rows1, err1 = process_file_ast("obsidian", f1.read_bytes(), None, 1)
        _, rows2, err2 = process_file_ast("twitter", f2.read_bytes(), None, 1)
        self.assertIsNone(err1)
        self.assertIsNone(err2)

        root1 = next(r for r in rows1 if r.is_root)
        root2 = next(r for r in rows2 if r.is_root)
        self.assertEqual(root1.weight, 338)
        self.assertEqual(root2.weight, 243)

        d1 = {r.digest: r for r in rows1}
        d2 = {r.digest: r for r in rows2}
        common = set(d1.keys()) & set(d2.keys())
        self.assertGreater(len(common), 0)


if __name__ == "__main__":
    unittest.main()
