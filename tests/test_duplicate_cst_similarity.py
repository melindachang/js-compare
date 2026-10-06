"""Comprehensive tests ensuring duplicate CSTs yield a similarity score of
exactly 1.0 when the underlying source text differs only in cosmetic aspects
such as whitespace, comments, import ordering, semicolons, trailing commas,
and formatting.

These tests exercise both the in-memory comparison path
(``compare_graphs``) and the database pipeline extraction path
(``process_file_ast``) to verify that the Merkle-tree digests are stable
across structurally identical files whose raw text diverges.
"""

from __future__ import annotations

import pytest

from db_compare import process_file_ast
from js_compare.ast_tree import ASTTree
from js_compare.compare import compare_graphs
from js_compare.parse import source_to_graph


# ── Helpers ──────────────────────────────────────────────────────────


def _similarity(code_a: str, code_b: str) -> float:
    """Return the Sørensen-Dice similarity between two JS/TS snippets."""
    return compare_graphs(source_to_graph(code_a), source_to_graph(code_b)).similarity


def _root_digest(code: str) -> str:
    """Return the Merkle root digest for *code* via the ASTTree path."""
    tree = ASTTree(source_to_graph(code))
    return tree.attrs_for_node(tree.root).digest


def _pipeline_root_digest(code: str, sha: str = "test") -> str:
    """Return the root digest via ``process_file_ast`` (the DB pipeline
    path), ensuring the pipeline itself does not corrupt the digest."""
    _, rows, err = process_file_ast(sha, code, node_types=None, min_weight=1)
    assert err is None, f"process_file_ast error: {err}"
    roots = [r for r in rows if r.is_root]
    assert len(roots) == 1
    return roots[0].digest


# ── Sanity checks ────────────────────────────────────────────────────


class TestSanity:
    """Guard-rail tests that confirm the helpers are measuring something
    real: structurally *different* code must NOT score 1.0."""

    def test_different_code_is_not_one(self) -> None:
        a = "function f() { return 1; }"
        b = "function f() { if (true) { return 1; } else { return 2; } }"
        sim = _similarity(a, b)
        assert sim < 1.0

    def test_different_root_digest(self) -> None:
        a = "function f() { return 1; }"
        b = "function f() { if (true) { return 1; } }"
        assert _root_digest(a) != _root_digest(b)

    def test_identical_code_is_exactly_one(self) -> None:
        code = "function id(x) { return x; }"
        assert _similarity(code, code) == 1.0
        assert _root_digest(code) == _root_digest(code)


# ── Whitespace variations ────────────────────────────────────────────

CANONICAL_FUNCTION = "function add(a, b) { return a + b; }"


class TestWhitespaceSimilarity:
    """Whitespace between tokens is invisible to the CST, so all
    formatting variations must produce an identical tree."""

    @pytest.mark.parametrize(
        "variant",
        [
            # extra spaces between every token
            "function  add( a ,  b )  {  return  a  +  b ;  }",
            # tabs instead of spaces
            "function\tadd(a,\tb)\t{\treturn\ta\t+\tb;\t}",
            # newlines in safe positions (not between return and its argument)
            "function\nadd\n(a, b)\n{\nreturn a + b;\n}",
            # leading and trailing blank lines
            "\n\n\nfunction add(a, b) { return a + b; }\n\n\n",
            # mixed indentation (spaces + tabs)
            "  function add(a, b) {\n\t\treturn a + b;\n  }",
            # CRLF line endings
            "function add(a, b) {\r\n  return a + b;\r\n}",
            # CRLF with extra blank lines
            "function add(a, b) {\r\n\r\n  return a + b;\r\n\r\n}",
            # deep indentation
            "        function add(a, b) {\n            return a + b;\n        }",
        ],
        ids=[
            "extra_spaces",
            "tabs",
            "newlines_between_tokens",
            "leading_trailing_blank_lines",
            "mixed_indentation",
            "crlf",
            "crlf_blank_lines",
            "deep_indent",
        ],
    )
    def test_similarity_is_one(self, variant: str) -> None:
        assert _similarity(CANONICAL_FUNCTION, variant) == 1.0

    @pytest.mark.parametrize(
        "variant",
        [
            "function  add( a ,  b )  {  return  a  +  b ;  }",
            "\n\n  function add(a, b) {\n\t\treturn a + b;\n  }\n\n",
        ],
        ids=["extra_spaces", "mixed_indentation"],
    )
    def test_root_digest_matches(self, variant: str) -> None:
        assert _root_digest(CANONICAL_FUNCTION) == _root_digest(variant)

    def test_pipeline_digest_stable_across_whitespace(self) -> None:
        variant = "\n  function  add(a,  b)  {\n\treturn  a + b;\n  }\n\n"
        assert _pipeline_root_digest(CANONICAL_FUNCTION) == _pipeline_root_digest(variant)


# ── Comment variations ───────────────────────────────────────────────

BARE_FUNCTION = "function greet() { return 'hello'; }"


class TestCommentSimilarity:
    """Comment nodes are in NEVER_TYPES and stripped from the CST, so
    adding, removing, or changing comments must yield similarity 1.0."""

    @pytest.mark.parametrize(
        "commented",
        [
            # single-line comment before
            "// greeting helper\nfunction greet() { return 'hello'; }",
            # single-line comment after
            "function greet() { return 'hello'; } // end",
            # single-line comment inside body
            "function greet() {\n  // body\n  return 'hello';\n}",
            # multi-line comment before
            "/* This is greet */\nfunction greet() { return 'hello'; }",
            # multi-line comment inside body
            "function greet() { /* x */ return 'hello'; }",
            # JSDoc before
            "/** @returns {string} greeting */\nfunction greet() { return 'hello'; }",
            # many comments scattered throughout
            "// top\n/* mid */function greet() { // inline\n  return 'hello'; /* trail */ }",
        ],
        ids=[
            "single_before",
            "single_after",
            "single_inside",
            "multiline_before",
            "multiline_inside",
            "jsdoc",
            "scattered",
        ],
    )
    def test_similarity_is_one(self, commented: str) -> None:
        assert _similarity(BARE_FUNCTION, commented) == 1.0

    def test_completely_different_comments_same_code(self) -> None:
        """Two files with the same code but entirely different comments."""
        a = "// version 1\nfunction greet() { /* old */ return 'hello'; }"
        b = "/** version 2 */\nfunction greet() { // new\n  return 'hello';\n}"
        assert _similarity(a, b) == 1.0

    def test_root_digest_unaffected_by_comments(self) -> None:
        commented = "// copyright 2026\n/** @module */\nfunction greet() { return 'hello'; }"
        assert _root_digest(BARE_FUNCTION) == _root_digest(commented)

    def test_pipeline_digest_unaffected_by_comments(self) -> None:
        commented = "/* license header */\nfunction greet() { return 'hello'; }"
        assert _pipeline_root_digest(BARE_FUNCTION) == _pipeline_root_digest(commented)

    def test_license_header_delta(self) -> None:
        """A realistic file with a license header vs. without one."""
        code = (
            "const API_URL = 'https://api.example.com';\n"
            "function fetchData(id) {\n"
            "  return fetch(API_URL + '/' + id);\n"
            "}\n"
            "function processResponse(resp) {\n"
            "  return resp.json();\n"
            "}\n"
        )
        with_header = (
            "// Copyright (c) 2026 Acme Corp.\n"
            "// Licensed under MIT.\n\n"
            "/**\n"
            " * API client module.\n"
            " * @module api-client\n"
            " */\n\n"
        ) + code
        assert _similarity(code, with_header) == 1.0
        assert _root_digest(code) == _root_digest(with_header)


# ── Import reordering ────────────────────────────────────────────────


class TestImportReorderingSimilarity:
    """Reordering import statements that have identical CST shapes
    preserves the root digest because identically-shaped children
    produce identical subtree digests regardless of content."""

    def test_two_named_imports_reordered(self) -> None:
        a = "import { x } from 'x';\nimport { y } from 'y';"
        b = "import { y } from 'y';\nimport { x } from 'x';"
        assert _similarity(a, b) == 1.0

    def test_three_named_imports_reordered(self) -> None:
        a = "import { a } from 'a';\nimport { b } from 'b';\nimport { c } from 'c';"
        b = "import { c } from 'c';\nimport { a } from 'a';\nimport { b } from 'b';"
        assert _similarity(a, b) == 1.0

    def test_named_imports_before_function(self) -> None:
        a = "import { a } from 'a';\nimport { b } from 'b';\nfunction foo() { return 1; }"
        b = "import { b } from 'b';\nimport { a } from 'a';\nfunction foo() { return 1; }"
        assert _similarity(a, b) == 1.0

    def test_default_imports_reordered(self) -> None:
        a = "import Foo from 'foo';\nimport Bar from 'bar';"
        b = "import Bar from 'bar';\nimport Foo from 'foo';"
        assert _similarity(a, b) == 1.0

    def test_namespace_imports_reordered(self) -> None:
        a = "import * as A from 'a';\nimport * as B from 'b';"
        b = "import * as B from 'b';\nimport * as A from 'a';"
        assert _similarity(a, b) == 1.0

    def test_root_digest_stable_under_reorder(self) -> None:
        a = "import { x } from 'x';\nimport { y } from 'y';"
        b = "import { y } from 'y';\nimport { x } from 'x';"
        assert _root_digest(a) == _root_digest(b)

    def test_pipeline_digest_stable_under_reorder(self) -> None:
        a = "import { x } from 'x';\nimport { y } from 'y';\nfunction f() { return 0; }"
        b = "import { y } from 'y';\nimport { x } from 'x';\nfunction f() { return 0; }"
        assert _pipeline_root_digest(a, "sha_a") == _pipeline_root_digest(b, "sha_b")

    def test_exports_reordered(self) -> None:
        """Export statements with the same shape are also reorderable."""
        a = "export { foo };\nexport { bar };"
        b = "export { bar };\nexport { foo };"
        assert _similarity(a, b) == 1.0


# ── Semicolon / trailing-comma insertion ─────────────────────────────


class TestSemicolonAndTrailingComma:
    """Semicolons and trailing commas are anonymous tokens in tree-sitter
    and excluded from the named-node graph."""

    def test_optional_semicolons(self) -> None:
        a = "function f() { return 1 }"
        b = "function f() { return 1; }"
        assert _similarity(a, b) == 1.0

    def test_trailing_comma_in_arguments(self) -> None:
        a = "foo(a, b)"
        b = "foo(a, b,)"
        assert _similarity(a, b) == 1.0

    def test_trailing_comma_in_array(self) -> None:
        a = "const arr = [1, 2, 3];"
        b = "const arr = [1, 2, 3,];"
        assert _similarity(a, b) == 1.0


# ── bytes vs str input ───────────────────────────────────────────────


class TestInputEncoding:
    """``source_to_graph`` accepts both ``str`` and ``bytes``; both must
    yield identical CSTs."""

    def test_str_vs_bytes_similarity(self) -> None:
        code = "function f() { return 1; }"
        g1 = source_to_graph(code)
        g2 = source_to_graph(code.encode("utf-8"))
        assert compare_graphs(g1, g2).similarity == 1.0

    def test_str_vs_bytes_root_digest(self) -> None:
        code = "const x = 42;\nconsole.log(x);"
        assert _root_digest(code) == _root_digest(code)
        # also via pipeline which can receive bytes
        _, rows_str, _ = process_file_ast("s", code, None, 1)
        _, rows_bytes, _ = process_file_ast("b", code.encode(), None, 1)
        root_str = next(r for r in rows_str if r.is_root)
        root_bytes = next(r for r in rows_bytes if r.is_root)
        assert root_str.digest == root_bytes.digest


# ── Combined variations ──────────────────────────────────────────────


class TestCombinedVariations:
    """Multiple cosmetic differences applied simultaneously."""

    def test_whitespace_plus_comments(self) -> None:
        a = "function foo() { return 1; }"
        b = "  // header\n  function  foo()  {\n    /* body */\n    return 1;\n  }\n"
        assert _similarity(a, b) == 1.0

    def test_comments_plus_import_reorder(self) -> None:
        a = "import { a } from 'a';\nimport { b } from 'b';\nfunction f() { return 0; }"
        b = (
            "// reordered\nimport { b } from 'b';\n"
            "/* was second */\nimport { a } from 'a';\n"
            "function f() { return 0; }"
        )
        assert _similarity(a, b) == 1.0

    def test_whitespace_comments_and_import_reorder(self) -> None:
        a = "import { x } from 'x'; import { y } from 'y'; function run() { return x + y; }"
        b = (
            "\n\n// Module\nimport { y } from 'y';\n"
            "import { x } from 'x';\n\n"
            "/** Main entry */\nfunction  run()  {\n  return  x  +  y;\n}\n"
        )
        assert _similarity(a, b) == 1.0

    def test_pipeline_digest_all_combined(self) -> None:
        a = "import { x } from 'x';\nimport { y } from 'y';\nfunction run() { return x + y; }"
        b = (
            "// header\n\nimport { y } from 'y';\n"
            "import { x } from 'x';\n\n"
            "function  run()  {\n  // body\n  return  x  +  y;\n}\n"
        )
        assert _pipeline_root_digest(a, "sha1") == _pipeline_root_digest(b, "sha2")


# ── TypeScript-specific ──────────────────────────────────────────────


class TestTypeScriptDuplicates:
    """TypeScript type annotations and formatting differences."""

    def test_whitespace_in_type_annotations(self) -> None:
        a = "function add(a:number,b:number):number{return a+b;}"
        b = "function add(a : number, b : number) : number {\n  return a + b;\n}"
        assert _similarity(a, b) == 1.0

    def test_interface_with_comments(self) -> None:
        a = "interface User { id: number; name: string; }"
        b = (
            "// User model\n"
            "interface User {\n"
            "  /** unique id */\n"
            "  id: number;\n"
            "  /** display name */\n"
            "  name: string;\n"
            "}"
        )
        assert _similarity(a, b) == 1.0

    def test_ts_function_with_generics(self) -> None:
        a = "function identity<T>(arg:T):T{return arg;}"
        b = "function identity<T>(arg : T) : T {\n  return arg;\n}"
        assert _similarity(a, b) == 1.0


# ── Larger / realistic duplicates ────────────────────────────────────


class TestRealisticDuplicates:
    """Larger, more realistic code pairs that differ only cosmetically."""

    def test_multiline_function_formatting(self) -> None:
        compact = (
            "function process(items) { for (const item of items) "
            "{ console.log(item); } return items.length; }"
        )
        expanded = (
            "\n"
            "// Process all items in the list.\n"
            "function process(items) {\n"
            "    for (const item of items) {\n"
            "        console.log(item);\n"
            "    }\n"
            "    return items.length;\n"
            "}\n"
        )
        assert _similarity(compact, expanded) == 1.0

    def test_class_with_methods(self) -> None:
        a = (
            "class Dog { constructor(name) { this.name = name; } "
            "bark() { return 'woof'; } }"
        )
        b = (
            "/**\n"
            " * Represents a dog.\n"
            " */\n"
            "class Dog {\n"
            "    constructor(name) {\n"
            "        this.name = name;\n"
            "    }\n\n"
            "    // Make noise\n"
            "    bark() {\n"
            "        return 'woof';\n"
            "    }\n"
            "}\n"
        )
        assert _similarity(a, b) == 1.0

    def test_arrow_function_formatting(self) -> None:
        a = "const add = (a, b) => a + b;"
        b = "const add = (a, b) =>\n  a + b;"
        assert _similarity(a, b) == 1.0

    def test_switch_statement_formatting(self) -> None:
        a = (
            "function t(x){switch(x){case 1:return 'one';"
            "case 2:return 'two';default:return 'other';}}"
        )
        b = (
            "function t(x) {\n"
            "  switch (x) {\n"
            "    case 1:\n"
            "      return 'one';\n"
            "    case 2:\n"
            "      return 'two';\n"
            "    default:\n"
            "      return 'other';\n"
            "  }\n"
            "}\n"
        )
        assert _similarity(a, b) == 1.0

    def test_try_catch_finally_formatting(self) -> None:
        a = "function safe(fn){try{return fn();}catch(e){console.log(e);}finally{cleanup();}}"
        b = (
            "function safe(fn) {\n"
            "  try {\n"
            "    return fn();\n"
            "  } catch (e) {\n"
            "    console.log(e);\n"
            "  } finally {\n"
            "    cleanup();\n"
            "  }\n"
            "}\n"
        )
        assert _similarity(a, b) == 1.0

    def test_nested_callbacks_formatting(self) -> None:
        a = (
            "fetch(url).then(function(r){return r.json();})"
            ".then(function(data){console.log(data);});"
        )
        b = (
            "fetch(url)\n"
            "  .then(function(r) {\n"
            "    return r.json();\n"
            "  })\n"
            "  .then(function(data) {\n"
            "    console.log(data);\n"
            "  });\n"
        )
        assert _similarity(a, b) == 1.0

    def test_pipeline_complex_duplicate(self) -> None:
        a = "function f(x) { if (x > 0) { return x; } else { return -x; } }"
        b = (
            "// absolute value\n"
            "function f(x) {\n"
            "    if (x > 0) {\n"
            "        return x;\n"
            "    } else {\n"
            "        return -x;\n"
            "    }\n"
            "}\n"
        )
        assert _pipeline_root_digest(a, "sha_a") == _pipeline_root_digest(b, "sha_b")

    def test_telegram_bot_style_formatting(self) -> None:
        """Minified vs. expanded version of a realistic bot handler."""
        a = (
            "bot.on('message',msg=>{const isPrivate=msg.chat?.type!=='supergroup';"
            "if(!isPrivate){return;}processPrivateMessage(msg);});"
        )
        b = (
            "bot.on('message', msg => {\n"
            "  // Check if this is a private chat\n"
            "  const isPrivate = msg.chat?.type !== 'supergroup';\n\n"
            "  if (!isPrivate) {\n"
            "    return;\n"
            "  }\n\n"
            "  processPrivateMessage(msg);\n"
            "});\n"
        )
        assert _similarity(a, b) == 1.0
