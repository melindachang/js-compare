"""Parse JavaScript / TypeScript source code into a networkx DiGraph
using tree-sitter.

This module replaces the former js2graphml.js + Node.js / Babel pipeline.
Instead of shelling out to a Node.js process that emits GraphML XML, we
parse the source directly in Python via tree-sitter and build a networkx
directed graph in-memory.

The resulting graph has the same shape as the old GraphML-based one: each
node carries a ``label`` attribute equal to the CST node type string, and
directed edges run from parent to child.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from networkx import DiGraph
from tree_sitter import Language, Parser
import tree_sitter_javascript as tsjs
import tree_sitter_typescript as tsts

from js_compare.types import (
    ALL_CST_NODE_TYPES,
    CST_NODE_CATEGORIES,
    NEVER_TYPES,
)

if TYPE_CHECKING:
    from tree_sitter import Node as TSNode

    from js_compare.types import CstNodeType

# ── Language singletons ──────────────────────────────────────────────
_JS_LANGUAGE = Language(tsjs.language())
_TS_LANGUAGE = Language(tsts.language_typescript())
_TSX_LANGUAGE = Language(tsts.language_tsx())

# tree-sitter-javascript already handles JSX syntax.
_LANGUAGES: list[Language] = [_JS_LANGUAGE, _TS_LANGUAGE, _TSX_LANGUAGE]


def _resolve_node_types(categories: list[CstNodeType | str]) -> set[str]:
    """Expand a list of high-level category names into the flat set of
    concrete CST node type strings that should be included."""
    types: set[str] = set()
    for cat in categories:
        if cat == "all":
            types |= ALL_CST_NODE_TYPES
        elif cat == "loose":
            for loose_cat in ("Programs", "Functions", "Declarations", "Statements"):
                types |= CST_NODE_CATEGORIES[loose_cat]  # type: ignore[index]
        else:
            cat_types = CST_NODE_CATEGORIES.get(cat)  # type: ignore[arg-type]
            if cat_types is None:
                raise ValueError(f"Unrecognised CST node category: {cat!r}")
            types |= cat_types
    return types


def source_to_graph(
    source: bytes | str,
    node_types: list[CstNodeType] | None = None,
) -> DiGraph:
    """Parse JavaScript or TypeScript source code (as bytes or str) and return
    a networkx DiGraph whose nodes carry a ``label`` attribute.

    *node_types* selects which CST node categories to include.  When
    ``None`` (the default) all categories are included.

    Raises ``ValueError`` if the source cannot be parsed by any of the
    available tree-sitter grammars.
    """
    raw_source = source.encode("utf-8") if isinstance(source, str) else source

    types_to_include = (
        _resolve_node_types(node_types) if node_types else ALL_CST_NODE_TYPES
    )

    for lang in _LANGUAGES:
        parser = Parser(lang)
        tree = parser.parse(raw_source)
        root = tree.root_node
        if not root.has_error:
            graph = DiGraph()
            _walk(root, graph, types_to_include)
            if len(graph) > 0:
                return graph

    raise ValueError("Unable to parse source code with any available grammar.")


def code_to_graph(
    path: Path,
    node_types: list[CstNodeType] | None = None,
) -> DiGraph:
    """Parse a JavaScript or TypeScript file and return a networkx
    DiGraph whose nodes carry a ``label`` attribute.

    *node_types* selects which CST node categories to include.  When
    ``None`` (the default) all categories are included.

    Raises ``ValueError`` if the file cannot be parsed by any of the
    available tree-sitter grammars.
    """
    return source_to_graph(path.read_bytes(), node_types)


# ── Internal helpers ─────────────────────────────────────────────────

def _node_id(node: TSNode) -> str:
    """Stable, unique identifier for a tree-sitter node within its tree."""
    return f"{node.type}:{node.start_byte}:{node.end_byte}"


def _walk(
    node: TSNode,
    graph: DiGraph,
    types_to_include: set[str],
    parent_id: str | None = None,
) -> None:
    """Depth-first walk of the tree-sitter CST, adding matching named
    nodes to *graph* and connecting each to its nearest included
    ancestor via a directed edge.

    The algorithm mirrors what ``js2graphml.js`` did with Babel's
    ``traverse``: when a node's type is in *types_to_include* it
    becomes a graph node; its parent edge points to the closest
    ancestor that was also included (skipping intermediate nodes whose
    types are not in the set).
    """
    node_type = node.type

    if node_type in NEVER_TYPES:
        return

    current_parent_id = parent_id

    if node.is_named and node_type in types_to_include:
        nid = _node_id(node)
        graph.add_node(nid, label=node_type)
        if current_parent_id is not None:
            graph.add_edge(current_parent_id, nid)
        current_parent_id = nid

    for child in node.children:
        _walk(child, graph, types_to_include, current_parent_id)
