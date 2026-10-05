"""Code for comparing two CST-based code graphs for similarity."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from js_compare.ast_tree import ASTTree
from js_compare.parse import code_to_graph

if TYPE_CHECKING:
    from networkx import DiGraph
    from networkx.classes.graph import _Node

    from js_compare.types import CstNodeType


@dataclass
class Comparison:
    graph1: int
    graph2: int
    # Number of nodes in subtrees that appear in graph1 that also appear in
    # graph2
    overlap: int
    # % of nodes in graph1 that appear in identical subtrees in graph2
    normalized: float
    # Symmetric similarity score (Sørensen-Dice coefficient: 2 * overlap / (graph1 + graph2))
    similarity: float


def compare_graphs(graph1: DiGraph, graph2: DiGraph) -> Comparison:
    tree1 = ASTTree(graph1)
    tree2 = ASTTree(graph2)

    overlap_nodes = 0
    if __debug__:
        observed_subtree_roots: set[_Node] = set()
    for common_subtree_root in tree1.common_subtree_roots(tree2):
        if __debug__:
            assert common_subtree_root.node not in observed_subtree_roots
            observed_subtree_roots.add(common_subtree_root.node)
        overlap_nodes += common_subtree_root.weight

    normalized = overlap_nodes / float(tree1.num_nodes()) if tree1.num_nodes() > 0 else 0.0
    total_nodes = len(graph1) + len(graph2)
    similarity = (2.0 * overlap_nodes / total_nodes) if total_nodes > 0 else 0.0
    comparison = Comparison(len(graph1), len(graph2), overlap_nodes, normalized, similarity)
    return comparison


def compare_code(code1: Path, code2: Path,
                 node_types: list[CstNodeType] | None = None) -> Comparison:
    """Parse two JavaScript / TypeScript files and compare their code
    graphs.

    *node_types* selects which CST node categories to include.  Pass
    ``None`` to include all categories.
    """
    g1 = code_to_graph(code1, node_types)
    g2 = code_to_graph(code2, node_types)
    return compare_graphs(g1, g2)
