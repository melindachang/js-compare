"""Maps tree-sitter CST node types into high-level categories for
selective graph construction.

These categories are analogous to the Babel AST node categories that
were previously used, but use tree-sitter's concrete syntax tree node
type names instead."""

from typing import Literal

type CstNodeType = Literal[
    "Identifier",
    "Literals",
    "Programs",
    "Functions",
    "Statements",
    "Declarations",
    "Expressions",
    "Template Literals",
    "Patterns",
    "Classes",
    "Modules",
]

cst_node_types: list[CstNodeType] = [
    "Identifier",
    "Literals",
    "Programs",
    "Functions",
    "Statements",
    "Declarations",
    "Expressions",
    "Template Literals",
    "Patterns",
    "Classes",
    "Modules",
]

# Mapping from high-level category names to sets of tree-sitter CST
# node type strings.  These correspond to the named node types emitted
# by tree-sitter-javascript and tree-sitter-typescript.
CST_NODE_CATEGORIES: dict[CstNodeType, set[str]] = {
    "Identifier": {
        "identifier",
        "property_identifier",
        "shorthand_property_identifier",
        "shorthand_property_identifier_pattern",
        "type_identifier",
        "private_property_identifier",
    },
    "Literals": {
        "number",
        "string",
        "string_fragment",
        "template_string",
        "regex",
        "true",
        "false",
        "null",
        "undefined",
    },
    "Programs": {
        "program",
    },
    "Functions": {
        "function_declaration",
        "function",
        "arrow_function",
        "generator_function_declaration",
        "generator_function",
        "method_definition",
    },
    "Statements": {
        "statement_block",
        "expression_statement",
        "empty_statement",
        "debugger_statement",
        "with_statement",
        "return_statement",
        "labeled_statement",
        "break_statement",
        "continue_statement",
        "if_statement",
        "else_clause",
        "switch_statement",
        "switch_case",
        "switch_default",
        "throw_statement",
        "try_statement",
        "catch_clause",
        "finally_clause",
        "while_statement",
        "do_statement",
        "for_statement",
        "for_in_statement",
    },
    "Declarations": {
        "variable_declaration",
        "variable_declarator",
        "lexical_declaration",
    },
    "Expressions": {
        "this",
        "super",
        "import",
        "yield_expression",
        "await_expression",
        "array",
        "object",
        "pair",
        "spread_element",
        "unary_expression",
        "update_expression",
        "binary_expression",
        "assignment_expression",
        "augmented_assignment_expression",
        "ternary_expression",
        "member_expression",
        "subscript_expression",
        "optional_chain_expression",
        "call_expression",
        "new_expression",
        "sequence_expression",
        "parenthesized_expression",
        "comma_operator",
        "arguments",
    },
    "Template Literals": {
        "template_string",
        "template_substitution",
    },
    "Patterns": {
        "object_pattern",
        "array_pattern",
        "rest_pattern",
        "assignment_pattern",
    },
    "Classes": {
        "class_declaration",
        "class",
        "class_body",
        "class_heritage",
        "method_definition",
        "public_field_definition",
        "field_definition",
        "static_block",
        "decorator",
    },
    "Modules": {
        "import_statement",
        "import_clause",
        "import_specifier",
        "named_imports",
        "namespace_import",
        "export_statement",
        "export_clause",
        "export_specifier",
    },
}

# Flattened set of all known CST node types across all categories.
ALL_CST_NODE_TYPES: set[str] = set()
for _types in CST_NODE_CATEGORIES.values():
    ALL_CST_NODE_TYPES |= _types

# Node types that should never be included in the graph (comments, etc.)
NEVER_TYPES: frozenset[str] = frozenset({
    "comment",
    "multiline_comment",
    "html_comment",
})
