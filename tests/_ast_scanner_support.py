# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Shared syntax contracts for the test-suite AST scanners."""

from __future__ import annotations

import ast


def argument_names(arguments: ast.arguments) -> set[str]:
    """Return every name bound by a function or lambda signature."""
    return (
        {argument.arg for argument in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)}
        | ({arguments.vararg.arg} if arguments.vararg is not None else set())
        | ({arguments.kwarg.arg} if arguments.kwarg is not None else set())
    )


def match_capture_names(pattern: ast.pattern) -> set[str]:
    """Return every name bound by a match pattern."""
    names: set[str] = set()
    for part in ast.walk(pattern):
        if isinstance(part, (ast.MatchAs, ast.MatchStar)) and part.name is not None:
            names.add(part.name)
        elif isinstance(part, ast.MatchMapping) and part.rest is not None:
            names.add(part.rest)
    return names


def pattern_is_irrefutable(pattern: ast.pattern) -> bool:
    """Return whether a match pattern accepts every subject."""
    if isinstance(pattern, ast.MatchAs):
        return pattern.pattern is None or pattern_is_irrefutable(pattern.pattern)
    return isinstance(pattern, ast.MatchOr) and any(pattern_is_irrefutable(part) for part in pattern.patterns)


class _ScopeBindingCollector(ast.NodeVisitor):
    """Collect names bound in one lexical scope without entering child scopes."""

    def __init__(self) -> None:
        self.names: set[str] = set()

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802 - ast visitor API
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.names.add(node.id)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:  # noqa: N802 - ast visitor API
        if node.name is not None:
            self.names.add(node.name)
        self.generic_visit(node)

    def visit_alias(self, node: ast.alias) -> None:  # noqa: N802 - ast visitor API
        # An import binds a name with no ast.Name store: `import a.b` binds `a`.
        self.names.add(node.asname or node.name.split(".", maxsplit=1)[0])

    def visit_MatchAs(self, node: ast.MatchAs) -> None:  # noqa: N802 - ast visitor API
        self.names.update(match_capture_names(node))
        self.generic_visit(node)

    visit_MatchStar = visit_MatchAs  # type: ignore[assignment]
    visit_MatchMapping = visit_MatchAs  # type: ignore[assignment]

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 - ast visitor API
        self.names.add(node.name)

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802 - ast visitor API
        self.names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802 - ast visitor API
        return

    def visit_ListComp(self, node: ast.ListComp) -> None:  # noqa: N802 - ast visitor API
        # The comprehension is its own scope, but a walrus in it binds HERE (PEP 572).
        self.names.update(walrus_target_names(node))

    visit_SetComp = visit_ListComp  # type: ignore[assignment]
    visit_GeneratorExp = visit_ListComp  # type: ignore[assignment]
    visit_DictComp = visit_ListComp  # type: ignore[assignment]


def walrus_expressions(node: ast.AST) -> list[ast.NamedExpr]:
    """Return the assignment expressions inside *node*.

    PEP 572: a walrus inside a comprehension binds its target in the CONTAINING scope, unlike the
    generator targets, which stay isolated. Every comprehension-scoped scanner owes this, and a
    scanner that tracks values (not just names) needs the expression, not the name alone.
    """
    return [binding for binding in ast.walk(node) if isinstance(binding, ast.NamedExpr)]


def walrus_target_names(node: ast.AST) -> set[str]:
    """Return the names an assignment expression binds inside *node*."""
    return {binding.target.id for binding in walrus_expressions(node) if isinstance(binding.target, ast.Name)}


def scope_bound_names(nodes: list[ast.AST]) -> set[str]:
    """Return names bound by nodes in one lexical scope."""
    collector = _ScopeBindingCollector()
    for node in nodes:
        collector.visit(node)
    return collector.names


def statement_may_raise(statement: ast.stmt) -> bool:
    """Return whether a statement needs an exceptional input state."""
    if isinstance(statement, (ast.Pass, ast.Break, ast.Continue)) or (
        isinstance(statement, ast.Return) and statement.value is None
    ):
        return False
    if isinstance(statement, (ast.Try, ast.TryStar)):
        return False
    if isinstance(statement, ast.Assign):
        return not isinstance(statement.value, ast.Constant) or not all(
            isinstance(target, ast.Name) for target in statement.targets
        )
    return (
        not isinstance(statement, ast.AnnAssign)
        or not isinstance(statement.target, ast.Name)
        or not isinstance(statement.value, ast.Constant)
    )
