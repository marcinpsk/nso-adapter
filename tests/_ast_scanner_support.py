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

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 - ast visitor API
        self.names.add(node.name)

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802 - ast visitor API
        self.names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802 - ast visitor API
        return

    def visit_ListComp(self, node: ast.ListComp) -> None:  # noqa: N802 - ast visitor API
        return

    visit_SetComp = visit_ListComp  # type: ignore[assignment]
    visit_GeneratorExp = visit_ListComp  # type: ignore[assignment]
    visit_DictComp = visit_ListComp  # type: ignore[assignment]


def scope_bound_names(nodes: list[ast.AST]) -> set[str]:
    """Return names bound by nodes in one lexical scope."""
    collector = _ScopeBindingCollector()
    for node in nodes:
        collector.visit(node)
    return collector.names


def statement_may_raise(statement: ast.stmt) -> bool:
    """Return whether a statement needs an exceptional input state."""
    if isinstance(statement, (ast.Pass, ast.Break, ast.Continue)):
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
