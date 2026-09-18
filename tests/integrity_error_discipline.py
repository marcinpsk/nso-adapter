# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Require every IntegrityError handler to classify its constraint or re-raise."""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = ROOT / "nso_adapter"
_INTEGRITY_ERROR = "sqlalchemy.exc.IntegrityError"
_CONSTRAINT_HELPER = "nso_adapter.store.db._violated_constraint"


@dataclass(frozen=True)
class Violation:
    """One IntegrityError handler that can convert an unclassified failure."""

    path: str
    lineno: int
    qualname: str

    def __str__(self) -> str:
        return (
            f"{self.path}:{self.lineno}: {self.qualname} must bind IntegrityError and call "
            "nso_adapter.store.db._violated_constraint(exc), or re-raise it with a bare raise"
        )


def _import_bindings(tree: ast.AST) -> dict[str, str]:
    """Map imported names to their fully qualified targets."""
    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".", maxsplit=1)[0]
                bindings[bound] = alias.name if alias.asname else bound
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            for alias in node.names:
                if alias.name != "*":
                    bindings[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return bindings


def _qualified_name(node: ast.AST | None, imports: dict[str, str]) -> str | None:
    if isinstance(node, ast.Name):
        return imports.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        owner = _qualified_name(node.value, imports)
        return f"{owner}.{node.attr}" if owner is not None else None
    return None


def _catches_integrity_error(node: ast.AST | None, imports: dict[str, str]) -> bool:
    if isinstance(node, ast.Tuple):
        return any(_catches_integrity_error(item, imports) for item in node.elts)
    return _qualified_name(node, imports) == _INTEGRITY_ERROR


def _expression_classifies(node: ast.AST | None, imports: dict[str, str], exception_name: str) -> bool:
    """Return true when every normal evaluation calls the helper on the bound error."""
    if node is None or isinstance(node, (ast.Lambda, ast.comprehension)):
        return False
    if isinstance(node, ast.Call) and (
        _qualified_name(node.func, imports) == _CONSTRAINT_HELPER
        and node.args
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == exception_name
    ):
        return True
    if isinstance(node, ast.BoolOp):
        return bool(node.values) and _expression_classifies(node.values[0], imports, exception_name)
    if isinstance(node, ast.IfExp):
        return _expression_classifies(node.test, imports, exception_name) or (
            _expression_classifies(node.body, imports, exception_name)
            and _expression_classifies(node.orelse, imports, exception_name)
        )
    if isinstance(node, ast.Compare):
        return _expression_classifies(node.left, imports, exception_name) or (
            bool(node.comparators) and _expression_classifies(node.comparators[0], imports, exception_name)
        )
    if isinstance(node, (ast.DictComp, ast.GeneratorExp, ast.ListComp, ast.SetComp)):
        return False
    return any(_expression_classifies(child, imports, exception_name) for child in ast.iter_child_nodes(node))


def _statement_expression(statement: ast.stmt) -> ast.AST | None:
    if isinstance(statement, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
        return statement.value
    if isinstance(statement, (ast.Expr, ast.Return)):
        return statement.value
    if isinstance(statement, ast.Assert):
        return statement.test
    if isinstance(statement, ast.Raise):
        return statement.exc
    return None


def _flow_statements(
    body: list[ast.stmt],
    states: set[bool],
    imports: dict[str, str],
    exception_name: str,
) -> tuple[set[bool], bool]:
    """Return fall-through classification states and whether an unsafe exit exists."""
    unsafe_exit = False
    for statement in body:
        if not states:
            break
        expression = _statement_expression(statement)
        if _expression_classifies(expression, imports, exception_name):
            states = {True}

        if isinstance(statement, ast.If):
            if _expression_classifies(statement.test, imports, exception_name):
                states = {True}
            body_states, body_unsafe = _flow_statements(statement.body, states.copy(), imports, exception_name)
            else_states, else_unsafe = _flow_statements(statement.orelse, states.copy(), imports, exception_name)
            states = body_states | else_states
            unsafe_exit = unsafe_exit or body_unsafe or else_unsafe
        elif isinstance(statement, ast.Raise):
            if statement.exc is not None and False in states:
                unsafe_exit = True
            states = set()
        elif isinstance(statement, (ast.Break, ast.Continue, ast.Return)):
            if False in states:
                unsafe_exit = True
            states = set()
        elif isinstance(statement, (ast.With, ast.AsyncWith)):
            if any(_expression_classifies(item.context_expr, imports, exception_name) for item in statement.items):
                states = {True}
            states, nested_unsafe = _flow_statements(statement.body, states, imports, exception_name)
            unsafe_exit = unsafe_exit or nested_unsafe
    return states, unsafe_exit


def _handler_is_safe(
    body: list[ast.stmt],
    imports: dict[str, str],
    exception_name: str,
) -> bool:
    states, unsafe_exit = _flow_statements(body, {False}, imports, exception_name)
    return not unsafe_exit and False not in states


class _Scanner(ast.NodeVisitor):
    """Collect unclassified IntegrityError handlers with their lexical scope."""

    def __init__(self, rel: str, tree: ast.AST) -> None:
        self._rel = rel
        self._imports = _import_bindings(tree)
        self._scope: list[str] = []
        self.hits: list[Violation] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 - ast visitor API
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802 - ast visitor API
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:  # noqa: N802 - ast visitor API
        if _catches_integrity_error(node.type, self._imports):
            safe = _handler_is_safe(node.body, self._imports, node.name or "")
            if not safe:
                self.hits.append(
                    Violation(
                        path=self._rel,
                        lineno=node.lineno,
                        qualname=".".join(self._scope) or "<module>",
                    )
                )
        self.generic_visit(node)


def scan_source(src: str, rel: str = "<source>") -> list[Violation]:
    """Scan one module's source text."""
    tree = ast.parse(src, filename=rel)
    scanner = _Scanner(rel, tree)
    scanner.visit(tree)
    return scanner.hits


def scan_tree(root: Path = SOURCE_ROOT) -> list[Violation]:
    """Scan every Python module under the adapter package."""
    violations: list[Violation] = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        violations.extend(scan_source(path.read_text(encoding="utf-8"), rel))
    return violations


def _main(argv: list[str]) -> int:
    if argv:
        print("usage: python -m tests.integrity_error_discipline", file=sys.stderr)
        return 2
    violations = scan_tree()
    for violation in violations:
        print(violation)
    print(f"\n{len(violations)} unclassified IntegrityError handler(s)")
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
