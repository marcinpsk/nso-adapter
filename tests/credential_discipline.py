# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""AST credential discipline guard: flag forbidden test credential values.

The live NSO rule says never authenticate as admin. Test fixtures use neutral
placeholders so that this spelling does not spread into real configuration.

This scanner is deliberately aggressive. It flags case-insensitive admin literals
in credential assignments, dictionary values, defaults, and keyword arguments.
Positional string arguments and their tuple/list members are potential credentials,
including client constructors, auth tuples, and environment setters. It does not
resolve callable signatures. It follows constant strings within one lexical scope.

There are two carve-outs:

  1. **Bound the use.** Explicit noncredential fields such as role, reference fields
     such as username_ref, lookup keys, and comparisons are legitimate uses.
  2. **Mark it.** Add an inline # credential-ok: <reason> comment to the statement,
     or a contiguous comment block directly above it, for a deliberate exception.
Every other occurrence fails. Prefer neutral placeholders or a documented legitimate
exception.
"""

from __future__ import annotations

import ast
import io
import re
import sys
import tokenize
from dataclasses import dataclass
from itertools import product
from pathlib import Path

TESTS_ROOT = Path(__file__).resolve().parent
_OBSOLETE_BASELINE_PATH = TESTS_ROOT / "credential_discipline_baseline.txt"
_CREDENTIAL_WORDS = {"username", "user", "password", "passwd", "pwd", "secret", "token", "auth", "credentials"}
_REFERENCE_SUFFIXES = {"ref", "reference", "path", "file"}
_MARKER = re.compile(r"#\s*credential-ok:\s*\S")
_SELF = {"credential_discipline.py", "test_credential_discipline.py"}


@dataclass(frozen=True)
class Violation:
    """One forbidden credential literal."""

    path: str
    lineno: int
    qualname: str

    def __str__(self) -> str:
        return f"{self.path}:{self.lineno}: unapproved credential literal 'admin' in {self.qualname}()"


def _comment_lines(src: str) -> dict[int, str]:
    """Collect real comments, excluding markers inside strings."""
    return {
        tok.start[0]: tok.string
        for tok in tokenize.generate_tokens(io.StringIO(src).readline)
        if tok.type == tokenize.COMMENT
    }


def _credential_name(name: str) -> bool:
    words = re.sub(r"([a-z])([A-Z])", r"\1_\2", name).lower().replace("-", "_").split("_")
    return words[-1] not in _REFERENCE_SUFFIXES and bool(_CREDENTIAL_WORDS.intersection(words))


def _name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Subscript):
        return _name(node.slice)
    return ""


def _constant_strings(node: ast.AST, aliases: dict[str, set[str]] | None = None) -> set[str] | None:
    """Return every possible value of a statically constant string expression."""
    if isinstance(node, ast.Name) and aliases is not None:
        return aliases.get(node.id)
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}
    if isinstance(node, ast.FormattedValue):
        if node.conversion in (-1, ord("s")) and node.format_spec is None:
            return _constant_strings(node.value, aliases)
        return None
    if isinstance(node, ast.JoinedStr):
        parts = [_constant_strings(value, aliases) for value in node.values]
        if all(part is not None for part in parts):
            return {"".join(values) for values in product(*(part for part in parts if part is not None))}
        return None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _constant_strings(node.left, aliases)
        right = _constant_strings(node.right, aliases)
        if left is not None and right is not None:
            return {first + second for first in left for second in right}
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "lower"
        and not node.args
        and not node.keywords
    ):
        value = _constant_strings(node.func.value, aliases)
        if value is not None:
            return {item.lower() for item in value}
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "join"
        and not node.keywords
        and len(node.args) == 1
        and isinstance(node.args[0], (ast.List, ast.Tuple))
    ):
        separator = _constant_strings(node.func.value, aliases)
        parts = [_constant_strings(item, aliases) for item in node.args[0].elts]
        if separator is not None and all(part is not None for part in parts):
            choices = list(product(*(part for part in parts if part is not None)))
            return {joiner.join(values) for joiner in separator for values in choices}
    return None


class _Scanner(ast.NodeVisitor):
    """Collect credential literals with their lexical scope."""

    def __init__(self, rel: str, src: str):
        self._rel = rel
        self._lines = src.splitlines()
        self._comments = _comment_lines(src)
        self._scope: list[str] = []
        self._constant_scopes: list[dict[str, set[str]]] = [{}]
        self._hits: dict[tuple[int, int], Violation] = {}

    @property
    def hits(self) -> list[Violation]:
        return [self._hits[key] for key in sorted(self._hits)]

    def _check_defaults(self, args: ast.arguments) -> None:
        positional = args.posonlyargs + args.args
        for arg, value in zip(positional[-len(args.defaults) :], args.defaults):
            if _credential_name(arg.arg):
                self._check_value(value, value)
        for arg, value in zip(args.kwonlyargs, args.kw_defaults):
            if value is not None and _credential_name(arg.arg):
                self._check_value(value, value)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_defaults(node.args)
        self._scope.append(node.name)
        self._constant_scopes.append({})
        self.generic_visit(node)
        self._constant_scopes.pop()
        self._scope.pop()

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self._check_defaults(node.args)
        self._constant_scopes.append({})
        self.generic_visit(node)
        self._constant_scopes.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._scope.append(node.name)
        self._constant_scopes.append({})
        self.generic_visit(node)
        self._constant_scopes.pop()
        self._scope.pop()

    def _is_marked(self, node: ast.AST) -> bool:
        start = node.lineno
        end = node.end_lineno or start
        if any(_MARKER.search(self._comments.get(line, "")) for line in range(start, end + 1)):
            return True
        line = start - 1
        while line in self._comments and self._lines[line - 1].lstrip().startswith("#"):
            if _MARKER.search(self._comments[line]):
                return True
            line -= 1
        return False

    def _check_value(self, value: ast.AST, statement: ast.AST) -> None:
        if self._is_marked(statement):
            return
        if isinstance(value, (ast.Tuple, ast.List)):
            for item in value.elts:
                self._check_value(item, statement)
        elif (literals := _constant_strings(value, self._constant_scopes[-1])) is not None and any(
            literal.casefold() == "admin" for literal in literals
        ):
            self._hits[(value.lineno, value.col_offset)] = Violation(
                self._rel, value.lineno, ".".join(self._scope) or "<module>"
            )

    def _track_constant(self, target: ast.AST, value: ast.AST) -> None:
        if not isinstance(target, ast.Name):
            return
        aliases = self._constant_scopes[-1]
        literals = _constant_strings(value, aliases)
        if literals is None:
            aliases.pop(target.id, None)
        else:
            aliases[target.id] = literals

    def _assignment(self, target: ast.AST, value: ast.AST, statement: ast.AST) -> None:
        if isinstance(target, (ast.Tuple, ast.List)) and isinstance(value, (ast.Tuple, ast.List)):
            for item, supplied in zip(target.elts, value.elts):
                self._assignment(item, supplied, statement)
        else:
            if _credential_name(_name(target)):
                self._check_value(value, statement)
            self._track_constant(target, value)

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._assignment(target, node.value, node)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self._assignment(node.target, node.value, node)
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        if isinstance(node.target, ast.Name):
            aliases = self._constant_scopes[-1]
            left = aliases.get(node.target.id)
            right = _constant_strings(node.value, aliases)
            if isinstance(node.op, ast.Add) and left is not None and right is not None:
                aliases[node.target.id] = {first + second for first in left for second in right}
            else:
                aliases.pop(node.target.id, None)
        self.generic_visit(node)

    def visit_If(self, node: ast.If) -> None:
        self.visit(node.test)
        incoming = {name: values.copy() for name, values in self._constant_scopes[-1].items()}

        self._constant_scopes[-1] = {name: values.copy() for name, values in incoming.items()}
        for statement in node.body:
            self.visit(statement)
        body_aliases = self._constant_scopes[-1]

        self._constant_scopes[-1] = {name: values.copy() for name, values in incoming.items()}
        for statement in node.orelse:
            self.visit(statement)
        else_aliases = self._constant_scopes[-1]

        merged: dict[str, set[str]] = {}
        for aliases in (body_aliases, else_aliases):
            for name, values in aliases.items():
                merged.setdefault(name, set()).update(values)
        self._constant_scopes[-1] = merged

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self._assignment(node.target, node.value, node)
        self.generic_visit(node)

    def visit_Dict(self, node: ast.Dict) -> None:
        for key, value in zip(node.keys, node.values):
            if key is not None and _credential_name(_name(key)):
                self._check_value(value, node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        for arg in node.args:
            self._check_value(arg, node)
        for kw in node.keywords:
            if kw.arg is not None and _credential_name(kw.arg):
                self._check_value(kw.value, node)
        self.generic_visit(node)


def scan_source(src: str, rel: str = "<source>") -> list[Violation]:
    """Scan one module's source text."""
    tree = ast.parse(src, filename=rel)
    scanner = _Scanner(rel, src)
    scanner.visit(tree)
    return scanner.hits


def scan_tree(root: Path = TESTS_ROOT) -> list[Violation]:
    """Scan every test module except the guard and its self-tests."""
    out: list[Violation] = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root).as_posix()
        if rel in _SELF or "__pycache__" in path.parts:
            continue
        out.extend(scan_source(path.read_text(encoding="utf-8"), rel))
    return out


def _main(argv: list[str]) -> int:
    if argv:
        print("usage: python -m tests.credential_discipline", file=sys.stderr)
        return 2
    if _OBSOLETE_BASELINE_PATH.exists():
        print(f"{_OBSOLETE_BASELINE_PATH}: obsolete credential baseline is not allowed", file=sys.stderr)
        return 1
    bad = scan_tree()
    for v in bad:
        print(str(v))
    print(f"\n{len(bad)} unapproved credential(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
