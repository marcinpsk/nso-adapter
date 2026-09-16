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


def _match_capture_names(pattern: ast.pattern) -> set[str]:
    names: set[str] = set()
    for part in ast.walk(pattern):
        if isinstance(part, (ast.MatchAs, ast.MatchStar)) and part.name is not None:
            names.add(part.name)
        elif isinstance(part, ast.MatchMapping) and part.rest is not None:
            names.add(part.rest)
    return names


def _pattern_is_irrefutable(pattern: ast.pattern) -> bool:
    if isinstance(pattern, ast.MatchAs):
        return pattern.pattern is None or _pattern_is_irrefutable(pattern.pattern)
    return isinstance(pattern, ast.MatchOr) and any(_pattern_is_irrefutable(part) for part in pattern.patterns)


class _Scanner(ast.NodeVisitor):
    """Collect credential literals with their lexical scope."""

    def __init__(self, rel: str, src: str):
        self._rel = rel
        self._lines = src.splitlines()
        self._comments = _comment_lines(src)
        self._scope: list[str] = []
        self._constant_scopes: list[dict[str, set[str]]] = [{}]
        self._hits: dict[tuple[int, int], Violation] = {}
        self._try_handler_inputs: list[dict[str, set[str]]] = []
        self._loop_break_states: list[list[dict[str, set[str]]]] = []
        self._loop_continue_states: list[list[dict[str, set[str]]]] = []

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

    def _copy_constants(self) -> dict[str, set[str]]:
        return {name: values.copy() for name, values in self._constant_scopes[-1].items()}

    @staticmethod
    def _merge_constants(*states: dict[str, set[str]]) -> dict[str, set[str]]:
        merged: dict[str, set[str]] = {}
        for state in states:
            for name, values in state.items():
                merged.setdefault(name, set()).update(values)
        return merged

    def _record_handler_input(self) -> None:
        for index, handler_input in enumerate(self._try_handler_inputs):
            self._try_handler_inputs[index] = self._merge_constants(handler_input, self._constant_scopes[-1])

    def _visit_statements(self, statements: list[ast.stmt]) -> bool:
        for statement in statements:
            self._record_handler_input()
            if self.visit(statement) is False:
                return False
        return True

    def _clear_target(self, target: ast.AST) -> None:
        if isinstance(target, ast.Name):
            self._constant_scopes[-1].pop(target.id, None)
        elif isinstance(target, (ast.List, ast.Tuple)):
            for element in target.elts:
                self._clear_target(element)
        elif isinstance(target, ast.Starred):
            self._clear_target(target.value)

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
        # Python forbids a tuple target here, so the check runs directly rather than through
        # _assignment: the value is what is APPENDED, never the whole new value of the name,
        # and _assignment would re-track the alias as a plain assignment.
        if _credential_name(_name(node.target)):
            self._check_value(node.value, node)
        if isinstance(node.target, ast.Name):
            aliases = self._constant_scopes[-1]
            left = aliases.get(node.target.id)
            right = _constant_strings(node.value, aliases)
            if isinstance(node.op, ast.Add) and left is not None and right is not None:
                aliases[node.target.id] = {first + second for first in left for second in right}
            else:
                aliases.pop(node.target.id, None)
        self.generic_visit(node)

    def visit_If(self, node: ast.If) -> bool:
        self.visit(node.test)
        incoming = {name: values.copy() for name, values in self._constant_scopes[-1].items()}

        self._constant_scopes[-1] = {name: values.copy() for name, values in incoming.items()}
        body_falls_through = self._visit_statements(node.body)
        body_state = self._constant_scopes[-1]

        self._constant_scopes[-1] = {name: values.copy() for name, values in incoming.items()}
        else_falls_through = self._visit_statements(node.orelse)
        else_state = self._constant_scopes[-1]

        fallthrough_states = []
        if body_falls_through:
            fallthrough_states.append(body_state)
        if else_falls_through:
            fallthrough_states.append(else_state)
        self._constant_scopes[-1] = self._merge_constants(*fallthrough_states)
        return body_falls_through or else_falls_through

    def _visit_loop(self, node: ast.For | ast.AsyncFor | ast.While) -> bool:
        if isinstance(node, (ast.For, ast.AsyncFor)):
            self.visit(node.iter)
        else:
            self.visit(node.test)
        incoming = self._copy_constants()
        loop_inputs = incoming
        break_states: list[dict[str, set[str]]] = []
        max_passes = len({part.id for part in ast.walk(node) if isinstance(part, ast.Name)}) + 2

        for _ in range(max_passes):
            self._constant_scopes[-1] = {name: values.copy() for name, values in loop_inputs.items()}
            if isinstance(node, (ast.For, ast.AsyncFor)):
                self._clear_target(node.target)

            self._loop_break_states.append([])
            self._loop_continue_states.append([])
            body_falls_through = self._visit_statements(node.body)
            continue_states = self._loop_continue_states.pop()
            break_states.extend(self._loop_break_states.pop())

            next_states = [incoming]
            if body_falls_through:
                next_states.append(self._copy_constants())
            next_states.extend(continue_states)
            merged = self._merge_constants(loop_inputs, *next_states)
            if merged == loop_inputs:
                break
            loop_inputs = merged

        self._constant_scopes[-1] = loop_inputs
        else_falls_through = self._visit_statements(node.orelse)
        exit_states = break_states
        if else_falls_through:
            exit_states.append(self._copy_constants())
        self._constant_scopes[-1] = self._merge_constants(*exit_states)
        return True

    def visit_For(self, node: ast.For) -> bool:
        return self._visit_loop(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> bool:
        return self._visit_loop(node)

    def visit_While(self, node: ast.While) -> bool:
        return self._visit_loop(node)

    def visit_Break(self, node: ast.Break) -> bool:
        if self._loop_break_states:
            self._loop_break_states[-1].append(self._copy_constants())
        return False

    def visit_Continue(self, node: ast.Continue) -> bool:
        if self._loop_continue_states:
            self._loop_continue_states[-1].append(self._copy_constants())
        return False

    def visit_Match(self, node: ast.Match) -> bool:
        self.visit(node.subject)
        incoming = self._copy_constants()
        subject_values = _constant_strings(node.subject, incoming)
        case_states: list[dict[str, set[str]]] = []
        exhaustive = False
        falls_through = False

        for case in node.cases:
            self._constant_scopes[-1] = {name: values.copy() for name, values in incoming.items()}
            for name in _match_capture_names(case.pattern):
                if subject_values is None:
                    self._constant_scopes[-1].pop(name, None)
                else:
                    self._constant_scopes[-1][name] = subject_values.copy()
            if case.guard is not None:
                self.visit(case.guard)
            case_falls_through = self._visit_statements(case.body)
            if case_falls_through:
                case_states.append(self._copy_constants())
                falls_through = True
            exhaustive |= case.guard is None and _pattern_is_irrefutable(case.pattern)

        if not exhaustive:
            case_states.append(incoming)
            falls_through = True
        self._constant_scopes[-1] = self._merge_constants(*case_states)
        return falls_through

    def _visit_with(self, node: ast.With | ast.AsyncWith) -> bool:
        for item in node.items:
            self._record_handler_input()
            self.visit(item.context_expr)
            self._record_handler_input()
            if item.optional_vars is not None:
                self._clear_target(item.optional_vars)
        falls_through = self._visit_statements(node.body)
        self._record_handler_input()
        return falls_through

    def visit_With(self, node: ast.With) -> bool:
        return self._visit_with(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> bool:
        return self._visit_with(node)

    def _visit_try(self, node: ast.Try | ast.TryStar) -> bool:
        break_bucket = self._loop_break_states[-1] if self._loop_break_states else None
        continue_bucket = self._loop_continue_states[-1] if self._loop_continue_states else None
        break_start = len(break_bucket) if break_bucket is not None else 0
        continue_start = len(continue_bucket) if continue_bucket is not None else 0
        incoming = self._copy_constants()
        handler_input = incoming

        self._constant_scopes[-1] = {name: values.copy() for name, values in incoming.items()}
        self._try_handler_inputs.append(handler_input)
        body_falls_through = self._visit_statements(node.body)
        handler_input = self._try_handler_inputs.pop()
        normal_states: list[dict[str, set[str]]] = []

        if body_falls_through:
            if self._visit_statements(node.orelse):
                normal_states.append(self._copy_constants())

        for handler in node.handlers:
            self._constant_scopes[-1] = {name: values.copy() for name, values in handler_input.items()}
            if self.visit(handler) is not False:
                normal_states.append(self._copy_constants())

        break_states = break_bucket[break_start:] if break_bucket is not None else []
        continue_states = continue_bucket[continue_start:] if continue_bucket is not None else []
        if break_bucket is not None:
            del break_bucket[break_start:]
        if continue_bucket is not None:
            del continue_bucket[continue_start:]

        normal_falls_through = False
        normal_state: dict[str, set[str]] = {}
        if normal_states:
            self._constant_scopes[-1] = self._merge_constants(*normal_states)
            normal_falls_through = self._visit_statements(node.finalbody)
            if normal_falls_through:
                normal_state = self._copy_constants()

        for bucket, states in ((break_bucket, break_states), (continue_bucket, continue_states)):
            if bucket is None or not states:
                continue
            self._constant_scopes[-1] = self._merge_constants(*states)
            if self._visit_statements(node.finalbody):
                bucket.append(self._copy_constants())

        self._constant_scopes[-1] = normal_state
        return normal_falls_through

    def visit_Try(self, node: ast.Try) -> bool:
        return self._visit_try(node)

    def visit_TryStar(self, node: ast.TryStar) -> bool:
        return self._visit_try(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> bool:
        if node.type is not None:
            self.visit(node.type)
        if node.name is not None:
            self._constant_scopes[-1].pop(node.name, None)
        falls_through = self._visit_statements(node.body)
        if node.name is not None:
            self._constant_scopes[-1].pop(node.name, None)
        return falls_through

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
