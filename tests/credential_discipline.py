# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""AST credential discipline guard: flag forbidden test credential values.

The live NSO rule says never authenticate as admin. Test fixtures use neutral
placeholders so that this spelling does not spread into real configuration.

This scanner is deliberately aggressive. It flags case-insensitive admin literals
in credential assignments, dictionary values, defaults, and keyword arguments.
Positional string arguments and their tuple/list members are potential credentials,
including client constructors, auth tuples, and environment setters. It does not
resolve callable signatures or follow values through variables.

There are three carve-outs:

  1. **Bound the use.** Explicit noncredential fields such as role, reference fields
     such as username_ref, lookup keys, and comparisons are legitimate uses.
  2. **Mark it.** Add an inline # credential-ok: <reason> comment to the statement,
     or a contiguous comment block directly above it, for a deliberate exception.
  3. **Grandfather it.** The baseline records accepted literal counts per
     (file, function). New literals beyond those counts fail. Regenerate with::

         python -m tests.credential_discipline --update-baseline

Each username and password literal counts separately. Keep new code out of the
baseline. Prefer neutral placeholders or a documented legitimate exception.
"""

from __future__ import annotations

import ast
import io
import re
import tokenize
from dataclasses import dataclass
from pathlib import Path

TESTS_ROOT = Path(__file__).resolve().parent
_BASELINE_PATH = TESTS_ROOT / "credential_discipline_baseline.txt"
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

    @property
    def site(self) -> str:
        """Return the stable baseline key."""
        return f"{self.path}::{self.qualname}"

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


class _Scanner(ast.NodeVisitor):
    """Collect credential literals with their lexical scope."""

    def __init__(self, rel: str, src: str):
        self._rel = rel
        self._lines = src.splitlines()
        self._comments = _comment_lines(src)
        self._scope: list[str] = []
        self._hits: dict[tuple[int, int], Violation] = {}

    @property
    def hits(self) -> list[Violation]:
        return [self._hits[key] for key in sorted(self._hits)]

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._scope.append(node.name)
        args = node.args
        positional = args.posonlyargs + args.args
        for arg, value in zip(positional[-len(args.defaults) :], args.defaults):
            if _credential_name(arg.arg):
                self._check_value(value, value)
        for arg, value in zip(args.kwonlyargs, args.kw_defaults):
            if value is not None and _credential_name(arg.arg):
                self._check_value(value, value)
        self.generic_visit(node)
        self._scope.pop()

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._scope.append(node.name)
        self.generic_visit(node)
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
        elif isinstance(value, ast.Constant) and isinstance(value.value, str) and value.value.casefold() == "admin":
            self._hits[(value.lineno, value.col_offset)] = Violation(
                self._rel, value.lineno, ".".join(self._scope) or "<module>"
            )

    def _assignment(self, target: ast.AST, value: ast.AST, statement: ast.AST) -> None:
        if isinstance(target, (ast.Tuple, ast.List)) and isinstance(value, (ast.Tuple, ast.List)):
            for item, supplied in zip(target.elts, value.elts):
                self._assignment(item, supplied, statement)
        elif _credential_name(_name(target)):
            self._check_value(value, statement)

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._assignment(target, node.value, node)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self._assignment(node.target, node.value, node)
        self.generic_visit(node)

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
        if path.name in _SELF or "__pycache__" in path.parts:
            continue
        out.extend(scan_source(path.read_text(encoding="utf-8"), path.relative_to(root).as_posix()))
    return out


def _counts_by_site(violations: list[Violation]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for v in violations:
        counts[v.site] = counts.get(v.site, 0) + 1
    return counts


def load_baseline(path: Path = _BASELINE_PATH) -> dict[str, int]:
    """Read the grandfathered per-site allowance (``site\\tcount`` lines)."""
    if not path.exists():
        return {}
    allowed: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        site, _, count = line.rpartition("\t")
        allowed[site] = int(count)
    return allowed


def save_baseline(counts: dict[str, int], path: Path = _BASELINE_PATH) -> None:
    """Write the per-site allowance file (sorted, with an explanatory header)."""
    header = [
        "# SPDX-License-Identifier: Apache-2.0",
        "# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>",
        "# Credential-discipline baseline: accepted forbidden credential literals.",
        "# Each line: <relpath-from-tests>::<qualname>\\t<allowed-count>.",
        "# Shrink this file with neutral placeholders or '# credential-ok: <reason>'.",
        "# Regenerate with: python -m tests.credential_discipline --update-baseline",
        "",
    ]
    body = [f"{site}\t{counts[site]}" for site in sorted(counts)]
    path.write_text("\n".join(header + body) + "\n", encoding="utf-8")


def unapproved(root: Path = TESTS_ROOT, baseline: dict[str, int] | None = None) -> list[Violation]:
    """Return violations beyond the baseline allowance, sorted by file then line."""
    allowed = load_baseline() if baseline is None else baseline
    by_site: dict[str, list[Violation]] = {}
    for v in scan_tree(root):
        by_site.setdefault(v.site, []).append(v)
    extra: list[Violation] = []
    for site, found in by_site.items():
        budget = allowed.get(site, 0)
        if len(found) > budget:
            # Report the excess (the newest-by-line ones beyond the grandfathered count).
            extra.extend(sorted(found, key=lambda v: v.lineno)[budget:])
    return sorted(extra, key=lambda v: (v.path, v.lineno))


def _main(argv: list[str]) -> int:
    if "--update-baseline" in argv:
        counts = _counts_by_site(scan_tree())
        save_baseline(counts)
        print(f"baseline updated: {sum(counts.values())} credential(s) grandfathered across {len(counts)} site(s)")
        return 0
    bad = unapproved()
    for v in bad:
        print(str(v))
    print(f"\n{len(bad)} unapproved credential(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    import sys

    raise SystemExit(_main(sys.argv[1:]))
