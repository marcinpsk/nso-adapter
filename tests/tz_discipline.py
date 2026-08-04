# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""AST 'tz discipline' guard — ban ``.replace(tzinfo=None)`` outside the wire serializer.

The store is timestamptz throughout and the invariant is "never write a naive datetime";
ruff's DTZ rules police naive *construction* (``utcnow()``, ``datetime(...)`` without
``tzinfo``), but nothing stops code from *stripping* the zone off an aware value on its
way to the store — which asyncpg then re-interprets in the process timezone, silently
shifting the instant. The only legitimate strip is ``iso_z``'s format normalization in
``nso_adapter/api/timestamps.py``. New legitimate cases take an inline ``# tz-ok: <reason>``.
"""

from __future__ import annotations

import ast
import io
import tokenize
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCAN_ROOTS = ("nso_adapter", "tests", "scripts", "alembic")
# The wire serializer is the one sanctioned zone-strip (format, not instant, changes).
_ALLOWED_FILES = {"nso_adapter/api/timestamps.py"}
_MARKER = "tz-ok"
_SELF = {"tz_discipline.py", "test_tz_discipline.py"}


def _comment_lines(src: str) -> dict[int, str]:
    """Map line-number → comment text (string-safe, via tokenize)."""
    comments: dict[int, str] = {}
    try:
        for tok in tokenize.generate_tokens(io.StringIO(src).readline):
            if tok.type == tokenize.COMMENT:
                comments[tok.start[0]] = tok.string
    except (tokenize.TokenError, IndentationError, SyntaxError):
        pass
    return comments


def _is_tzinfo_none_replace(node: ast.Call) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "replace"
        and any(
            kw.arg == "tzinfo" and isinstance(kw.value, ast.Constant) and kw.value.value is None for kw in node.keywords
        )
    )


def scan_source(src: str, rel: str = "<source>") -> list[tuple[str, int]]:
    """Return ``(relpath, lineno)`` for every unmarked ``.replace(tzinfo=None)``."""
    comments = _comment_lines(src)
    hits: list[tuple[str, int]] = []
    for node in ast.walk(ast.parse(src, filename=rel)):
        if isinstance(node, ast.Call) and _is_tzinfo_none_replace(node):
            span = range(node.lineno, (node.end_lineno or node.lineno) + 1)
            if not any(_MARKER in comments.get(ln, "") for ln in span):
                hits.append((rel, node.lineno))
    return hits


def scan_tree(root: Path = _REPO_ROOT) -> list[tuple[str, int]]:
    """Scan the production and test trees, honoring the serializer allowlist."""
    out: list[tuple[str, int]] = []
    for sub in _SCAN_ROOTS:
        for path in sorted((root / sub).rglob("*.py")):
            rel = path.relative_to(root).as_posix()
            if path.name in _SELF or "__pycache__" in path.parts or rel in _ALLOWED_FILES:
                continue
            out.extend(scan_source(path.read_text(encoding="utf-8"), rel))
    return out


def _main() -> int:
    bad = scan_tree()
    for rel, lineno in bad:
        print(f"{rel}:{lineno}: .replace(tzinfo=None) strips the zone from an aware datetime")
    print(f"\n{len(bad)} tz-discipline violation(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(_main())
