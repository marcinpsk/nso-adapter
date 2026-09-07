# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""No ``raise ... from None`` inside an ``except`` handler.

``from None`` only sets ``__suppress_context__``. The original exception stays reachable as
``__context__``, so any secret in it survives every surface that walks the chain, and the
traceback module still prints it when a handler asks for the full chain. Suppression is not
removal.

Capture what the handler needs in a local (the sanitized exception itself is the usual one),
then raise AFTER the handler, where the interpreter attaches nothing. A ``raise ... from
None`` outside a handler has no context to attach and is not flagged.
"""

from __future__ import annotations

import ast
from pathlib import Path

_PACKAGE = Path(__file__).resolve().parents[1] / "nso_adapter"


def _suppressing_raises(handler: ast.ExceptHandler) -> list[ast.Raise]:
    """Every ``raise ... from None`` lexically inside *handler*."""
    return [
        node
        for node in ast.walk(handler)
        if isinstance(node, ast.Raise) and isinstance(node.cause, ast.Constant) and node.cause.value is None
    ]


def scan_source(source: str, path: str) -> list[str]:
    """Every ``raise ... from None`` written inside an except handler, as ``path:line``."""
    lines: list[int] = []
    for node in ast.walk(ast.parse(source, filename=path)):
        if isinstance(node, ast.ExceptHandler):
            lines += [raised.lineno for raised in _suppressing_raises(node)]
    return [f"{path}:{line}" for line in sorted(lines)]


# ── the guard ────────────────────────────────────────────────────────────────


def test_no_raise_from_none_is_written_inside_an_except_handler() -> None:
    violations: list[str] = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        violations.extend(scan_source(path.read_text(encoding="utf-8"), str(path.relative_to(_PACKAGE.parent))))
    assert not violations, (
        "`from None` leaves the original exception on __context__ — build the sanitized "
        "exception in the handler and raise it AFTER the handler: " + ", ".join(violations)
    )


# ── analyzer self-tests (real AST parsing) ───────────────────────────────────


def test_flags_a_direct_raise_from_none() -> None:
    source = "try:\n    f()\nexcept ValueError:\n    raise Boom() from None\n"
    assert scan_source(source, "t.py") == ["t.py:4"]


def test_flags_a_conditional_raise_from_none() -> None:
    source = "try:\n    f()\nexcept ValueError as exc:\n    if exc.args:\n        raise Boom() from None\n"
    assert scan_source(source, "t.py") == ["t.py:5"]


def test_flags_every_site_in_one_handler() -> None:
    source = "try:\n    f()\nexcept ValueError:\n    if x:\n        raise A() from None\n    raise B() from None\n"
    assert scan_source(source, "t.py") == ["t.py:5", "t.py:6"]


def test_a_raise_from_none_outside_a_handler_stays_legal() -> None:
    """Nothing is being handled, so nothing attaches; the suppression is a no-op."""
    assert scan_source("if bad:\n    raise Boom() from None\n", "t.py") == []


def test_a_raise_after_the_handler_stays_legal() -> None:
    source = "err = None\ntry:\n    f()\nexcept ValueError:\n    err = Boom()\nif err is not None:\n    raise err\n"
    assert scan_source(source, "t.py") == []


def test_raise_from_exc_stays_legal() -> None:
    """Chaining deliberately is a different decision; this guard only bans the false erasure."""
    assert scan_source("try:\n    f()\nexcept ValueError as exc:\n    raise Boom() from exc\n", "t.py") == []


def test_a_bare_reraise_stays_legal() -> None:
    assert scan_source("try:\n    f()\nexcept ValueError:\n    raise\n", "t.py") == []
