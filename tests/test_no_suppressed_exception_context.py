# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""No ``raise X`` and no ``raise X from None`` inside an ``except`` handler.

Both spellings leave the caught exception on ``__context__``. A plain ``raise X`` attaches it
implicitly; ``from None`` attaches it and only sets ``__suppress_context__``. Either way any
secret in the caught exception survives every surface that walks the chain, and the traceback
module still prints it when a caller asks for the full chain. Suppression is not removal, and
saying nothing is not suppression.

Capture what the handler needs in a local (the sanitized exception itself is the usual one),
then raise AFTER the handler, where the interpreter attaches nothing. A raise outside a
handler has no context to attach and is not flagged, a bare ``raise`` re-raises the caught
exception on purpose, and ``raise X from exc`` chains it deliberately.
"""

from __future__ import annotations

import ast
from pathlib import Path

_PACKAGE = Path(__file__).resolve().parents[1] / "nso_adapter"


def _context_attaching_raises(handler: ast.ExceptHandler) -> list[ast.Raise]:
    """Every raise lexically inside *handler* that leaves the caught exception attached."""
    found: list[ast.Raise] = []
    for node in ast.walk(handler):
        if not isinstance(node, ast.Raise) or node.exc is None:
            continue  # a bare `raise` re-raises the caught exception on purpose
        suppressed = isinstance(node.cause, ast.Constant) and node.cause.value is None
        if node.cause is None or suppressed:
            found.append(node)
    return found


def scan_source(source: str, path: str) -> list[str]:
    """Every context-attaching raise written inside an except handler, as ``path:line``."""
    lines: list[int] = []
    for node in ast.walk(ast.parse(source, filename=path)):
        if isinstance(node, ast.ExceptHandler):
            lines += [raised.lineno for raised in _context_attaching_raises(node)]
    return [f"{path}:{line}" for line in sorted(lines)]


# ── the guard ────────────────────────────────────────────────────────────────


def test_no_raise_inside_an_except_handler_keeps_the_caught_exception_attached() -> None:
    violations: list[str] = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        violations.extend(scan_source(path.read_text(encoding="utf-8"), str(path.relative_to(_PACKAGE.parent))))
    assert not violations, (
        "a plain `raise X` attaches the caught exception to __context__ and `from None` only "
        "hides it — build the sanitized exception in the handler and raise it AFTER the "
        "handler: " + ", ".join(violations)
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


def test_flags_a_plain_raise() -> None:
    """The implicit half of the class: the interpreter attaches the caught exception itself."""
    source = "try:\n    f()\nexcept ValueError:\n    raise Boom()\n"
    assert scan_source(source, "t.py") == ["t.py:4"]


def test_flags_a_conditional_plain_raise() -> None:
    source = "try:\n    f()\nexcept ValueError as exc:\n    if exc.args:\n        raise Boom()\n"
    assert scan_source(source, "t.py") == ["t.py:5"]


def test_flags_both_spellings_in_one_handler() -> None:
    source = "try:\n    f()\nexcept ValueError:\n    if x:\n        raise A()\n    raise B() from None\n"
    assert scan_source(source, "t.py") == ["t.py:5", "t.py:6"]


def test_a_plain_raise_outside_a_handler_stays_legal() -> None:
    """Nothing is being handled, so the interpreter has nothing to attach."""
    assert scan_source("if bad:\n    raise Boom()\n", "t.py") == []


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
