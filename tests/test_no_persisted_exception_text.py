# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""No persisted error ``message`` or ``error`` may be built from ``repr()``.

Exception text can embed credentials — a RESTCONF error echoes the request, an httpx
error its headers. The sanctioned envelopes are ``core.claim.internal_error`` (the
exception TYPE only; the text belongs in the server log) and ``core.claim.JobError``
(author-controlled message, raised deliberately). This guard bans the shape that
leaked: a ``"message"`` or ``"error"`` value built from ``repr()`` — as a call, as
``builtins.repr``, or as an ``!r``/``!a`` conversion in an f-string or a
``str.format()`` template — inside any dict literal in the package. ``str(exc)`` and
a ``!s`` conversion stay legal because every current use is a typed domain exception
whose text is author-controlled; log calls are exempt by construction (structlog
kwargs are not dict literals).
"""

from __future__ import annotations

import ast
import string
from pathlib import Path

_PACKAGE = Path(__file__).resolve().parents[1] / "nso_adapter"
_GUARDED_KEYS = frozenset({"message", "error"})
# {x!r} and {x!a} both emit repr-style text, in an f-string or a str.format() template.
_REPR_CONVERSIONS = frozenset({"r", "a"})
# ast.FormattedValue.conversion carries the same letters as ordinals.
_REPR_CONVERSION_CODES = frozenset(ord(c) for c in _REPR_CONVERSIONS)


def _template_has_repr_conversion(template: str) -> bool:
    try:
        fields = list(string.Formatter().parse(template))
    except ValueError:  # a malformed template is not a repr, and must not crash the guard
        return False
    return any(conversion in _REPR_CONVERSIONS for _, _, _, conversion in fields)


def _is_repr(node: ast.AST) -> bool:
    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Name) and func.id == "repr":
            return True
        if isinstance(func, ast.Attribute):
            if func.attr == "repr":
                return True
            # "...{!r}...".format(exc) reaches the same text with no repr() call to see.
            if (
                func.attr == "format"
                and isinstance(func.value, ast.Constant)
                and isinstance(func.value.value, str)
                and _template_has_repr_conversion(func.value.value)
            ):
                return True
    return isinstance(node, ast.FormattedValue) and node.conversion in _REPR_CONVERSION_CODES


def _has_repr(value: ast.AST) -> bool:
    return any(_is_repr(sub) for sub in ast.walk(value))


def scan_source(source: str, path: str) -> list[str]:
    """Every guarded-key dict value built from repr(), as ``path:line`` strings."""
    violations: list[str] = []
    for node in ast.walk(ast.parse(source, filename=path)):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant) and key.value in _GUARDED_KEYS and _has_repr(value):
                violations.append(f"{path}:{value.lineno}")
    return violations


# ── the guard ────────────────────────────────────────────────────────────────


def test_no_message_value_is_built_from_exception_text() -> None:
    violations: list[str] = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        violations.extend(scan_source(path.read_text(encoding="utf-8"), str(path.relative_to(_PACKAGE.parent))))
    assert not violations, (
        "persisted 'message'/'error' built from repr() — route it through "
        "core.claim.internal_error / error_envelope: " + ", ".join(violations)
    )


# ── analyzer self-tests (real AST parsing) ───────────────────────────────────


def test_flags_a_repr_call() -> None:
    assert scan_source('e = {"message": repr(exc)}\n', "t.py") == ["t.py:1"]


def test_flags_an_fstring_repr_conversion() -> None:
    assert scan_source('e = {"error": f"boom: {exc!r}"}\n', "t.py") == ["t.py:1"]


def test_flags_builtins_repr() -> None:
    assert scan_source('e = {"message": builtins.repr(exc)}\n', "t.py") == ["t.py:1"]


def test_flags_a_format_repr_conversion() -> None:
    assert scan_source('e = {"message": "{!r}".format(exc)}\n', "t.py") == ["t.py:1"]


def test_flags_a_format_ascii_conversion() -> None:
    assert scan_source('e = {"error": "{0!a}".format(exc)}\n', "t.py") == ["t.py:1"]


def test_format_str_conversion_stays_legal() -> None:
    assert scan_source('e = {"message": "{0!s}".format(exc)}\n', "t.py") == []


def test_str_on_a_typed_exception_stays_legal() -> None:
    assert scan_source('e = {"message": str(exc), "detail": {}}\n', "t.py") == []
