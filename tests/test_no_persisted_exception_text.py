# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""No persisted error ``message`` may be built from ``repr()``.

Exception text can embed credentials — a RESTCONF error echoes the request, an httpx
error its headers. The sanctioned envelopes are ``core.claim.internal_error`` (the
exception TYPE only; the text belongs in the server log) and ``core.claim.JobError``
(author-controlled message, raised deliberately). This guard bans the shape that
leaked: a ``"message"`` value containing a ``repr()`` call inside any dict literal in
the package. ``str(exc)`` stays legal because every current use is a typed domain
exception whose text is author-controlled; log calls are exempt by construction
(structlog kwargs are not dict literals).
"""

from __future__ import annotations

import ast
from pathlib import Path

_PACKAGE = Path(__file__).resolve().parents[1] / "nso_adapter"
_BANNED_CALLS = frozenset({"repr"})


def _banned_calls(node: ast.AST):
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id in _BANNED_CALLS:
            yield sub


def test_no_message_value_is_built_from_exception_text() -> None:
    violations: list[str] = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            for key, value in zip(node.keys, node.values):
                if isinstance(key, ast.Constant) and key.value == "message" and any(_banned_calls(value)):
                    violations.append(f"{path.relative_to(_PACKAGE.parent)}:{value.lineno}")
    assert not violations, (
        "persisted 'message' built from repr()/str() — route it through core.claim.internal_error: "
        + ", ".join(violations)
    )
