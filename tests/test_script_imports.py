# SPDX-License-Identifier: Apache-2.0
"""Maintenance scripts must not name anything the aggregate port deleted.

Two rules: a symbol taken from the sender module has to exist, and no script may address a
retired per-service reconciler namespace. Neither obliges a script to SEND device intent, so
a read-only spike is subject to both and required to call nothing.
"""

import ast
import re
from pathlib import Path

from nso_adapter.nso import apply

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"

_SENDER = "nso_adapter.nso.apply"

#: The retired per-service RESTCONF namespaces. One aggregate service replaced the sixteen
#: reconcilers, so a script naming one of them writes to a service that no longer exists.
_RETIRED_NAMESPACE = re.compile(r"[a-z0-9-]+-reconciler:")


def _dotted(node: ast.AST) -> str | None:
    """Resolve a name/attribute chain to its dotted string, else ``None``."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    return ".".join([node.id, *reversed(parts)])


def sender_symbols_named(source: str) -> set[str]:
    """Return every sender-module symbol *source* names, in any import spelling.

    Three spellings reach the module, and matching only the first is how this guard came to
    check nothing: ``from nso_adapter.nso.apply import x``; ``from nso_adapter.nso import
    apply`` then ``apply.x``; ``import nso_adapter.nso.apply`` then the full dotted path.
    """
    tree = ast.parse(source)
    named: set[str] = set()
    module_aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == _SENDER:
            named.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module == "nso_adapter.nso":
            module_aliases.update(alias.asname or alias.name for alias in node.names if alias.name == "apply")
        elif isinstance(node, ast.Import):
            module_aliases.update(alias.asname or _SENDER for alias in node.names if alias.name == _SENDER)
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and _dotted(node.value) in module_aliases:
            named.add(node.attr)
    return named


def test_scripts_name_only_existing_sender_symbols():
    missing = sorted(
        f"{path.name}: {symbol}"
        for path in _SCRIPTS.glob("*.py")
        for symbol in sender_symbols_named(path.read_text())
        if not hasattr(apply, symbol)
    )
    assert missing == []


def test_the_sender_guard_sees_every_import_spelling():
    """Matching one spelling is why this guard checked nothing; these are the others."""
    assert sender_symbols_named("from nso_adapter.nso.apply import apply_device_intent") == {"apply_device_intent"}
    assert sender_symbols_named("from nso_adapter.nso import apply\napply.deleted_sender()") == {"deleted_sender"}
    assert sender_symbols_named("import nso_adapter.nso.apply\nnso_adapter.nso.apply.gone()") == {"gone"}
    assert sender_symbols_named("from nso_adapter.nso import apply as a\na.gone()") == {"gone"}
    # A sibling module of the same shape is not the sender.
    assert sender_symbols_named("from nso_adapter.nso import actions\nactions.probe()") == set()


def test_scripts_address_no_retired_reconciler_namespace():
    named = sorted(
        f"{path.name}: {match}"
        for path in _SCRIPTS.glob("*.py")
        for match in set(_RETIRED_NAMESPACE.findall(path.read_text()))
    )
    assert named == []
