# SPDX-License-Identifier: Apache-2.0
"""Maintenance scripts must use the installed aggregate sender API."""

import ast
import re
from pathlib import Path

from nso_adapter.nso import apply

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"

#: The retired per-service RESTCONF namespaces. One aggregate service replaced the sixteen
#: reconcilers, so a script naming one of them writes to a service that no longer exists.
_RETIRED_NAMESPACE = re.compile(r"[a-z0-9-]+-reconciler:")


def test_scripts_import_only_existing_sender_symbols():
    missing = []
    for path in _SCRIPTS.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.module == "nso_adapter.nso.apply":
                missing.extend(f"{path.name}: {name.name}" for name in node.names if not hasattr(apply, name.name))
    assert missing == []


def test_scripts_address_no_retired_reconciler_namespace():
    named = sorted(
        f"{path.name}: {match}"
        for path in _SCRIPTS.glob("*.py")
        for match in set(_RETIRED_NAMESPACE.findall(path.read_text()))
    )
    assert named == []
