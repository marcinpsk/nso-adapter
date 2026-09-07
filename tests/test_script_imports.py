# SPDX-License-Identifier: Apache-2.0
"""Maintenance scripts must use the installed aggregate sender API."""

import ast
from pathlib import Path

from nso_adapter.nso import apply


def test_scripts_import_only_existing_sender_symbols():
    missing = []
    for path in (Path(__file__).resolve().parents[1] / "scripts").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.module == "nso_adapter.nso.apply":
                missing.extend(f"{path.name}: {name.name}" for name in node.names if not hasattr(apply, name.name))
    assert missing == []
