# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Verify authority-rule configuration and executable SQL examples."""

import ast
import json
from pathlib import Path

import pytest
import yaml
from sqlalchemy import select

from nso_adapter.store.models import Device, DeviceProjectionStream

FIXTURES = Path(__file__).resolve().parents[1] / ".opengrep/tests/nso_adapter/core"


def test_new_authority_write_forms_propagate_to_every_scope():
    config = FIXTURES.parents[2] / "nso-rules.yaml"
    rules = {rule["id"]: rule for rule in yaml.safe_load(config.read_text())["rules"]}
    new_form = {"pattern": "$ROW.set_authority($FIELD, $VALUE)"}
    rules["nso-authority-write"]["patterns"][0]["pattern-either"].append(new_form)
    # The runner expands YAML aliases before it passes the configuration to OpenGrep.
    expanded = json.loads(json.dumps(rules))
    consumers = (
        "nso-authority-write-generation",
        "nso-authority-write-cutover",
        "nso-authority-write-generation-shadow",
        "nso-authority-write-cutover-shadow",
        "nso-authority-reset-value",
        "nso-authority-reset-revision",
        "nso-authority-reset-rebound-null",
    )
    for name in consumers:
        forms = next(pattern["pattern-either"] for pattern in expanded[name]["patterns"] if "pattern-either" in pattern)
        assert new_form in forms, name


def _snippet(name):
    path = FIXTURES / name
    tree = ast.parse(path.read_text())
    imports = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    namespace = {}
    exec(compile(ast.Module(body=imports, type_ignores=[]), str(path), "exec"), namespace)
    return path, tree, namespace


@pytest.fixture
def authority_row(pg_sync_session):
    device = Device(nso_instance="nso-dev", nso_device_name="fixture-device")
    pg_sync_session.add(device)
    pg_sync_session.flush()
    row = DeviceProjectionStream(
        device_id=device.id, stream="vlan", authorized_revision=2, authorized_document={"vlan_intent": []}
    )
    pg_sync_session.add(row)
    pg_sync_session.flush()
    return row


@pytest.mark.anyio
async def test_accepted_reset_example_executes_sql_null(pg_sync_session, authority_row):
    path, tree, namespace = _snippet("cutover.py")
    snippet = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef))
    exec(compile(ast.Module(body=[snippet], type_ignores=[]), str(path), "exec"), namespace)
    await namespace[snippet.name](pg_sync_session, authority_row)
    pg_sync_session.expire_all()
    assert authority_row.authorized_document is None
    assert authority_row.authorized_revision == 0
    assert pg_sync_session.scalar(
        select(DeviceProjectionStream.authorized_document.is_(None)).where(
            DeviceProjectionStream.id == authority_row.id
        )
    )


@pytest.mark.parametrize("example", [0, 1])
def test_upsert_examples_update_a_real_conflicting_row(pg_sync_session, authority_row, example):
    path, tree, namespace = _snippet("shadow_writers.py")
    upserts = sorted(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "on_conflict_do_update"
        ),
        key=lambda node: node.lineno,
    )
    assert len(upserts) == 2
    namespace.update(row=authority_row, fields={"authorized_revision": 5})
    statement = eval(compile(ast.Expression(upserts[example]), str(path), "eval"), namespace)
    pg_sync_session.execute(statement)
    pg_sync_session.expire_all()
    assert authority_row.authorized_revision == 5
