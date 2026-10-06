# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Owned empty descriptions remain in sync after device cleanup."""

from unittest.mock import AsyncMock

import pytest

from nso_adapter.core import importer
from nso_adapter.store.models import DbInterface, Device, InterfaceAttrState, InterfaceIntent, ManagedScope, SyncState
from tests.core.test_importer import _make_nso_client


@pytest.mark.parametrize("operation", ["sync_device", "detect_drift"])
@pytest.mark.parametrize("intent_value", ["", None], ids=["empty", "null"])
@pytest.mark.parametrize(
    "native_description,expected",
    [(None, SyncState.in_sync), ("", SyncState.in_sync), ("unexpected", SyncState.drifted), (" ", SyncState.drifted)],
    ids=["absent", "empty", "nonempty", "whitespace"],
)
async def test_owned_empty_description_state(
    db_session, monkeypatch, operation, intent_value, native_description, expected
):
    device = Device(nso_instance="description-test", nso_device_name="description-device", ned_id="cisco-ios-cli-6.95")
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.flush()
    interface = DbInterface(device=device, name="Loopback100")
    db_session.add(interface)
    await db_session.flush()
    state = InterfaceAttrState(interface_id=interface.id, attribute="description", sync_state=SyncState.in_sync)
    db_session.add(state)
    intent = InterfaceIntent(interface_id=interface.id, attribute="description", intent_value=intent_value)
    db_session.add(intent)
    await db_session.commit()

    entry = {"interface-name": interface.name}
    if native_description is not None:
        entry["description"] = native_description
    client = _make_nso_client({"interface": [entry]})
    monkeypatch.setitem(importer._nso_clients, device.nso_instance, client)
    monkeypatch.setattr(importer, "_netbox_client", None)
    action = "sync_from" if operation == "sync_device" else "compare_config"
    monkeypatch.setattr(importer.nso_actions, action, AsyncMock(return_value={"result": True}))

    summary = await getattr(importer, operation)(device.id, db_session)

    await db_session.refresh(state)
    assert state.sync_state == expected
    assert summary["changes_detected"] == (1 if operation == "detect_drift" and expected == SyncState.drifted else 0)
    await db_session.refresh(intent)
    assert intent.intent_value == intent_value


@pytest.mark.parametrize(
    "operation,expected", [("sync_device", SyncState.imported), ("detect_drift", SyncState.unknown)]
)
async def test_unowned_absent_description_keeps_import_state(db_session, monkeypatch, operation, expected):
    device = Device(nso_instance="description-test", nso_device_name="description-device", ned_id="cisco-ios-cli-6.95")
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.flush()
    interface = DbInterface(device=device, name="Loopback100")
    db_session.add(interface)
    await db_session.flush()
    state = InterfaceAttrState(interface_id=interface.id, attribute="description", sync_state=SyncState.unknown)
    db_session.add(state)
    await db_session.commit()
    client = _make_nso_client({"interface": [{"interface-name": interface.name}]})
    monkeypatch.setitem(importer._nso_clients, device.nso_instance, client)
    monkeypatch.setattr(importer, "_netbox_client", None)
    action = "sync_from" if operation == "sync_device" else "compare_config"
    monkeypatch.setattr(importer.nso_actions, action, AsyncMock(return_value={"result": True}))

    summary = await getattr(importer, operation)(device.id, db_session)

    await db_session.refresh(state)
    assert state.sync_state == expected
    assert summary["changes_detected"] == 0
