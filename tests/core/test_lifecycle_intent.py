# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Lifecycle / characterization — adapter intent ownership across the importer.

Walking the adapter's ownership arc (import -> PUT intent -> sync/drift) surfaced a
SPLIT-BRAIN in how "intent" is stored:

  * PUT /intent (api/intent.py), the scheduler, and the NetBox intent binding all write
    the **InterfaceIntent** table and stamp ``attr_state.sync_state = accepted``.
  * BUT ``sync_device`` and ``detect_drift`` (core/importer.py) decide Phase 1 vs
    Phase 2 from ``attr_state.intent_value`` — a DIFFERENT column that nothing ever
    writes. So it is always ``None`` and the importer never enters Phase 2.

Consequence: the Phase-2 "in_sync vs drifted against deployed intent" logic in the
importer is effectively dead — after intent is deployed, a later sync/drift compares
the device against the cached ``netbox_value`` (Phase 1) instead of the intent.

These tests PIN that current behavior (passing) and express the intended behavior as
xfail, so the day the split-brain is fixed the xfail flips green and tells us.

See netbox-nso-plugin memory drift-netbox-vs-device-value — the plugin no longer relies
on the adapter's Phase-2 status for the interfaces tab, which is why this hasn't been
louder in the UI.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from nso_adapter.core import importer as imp
from nso_adapter.store.db import get_session
from nso_adapter.store.models import DbInterface, InterfaceAttrState, InterfaceIntent, SyncState
from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _seed(device_id, *, netbox_value, nso_value, status=SyncState.imported):
    """Seed an interface + description attr_state, return (interface_id, attr_id)."""
    async for db in get_session():
        iface = DbInterface(device_id=device_id, name="GE0/0", netbox_interface_id=100)
        db.add(iface)
        await db.flush()
        attr = InterfaceAttrState(
            interface_id=iface.id,
            attribute="description",
            netbox_value=netbox_value,
            nso_value=nso_value,
            sync_state=status,
        )
        db.add(attr)
        await db.commit()
        return iface.id, attr.id
    raise RuntimeError("no session")


async def _add_deployed_intent(iface_id, intent_value="op-desc"):
    async for db in get_session():
        db.add(InterfaceIntent(interface_id=iface_id, attribute="description", intent_value=intent_value))
        await db.commit()
        break


async def _get_attr(attr_id):
    async for db in get_session():
        return await db.get(InterfaceAttrState, attr_id)
    raise RuntimeError("no session")


async def _run_detect_drift(device_id, device_name, device_reports):
    """Run detect_drift with NSO reporting `device_reports` as the description; the
    NetBox client is forced None so the cached netbox_value is the Phase-1 baseline."""
    nso = AsyncMock()
    nso.get_interface_attributes = AsyncMock(
        return_value={
            "device-name": device_name,
            "interface": [{"interface-name": "GE0/0", "description": device_reports}],
        }
    )
    imp._nso_clients["nso-dev"] = nso
    imp._netbox_client = None
    with patch("nso_adapter.core.importer.nso_actions.compare_config", new_callable=AsyncMock):
        async for db in get_session():
            await imp.detect_drift(device_id, db)
            break


async def test_put_intent_leaves_attr_state_intent_value_unset(adapter_client):
    """CHARACTERIZATION of the split-brain: PUT /intent writes InterfaceIntent and
    stamps sync_state=accepted, but does NOT populate attr_state.intent_value."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="li-dev", netbox_device_id=7950)
    iface_id, attr_id = await _seed(device_id, netbox_value="dev-desc", nso_value="dev-desc")

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "GE0/0", "attribute": "description", "intent_value": "op-desc"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    attr = await _get_attr(attr_id)
    assert attr.sync_state == SyncState.accepted  # ownership stamped here...
    assert attr.intent_value is None  # ...but NOT here (the split-brain / dead Phase 2)

    # The intent really did land — in the OTHER table.
    async for db in get_session():
        rows = (
            (await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        break
    assert len(rows) == 1
    assert rows[0].intent_value == "op-desc"


async def test_detect_drift_stays_phase1_after_intent_is_deployed(adapter_client):
    """CHARACTERIZATION: because attr_state.intent_value is never set, detect_drift runs
    in Phase 1 even after intent is deployed and the device matches that intent.

    A correct Phase-2 check would say in_sync (device == deployed intent). Phase 1
    instead compares the device against the stale cached netbox_value and says 'changed'.
    """
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="li-dev2", netbox_device_id=7951)
    iface_id, attr_id = await _seed(
        device_id, netbox_value="dev-desc", nso_value="dev-desc", status=SyncState.accepted
    )
    await _add_deployed_intent(iface_id, "op-desc")

    await _run_detect_drift(device_id, "li-dev2", device_reports="op-desc")

    attr = await _get_attr(attr_id)
    # Phase-1 outcome (device != cached netbox_value), NOT the Phase-2 'in_sync' we'd want.
    assert attr.sync_state == SyncState.changed


@pytest.mark.xfail(reason="split-brain: importer reads attr_state.intent_value, which PUT /intent never sets")
async def test_phase2_in_sync_when_device_matches_deployed_intent(adapter_client):
    """INTENDED behavior (xfail until the intent split-brain is resolved): once intent is
    deployed and the device matches it, detect_drift should report in_sync, not changed."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="li-dev3", netbox_device_id=7952)
    iface_id, attr_id = await _seed(
        device_id, netbox_value="dev-desc", nso_value="dev-desc", status=SyncState.accepted
    )
    await _add_deployed_intent(iface_id, "op-desc")

    await _run_detect_drift(device_id, "li-dev3", device_reports="op-desc")

    attr = await _get_attr(attr_id)
    assert attr.sync_state == SyncState.in_sync
