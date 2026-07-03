# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Lifecycle — adapter intent ownership across the importer (Phase 2).

Deployed intent has a single source of truth: the ``InterfaceIntent`` table, written by
PUT /intent, apply and the scheduler. ``sync_device``/``detect_drift`` read it (via
``_load_intent_by_attr``) to decide Phase 1 vs Phase 2.

History: there used to be a second, never-written copy — ``attr_state.intent_value`` —
which the importer read instead, so Phase 2 was dead and a device that matched its
deployed intent was reported ``changed``. That column has been dropped; these tests
guard that the importer now enters Phase 2 from ``InterfaceIntent``.
See netbox-nso-plugin memory adapter-intent-split-brain.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

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
    """Run detect_drift with NSO reporting `device_reports`; NetBox client forced None so
    the cached netbox_value is the Phase-1 baseline (proving Phase 2 ignores it)."""
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


async def test_put_intent_stamps_accepted_and_writes_intent_table(adapter_client):
    """PUT /intent records intent in InterfaceIntent and stamps attr_state accepted."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="li-dev", netbox_device_id=7950)
    iface_id, attr_id = await _seed(device_id, netbox_value="dev-desc", nso_value="dev-desc")

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "GE0/0", "attribute": "description", "intent_value": "op-desc"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    attr = await _get_attr(attr_id)
    assert attr.sync_state == SyncState.accepted

    async for db in get_session():
        rows = (
            (await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface_id))).scalars().all()
        )
        break
    assert len(rows) == 1
    assert rows[0].intent_value == "op-desc"


async def test_detect_drift_in_sync_when_device_matches_deployed_intent(adapter_client):
    """Phase 2: once intent is deployed and the device matches it, detect_drift reports
    in_sync — comparing against the deployed intent, not the cached netbox_value."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="li-dev2", netbox_device_id=7951)
    iface_id, attr_id = await _seed(device_id, netbox_value="dev-desc", nso_value="dev-desc", status=SyncState.accepted)
    await _add_deployed_intent(iface_id, "op-desc")

    await _run_detect_drift(device_id, "li-dev2", device_reports="op-desc")

    assert (await _get_attr(attr_id)).sync_state == SyncState.in_sync


async def test_detect_drift_drifted_when_device_differs_from_deployed_intent(adapter_client):
    """Phase 2: a device value that diverges from the deployed intent is drifted."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="li-dev3", netbox_device_id=7952)
    iface_id, attr_id = await _seed(device_id, netbox_value="dev-desc", nso_value="dev-desc", status=SyncState.in_sync)
    await _add_deployed_intent(iface_id, "op-desc")

    await _run_detect_drift(device_id, "li-dev3", device_reports="hand-edited-on-device")

    assert (await _get_attr(attr_id)).sync_state == SyncState.drifted
