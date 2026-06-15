# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M34: GET /devices/{id}/vlan-database and /switchport."""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device, seed_switchport, seed_vlan_database

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_vlan_database_returns_seeded_rows(adapter_client):
    device_id = await seed_device(nso_device_name="vlan-sw", netbox_device_id=1200)
    await seed_vlan_database(device_id, [{"vlan_id": 10, "name": "MGMT"}])
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/vlan-database", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == {
        "device_id": device_id,
        "vlans": [{"vlan_id": 10, "name": "MGMT", "source": "vlan-database"}],
    }


@pytest.mark.anyio
async def test_switchport_returns_seeded_rows(adapter_client):
    device_id = await seed_device(nso_device_name="vlan-sw2", netbox_device_id=1201)
    await seed_switchport(
        device_id,
        [
            {"interface_name": "GigabitEthernet0/1", "mode": "access", "untagged_vlan": 10, "tagged_vlans": []},
            {"interface_name": "GigabitEthernet0/2", "mode": "trunk", "untagged_vlan": 99, "tagged_vlans": [20, 30]},
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/switchport", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    by_name = {i["interface_name"]: i for i in body["interfaces"]}
    assert by_name["GigabitEthernet0/1"] == {
        "interface_name": "GigabitEthernet0/1",
        "mode": "access",
        "untagged_vlan": 10,
        "tagged_vlans": [],
        "source": "switchport",
    }
    assert by_name["GigabitEthernet0/2"]["untagged_vlan"] == 99
    assert by_name["GigabitEthernet0/2"]["tagged_vlans"] == [20, 30]


@pytest.mark.anyio
async def test_vlan_database_unknown_device_is_404(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/999999/vlan-database", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_vlan_database_requires_auth(adapter_client):
    device_id = await seed_device(nso_device_name="vlan-noauth", netbox_device_id=1202)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/vlan-database")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_apply_switchport_builds_payload(adapter_client):
    from unittest.mock import AsyncMock, patch

    device_id = await seed_device(nso_device_name="sw-apply", netbox_device_id=1210)
    nso_write = AsyncMock()
    body = {
        "interfaces": [
            {"interface_name": "Gi0/1", "mode": "access", "untagged_vlan": 10, "tagged_vlans": []},
            {"interface_name": "Gi0/2", "mode": "trunk", "untagged_vlan": 99, "tagged_vlans": [20, 30]},
        ]
    }
    with (
        patch("nso_adapter.api.vlan.get_nso_client", return_value=AsyncMock()),
        patch("nso_adapter.core.switchport_intent._nso_apply_switchport_config", nso_write),
    ):
        resp = await adapter_client.post(f"/api/v1/devices/{device_id}/switchport/apply", json=body, headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["status"] == "deployed"
    _c, dev_name, ifaces = nso_write.await_args.args
    assert dev_name == "sw-apply"
    by = {i["interface-name"]: i for i in ifaces}
    assert by["Gi0/2"]["tagged-vlan"] == [20, 30]


@pytest.mark.anyio
async def test_apply_switchport_device_not_found(adapter_client):
    resp = await adapter_client.post("/api/v1/devices/999999/switchport/apply", json={"interfaces": []}, headers=AUTH)
    assert resp.status_code == 404


async def _count_vlan_intent(device_id: int) -> int:
    from sqlalchemy import select

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import VlanIntent

    async for db in get_session():
        rows = (await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))).scalars().all()
        return len(rows)
    return 0


@pytest.mark.anyio
async def test_put_vlan_intent_stores_and_full_replaces(adapter_client):
    device_id = await seed_device(nso_device_name="vlan-wp", netbox_device_id=1250)
    body = {"vlans": [{"vlan_id": 10, "name": "MGMT"}, {"vlan_id": 2213, "name": "RENAMED"}]}
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/vlan-intent", json=body, headers=AUTH)
    assert resp.status_code == 200 and resp.json()["count"] == 2
    assert await _count_vlan_intent(device_id) == 2
    # full-replace: one VLAN → the other is deleted
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent",
        json={"vlans": [{"vlan_id": 2213, "name": "RENAMED"}]},
        headers=AUTH,
    )
    assert resp.json()["count"] == 1
    assert await _count_vlan_intent(device_id) == 1


@pytest.mark.anyio
async def test_put_vlan_intent_unknown_device_404(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/999999/vlan-intent", json={"vlans": []}, headers=AUTH)
    assert resp.status_code == 404
