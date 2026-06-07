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
    await seed_switchport(device_id, [
        {"interface_name": "GigabitEthernet0/1", "mode": "access", "untagged_vlan": 10, "tagged_vlans": []},
        {"interface_name": "GigabitEthernet0/2", "mode": "trunk", "untagged_vlan": 99, "tagged_vlans": [20, 30]},
    ])
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/switchport", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    by_name = {i["interface_name"]: i for i in body["interfaces"]}
    assert by_name["GigabitEthernet0/1"] == {
        "interface_name": "GigabitEthernet0/1", "mode": "access", "untagged_vlan": 10,
        "tagged_vlans": [], "source": "switchport",
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
