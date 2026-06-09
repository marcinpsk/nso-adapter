# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M35: GET /api/v1/devices/{id}/svi."""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device, seed_svi

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_svi_returns_seeded_rows(adapter_client):
    device_id = await seed_device()
    await seed_svi(device_id, [{"interface_name": "Vlan100", "vlan_id": 100, "type": "svi", "vrf": "MGMT"}])
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/svi", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == {
        "device_id": device_id,
        "interfaces": [
            {"interface_name": "Vlan100", "vlan_id": 100, "type": "svi", "vrf": "MGMT", "source": "svi"}
        ],
    }


@pytest.mark.anyio
async def test_svi_ordered_by_vlan_id(adapter_client):
    device_id = await seed_device()
    await seed_svi(device_id, [
        {"interface_name": "Vlan200", "vlan_id": 200, "type": "svi"},
        {"interface_name": "Vlan100", "vlan_id": 100, "type": "svi"},
    ])
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/svi", headers=AUTH)
    vids = [i["vlan_id"] for i in resp.json()["interfaces"]]
    assert vids == [100, 200]


@pytest.mark.anyio
async def test_svi_unknown_device_is_404(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/999999/svi", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_svi_requires_auth(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/1/svi")
    assert resp.status_code in (401, 403)
