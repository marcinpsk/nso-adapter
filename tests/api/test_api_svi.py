# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/svi."""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, push_seq, seed_device, seed_svi, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_svi_returns_seeded_rows(adapter_client):
    device_id = await seed_device()
    await seed_svi(device_id, [{"interface_name": "Vlan100", "vlan_id": 100, "type": "svi", "vrf": "MGMT"}])
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/svi", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    # read_state rides every family GET (S4); byte-level pins live in the golden test.
    assert body.pop("read_state")["outcome"] == "unavailable"
    assert body == {
        "device_id": device_id,
        "interfaces": [{"interface_name": "Vlan100", "vlan_id": 100, "type": "svi", "vrf": "MGMT", "source": "svi"}],
    }


@pytest.mark.anyio
async def test_svi_ordered_by_vlan_id(adapter_client):
    device_id = await seed_device()
    await seed_svi(
        device_id,
        [
            {"interface_name": "Vlan200", "vlan_id": 200, "type": "svi"},
            {"interface_name": "Vlan100", "vlan_id": 100, "type": "svi"},
        ],
    )
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


async def _count_svi_intent(device_id: int) -> int:
    from sqlalchemy import select

    from nso_adapter.store.models import SviIntent

    async with session() as db:
        rows = (await db.execute(select(SviIntent).where(SviIntent.device_id == device_id))).scalars().all()
        return len(rows)
    return 0


@pytest.mark.anyio
async def test_put_svi_intent_stores_and_full_replaces(adapter_client):
    device_id = await seed_device()
    body = {
        "interfaces": [
            {"interface_name": "Vlan100", "vlan_id": 100, "type": "svi", "vrf": "MGMT"},
            {"interface_name": "Vlan200", "vlan_id": 200, "type": "svi"},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/svi-intent", json=body, headers=AUTH | push_seq())
    assert resp.status_code == 200 and resp.json()["count"] == 2
    assert await _count_svi_intent(device_id) == 2
    # full-replace: one interface → the other is deleted
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/svi-intent",
        json={"interfaces": [{"interface_name": "Vlan200", "vlan_id": 200, "type": "svi"}]},
        headers=AUTH | push_seq(),
    )
    assert resp.json()["count"] == 1
    assert await _count_svi_intent(device_id) == 1


@pytest.mark.anyio
async def test_put_svi_intent_unknown_device_404(adapter_client):
    resp = await adapter_client.put(
        "/api/v1/devices/999999/svi-intent", json={"interfaces": []}, headers=AUTH | push_seq()
    )
    assert resp.status_code == 404


@pytest.mark.anyio
@pytest.mark.parametrize("vlan_id", [None, 0, -1, 4095, True, "7"])
async def test_put_svi_intent_refuses_invalid_vlan_id_before_replacement(adapter_client, vlan_id):
    device_id = await seed_device()
    path = f"/api/v1/devices/{device_id}/svi-intent"
    valid = {"interfaces": [{"interface_name": "irb.0", "vlan_id": 1}]}
    response = await adapter_client.put(path, json=valid, headers=AUTH | push_seq())
    assert response.status_code == 200

    invalid = {"interfaces": [{"interface_name": "irb.7"}]}
    if vlan_id is not None:
        invalid["interfaces"][0]["vlan_id"] = vlan_id
    response = await adapter_client.put(path, json=invalid, headers=AUTH | push_seq())
    assert response.status_code == 422
    from sqlalchemy import select

    from nso_adapter.store.models import SviIntent

    async with session() as db:
        rows = (await db.execute(select(SviIntent).where(SviIntent.device_id == device_id))).scalars().all()
        assert [(row.interface_name, row.vlan_id) for row in rows] == [("irb.0", 1)]
