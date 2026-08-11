# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/subinterface + PUT /subinterface-intent."""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, push_seq, seed_device, seed_subinterface, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_subinterface_returns_seeded_rows(adapter_client):
    device_id = await seed_device()
    await seed_subinterface(
        device_id,
        [
            {
                "interface_name": "GigabitEthernet0/1.100",
                "parent_interface": "GigabitEthernet0/1",
                "dot1q_vlan": 100,
                "type": "subinterface",
                "vrf": "TENANT_A",
            },
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/subinterface", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    # read_state rides every family GET (S4); byte-level pins live in the golden test.
    assert body.pop("read_state")["outcome"] == "unavailable"
    assert body == {
        "device_id": device_id,
        "interfaces": [
            {
                "interface_name": "GigabitEthernet0/1.100",
                "parent_interface": "GigabitEthernet0/1",
                "dot1q_vlan": 100,
                "type": "subinterface",
                "vrf": "TENANT_A",
                "source": "subinterface",
            }
        ],
    }


@pytest.mark.anyio
async def test_subinterface_ordered_by_name(adapter_client):
    device_id = await seed_device()
    await seed_subinterface(
        device_id,
        [
            {"interface_name": "ge-0/0/0.200", "dot1q_vlan": 200},
            {"interface_name": "ge-0/0/0.100", "dot1q_vlan": 100},
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/subinterface", headers=AUTH)
    names = [i["interface_name"] for i in resp.json()["interfaces"]]
    assert names == ["ge-0/0/0.100", "ge-0/0/0.200"]


@pytest.mark.anyio
async def test_subinterface_unknown_device_is_404(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/999999/subinterface", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_subinterface_requires_auth(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/1/subinterface")
    assert resp.status_code in (401, 403)


async def _count_subif_intent(device_id: int) -> int:
    from sqlalchemy import select

    from nso_adapter.store.models import SubinterfaceIntent

    async with session() as db:
        rows = (
            (await db.execute(select(SubinterfaceIntent).where(SubinterfaceIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return len(rows)
    return 0


@pytest.mark.anyio
async def test_put_subinterface_intent_stores_and_full_replaces(adapter_client):
    device_id = await seed_device()
    body = {
        "interfaces": [
            {
                "interface_name": "GigabitEthernet0/1.100",
                "parent_interface": "GigabitEthernet0/1",
                "dot1q_vlan": 100,
                "type": "subinterface",
                "vrf": "MTI",
            },
            {
                "interface_name": "ge-0/0/0.200",
                "parent_interface": "ge-0/0/0",
                "dot1q_vlan": 200,
                "type": "subinterface",
            },
        ]
    }
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/subinterface-intent", json=body, headers=AUTH | push_seq()
    )
    assert resp.status_code == 200 and resp.json()["count"] == 2
    assert await _count_subif_intent(device_id) == 2
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/subinterface-intent",
        json={
            "interfaces": [
                {
                    "interface_name": "ge-0/0/0.200",
                    "parent_interface": "ge-0/0/0",
                    "dot1q_vlan": 200,
                    "type": "subinterface",
                }
            ]
        },
        headers=AUTH | push_seq(),
    )
    assert resp.json()["count"] == 1
    assert await _count_subif_intent(device_id) == 1


@pytest.mark.anyio
async def test_put_subinterface_intent_unknown_device_404(adapter_client):
    resp = await adapter_client.put(
        "/api/v1/devices/999999/subinterface-intent", json={"interfaces": []}, headers=AUTH | push_seq()
    )
    assert resp.status_code == 404
