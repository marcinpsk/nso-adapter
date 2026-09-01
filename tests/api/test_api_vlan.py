# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /devices/{id}/vlan-database and /switchport."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.conftest import VALID_TOKEN, push_seq, seed_device, seed_switchport, seed_vlan_database, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_vlan_database_returns_seeded_rows(adapter_client):
    device_id = await seed_device(nso_device_name="vlan-sw", netbox_device_id=1200)
    await seed_vlan_database(device_id, [{"vlan_id": 10, "name": "MGMT"}])
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/vlan-database", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    # read_state rides every family GET (S4); byte-level pins live in the golden test.
    assert body.pop("read_state")["outcome"] == "unavailable"
    assert body == {
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
async def test_apply_switchport_stores_full_snapshot(adapter_client):
    device_id = await seed_device(nso_device_name="sw-apply", netbox_device_id=1210)
    body = {
        "interfaces": [
            {"interface_name": "Gi0/1", "mode": "access", "untagged_vlan": 10, "tagged_vlans": []},
            {"interface_name": "Gi0/2", "mode": "trunk", "untagged_vlan": 99, "tagged_vlans": [20, 30]},
        ]
    }
    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/switchport/apply", json=body, headers=AUTH)

    assert resp.status_code == 200
    assert resp.json() == {"status": "stored", "device_id": device_id, "count": 2, "removed": 0}

    async with session() as db:
        interfaces = (
            await db.execute(
                text(
                    "SELECT id, interface_name, mode, untagged_vlan FROM switchport_intent "
                    "WHERE device_id = :device_id ORDER BY interface_name"
                ),
                {"device_id": device_id},
            )
        ).all()
        tagged_vlans = (
            await db.execute(
                text(
                    "SELECT s.interface_name, t.vlan_id FROM switchport_tagged_vlan_intent t "
                    "JOIN switchport_intent s ON s.id = t.switchport_id "
                    "WHERE s.device_id = :device_id ORDER BY s.interface_name, t.vlan_id"
                ),
                {"device_id": device_id},
            )
        ).all()
        side_effect_counts = (
            await db.execute(
                text(
                    "SELECT "
                    "(SELECT count(*) FROM device_projection_stream WHERE device_id = :device_id) AS streams, "
                    "(SELECT count(*) FROM device_generation_counter WHERE device_id = :device_id) AS counters, "
                    "(SELECT count(*) FROM intent_push_receipt WHERE device_id = :device_id) AS receipts, "
                    "(SELECT count(*) FROM jobs WHERE device_id = :device_id) AS jobs"
                ),
                {"device_id": device_id},
            )
        ).one()

    assert [(row.interface_name, row.mode, row.untagged_vlan) for row in interfaces] == [
        ("Gi0/1", "access", 10),
        ("Gi0/2", "trunk", 99),
    ]
    assert [tuple(row) for row in tagged_vlans] == [("Gi0/2", 20), ("Gi0/2", 30)]
    assert tuple(side_effect_counts) == (0, 0, 0, 0)


@pytest.mark.anyio
async def test_apply_switchport_device_not_found(adapter_client):
    resp = await adapter_client.post("/api/v1/devices/999999/switchport/apply", json={"interfaces": []}, headers=AUTH)
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_apply_switchport_requires_explicit_snapshot_without_mutating_store(adapter_client):
    device_id = await seed_device(nso_device_name="switchport-required-snapshot", netbox_device_id=None)
    stored = await adapter_client.post(
        f"/api/v1/devices/{device_id}/switchport/apply",
        json={"interfaces": [{"interface_name": "Gi0/1", "untagged_vlan": 10}]},
        headers=AUTH,
    )
    assert stored.status_code == 200

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/switchport/apply",
        json={},
        headers=AUTH,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    async with session() as db:
        names = (
            (
                await db.execute(
                    text("SELECT interface_name FROM switchport_intent WHERE device_id = :device_id"),
                    {"device_id": device_id},
                )
            )
            .scalars()
            .all()
        )
    assert names == ["Gi0/1"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "interfaces",
    [
        [{"interface_name": "Gi0/1", "untagged_vlan": True}],
        [{"interface_name": "Gi0/1", "untagged_vlan": "10"}],
        [{"interface_name": "Gi0/1", "tagged_vlans": [65536]}],
        [
            {"interface_name": "Gi0/1"},
            {"interface_name": "Gi0/1"},
        ],
        [{"interface_name": "Gi0/1", "tagged_vlans": [10, 10]}],
    ],
)
async def test_apply_switchport_rejects_invalid_graph_without_mutating_store(adapter_client, interfaces):
    device_id = await seed_device(nso_device_name="switchport-invalid-request", netbox_device_id=None)
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/switchport/apply",
        json={"interfaces": interfaces},
        headers=AUTH,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    async with session() as db:
        count = await db.scalar(
            text("SELECT count(*) FROM switchport_intent WHERE device_id = :device_id"),
            {"device_id": device_id},
        )
    assert count == 0


@pytest.mark.anyio
async def test_apply_switchport_full_replace_reports_removed_roots(adapter_client):
    device_id = await seed_device(nso_device_name="switchport-full-replace", netbox_device_id=1211)
    first = {
        "interfaces": [
            {"interface_name": "Gi0/1", "untagged_vlan": 10},
            {"interface_name": "Gi0/2", "tagged_vlans": [20, 30]},
        ]
    }
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/switchport/apply",
        json=first,
        headers=AUTH,
    )
    assert response.json() == {"status": "stored", "device_id": device_id, "count": 2, "removed": 0}

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/switchport/apply",
        json={"interfaces": [{"interface_name": "Gi0/2", "tagged_vlans": [30]}]},
        headers=AUTH,
    )
    assert response.json() == {"status": "stored", "device_id": device_id, "count": 1, "removed": 1}

    async with session() as db:
        names = (
            (
                await db.execute(
                    text("SELECT interface_name FROM switchport_intent WHERE device_id = :device_id"),
                    {"device_id": device_id},
                )
            )
            .scalars()
            .all()
        )
        tags = (
            (
                await db.execute(
                    text(
                        "SELECT vlan_id FROM switchport_tagged_vlan_intent t "
                        "JOIN switchport_intent s ON s.id = t.switchport_id "
                        "WHERE s.device_id = :device_id"
                    ),
                    {"device_id": device_id},
                )
            )
            .scalars()
            .all()
        )
    assert names == ["Gi0/2"]
    assert tags == [30]


async def _count_vlan_intent(device_id: int) -> int:
    from sqlalchemy import select

    from nso_adapter.store.models import VlanIntent

    async with session() as db:
        rows = (await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))).scalars().all()
        return len(rows)
    return 0


@pytest.mark.anyio
async def test_put_vlan_intent_stores_and_full_replaces(adapter_client):
    device_id = await seed_device(nso_device_name="vlan-wp", netbox_device_id=1250)
    body = {"vlans": [{"vlan_id": 10, "name": "MGMT"}, {"vlan_id": 2213, "name": "RENAMED"}]}
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/vlan-intent", json=body, headers=AUTH | push_seq())
    assert resp.status_code == 200 and resp.json()["count"] == 2
    assert await _count_vlan_intent(device_id) == 2
    # full-replace: one VLAN → the other is deleted
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent",
        json={"vlans": [{"vlan_id": 2213, "name": "RENAMED"}]},
        headers=AUTH | push_seq(),
    )
    assert resp.json()["count"] == 1
    assert await _count_vlan_intent(device_id) == 1


@pytest.mark.anyio
async def test_put_vlan_intent_unknown_device_404(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/999999/vlan-intent", json={"vlans": []}, headers=AUTH | push_seq())
    assert resp.status_code == 404
