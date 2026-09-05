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
        ],
        "deleted_roots": [],
    }
    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/switchport/apply", json=body, headers=AUTH)

    assert resp.status_code == 200
    assert resp.json() == {
        "status": "prepared",
        "device_id": device_id,
        "stream": "switchport",
        "count": 2,
        "removed": 0,
        "desired_revision": 1,
        "selection_revision": 1,
    }

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
    # One projection revision and its counter; still no receipt and no device job.
    assert tuple(side_effect_counts) == (1, 1, 0, 0)


@pytest.mark.anyio
async def test_apply_switchport_device_not_found(adapter_client):
    resp = await adapter_client.post(
        "/api/v1/devices/999999/switchport/apply",
        json={"interfaces": [], "deleted_roots": []},
        headers=AUTH,
    )
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_apply_switchport_requires_explicit_snapshot_without_mutating_store(adapter_client):
    device_id = await seed_device(nso_device_name="switchport-required-snapshot", netbox_device_id=None)
    stored = await adapter_client.post(
        f"/api/v1/devices/{device_id}/switchport/apply",
        json={"interfaces": [{"interface_name": "Gi0/1", "untagged_vlan": 10}], "deleted_roots": []},
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
        json={"interfaces": interfaces, "deleted_roots": []},
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
        ],
        "deleted_roots": [],
    }
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/switchport/apply",
        json=first,
        headers=AUTH,
    )
    assert (response.json()["count"], response.json()["removed"]) == (2, 0)

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/switchport/apply",
        json={"interfaces": [{"interface_name": "Gi0/2", "tagged_vlans": [30]}], "deleted_roots": []},
        headers=AUTH,
    )
    assert (response.json()["count"], response.json()["removed"]) == (1, 1)

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


# ── #1612: the POST prepares, Apply authorizes ────────────────────────────────

_SWITCHPORT_A = {
    "interfaces": [{"interface_name": "Gi0/1", "mode": "access", "untagged_vlan": 10}],
    "deleted_roots": [],
}
_SWITCHPORT_B = {"interfaces": [{"interface_name": "Gi0/2", "tagged_vlans": [20]}], "deleted_roots": []}


async def _post_switchport(client, device_id: int, body: dict, *, query: str = ""):
    return await client.post(f"/api/v1/devices/{device_id}/switchport/apply{query}", json=body, headers=AUTH)


async def _switchport_stream_row(device_id: int):
    from sqlalchemy import select

    from nso_adapter.store.models import DeviceProjectionStream

    async with session() as db:
        return await db.scalar(
            select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "switchport",
            )
        )


@pytest.mark.anyio
async def test_apply_switchport_prepares_a_selectable_snapshot(adapter_client):
    device_id = await seed_device(nso_device_name="switchport-prepared", netbox_device_id=1622)

    response = await _post_switchport(adapter_client, device_id, _SWITCHPORT_A)

    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "prepared",
        "device_id": device_id,
        "stream": "switchport",
        "count": 1,
        "removed": 0,
        "desired_revision": 1,
        "selection_revision": 1,
    }
    row = await _switchport_stream_row(device_id)
    assert (row.desired_revision, row.authorized_revision, row.prepared_revision) == (1, 0, 1)
    assert row.source_push_seq is None
    assert set(row.prepared_tables) == {"switchport_intent", "switchport_tagged_vlan_intent"}
    assert "_execution" not in row.prepared_tables
    assert row.prepared_deletions == {"delete_origin": {}, "detach": {}, "owned_content": {}}


@pytest.mark.anyio
async def test_apply_switchport_store_only_preserves_the_prepared_slot(adapter_client):
    device_id = await seed_device(nso_device_name="switchport-store-only", netbox_device_id=1623)
    assert (await _post_switchport(adapter_client, device_id, _SWITCHPORT_A)).status_code == 200

    response = await _post_switchport(adapter_client, device_id, _SWITCHPORT_B, query="?store_only=true")

    assert response.json() == {
        "status": "stored",
        "device_id": device_id,
        "stream": "switchport",
        "count": 1,
        "removed": 1,
        "desired_revision": 2,
        "selection_revision": None,
    }
    row = await _switchport_stream_row(device_id)
    assert row.prepared_revision == 1
    assert [item["interface_name"] for item in row.prepared_tables["switchport_intent"]] == ["Gi0/1"]


@pytest.mark.anyio
@pytest.mark.parametrize("query", ["?delete_origin=true", "?backfill_only=true"])
async def test_apply_switchport_refuses_the_request_modes_it_does_not_implement(adapter_client, query):
    device_id = await seed_device(nso_device_name=f"switchport-mode{query[1:9]}", netbox_device_id=None)

    response = await _post_switchport(adapter_client, device_id, _SWITCHPORT_A, query=query)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert await _switchport_stream_row(device_id) is None


@pytest.mark.anyio
async def test_apply_switchport_store_only_accepts_a_deletion_authority_and_records_none(adapter_client):
    """A store-only replacement replaces the rows and the revision, and prepares nothing."""
    from sqlalchemy import update

    from nso_adapter.store.models import DeviceProjectionStream

    device_id = await seed_device(nso_device_name="switchport-store-only-roots", netbox_device_id=None)
    assert (await _post_switchport(adapter_client, device_id, _SWITCHPORT_A)).status_code == 200
    async with session() as db:
        authorized = (await _switchport_stream_row(device_id)).prepared_tables
        await db.execute(
            update(DeviceProjectionStream)
            .where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "switchport",
            )
            .values(authorized_document=authorized, authorized_revision=1)
        )
        await db.commit()

    response = await _post_switchport(
        adapter_client,
        device_id,
        {"interfaces": [], "deleted_roots": ["Gi0/1"]},
        query="?store_only=true",
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "stored",
        "device_id": device_id,
        "stream": "switchport",
        "count": 0,
        "removed": 1,
        "desired_revision": 2,
        "selection_revision": None,
    }
    row = await _switchport_stream_row(device_id)
    assert (row.desired_revision, row.authorized_revision, row.prepared_revision) == (2, 1, 1)
    assert row.authorized_document == authorized
    async with session() as db:
        assert (
            await db.scalar(
                text("SELECT count(*) FROM switchport_intent WHERE device_id = :device_id"),
                {"device_id": device_id},
            )
            == 0
        )


@pytest.mark.anyio
async def test_apply_switchport_requires_an_explicit_deletion_authority(adapter_client):
    device_id = await seed_device(nso_device_name="switchport-roots-required", netbox_device_id=None)

    response = await _post_switchport(adapter_client, device_id, {"interfaces": []})

    assert response.status_code == 422
    assert await _switchport_stream_row(device_id) is None
