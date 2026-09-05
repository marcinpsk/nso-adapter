# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for the /api/v1/devices/{id}/lag-config endpoints."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from nso_adapter.store.models import LagBundleConfig
from tests.conftest import VALID_TOKEN, seed_device, seed_lag_config, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_lag_config_empty_returns_never(adapter_client):
    device_id = await seed_device(nso_device_name="lag-config-empty", netbox_device_id=1100)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "never"
    assert body["last_refreshed_at"] is None
    assert body["bundles"] == []


@pytest.mark.anyio
async def test_lag_config_requires_auth(adapter_client):
    device_id = await seed_device(nso_device_name="lag-config-noauth", netbox_device_id=1101)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_lag_config_device_not_found(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/99999/lag-config", headers=AUTH)
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_lag_config_bundle_with_members(adapter_client):
    device_id = await seed_device(nso_device_name="lag-config-full", netbox_device_id=1102)
    await seed_lag_config(
        device_id,
        bundles=[
            {
                "name": "Port-channel1",
                "lag_id": 1,
                "min_links": 2,
                "system_priority": 100,
                "timer": "fast",
                "members": [
                    {"interface_name": "GigabitEthernet0/1", "mode": "active", "port_priority": 200},
                    {"interface_name": "GigabitEthernet0/2", "mode": "active"},
                ],
            }
        ],
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["refresh_source"] == "test"
    assert len(body["bundles"]) == 1

    bundle = body["bundles"][0]
    assert bundle["name"] == "Port-channel1"
    assert bundle["lag_id"] == 1
    assert bundle["min_links"] == 2
    assert bundle["system_priority"] == 100
    assert bundle["timer"] == "fast"
    assert len(bundle["members"]) == 2
    m1 = next(m for m in bundle["members"] if m["interface_name"] == "GigabitEthernet0/1")
    assert m1["mode"] == "active"
    assert m1["port_priority"] == 200
    m2 = next(m for m in bundle["members"] if m["interface_name"] == "GigabitEthernet0/2")
    assert m2.get("port_priority") is None


@pytest.mark.anyio
async def test_lag_config_selects_latest_when_a_bundle_has_no_refresh_time(adapter_client):
    device_id = await seed_device(nso_device_name="lag-config-mixed-time", netbox_device_id=1103)
    async with session() as db:
        db.add_all(
            [
                LagBundleConfig(
                    device_id=device_id,
                    name="Port-channel1",
                    lag_id=1,
                    last_refreshed_at=None,
                    refresh_source="never",
                ),
                LagBundleConfig(
                    device_id=device_id,
                    name="Port-channel2",
                    lag_id=2,
                    last_refreshed_at=datetime(2026, 1, 2, tzinfo=UTC),
                    refresh_source="poll",
                ),
            ]
        )
        await db.commit()

    response = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config", headers=AUTH)

    assert response.status_code == 200
    assert response.json()["refresh_source"] == "poll"


_APPLY_BODY = {
    "bundles": [
        {
            "name": "Port-channel1",
            "lag_id": 1,
            "min_links": 2,
            "system_priority": 100,
            "timer": "fast",
            "members": [
                {"interface_name": "GigabitEthernet0/1", "mode": "active", "port_priority": 200},
                {"interface_name": "GigabitEthernet0/2", "mode": "active"},
            ],
        }
    ],
    "deleted_roots": [],
}


@pytest.mark.anyio
async def test_apply_lag_config_stores_full_snapshot(adapter_client):
    device_id = await seed_device(nso_device_name="lag-apply-ok", netbox_device_id=1110)
    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/lag-config/apply", json=_APPLY_BODY, headers=AUTH)

    assert resp.status_code == 200
    assert resp.json() == {
        "status": "prepared",
        "device_id": device_id,
        "stream": "lag",
        "count": 1,
        "removed": 0,
        "desired_revision": 1,
        "selection_revision": 1,
    }

    async with session() as db:
        bundle = (
            await db.execute(
                text(
                    "SELECT name, lag_id, min_links, system_priority, timer "
                    "FROM lag_bundle_intent WHERE device_id = :device_id"
                ),
                {"device_id": device_id},
            )
        ).one()
        members = (
            await db.execute(
                text(
                    "SELECT m.interface_name, m.mode, m.port_priority "
                    "FROM lag_member_intent m "
                    "JOIN lag_bundle_intent b ON b.id = m.lag_bundle_id "
                    "WHERE b.device_id = :device_id ORDER BY m.interface_name"
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

    assert tuple(bundle) == ("Port-channel1", 1, 2, 100, "fast")
    assert [tuple(member) for member in members] == [
        ("GigabitEthernet0/1", "active", 200),
        ("GigabitEthernet0/2", "active", None),
    ]
    # One projection revision and its counter; still no receipt and no device job.
    assert tuple(side_effect_counts) == (1, 1, 0, 0)


@pytest.mark.anyio
async def test_apply_lag_config_full_replace_reports_removed_roots(adapter_client):
    device_id = await seed_device(nso_device_name="lag-full-replace", netbox_device_id=1112)
    first = {
        "bundles": [
            {"name": "Port-channel1", "lag_id": 7},
            {"name": "Port-channel2", "lag_id": 7},
        ],
        "deleted_roots": [],
    }
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/lag-config/apply",
        json=first,
        headers=AUTH,
    )
    assert response.json()["count"] == 2
    assert response.json()["removed"] == 0

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/lag-config/apply",
        json={"bundles": [{"name": "Port-channel2", "lag_id": 7}], "deleted_roots": []},
        headers=AUTH,
    )
    assert response.json()["count"] == 1
    assert response.json()["removed"] == 1

    async with session() as db:
        names = (
            (
                await db.execute(
                    text("SELECT name FROM lag_bundle_intent WHERE device_id = :device_id"),
                    {"device_id": device_id},
                )
            )
            .scalars()
            .all()
        )
    assert names == ["Port-channel2"]


@pytest.mark.anyio
async def test_apply_lag_config_device_not_found(adapter_client):
    resp = await adapter_client.post("/api/v1/devices/99999/lag-config/apply", json=_APPLY_BODY, headers=AUTH)
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_apply_lag_config_requires_auth(adapter_client):
    device_id = await seed_device(nso_device_name="lag-apply-noauth", netbox_device_id=1111)
    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/lag-config/apply", json=_APPLY_BODY)
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_apply_lag_config_requires_explicit_snapshot_without_mutating_store(adapter_client):
    device_id = await seed_device(nso_device_name="lag-required-snapshot", netbox_device_id=None)
    stored = await adapter_client.post(
        f"/api/v1/devices/{device_id}/lag-config/apply",
        json={"bundles": [{"name": "Port-channel1", "lag_id": 1}], "deleted_roots": []},
        headers=AUTH,
    )
    assert stored.status_code == 200

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/lag-config/apply",
        json={},
        headers=AUTH,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    async with session() as db:
        names = (
            (
                await db.execute(
                    text("SELECT name FROM lag_bundle_intent WHERE device_id = :device_id"),
                    {"device_id": device_id},
                )
            )
            .scalars()
            .all()
        )
    assert names == ["Port-channel1"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "bundles",
    [
        [{"name": "Port-channel1"}],
        [{"name": "Port-channel1", "lag_id": True}],
        [{"name": "Port-channel1", "lag_id": "1"}],
        [{"name": "Port-channel1", "lag_id": 4294967296}],
        [
            {"name": "Port-channel1", "lag_id": 1},
            {"name": "Port-channel1", "lag_id": 2},
        ],
        [
            {
                "name": "Port-channel1",
                "lag_id": 1,
                "members": [
                    {"interface_name": "Gi0/1"},
                    {"interface_name": "Gi0/1"},
                ],
            }
        ],
    ],
)
async def test_apply_lag_config_rejects_invalid_graph_without_mutating_store(adapter_client, bundles):
    device_id = await seed_device(nso_device_name="lag-invalid-request", netbox_device_id=None)
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/lag-config/apply",
        json={"bundles": bundles, "deleted_roots": []},
        headers=AUTH,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    async with session() as db:
        count = await db.scalar(
            text("SELECT count(*) FROM lag_bundle_intent WHERE device_id = :device_id"),
            {"device_id": device_id},
        )
    assert count == 0


# ── #1612: the POST prepares, Apply authorizes ────────────────────────────────

_PREPARE_A = {"bundles": [{"name": "Port-channel1", "lag_id": 1}], "deleted_roots": []}
_PREPARE_B = {"bundles": [{"name": "Port-channel2", "lag_id": 2}], "deleted_roots": []}


async def _post_lag(client, device_id: int, body: dict, *, query: str = ""):
    return await client.post(f"/api/v1/devices/{device_id}/lag-config/apply{query}", json=body, headers=AUTH)


async def _stream_row(device_id: int, stream: str = "lag"):
    from sqlalchemy import select

    from nso_adapter.store.models import DeviceProjectionStream

    async with session() as db:
        return await db.scalar(
            select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == stream,
            )
        )


@pytest.mark.anyio
async def test_apply_lag_config_prepares_a_selectable_snapshot(adapter_client):
    device_id = await seed_device(nso_device_name="lag-prepared", netbox_device_id=1620)

    response = await _post_lag(adapter_client, device_id, _PREPARE_A)

    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "prepared",
        "device_id": device_id,
        "stream": "lag",
        "count": 1,
        "removed": 0,
        "desired_revision": 1,
        "selection_revision": 1,
    }
    row = await _stream_row(device_id)
    assert (row.desired_revision, row.authorized_revision, row.applied_revision) == (1, 0, 0)
    assert row.source_push_seq is None
    assert row.authorized_document is None
    assert row.prepared_revision == 1
    assert set(row.prepared_tables) == {"lag_bundle_intent", "lag_member_intent"}
    assert "_execution" not in row.prepared_tables, "the slot holds tables only; freezing happens at Apply"
    assert [bundle["name"] for bundle in row.prepared_tables["lag_bundle_intent"]] == ["Port-channel1"]
    assert row.prepared_deletions == {"delete_origin": {}, "detach": {}, "owned_content": {}}

    async with session() as db:
        counts = (
            await db.execute(
                text(
                    "SELECT "
                    "(SELECT count(*) FROM intent_push_receipt WHERE device_id = :device_id) AS receipts, "
                    "(SELECT count(*) FROM jobs WHERE device_id = :device_id) AS jobs, "
                    "(SELECT count(*) FROM deployment_generation WHERE device_id = :device_id) AS generations"
                ),
                {"device_id": device_id},
            )
        ).one()
    assert tuple(counts) == (0, 0, 0)


@pytest.mark.anyio
async def test_apply_lag_config_store_only_bumps_the_revision_and_preserves_the_slot(adapter_client):
    device_id = await seed_device(nso_device_name="lag-store-only", netbox_device_id=1621)
    prepared = await _post_lag(adapter_client, device_id, _PREPARE_A)
    assert prepared.json()["selection_revision"] == 1

    response = await _post_lag(adapter_client, device_id, _PREPARE_B, query="?store_only=true")

    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "stored",
        "device_id": device_id,
        "stream": "lag",
        "count": 1,
        "removed": 1,
        "desired_revision": 2,
        "selection_revision": None,
    }
    row = await _stream_row(device_id)
    assert row.desired_revision == 2
    assert row.prepared_revision == 1, "a store-only replacement never replaces the prepared slot"
    assert [bundle["name"] for bundle in row.prepared_tables["lag_bundle_intent"]] == ["Port-channel1"]
    async with session() as db:
        live = (
            (
                await db.execute(
                    text("SELECT name FROM lag_bundle_intent WHERE device_id = :device_id"),
                    {"device_id": device_id},
                )
            )
            .scalars()
            .all()
        )
    assert live == ["Port-channel2"], "the live rows are the store-only replacement"


@pytest.mark.anyio
@pytest.mark.parametrize("query", ["?delete_origin=true", "?backfill_only=true"])
async def test_apply_lag_config_refuses_the_request_modes_it_does_not_implement(adapter_client, query):
    device_id = await seed_device(nso_device_name=f"lag-mode{query[1:9]}", netbox_device_id=None)

    response = await _post_lag(adapter_client, device_id, _PREPARE_A, query=query)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert await _stream_row(device_id) is None
    async with session() as db:
        assert (
            await db.scalar(
                text("SELECT count(*) FROM lag_bundle_intent WHERE device_id = :device_id"),
                {"device_id": device_id},
            )
            == 0
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("deleted_roots", "reason"),
    [
        pytest.param(["Port-channel2", "Port-channel2"], "repeats", id="duplicate"),
        pytest.param(["Port-channel1"], "still present", id="still-present"),
        pytest.param(["Port-channel9"], "not authorized", id="unauthorized"),
    ],
)
async def test_apply_lag_config_refuses_an_invalid_deletion_authority(adapter_client, deleted_roots, reason):
    device_id = await seed_device(nso_device_name=f"lag-roots-{reason.split()[0]}", netbox_device_id=None)
    assert (await _post_lag(adapter_client, device_id, _PREPARE_A)).status_code == 200

    response = await _post_lag(
        adapter_client,
        device_id,
        {"bundles": _PREPARE_A["bundles"], "deleted_roots": deleted_roots},
    )

    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "validation_error"
    assert reason in response.json()["error"]["message"]
    row = await _stream_row(device_id)
    assert (row.desired_revision, row.prepared_revision) == (1, 1), "a refusal leaves every revision untouched"


@pytest.mark.anyio
async def test_apply_lag_config_store_only_accepts_a_deletion_authority_and_records_none(adapter_client):
    """A store-only replacement replaces the rows and the revision, and prepares nothing."""
    from sqlalchemy import update

    from nso_adapter.store.models import DeviceProjectionStream

    device_id = await seed_device(nso_device_name="lag-store-only-roots", netbox_device_id=None)
    prepared = await _post_lag(adapter_client, device_id, _PREPARE_A)
    assert prepared.status_code == 200, prepared.text
    async with session() as db:
        # The state an Apply promotion leaves behind, so Port-channel1 is AUTHORIZED.
        authorized = (await _stream_row(device_id)).prepared_tables
        await db.execute(
            update(DeviceProjectionStream)
            .where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "lag",
            )
            .values(authorized_document=authorized, authorized_revision=1)
        )
        await db.commit()

    response = await _post_lag(
        adapter_client,
        device_id,
        {"bundles": [], "deleted_roots": ["Port-channel1"]},
        query="?store_only=true",
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "stored",
        "device_id": device_id,
        "stream": "lag",
        "count": 0,
        "removed": 1,
        "desired_revision": 2,
        "selection_revision": None,
    }
    row = await _stream_row(device_id)
    assert (row.desired_revision, row.authorized_revision, row.prepared_revision) == (2, 1, 1)
    assert row.authorized_document == authorized, "a store-only replacement authorizes nothing"
    assert row.prepared_deletions == {"delete_origin": {}, "detach": {}, "owned_content": {}}
    async with session() as db:
        assert (
            await db.scalar(
                text("SELECT count(*) FROM lag_bundle_intent WHERE device_id = :device_id"),
                {"device_id": device_id},
            )
            == 0
        )


@pytest.mark.anyio
async def test_apply_lag_config_store_only_still_validates_the_deletion_authority(adapter_client):
    device_id = await seed_device(nso_device_name="lag-store-only-invalid-roots", netbox_device_id=None)

    response = await _post_lag(
        adapter_client,
        device_id,
        {"bundles": [], "deleted_roots": ["Port-channel1"]},
        query="?store_only=true",
    )

    assert response.status_code == 422
    assert "not authorized" in response.json()["error"]["message"]
    assert await _stream_row(device_id) is None


@pytest.mark.anyio
async def test_apply_lag_config_requires_an_explicit_deletion_authority(adapter_client):
    device_id = await seed_device(nso_device_name="lag-roots-required", netbox_device_id=None)

    response = await _post_lag(adapter_client, device_id, {"bundles": []})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert await _stream_row(device_id) is None
