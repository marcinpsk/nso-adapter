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
    ]
}


@pytest.mark.anyio
async def test_apply_lag_config_stores_full_snapshot(adapter_client):
    device_id = await seed_device(nso_device_name="lag-apply-ok", netbox_device_id=1110)
    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/lag-config/apply", json=_APPLY_BODY, headers=AUTH)

    assert resp.status_code == 200
    assert resp.json() == {"status": "stored", "device_id": device_id, "count": 1, "removed": 0}

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
    assert tuple(side_effect_counts) == (0, 0, 0, 0)


@pytest.mark.anyio
async def test_apply_lag_config_full_replace_reports_removed_roots(adapter_client):
    device_id = await seed_device(nso_device_name="lag-full-replace", netbox_device_id=1112)
    first = {
        "bundles": [
            {"name": "Port-channel1", "lag_id": 7},
            {"name": "Port-channel2", "lag_id": 7},
        ]
    }
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/lag-config/apply",
        json=first,
        headers=AUTH,
    )
    assert response.json() == {"status": "stored", "device_id": device_id, "count": 2, "removed": 0}

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/lag-config/apply",
        json={"bundles": [{"name": "Port-channel2", "lag_id": 7}]},
        headers=AUTH,
    )
    assert response.json() == {"status": "stored", "device_id": device_id, "count": 1, "removed": 1}

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
        json={"bundles": bundles},
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
