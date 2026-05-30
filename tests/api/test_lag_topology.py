# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/lag-topology."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
TS = datetime(2026, 5, 27, 9, 41, 12, 221000, tzinfo=UTC)


async def _seed_lag(
    device_id: int,
    *,
    name: str = "Port-channel10",
    lag_id: int = 10,
    refresh_source: str = "notification",
    last_refreshed_at: datetime = TS,
    members: list[tuple[str, str]] | None = None,
) -> None:
    """Seed a LagInterface + LagMember rows for a device."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import LagInterface, LagMember

    async for db in get_session():
        lag = LagInterface(
            device_id=device_id,
            name=name,
            lag_id=lag_id,
            last_refreshed_at=last_refreshed_at,
            refresh_source=refresh_source,
        )
        db.add(lag)
        await db.flush()
        for iface_name, mode in (members or []):
            db.add(LagMember(lag_interface_id=lag.id, interface_name=iface_name, mode=mode))
        await db.commit()
        break


# ── GET /api/v1/devices/{id}/lag-topology ───────────────────────────────────


async def test_lag_topology_no_data_returns_never(adapter_client):
    """Device with no LAG rows → 200 with refresh_source='never' and empty lags."""
    device_id = await seed_device(nso_device_name="lag-empty-device", netbox_device_id=800)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-topology", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "never"
    assert body["last_refreshed_at"] is None
    assert body["lags"] == []


async def test_lag_topology_with_one_lag_and_members(adapter_client):
    """Device with one LAG + two members → 200 with correct shape."""
    device_id = await seed_device(nso_device_name="lag-one-device", netbox_device_id=801)
    await _seed_lag(
        device_id,
        name="Port-channel10",
        lag_id=10,
        refresh_source="notification",
        members=[("GigabitEthernet0/1", "active"), ("GigabitEthernet0/2", "active")],
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-topology", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "notification"
    assert body["last_refreshed_at"] is not None

    assert len(body["lags"]) == 1
    lag = body["lags"][0]
    assert lag["name"] == "Port-channel10"
    assert lag["id"] == 10
    members = {m["interface"]: m["mode"] for m in lag["members"]}
    assert members == {"GigabitEthernet0/1": "active", "GigabitEthernet0/2": "active"}


async def test_lag_topology_multiple_lags_returned(adapter_client):
    """Device with two LAGs → both appear in the response."""
    device_id = await seed_device(nso_device_name="lag-multi-device", netbox_device_id=802)
    await _seed_lag(device_id, name="Port-channel1", lag_id=1, members=[("GE0/1", "on")])
    await _seed_lag(device_id, name="Port-channel2", lag_id=2, members=[("GE0/2", "passive")])

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-topology", headers=AUTH)
    assert resp.status_code == 200
    names = {lag["name"] for lag in resp.json()["lags"]}
    assert names == {"Port-channel1", "Port-channel2"}


async def test_lag_topology_uses_most_recent_refresh_source(adapter_client):
    """When a device has multiple LAGs, last_refreshed_at is the most recent one."""
    device_id = await seed_device(nso_device_name="lag-ts-device", netbox_device_id=803)
    older_ts = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
    newer_ts = datetime(2026, 5, 27, 9, 41, 12, tzinfo=UTC)
    await _seed_lag(device_id, name="Port-channel1", lag_id=1, refresh_source="polled-sync", last_refreshed_at=older_ts)
    await _seed_lag(device_id, name="Port-channel2", lag_id=2, refresh_source="notification", last_refreshed_at=newer_ts)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-topology", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    # Most recent source wins
    assert body["refresh_source"] == "notification"
    assert "2026-05-27" in body["last_refreshed_at"]


async def test_lag_topology_unknown_device_returns_404(adapter_client):
    """Non-existent device_id → 404 not_found."""
    resp = await adapter_client.get("/api/v1/devices/9999/lag-topology", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_lag_topology_requires_auth(adapter_client):
    """Missing Authorization header → 401."""
    resp = await adapter_client.get("/api/v1/devices/1/lag-topology")
    assert resp.status_code == 401


async def test_lag_topology_lag_with_no_members(adapter_client):
    """LAG with no members is returned with an empty members list."""
    device_id = await seed_device(nso_device_name="lag-nomembers-device", netbox_device_id=804)
    await _seed_lag(device_id, name="Port-channel99", lag_id=99, members=[])

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-topology", headers=AUTH)
    assert resp.status_code == 200
    assert len(resp.json()["lags"]) == 1
    assert resp.json()["lags"][0]["members"] == []
