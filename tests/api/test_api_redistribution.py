# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/redistribution."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
TS = datetime(2026, 7, 25, 10, 0, 0, tzinfo=UTC)


async def _seed_redistribution(device_id: int, entries: list[dict], *, refresh_source: str = "poll") -> None:
    """Seed DeviceRedistribution rows for a device."""
    from nso_adapter.store.models import DeviceRedistribution

    ts = TS.replace(tzinfo=None)
    async with session() as db:
        for entry in entries:
            db.add(
                DeviceRedistribution(
                    device_id=device_id,
                    dest_protocol=entry["dest_protocol"],
                    dest_ref=entry.get("dest_ref", ""),
                    source_protocol=entry["source_protocol"],
                    source_ref=entry.get("source_ref", ""),
                    route_map=entry.get("route_map"),
                    metric=entry.get("metric"),
                    metric_type=entry.get("metric_type"),
                    last_refreshed_at=ts,
                    refresh_source=refresh_source,
                )
            )
        await db.commit()


@pytest.mark.anyio
async def test_redistribution_no_rows_returns_never(adapter_client):
    """Device with no redistribution rows → 200 with refresh_source='never', empty entries list."""
    device_id = await seed_device(nso_device_name="rd-api-empty", netbox_device_id=8800)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "never"
    assert body["entries"] == []


@pytest.mark.anyio
async def test_redistribution_returns_populated_entries(adapter_client):
    """Device with redistribution rows → entries list with all fields."""
    device_id = await seed_device(nso_device_name="rd-api-pop", netbox_device_id=8801)
    await _seed_redistribution(
        device_id,
        [
            {
                "dest_protocol": "ospf",
                "dest_ref": "1",
                "source_protocol": "connected",
                "source_ref": "",
            },
            {
                "dest_protocol": "ospf",
                "dest_ref": "1",
                "source_protocol": "static",
                "source_ref": "",
                "route_map": "RM-STATIC",
                "metric": 10,
                "metric_type": "2",
            },
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "poll"
    assert len(body["entries"]) == 2

    static_entry = next(e for e in body["entries"] if e["source_protocol"] == "static")
    assert static_entry["route_map"] == "RM-STATIC"
    assert static_entry["metric"] == 10
    assert static_entry["metric_type"] == "2"

    connected_entry = next(e for e in body["entries"] if e["source_protocol"] == "connected")
    assert connected_entry["dest_protocol"] == "ospf"
    assert "route_map" not in connected_entry  # omitted when None


@pytest.mark.anyio
async def test_redistribution_unknown_device_returns_404(adapter_client):
    """Non-existent device_id → 404."""
    resp = await adapter_client.get("/api/v1/devices/99999/redistribution", headers=AUTH)
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_redistribution_no_auth_returns_401(adapter_client):
    """Missing auth token → 401."""
    device_id = await seed_device(nso_device_name="rd-api-noauth", netbox_device_id=8802)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution")
    assert resp.status_code == 401
