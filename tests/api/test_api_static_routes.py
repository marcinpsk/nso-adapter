# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/static-routes."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
TS = datetime(2026, 6, 10, 9, 0, 0, tzinfo=UTC)


async def _seed_routes(
    device_id: int,
    routes: list[dict],
    *,
    refresh_source: str = "poll",
    last_refreshed_at: datetime = TS,
) -> None:
    """Seed DeviceStaticRoute rows for a device."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceStaticRoute

    ts = last_refreshed_at.replace(tzinfo=None)
    async for db in get_session():
        for route in routes:
            db.add(
                DeviceStaticRoute(
                    device_id=device_id,
                    vrf=route.get("vrf", ""),
                    prefix=route["prefix"],
                    next_hop=route.get("next_hop", ""),
                    interface_next_hop=route.get("interface_next_hop"),
                    metric=route.get("metric"),
                    permanent=route.get("permanent"),
                    tag=route.get("tag"),
                    name=route.get("name"),
                    last_refreshed_at=ts,
                    refresh_source=refresh_source,
                )
            )
        await db.commit()
        break


# ── GET /api/v1/devices/{id}/static-routes ──────────────────────────────────


@pytest.mark.anyio
async def test_static_routes_no_data_returns_never(adapter_client):
    """Device with no route rows → 200 with refresh_source='never'."""
    device_id = await seed_device(nso_device_name="sr-empty-dev", netbox_device_id=970)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "never"
    assert body["last_refreshed_at"] is None
    assert body["routes"] == []


@pytest.mark.anyio
async def test_static_routes_basic_routes_returned(adapter_client):
    """Global routes are returned with vrf, prefix, next_hop."""
    device_id = await seed_device(nso_device_name="sr-basic-dev", netbox_device_id=971)
    await _seed_routes(
        device_id,
        [
            {"vrf": "", "prefix": "10.0.0.0/8", "next_hop": "192.168.1.1"},
            {"vrf": "", "prefix": "172.16.0.0/12", "next_hop": "192.168.1.2"},
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["routes"]) == 2
    prefixes = {r["prefix"] for r in body["routes"]}
    assert prefixes == {"10.0.0.0/8", "172.16.0.0/12"}
    r1 = next(r for r in body["routes"] if r["prefix"] == "10.0.0.0/8")
    assert r1["next_hop"] == "192.168.1.1"
    assert r1["vrf"] == ""


@pytest.mark.anyio
async def test_static_routes_vrf_routes_returned(adapter_client):
    """VRF-scoped routes are returned with correct vrf field."""
    device_id = await seed_device(nso_device_name="sr-vrf-dev", netbox_device_id=972)
    await _seed_routes(
        device_id,
        [
            {"vrf": "MGMT", "prefix": "0.0.0.0/0", "next_hop": "10.10.10.1"},
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)
    assert resp.status_code == 200
    route = resp.json()["routes"][0]
    assert route["vrf"] == "MGMT"
    assert route["prefix"] == "0.0.0.0/0"
    assert route["next_hop"] == "10.10.10.1"


@pytest.mark.anyio
async def test_static_routes_optional_fields_present_when_set(adapter_client):
    """Optional fields (metric, permanent, tag, name) appear when non-null."""
    device_id = await seed_device(nso_device_name="sr-opts-dev", netbox_device_id=973)
    await _seed_routes(
        device_id,
        [
            {
                "vrf": "",
                "prefix": "10.1.0.0/16",
                "next_hop": "192.168.0.1",
                "metric": 5,
                "permanent": True,
                "tag": 100,
                "name": "mgmt-route",
            }
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)
    assert resp.status_code == 200
    route = resp.json()["routes"][0]
    assert route["metric"] == 5
    assert route["permanent"] is True
    assert route["tag"] == 100
    assert route["name"] == "mgmt-route"


@pytest.mark.anyio
async def test_static_routes_optional_fields_absent_when_null(adapter_client):
    """Optional fields are absent from response when null."""
    device_id = await seed_device(nso_device_name="sr-nullopts-dev", netbox_device_id=974)
    await _seed_routes(
        device_id,
        [{"vrf": "", "prefix": "10.2.0.0/16", "next_hop": "192.168.0.1"}],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)
    assert resp.status_code == 200
    route = resp.json()["routes"][0]
    assert "metric" not in route
    assert "permanent" not in route
    assert "tag" not in route
    assert "name" not in route


@pytest.mark.anyio
async def test_static_routes_last_refreshed_at_and_source(adapter_client):
    """last_refreshed_at and refresh_source are set from DB rows."""
    ts = datetime(2026, 5, 29, 10, 30, 0, tzinfo=UTC)
    device_id = await seed_device(nso_device_name="sr-ts-dev", netbox_device_id=975)
    await _seed_routes(
        device_id,
        [{"vrf": "", "prefix": "10.3.0.0/24", "next_hop": "10.0.0.1"}],
        refresh_source="sse",
        last_refreshed_at=ts,
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["refresh_source"] == "sse"
    assert "2026-05-29" in body["last_refreshed_at"]


@pytest.mark.anyio
async def test_static_routes_unknown_device_returns_404(adapter_client):
    """Non-existent device_id → 404 not_found."""
    resp = await adapter_client.get("/api/v1/devices/9999/static-routes", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_static_routes_requires_auth(adapter_client):
    """Missing Authorization header → 401."""
    resp = await adapter_client.get("/api/v1/devices/1/static-routes")
    assert resp.status_code == 401
