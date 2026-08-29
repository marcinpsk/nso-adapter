# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/static-routes."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, push_seq, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
TS = datetime(2026, 6, 10, 9, 0, 0, tzinfo=UTC)


async def _seed_routes(
    device_id: int,
    routes: list[dict],
    *,
    refresh_source: str = "poll",
    last_refreshed_at: datetime | None = TS,
) -> None:
    """Seed DeviceStaticRoute rows for a device."""
    from nso_adapter.store.models import DeviceStaticRoute

    ts = last_refreshed_at
    async with session() as db:
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
                    last_refreshed_at=route.get("last_refreshed_at", ts),
                    refresh_source=route.get("refresh_source", refresh_source),
                )
            )
        await db.commit()


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
@pytest.mark.parametrize(
    ("unrefreshed_prefix", "refreshed_prefix"),
    [
        ("198.18.0.0/24", "198.18.1.0/24"),
        ("198.18.1.0/24", "198.18.0.0/24"),
    ],
)
async def test_static_routes_select_latest_with_mixed_refresh_times(
    adapter_client, unrefreshed_prefix, refreshed_prefix
):
    device_id = await seed_device(nso_device_name="sr-mixed-refresh", netbox_device_id=976)
    await _seed_routes(
        device_id,
        [
            {
                "prefix": unrefreshed_prefix,
                "next_hop": "198.18.255.1",
                "last_refreshed_at": None,
                "refresh_source": "never",
            },
            {
                "prefix": refreshed_prefix,
                "next_hop": "198.18.255.2",
                "last_refreshed_at": TS,
                "refresh_source": "poll",
            },
        ],
    )

    response = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)

    assert response.status_code == 200
    assert response.json()["last_refreshed_at"] == "2026-06-10T09:00:00Z"
    assert response.json()["refresh_source"] == "poll"


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


# ── next-hop-vrf (leaked routes, #94) ────────────────────────────────────────


@pytest.mark.anyio
async def test_read_path_carries_next_hop_vrf(adapter_client):
    """A NED emitting IP next-hop + next-hop-vrf together (a leaked route) must not
    lose the leak VRF on the read path — reader persists it, GET serves it."""
    from unittest.mock import AsyncMock

    from nso_adapter.core.static_route import refresh_static_routes_for_device
    from nso_adapter.nso.client import NsoClient
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="sr-leak-dev", netbox_device_id=974)
    client = AsyncMock(spec=NsoClient)
    client.get_device_state_section.return_value = {
        "status": "ok",
        "device": "sr-leak-dev",
        "route": [
            {"vrf": "CUST-A", "prefix": "10.9.0.0/24", "next-hop": "192.0.2.9", "next-hop-vrf": "default"},
            {"vrf": "", "prefix": "10.0.0.0/24", "next-hop": "192.0.2.1"},
        ],
    }
    async with session() as db:
        device = await db.get(Device, device_id)
        assert await refresh_static_routes_for_device(db, device, client) is True

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)
    assert resp.status_code == 200
    routes = {r["prefix"]: r for r in resp.json()["routes"]}
    assert routes["10.9.0.0/24"]["next_hop_vrf"] == "default"
    assert "next_hop_vrf" not in routes["10.0.0.0/24"]  # plain routes stay lean


@pytest.mark.anyio
async def test_intent_put_carries_next_hop_vrf_and_interface_next_hop(adapter_client):
    """Accepting a leaked/interface route must not strip its next-hop forms: the intent
    PUT stores them and the apply body emits them (a replace-apply would otherwise
    silently rewrite the route without the leak VRF / egress interface)."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-leak-intent", netbox_device_id=975)
    body = {
        "routes": [
            {
                "vrf": "CUST-A",
                "prefix": "10.9.0.0/24",
                "next_hop": "192.0.2.9",
                "next_hop_vrf": "default",
                "interface_next_hop": "MgmtEth0/RSP0/CPU0/0",
            }
        ],
        "deleted_routes": [],
    }
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent", json=body, headers=AUTH | push_seq()
    )
    assert resp.status_code == 200

    async with session() as db:
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.next_hop_vrf == "default"
        assert row.interface_next_hop == "MgmtEth0/RSP0/CPU0/0"

    # Re-PUT the same key with the forms cleared → the update branch must clear them too.
    body["routes"][0].pop("next_hop_vrf")
    body["routes"][0].pop("interface_next_hop")
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent", json=body, headers=AUTH | push_seq()
    )
    assert resp.status_code == 200
    async with session() as db:
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.next_hop_vrf is None
        assert row.interface_next_hop is None
