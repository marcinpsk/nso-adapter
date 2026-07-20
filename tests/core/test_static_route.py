# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/static_route.py — refresh and SSE event handler."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.static_route import (
    STATIC_ROUTE_SPEC,
    handle_static_route_change,
    refresh_static_routes_for_device,
)
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceStaticRoute
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


@pytest.mark.anyio
async def test_refresh_inserts_routes(adapter_client):
    """NSO returns routes → DeviceStaticRoute rows inserted."""
    device_id = await seed_device(nso_device_name="sr-insert-sw01", netbox_device_id=980)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "name": "sr-insert-sw01",
            "route": [
                {"vrf": "", "prefix": "10.0.0.0/8", "next-hop": "192.168.1.1"},
                {"vrf": "MGMT", "prefix": "0.0.0.0/0", "next-hop": "10.10.10.1", "metric": 1},
            ],
        }

        await refresh_static_routes_for_device(db, device, nso_client, refresh_source="poll")

        nso_client.get_device_state_section.assert_awaited_once_with("sr-insert-sw01", "static-route")
        result = await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id))
        rows = result.scalars().all()
        assert len(rows) == 2
        prefixes = {r.prefix for r in rows}
        assert prefixes == {"10.0.0.0/8", "0.0.0.0/0"}
        mgmt = next(r for r in rows if r.vrf == "MGMT")
        assert mgmt.metric == 1
        assert mgmt.next_hop == "10.10.10.1"


@pytest.mark.anyio
async def test_refresh_replaces_existing_rows(adapter_client):
    """Second refresh fully replaces previous rows (full-replace pattern)."""
    device_id = await seed_device(nso_device_name="sr-replace-sw01", netbox_device_id=981)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "route": [{"vrf": "", "prefix": "10.0.0.0/8", "next-hop": "192.168.1.1"}],
        }
        await refresh_static_routes_for_device(db, device, nso_client)

        # Second call with different data
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "route": [{"vrf": "", "prefix": "172.16.0.0/12", "next-hop": "10.0.0.1"}],
        }
        await refresh_static_routes_for_device(db, device, nso_client)

        result = await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id))
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].prefix == "172.16.0.0/12"


@pytest.mark.anyio
async def test_refresh_nso_returns_none_clears_rows(adapter_client):
    """NSO returns None (device not found) → rows cleared."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceStaticRoute

    device_id = await seed_device(nso_device_name="sr-none-sw01", netbox_device_id=982)
    async for db in get_session():
        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="10.0.0.0/8",
                next_hop="1.1.1.1",
            )
        )
        await db.commit()
        break

    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = None
        await refresh_static_routes_for_device(db, device, nso_client)

        result = await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id))
        assert result.scalars().all() == []


@pytest.mark.anyio
async def test_refresh_nso_error_skips_update(adapter_client):
    """NSO transport error → rows untouched (graceful degradation)."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceStaticRoute

    device_id = await seed_device(nso_device_name="sr-error-sw01", netbox_device_id=983)
    async for db in get_session():
        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="10.0.0.0/8",
                next_hop="1.1.1.1",
            )
        )
        await db.commit()
        break

    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.side_effect = RuntimeError("NSO unreachable")
        await refresh_static_routes_for_device(db, device, nso_client)

        result = await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id))
        rows = result.scalars().all()
        # rows preserved because we never deleted them
        assert len(rows) == 1


@pytest.mark.anyio
async def test_refresh_skips_device_without_nso_name(adapter_client):
    """Device with no nso_device_name → NSO never called."""
    device_id = await seed_device(nso_device_name="sr-noname-placeholder", netbox_device_id=984)
    async with _device_session(device_id) as (db, device):
        device.nso_device_name = None  # simulate unmapped device
        nso_client = AsyncMock()
        await refresh_static_routes_for_device(db, device, nso_client)
        nso_client.get_device_state_section.assert_not_awaited()


@pytest.mark.anyio
async def test_handle_sse_event_refreshes_device(adapter_client):
    """SSE handler resolves Device by nso_device_name and triggers refresh."""
    device_id = await seed_device(nso_device_name="sr-sse-sw01", netbox_device_id=985)
    async for db in get_session():
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "route": [{"vrf": "", "prefix": "10.100.0.0/24", "next-hop": "192.168.0.1"}],
        }
        await handle_static_route_change(db, "sr-sse-sw01", nso_client)

        result = await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id))
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].prefix == "10.100.0.0/24"
        assert rows[0].refresh_source == "sse"
        break


@pytest.mark.anyio
async def test_handle_sse_event_unknown_device_is_noop(adapter_client):
    """SSE event for unknown nso_device_name → no error, no DB writes."""
    async for db in get_session():
        nso_client = AsyncMock()
        await handle_static_route_change(db, "nonexistent-device", nso_client)
        nso_client.get_device_state_section.assert_not_awaited()
        break


# ── READSEM S3: the B1 envelope flip ────────────────────────────────────────────────


def test_spec_is_flipped_to_the_envelope():
    """The S3 fetch-source flip pin — reverting wire_name reverts the family to legacy."""
    assert STATIC_ROUTE_SPEC.wire_name == "static-route"


@pytest.mark.anyio
async def test_unsupported_keeps_rows_and_reports_success(adapter_client):
    """RED-FIRST S3 delta: the legacy probe-confirmed 404 CLEARED an unsupported-NED
    device's routes; the envelope's declared `unsupported` keeps them."""
    device_id = await seed_device(nso_device_name="sr-unsup-sw01", netbox_device_id=9821)
    async for db in get_session():
        db.add(DeviceStaticRoute(device_id=device_id, vrf="", prefix="10.9.0.0/16", next_hop="1.1.1.1"))
        await db.commit()
        break
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {"status": "unsupported"}

        ok = await refresh_static_routes_for_device(db, device, nso_client)

        assert ok is True
        rows = (
            (await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
            .scalars()
            .all()
        )
        assert [r.prefix for r in rows] == ["10.9.0.0/16"], "unsupported must KEEP rows"
