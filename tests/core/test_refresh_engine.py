# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract tests for the shared read-mirror refresh engine (READSEM S1).

Two layers:
* ``classify_read`` — the single boundary that turns a getter's ``dict | None`` / exception
  into the explicit :data:`ReadOutcome` vocabulary, per empty-policy.
* ``run_family_refresh`` — the executor's outcome→mirror-action matrix, exercised end-to-end
  against a real DB + the real ``STATIC_ROUTE_SPEC`` (Present→replace, present-empty→clear,
  AbsentAuthoritative→clear, Unavailable→keep+False).
"""

from __future__ import annotations

import dataclasses
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.interface_ip import INTERFACE_IP_SPEC
from nso_adapter.core.refresh_engine import run_family_refresh
from nso_adapter.core.static_route import STATIC_ROUTE_SPEC
from nso_adapter.nso.client import NsoExportUnavailableError
from nso_adapter.nso.read_outcome import (
    AbsentAuthoritative,
    EmptyPolicy,
    Present,
    Unavailable,
    UnavailableReason,
    classify_read,
)
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceStaticRoute
from tests.conftest import seed_device

# The engine's LEGACY fetch path (classify_read over the per-family getter) still serves
# every unmigrated family until the S3 flips complete (and the code path itself lives to S5).
# static_route/interface_ip flipped to the envelope in B1, so their specs are un-flipped HERE
# to keep the legacy outcome->action matrix covered.
LEGACY_STATIC_ROUTE_SPEC = dataclasses.replace(STATIC_ROUTE_SPEC, wire_name=None)
LEGACY_INTERFACE_IP_SPEC = dataclasses.replace(INTERFACE_IP_SPEC, wire_name=None)


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


async def _seed_one_route(device_id: int) -> None:
    async for db in get_session():
        db.add(DeviceStaticRoute(device_id=device_id, vrf="", prefix="10.0.0.0/8", next_hop="1.1.1.1"))
        await db.commit()
        break


# ── classify_read: the vocabulary boundary ──────────────────────────────────────────────


@pytest.mark.anyio
async def test_classify_present_returns_present_with_data():
    async def getter():
        return {"route": [{"prefix": "10.0.0.0/8"}]}

    outcome = await classify_read(getter, EmptyPolicy.pop)
    assert isinstance(outcome, Present)
    assert outcome.data == {"route": [{"prefix": "10.0.0.0/8"}]}


@pytest.mark.anyio
async def test_classify_pop_none_is_absent_authoritative():
    """A clear-on-None family's None = container-confirmed absent → clear."""

    async def getter():
        return None

    assert await classify_read(getter, EmptyPolicy.pop) == AbsentAuthoritative()


@pytest.mark.anyio
async def test_classify_present_policy_none_is_unavailable_not_authoritative():
    """A keep-on-None inventory family's None = unsupported/unknown, NOT emptiness → keep."""

    async def getter():
        return None

    outcome = await classify_read(getter, EmptyPolicy.present)
    assert outcome == Unavailable(UnavailableReason.not_authoritative)


@pytest.mark.anyio
async def test_classify_export_unavailable_is_export_down():
    async def getter():
        raise NsoExportUnavailableError("network-state-export:static-route is not exported by NSO")

    outcome = await classify_read(getter, EmptyPolicy.pop)
    assert outcome == Unavailable(UnavailableReason.export_down)
    assert "not exported" in outcome.detail  # detail carries the exception repr for logs


@pytest.mark.anyio
async def test_classify_other_exception_is_read_error():
    async def getter():
        raise RuntimeError("connection refused")

    outcome = await classify_read(getter, EmptyPolicy.pop)
    assert outcome == Unavailable(UnavailableReason.read_error)
    assert "connection refused" in outcome.detail


def test_unavailable_equality_ignores_detail():
    """Reason is the identity; detail is diagnostic only (so tests assert on reason alone)."""
    assert Unavailable(UnavailableReason.read_error, detail="a") == Unavailable(
        UnavailableReason.read_error, detail="b"
    )
    assert Unavailable(UnavailableReason.read_error) != Unavailable(UnavailableReason.export_down)


def test_static_route_spec_is_pop_policy():
    assert STATIC_ROUTE_SPEC.empty_policy is EmptyPolicy.pop
    assert STATIC_ROUTE_SPEC.name == "static_route"


# ── run_family_refresh: the outcome→action matrix (real DB, real materializer) ──────────


@pytest.mark.anyio
async def test_engine_present_replaces_rows(adapter_client):
    device_id = await seed_device(nso_device_name="eng-present-sw01", netbox_device_id=9601)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = AsyncMock()
        client.get_static_routes.return_value = {
            "route": [{"vrf": "", "prefix": "172.16.0.0/12", "next-hop": "10.0.0.1"}]
        }

        ok = await run_family_refresh(db, device, client, LEGACY_STATIC_ROUTE_SPEC)

        assert ok is True
        rows = (
            (await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
            .scalars()
            .all()
        )
        assert [r.prefix for r in rows] == ["172.16.0.0/12"]


@pytest.mark.anyio
async def test_engine_present_empty_clears_rows(adapter_client):
    """A present entry with an empty child list is an authoritative clear."""
    device_id = await seed_device(nso_device_name="eng-empty-sw01", netbox_device_id=9602)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = AsyncMock()
        client.get_static_routes.return_value = {"name": "eng-empty-sw01", "route": []}

        ok = await run_family_refresh(db, device, client, LEGACY_STATIC_ROUTE_SPEC)

        assert ok is True
        rows = (
            (await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
            .scalars()
            .all()
        )
        assert rows == []


@pytest.mark.anyio
async def test_engine_absent_authoritative_clears_rows(adapter_client):
    """None from a pop-policy getter = confirmed absent → clear."""
    device_id = await seed_device(nso_device_name="eng-absent-sw01", netbox_device_id=9603)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = AsyncMock()
        client.get_static_routes.return_value = None

        ok = await run_family_refresh(db, device, client, LEGACY_STATIC_ROUTE_SPEC)

        assert ok is True
        rows = (
            (await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
            .scalars()
            .all()
        )
        assert rows == []


@pytest.mark.anyio
async def test_engine_export_down_keeps_rows(adapter_client):
    """A confirmed export outage must NOT wipe the mirror."""
    device_id = await seed_device(nso_device_name="eng-outage-sw01", netbox_device_id=9604)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = AsyncMock()
        client.get_static_routes.side_effect = NsoExportUnavailableError("export down")

        ok = await run_family_refresh(db, device, client, LEGACY_STATIC_ROUTE_SPEC)

        assert ok is False
        rows = (
            (await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1  # kept


@pytest.mark.anyio
async def test_engine_read_error_keeps_rows(adapter_client):
    device_id = await seed_device(nso_device_name="eng-err-sw01", netbox_device_id=9605)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = AsyncMock()
        client.get_static_routes.side_effect = RuntimeError("NSO unreachable")

        ok = await run_family_refresh(db, device, client, LEGACY_STATIC_ROUTE_SPEC)

        assert ok is False
        rows = (
            (await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1  # kept


@pytest.mark.anyio
async def test_engine_skips_device_without_nso_name(adapter_client):
    device_id = await seed_device(nso_device_name="eng-noname-sw01", netbox_device_id=9606)
    async with _device_session(device_id) as (db, device):
        device.nso_device_name = None
        client = AsyncMock()

        ok = await run_family_refresh(db, device, client, LEGACY_STATIC_ROUTE_SPEC)

        assert ok is True
        client.get_static_routes.assert_not_awaited()


@pytest.mark.anyio
async def test_engine_present_policy_not_authoritative_keeps_rows_and_reports_success(adapter_client):
    """A present-policy family's None (404) is an EXPECTED absence, not degradation:

    rows are KEPT and the surface reports success (True), so it never flips the device to
    ``partial`` on every poll. Contrast with export_down/read_error, which return False.
    """
    from nso_adapter.store.models import InterfaceIpAddress

    device_id = await seed_device(nso_device_name="eng-present-none-sw01", netbox_device_id=9607)
    async for db in get_session():
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name="GE0/1",
                address="10.0.0.1/24",
                vrf="",
                family="ipv4",
                secondary=False,
                last_refreshed_at=datetime.now(UTC).replace(tzinfo=None),
                refresh_source="poll",
            )
        )
        await db.commit()
        break

    async with _device_session(device_id) as (db, device):
        client = AsyncMock()
        client.get_interface_ips.return_value = None  # 404 → not-authoritative for a present family

        ok = await run_family_refresh(db, device, client, LEGACY_INTERFACE_IP_SPEC)

        assert ok is True  # NOT degraded
        rows = (
            (await db.execute(select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1  # kept


@pytest.mark.anyio
async def test_engine_present_policy_read_error_reports_degraded(adapter_client):
    """A genuine read failure on a present-policy family DOES report degraded (False), rows kept."""
    from nso_adapter.store.models import InterfaceIpAddress

    device_id = await seed_device(nso_device_name="eng-present-err-sw01", netbox_device_id=9608)
    async for db in get_session():
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name="GE0/1",
                address="10.0.0.1/24",
                vrf="",
                family="ipv4",
                secondary=False,
                last_refreshed_at=datetime.now(UTC).replace(tzinfo=None),
                refresh_source="poll",
            )
        )
        await db.commit()
        break

    async with _device_session(device_id) as (db, device):
        client = AsyncMock()
        client.get_interface_ips.side_effect = RuntimeError("timeout")

        ok = await run_family_refresh(db, device, client, LEGACY_INTERFACE_IP_SPEC)

        assert ok is False  # degraded
        rows = (
            (await db.execute(select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1  # kept
