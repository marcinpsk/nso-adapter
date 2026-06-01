# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for devices.py endpoint functions.

Calls endpoint functions directly to guarantee coverage.py tracks all async
function bodies (bypasses the Python 3.12 async tracing gap with ASGITransport).
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.devices import (
    DeviceCreate,
    DevicePatch,
    get_device,
    get_device_by_nso,
    list_devices,
    offboard_device,
    onboard_device,
    rekey_device,
)
from nso_adapter.api.errors import ApiError
from nso_adapter.store.db import get_session
from nso_adapter.store.models import (
    ComplianceStatus,
    DbInterface,
    Device,
    InterfaceAttrState,
    ManagedScope,
)


async def _seed_device(
    db: AsyncSession,
    nso_device_name: str,
    netbox_id: int,
    nso_instance: str = "nso-dev",
) -> Device:
    d = Device(nso_instance=nso_instance, nso_device_name=nso_device_name, netbox_device_id=netbox_id)
    db.add(d)
    await db.flush()
    await db.commit()
    await db.refresh(d)
    return d


async def _seed_with_interface(db: AsyncSession, nso_device_name: str, netbox_id: int) -> Device:
    """Seed a device with one interface and one attr state (for _compliance_summary coverage)."""
    d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
    db.add(d)
    await db.flush()
    iface = DbInterface(device_id=d.id, name="GigabitEthernet0/0", netbox_interface_id=100)
    db.add(iface)
    await db.flush()
    attr = InterfaceAttrState(
        interface_id=iface.id,
        attribute="description",
        nso_value="nso",
        netbox_value="netbox",
        compliance_status=ComplianceStatus.imported,
    )
    db.add(attr)
    db.add(ManagedScope(device_id=d.id, attribute="description"))
    await db.commit()
    await db.refresh(d)
    return d


# ── list_devices ──────────────────────────────────────────────────────────────


async def test_list_devices_empty(adapter_client):
    """list_devices() returns empty list when no devices exist."""
    async for db in get_session():
        result = await list_devices(db=db)
        assert isinstance(result, list)
        break


async def test_list_devices_with_compliance_summary(adapter_client):
    """list_devices() aggregates compliance counts via _compliance_summary."""
    async for db in get_session():
        await _seed_with_interface(db, "list-dev-01", 800)
        result = await list_devices(db=db)
        assert len(result) >= 1
        row = next(r for r in result if r["nso_device_name"] == "list-dev-01")
        summary = row["compliance_summary"]
        assert summary["managed_interfaces"] == 1
        assert summary["imported"] == 1
        break


# ── get_device_by_nso ─────────────────────────────────────────────────────────


async def test_get_device_by_nso_not_found(adapter_client):
    """get_device_by_nso() raises 404 when no matching device exists."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await get_device_by_nso(instance="nso-dev", name="nonexistent-device", db=db)
        assert exc_info.value.status_code == 404
        break


async def test_get_device_by_nso_found(adapter_client):
    """get_device_by_nso() returns device + scope + last_job_id on hit."""
    async for db in get_session():
        d = await _seed_device(db, "by-nso-01", 810, nso_instance="nso-dev")
        db.add(ManagedScope(device_id=d.id, attribute="description"))
        await db.commit()

        result = await get_device_by_nso(instance="nso-dev", name="by-nso-01", db=db)
        assert result["nso_device_name"] == "by-nso-01"
        assert "scope" in result
        assert result["last_job_id"] is None
        break


# ── get_device ────────────────────────────────────────────────────────────────


async def test_get_device_not_found(adapter_client):
    """get_device() raises 404 for unknown device_id."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await get_device(device_id=99997, db=db)
        assert exc_info.value.status_code == 404
        break


async def test_get_device_found(adapter_client):
    """get_device() returns device dict with scope and last_job_id."""
    async for db in get_session():
        d = await _seed_device(db, "get-dev-01", 820)
        result = await get_device(device_id=d.id, db=db)
        assert result["id"] == d.id
        assert result["scope"]["attributes"] == []
        assert result["last_job_id"] is None
        break


# ── onboard_device ────────────────────────────────────────────────────────────


async def test_onboard_device_success(adapter_client_with_nso):
    """onboard_device() creates device and returns device_out dict."""
    async for db in get_session():
        body = DeviceCreate(nso_instance="nso-dev", nso_device_name="onboard-01", netbox_device_id=900)
        result = await onboard_device(body=body, db=db)
        assert result["nso_device_name"] == "onboard-01"
        assert result["nso_instance"] == "nso-dev"
        break


async def test_onboard_device_lookup_error_on_duplicate(adapter_client_with_nso):
    """onboard_device() raises 409 when device is already onboarded."""
    async for db in get_session():
        # First onboard
        body = DeviceCreate(nso_instance="nso-dev", nso_device_name="onboard-dup-01", netbox_device_id=901)
        await onboard_device(body=body, db=db)
        # Duplicate — same netbox_device_id
        body2 = DeviceCreate(nso_instance="nso-dev", nso_device_name="onboard-dup-02", netbox_device_id=901)
        with pytest.raises(ApiError) as exc_info:
            await onboard_device(body=body2, db=db)
        assert exc_info.value.status_code == 409
        break


async def test_onboard_device_value_error_on_unknown_instance(adapter_client_with_nso):
    """onboard_device() raises 422 when NSO instance is not in config."""
    async for db in get_session():
        body = DeviceCreate(nso_instance="unknown-nso", nso_device_name="some-dev", netbox_device_id=902)
        with pytest.raises(ApiError) as exc_info:
            await onboard_device(body=body, db=db)
        assert exc_info.value.status_code == 422
        break


# ── rekey_device ──────────────────────────────────────────────────────────────


async def test_rekey_device_not_found(adapter_client):
    """rekey_device() raises 404 for unknown device_id."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await rekey_device(device_id=99996, body=DevicePatch(nso_device_name="new"), db=db)
        assert exc_info.value.status_code == 404
        break


async def test_rekey_device_noop_returns_current(adapter_client):
    """rekey_device() with no-op patch returns current device without modifying."""
    async for db in get_session():
        d = await _seed_device(db, "rekey-noop-01", 930)
        result = await rekey_device(device_id=d.id, body=DevicePatch(), db=db)
        assert result["nso_device_name"] == "rekey-noop-01"
        break


async def test_rekey_device_with_new_name(adapter_client_with_nso):
    """rekey_device() updates nso_device_name and clears interface state."""
    async for db in get_session():
        d = await _seed_device(db, "rekey-old-name", 940)
        result = await rekey_device(
            device_id=d.id,
            body=DevicePatch(nso_device_name="rekey-new-name"),
            db=db,
        )
        assert result["nso_device_name"] == "rekey-new-name"
        break


async def test_rekey_device_unknown_instance_raises_422(adapter_client_with_nso):
    """rekey_device() raises 422 when new NSO instance is unknown."""
    async for db in get_session():
        d = await _seed_device(db, "rekey-err-01", 950)
        with pytest.raises(ApiError) as exc_info:
            await rekey_device(
                device_id=d.id,
                body=DevicePatch(nso_instance="unknown-nso"),
                db=db,
            )
        assert exc_info.value.status_code == 422
        break


# ── offboard_device ───────────────────────────────────────────────────────────


async def test_offboard_device_not_found(adapter_client):
    """offboard_device() raises 404 for unknown device_id."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await offboard_device(device_id=99995, db=db)
        assert exc_info.value.status_code == 404
        break


async def test_offboard_device_success(adapter_client):
    """offboard_device() removes the device and returns None (204)."""
    async for db in get_session():
        d = await _seed_device(db, "offboard-01", 960)
        result = await offboard_device(device_id=d.id, db=db)
        assert result is None
        # Verify it's gone
        with pytest.raises(ApiError) as exc_info:
            await get_device(device_id=d.id, db=db)
        assert exc_info.value.status_code == 404
        break
