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
from nso_adapter.store.models import (
    DbInterface,
    Device,
    InterfaceAttrState,
    ManagedScope,
    SyncState,
)
from tests.conftest import session


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
    """Seed a device with one interface and one attr state (for _state_summary coverage)."""
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
        sync_state=SyncState.imported,
    )
    db.add(attr)
    db.add(ManagedScope(device_id=d.id, attribute="description"))
    await db.commit()
    await db.refresh(d)
    return d


# ── list_devices ──────────────────────────────────────────────────────────────


async def test_list_devices_empty(adapter_client):
    """list_devices() returns empty list when no devices exist."""
    async with session() as db:
        result = await list_devices(db=db)
        assert isinstance(result, list)


async def test_list_devices_with_state_summary(adapter_client):
    """list_devices() aggregates sync_state counts via _state_summary."""
    async with session() as db:
        await _seed_with_interface(db, "list-dev-01", 800)
        result = await list_devices(db=db)
        assert len(result) >= 1
        row = next(r for r in result if r["nso_device_name"] == "list-dev-01")
        summary = row["sync_state_summary"]
        assert summary["managed_interfaces"] == 1
        assert summary["imported"] == 1


async def test_list_devices_state_summary_is_not_n_plus_one(adapter_client):
    """s3-10: the summary must not run O(devices × interfaces) queries. Seed 3 devices,
    each with 5 interfaces (× 1 attr state); the query count must stay bounded — it must
    NOT grow with the interface fan-out (was 1 + Σ(1 + interfaces) per device)."""
    from tests.conftest import count_queries

    async with session() as db:
        for n in range(3):
            d = Device(nso_instance="nso-dev", nso_device_name=f"nplus-{n}", netbox_device_id=740 + n)
            db.add(d)
            await db.flush()
            for j in range(5):
                iface = DbInterface(device_id=d.id, name=f"Gi0/{j}", netbox_interface_id=7400 + n * 10 + j)
                db.add(iface)
                await db.flush()
                db.add(
                    InterfaceAttrState(interface_id=iface.id, attribute="description", sync_state=SyncState.imported)
                )
        await db.commit()

        with count_queries() as qc:
            result = await list_devices(db=db)

        seeded = [r for r in result if r["nso_device_name"].startswith("nplus-")]
        assert len(seeded) == 3
        assert all(r["sync_state_summary"]["managed_interfaces"] == 5 for r in seeded)
        # 3 devices × 5 interfaces = 15 attr rows; N+1 would be ~1 + 3×(1+5) = 19 queries.
        # Batched: a small constant, independent of the fan-out.
        assert qc.count <= 5, f"list_devices ran {qc.count} queries — N+1 across interfaces"


# ── get_device_by_nso ─────────────────────────────────────────────────────────


async def test_get_device_by_nso_not_found(adapter_client):
    """get_device_by_nso() raises 404 when no matching device exists."""
    async with session() as db:
        with pytest.raises(ApiError) as exc_info:
            await get_device_by_nso(instance="nso-dev", name="nonexistent-device", db=db)
        assert exc_info.value.status_code == 404


async def test_get_device_by_nso_found(adapter_client):
    """get_device_by_nso() returns device + scope + last_job_id on hit."""
    async with session() as db:
        d = await _seed_device(db, "by-nso-01", 810, nso_instance="nso-dev")
        db.add(ManagedScope(device_id=d.id, attribute="description"))
        await db.commit()

        result = await get_device_by_nso(instance="nso-dev", name="by-nso-01", db=db)
        assert result["nso_device_name"] == "by-nso-01"
        assert "scope" in result
        assert result["last_job_id"] is None


# ── get_device ────────────────────────────────────────────────────────────────


async def test_get_device_not_found(adapter_client):
    """get_device() raises 404 for unknown device_id."""
    async with session() as db:
        with pytest.raises(ApiError) as exc_info:
            await get_device(device_id=99997, db=db)
        assert exc_info.value.status_code == 404


async def test_get_device_found(adapter_client):
    """get_device() returns device dict with scope and last_job_id."""
    async with session() as db:
        d = await _seed_device(db, "get-dev-01", 820)
        result = await get_device(device_id=d.id, db=db)
        assert result["id"] == d.id
        assert result["scope"]["attributes"] == []
        assert result["last_job_id"] is None


# ── onboard_device ────────────────────────────────────────────────────────────


async def test_onboard_device_success(adapter_client_with_nso):
    """onboard_device() creates device and returns device_out dict."""
    async with session() as db:
        body = DeviceCreate(nso_instance="nso-dev", nso_device_name="onboard-01", netbox_device_id=900)
        result = await onboard_device(body=body, db=db)
        assert result["nso_device_name"] == "onboard-01"
        assert result["nso_instance"] == "nso-dev"


async def test_onboard_device_lookup_error_on_duplicate(adapter_client_with_nso):
    """onboard_device() raises 409 when device is already onboarded."""
    async with session() as db:
        # First onboard
        body = DeviceCreate(nso_instance="nso-dev", nso_device_name="onboard-dup-01", netbox_device_id=901)
        await onboard_device(body=body, db=db)
        # Duplicate — same netbox_device_id
        body2 = DeviceCreate(nso_instance="nso-dev", nso_device_name="onboard-dup-02", netbox_device_id=901)
        with pytest.raises(ApiError) as exc_info:
            await onboard_device(body=body2, db=db)
        assert exc_info.value.status_code == 409


async def test_onboard_device_value_error_on_unknown_instance(adapter_client_with_nso):
    """onboard_device() raises 422 when NSO instance is not in config."""
    async with session() as db:
        body = DeviceCreate(nso_instance="unknown-nso", nso_device_name="some-dev", netbox_device_id=902)
        with pytest.raises(ApiError) as exc_info:
            await onboard_device(body=body, db=db)
        assert exc_info.value.status_code == 422


# ── rekey_device ──────────────────────────────────────────────────────────────


async def test_rekey_device_not_found(adapter_client):
    """rekey_device() raises 404 for unknown device_id."""
    async with session() as db:
        with pytest.raises(ApiError) as exc_info:
            await rekey_device(device_id=99996, body=DevicePatch(nso_device_name="new"), db=db)
        assert exc_info.value.status_code == 404


async def test_rekey_device_noop_returns_current(adapter_client):
    """rekey_device() with no-op patch returns current device without modifying."""
    async with session() as db:
        d = await _seed_device(db, "rekey-noop-01", 930)
        result = await rekey_device(device_id=d.id, body=DevicePatch(), db=db)
        assert result["nso_device_name"] == "rekey-noop-01"


async def test_rekey_device_with_new_name(adapter_client_with_nso):
    """rekey_device() updates nso_device_name and clears interface state."""
    async with session() as db:
        d = await _seed_device(db, "rekey-old-name", 940)
        result = await rekey_device(
            device_id=d.id,
            body=DevicePatch(nso_device_name="rekey-new-name"),
            db=db,
        )
        assert result["nso_device_name"] == "rekey-new-name"


async def test_rekey_device_unknown_instance_raises_422(adapter_client_with_nso):
    """rekey_device() raises 422 when new NSO instance is unknown."""
    async with session() as db:
        d = await _seed_device(db, "rekey-err-01", 950)
        with pytest.raises(ApiError) as exc_info:
            await rekey_device(
                device_id=d.id,
                body=DevicePatch(nso_instance="unknown-nso"),
                db=db,
            )
        assert exc_info.value.status_code == 422


# ── offboard_device ───────────────────────────────────────────────────────────


async def test_offboard_device_not_found(adapter_client):
    """offboard_device() raises 404 for unknown device_id."""
    async with session() as db:
        with pytest.raises(ApiError) as exc_info:
            await offboard_device(device_id=99995, db=db)
        assert exc_info.value.status_code == 404


async def test_offboard_device_success(adapter_client):
    """offboard_device() removes the device and returns None (204)."""
    async with session() as db:
        d = await _seed_device(db, "offboard-01", 960)
        result = await offboard_device(device_id=d.id, db=db)
        assert result is None
        # Verify it's gone
        with pytest.raises(ApiError) as exc_info:
            await get_device(device_id=d.id, db=db)
        assert exc_info.value.status_code == 404
