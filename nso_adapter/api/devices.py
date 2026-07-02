# SPDX-License-Identifier: Apache-2.0
"""Devices API — list, onboard, get, re-key, offboard."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import (
    DbInterface,
    Device,
    DeviceFailover,
    InterfaceAttrState,
    Job,
    ManagedScope,
    SyncState,
)

router = APIRouter(prefix="/api/v1/devices", tags=["devices"])


# ── Response helpers ──────────────────────────────────────────────────────────


async def _state_summaries(device_ids: list[int], db: AsyncSession) -> dict[int, dict]:
    """Aggregate per-device sync_state counts in two grouped queries (not per-device N+1).

    ``managed_interfaces`` is the count of interfaces carrying at least one attr-state
    (distinct interface_id); the rest is a per-sync_state breakdown.
    """
    summaries: dict[int, dict] = {
        did: {"managed_interfaces": 0, **{s.value: 0 for s in SyncState}} for did in device_ids
    }
    if not device_ids:
        return summaries

    status_rows = await db.execute(
        select(DbInterface.device_id, InterfaceAttrState.sync_state, func.count())
        .join(InterfaceAttrState, InterfaceAttrState.interface_id == DbInterface.id)
        .where(DbInterface.device_id.in_(device_ids))
        .group_by(DbInterface.device_id, InterfaceAttrState.sync_state)
    )
    for device_id, sync_state, cnt in status_rows.all():
        summaries[device_id][sync_state.value] = cnt

    managed_rows = await db.execute(
        select(DbInterface.device_id, func.count(func.distinct(InterfaceAttrState.interface_id)))
        .join(InterfaceAttrState, InterfaceAttrState.interface_id == DbInterface.id)
        .where(DbInterface.device_id.in_(device_ids))
        .group_by(DbInterface.device_id)
    )
    for device_id, managed in managed_rows.all():
        summaries[device_id]["managed_interfaces"] = managed

    return summaries


async def _last_job_id(device_id: int, db: AsyncSession) -> int | None:
    result = await db.execute(select(Job.id).where(Job.device_id == device_id).order_by(Job.created_at.desc()).limit(1))
    return result.scalar_one_or_none()


def _device_out(d: Device) -> dict:
    return {
        "id": d.id,
        "nso_instance": d.nso_instance,
        "nso_device_name": d.nso_device_name,
        "netbox_device_id": d.netbox_device_id,
        "mapping_status": d.mapping_status.value,
        "last_sync_at": d.last_sync_at.isoformat() + "Z" if d.last_sync_at else None,
        "last_sync_status": d.last_sync_status.value if d.last_sync_status else None,
        # Populated only when last_sync_status == "partial": the routing surfaces whose
        # NSO read failed on the last sync (their mirror rows may be stale).
        "degraded_surfaces": d.degraded_surfaces or None,
    }


def _iso(dt) -> str | None:
    return dt.isoformat() + "Z" if dt else None


def _failover_out(fo: DeviceFailover | None) -> dict | None:
    """Serialize the device's failover status for the plugin's NSO tab, or None if not managed."""
    if fo is None:
        return None
    return {
        "active_address": fo.active_address,
        "primary_ip": fo.primary_ip,
        "oob_ip": fo.oob_ip,
        "last_probe_result": fo.last_probe_result,
        "last_probe_at": _iso(fo.last_probe_at),
        "oob_healthy": fo.oob_healthy,
        "oob_health_checked_at": _iso(fo.oob_health_checked_at),
        "last_switch_at": _iso(fo.last_switch_at),
        "manual_override": fo.manual_override,
    }


async def _load_failover(device_id: int, db: AsyncSession) -> DeviceFailover | None:
    return (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one_or_none()


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.get("", dependencies=[Depends(verify_token)])
async def list_devices(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Device))
    devices = result.scalars().all()
    summaries = await _state_summaries([d.id for d in devices], db)
    out = []
    for d in devices:
        row = _device_out(d)
        row["sync_state_summary"] = summaries[d.id]
        out.append(row)
    return out


class DeviceCreate(BaseModel):
    nso_instance: str
    nso_device_name: str
    netbox_device_id: int


@router.post("", status_code=201, dependencies=[Depends(verify_token)])
async def onboard_device(body: DeviceCreate, db: AsyncSession = Depends(get_db)):
    from nso_adapter.core.onboarding import onboard_device as _onboard

    try:
        device = await _onboard(db, body.nso_instance, body.nso_device_name, body.netbox_device_id)
    except LookupError as exc:
        raise api_error(409, "conflict", str(exc))
    except ValueError as exc:
        raise api_error(422, "validation_error", str(exc))
    return _device_out(device)


class DeviceProvision(BaseModel):
    nso_instance: str
    device_name: str
    address: str
    ned_id: str
    authgroup: str
    netbox_device_id: int | None = None
    ned_type: str | None = None  # None → derive transport (cli/netconf/…) from ned_id
    port: int | None = None
    admin_state: str = "unlocked"
    sync: bool = True
    oob_ip: str | None = None  # mgmt-IP failover fallback — bootstrap over OOB if primary is unreachable


@router.post("/provision", status_code=202, dependencies=[Depends(verify_token)])
async def provision_device(body: DeviceProvision, db: AsyncSession = Depends(get_db)):
    """Enqueue a device-onboarding job and return immediately.

    Provisioning (create node → fetch-host-keys → unlock → sync-from) can be slow — it may
    probe an unreachable primary, bootstrap over OOB, then run a full sync-from — and used to
    run inline, overrunning the plugin client's 30s read timeout. It now runs as a background
    ``provision`` job; this endpoint validates the instance and returns ``202`` with a
    ``job_id`` the caller polls (``GET /api/v1/jobs/{id}``). A double-submit for the same
    (instance, device_name) returns the in-flight job rather than provisioning twice.
    """
    from nso_adapter.config import get_config
    from nso_adapter.core.jobs import enqueue_provision_job

    known = {inst.name for inst in get_config().nso_instances}
    if body.nso_instance not in known:
        raise api_error(422, "validation_error", f"NSO instance {body.nso_instance!r} not found in config")

    params = {
        "nso_instance": body.nso_instance,
        "device_name": body.device_name,
        "address": body.address,
        "ned_id": body.ned_id,
        "authgroup": body.authgroup,
        "netbox_device_id": body.netbox_device_id,
        "ned_type": body.ned_type,
        "port": body.port,
        "admin_state": body.admin_state,
        "do_sync": body.sync,
        "oob_ip": body.oob_ip,
    }
    job, _created = await enqueue_provision_job(params, db)
    return {"job_id": str(job.id), "nso_device_name": body.device_name, "status": job.status.value}


@router.get("/by-nso", dependencies=[Depends(verify_token)])
async def get_device_by_nso(instance: str, name: str, db: AsyncSession = Depends(get_db)):
    """Look up an adapter Device by its NSO coordinates.

    Declared before ``/{device_id}`` so FastAPI doesn't attempt to coerce
    the literal string "by-nso" to an integer device_id.

    Returns the same shape as ``GET /api/v1/devices/{id}`` on hit, 404 on miss.
    """
    result = await db.execute(select(Device).where(Device.nso_instance == instance, Device.nso_device_name == name))
    device = result.scalar_one_or_none()
    if not device:
        raise api_error(404, "not_found", f"No device for instance='{instance}' name='{name}'")

    scope_result = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device.id))
    scope_attrs = [s.attribute for s in scope_result.scalars().all()]

    out = _device_out(device)
    out["scope"] = {"attributes": scope_attrs}
    out["last_job_id"] = await _last_job_id(device.id, db)
    return out


@router.get("/{device_id}", dependencies=[Depends(verify_token)])
async def get_device(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    scope_result = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
    scope_attrs = [s.attribute for s in scope_result.scalars().all()]

    out = _device_out(device)
    out["scope"] = {"attributes": scope_attrs}
    out["last_job_id"] = await _last_job_id(device_id, db)
    out["failover"] = _failover_out(await _load_failover(device_id, db))
    return out


class DevicePatch(BaseModel):
    nso_instance: str | None = None
    nso_device_name: str | None = None


@router.patch("/{device_id}", dependencies=[Depends(verify_token)])
async def rekey_device(device_id: int, body: DevicePatch, db: AsyncSession = Depends(get_db)):
    from nso_adapter.core.onboarding import rekey_device as _rekey

    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    if body.nso_instance is None and body.nso_device_name is None:
        return _device_out(device)
    try:
        device = await _rekey(db, device, body.nso_instance, body.nso_device_name)
    except LookupError as exc:
        raise api_error(409, "conflict", str(exc))
    except ValueError as exc:
        raise api_error(422, "validation_error", str(exc))
    return _device_out(device)


@router.delete("/{device_id}", status_code=204, dependencies=[Depends(verify_token)])
async def offboard_device(device_id: int, db: AsyncSession = Depends(get_db)):
    from nso_adapter.core.onboarding import offboard_device as _offboard

    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    await _offboard(db, device)
