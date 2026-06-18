# SPDX-License-Identifier: Apache-2.0
"""Devices API — list, onboard, get, re-key, offboard."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import DbInterface, Device, InterfaceAttrState, Job, ManagedScope, SyncState

router = APIRouter(prefix="/api/v1/devices", tags=["devices"])


# ── Response helpers ──────────────────────────────────────────────────────────


async def _state_summary(device_id: int, db: AsyncSession) -> dict:
    """Aggregate sync_state counts across all managed interfaces."""
    ifaces_result = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
    ifaces = ifaces_result.scalars().all()

    by_status: dict[str, int] = {s.value: 0 for s in SyncState}
    managed = 0
    for iface in ifaces:
        attrs_result = await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == iface.id))
        attrs = attrs_result.scalars().all()
        if attrs:
            managed += 1
        for attr in attrs:
            by_status[attr.sync_state.value] += 1

    return {"managed_interfaces": managed, **by_status}


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
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.get("", dependencies=[Depends(verify_token)])
async def list_devices(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Device))
    devices = result.scalars().all()
    out = []
    for d in devices:
        row = _device_out(d)
        row["sync_state_summary"] = await _state_summary(d.id, db)
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


@router.post("/provision", dependencies=[Depends(verify_token)])
async def provision_device(body: DeviceProvision, db: AsyncSession = Depends(get_db)):
    """Provision a device INTO NSO, then create the adapter mapping.

    Steps: create node → fetch-host-keys → unlock → sync-from. Returns the step-by-step
    result (200 even on a blocking step failure — inspect ``ok``/``steps``; the device is
    left for retry).
    """
    from nso_adapter.core.onboarding import provision_nso_device

    try:
        result = await provision_nso_device(
            db,
            nso_instance=body.nso_instance,
            device_name=body.device_name,
            address=body.address,
            ned_id=body.ned_id,
            authgroup=body.authgroup,
            netbox_device_id=body.netbox_device_id,
            ned_type=body.ned_type,
            port=body.port,
            admin_state=body.admin_state,
            do_sync=body.sync,
            oob_ip=body.oob_ip,
        )
    except ValueError as exc:
        raise api_error(422, "validation_error", str(exc))
    return result


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
