# SPDX-License-Identifier: Apache-2.0
"""Devices API — list, onboard, get, re-key, offboard."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import (
    RESP_401,
    RESP_404,
    RESP_404_DEVICE,
    RESP_409,
    RESP_422_VALIDATION,
    api_error,
)
from nso_adapter.api.pagination import DEFAULT_PAGE, validate_page_limit
from nso_adapter.api.timestamps import iso_z
from nso_adapter.store.models import (
    DbInterface,
    DeploymentGeneration,
    Device,
    DeviceFailover,
    GenerationMode,
    GenerationStatus,
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
        "source_epoch": d.source_epoch,
        "mapping_status": d.mapping_status.value,
        "last_sync_at": iso_z(d.last_sync_at),
        "last_sync_status": d.last_sync_status.value if d.last_sync_status else None,
        # Populated only when last_sync_status == "partial": the routing surfaces whose
        # NSO read failed on the last sync (their mirror rows may be stale).
        "degraded_surfaces": d.degraded_surfaces or None,
    }


def _failover_out(fo: DeviceFailover | None) -> dict | None:
    """Serialize the device's failover status for the plugin's NSO tab, or None if not managed."""
    if fo is None:
        return None
    return {
        "active_address": fo.active_address,
        "primary_ip": fo.primary_ip,
        "oob_ip": fo.oob_ip,
        "last_probe_result": fo.last_probe_result,
        "last_probe_target": fo.last_probe_target,
        "last_probe_detail": fo.last_probe_detail,
        "last_probe_at": iso_z(fo.last_probe_at),
        "oob_healthy": fo.oob_healthy,
        "oob_health_result": fo.oob_health_result,
        "oob_health_detail": fo.oob_health_detail,
        "oob_health_checked_at": iso_z(fo.oob_health_checked_at),
        "last_switch_at": iso_z(fo.last_switch_at),
        "manual_override": fo.manual_override,
        "failback_blocked_reason": fo.failback_blocked_reason,
    }


async def _load_failover(device_id: int, db: AsyncSession) -> DeviceFailover | None:
    return (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one_or_none()


# ── Response models ───────────────────────────────────────────────────────────
# One DeviceOut carries the 8 always-present base keys plus the additive keys the
# individual endpoints layer on (sync_state_summary / scope / last_job_id / failover).
# Those additive fields default to unset, and every device endpoint sets
# response_model_exclude_unset=True, so an endpoint that never added a key emits no key
# (list has only sync_state_summary; get has scope+last_job_id+failover; by-nso has
# scope+last_job_id but no failover; onboard/rekey have none). The golden tests are the
# byte-level arbiter (tests/api/test_golden_devices.py).


class DeviceScopeRef(BaseModel):
    attributes: list[str]


class DeviceFailoverOut(BaseModel):
    active_address: str
    primary_ip: str | None
    oob_ip: str | None
    last_probe_result: str | None
    last_probe_target: str | None
    last_probe_detail: str | None
    last_probe_at: str | None
    oob_healthy: bool | None
    oob_health_result: str | None
    oob_health_detail: str | None
    oob_health_checked_at: str | None
    last_switch_at: str | None
    manual_override: bool
    failback_blocked_reason: str | None


class DeviceOut(BaseModel):
    id: int
    nso_instance: str
    nso_device_name: str
    netbox_device_id: int | None
    source_epoch: int
    mapping_status: str
    last_sync_at: str | None
    last_sync_status: str | None
    degraded_surfaces: list | None
    # additive — unset unless the endpoint layers it on (exclude_unset keeps it absent)
    sync_state_summary: dict[str, int] | None = None
    scope: DeviceScopeRef | None = None
    last_job_id: int | None = None
    failover: DeviceFailoverOut | None = None


class ProvisionOut(BaseModel):
    job_id: str
    nso_device_name: str
    status: str


class DeviceGenerationOut(BaseModel):
    """One deployment generation, with every receipt-facing field always present."""

    generation_id: int
    seq: int
    status: GenerationStatus
    job_id: int | None
    mode: GenerationMode
    settlement_cohort: int | None
    digest: str
    stream_revisions: dict[str, int]
    # a reissue-shaped map value is None by design (create_generation persists it)
    source_push_seq: dict[str, int | None]
    created_at: str
    updated_at: str


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.get(
    "",
    dependencies=[Depends(verify_token)],
    response_model=list[DeviceOut],
    response_model_exclude_unset=True,
    responses={**RESP_401},
)
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


@router.post(
    "",
    status_code=201,
    dependencies=[Depends(verify_token)],
    response_model=DeviceOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_409, **RESP_422_VALIDATION},
)
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


@router.post(
    "/provision",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=ProvisionOut,
    responses={**RESP_401, **RESP_422_VALIDATION},
)
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


@router.get(
    "/by-nso",
    dependencies=[Depends(verify_token)],
    response_model=DeviceOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404, **RESP_422_VALIDATION},
)
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


@router.get(
    "/{device_id}",
    dependencies=[Depends(verify_token)],
    response_model=DeviceOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
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


@router.get(
    "/{device_id}/generations",
    dependencies=[Depends(verify_token)],
    response_model=list[DeviceGenerationOut],
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def list_device_generations(
    device_id: int,
    since_seq: int | None = None,
    limit: int = DEFAULT_PAGE,
    db: AsyncSession = Depends(get_read_db),
):
    limit = validate_page_limit(limit)
    if not await db.get(Device, device_id):
        raise api_error(404, "not_found", "Device not found")

    # metadata columns only: the immutable document JSON is large and never served here
    query = (
        select(
            DeploymentGeneration.id,
            DeploymentGeneration.seq,
            DeploymentGeneration.status,
            DeploymentGeneration.job_id,
            DeploymentGeneration.mode,
            DeploymentGeneration.settlement_cohort,
            DeploymentGeneration.digest,
            DeploymentGeneration.stream_revisions,
            DeploymentGeneration.source_push_seq,
            DeploymentGeneration.created_at,
            DeploymentGeneration.updated_at,
        )
        .where(DeploymentGeneration.device_id == device_id)
        .order_by(DeploymentGeneration.seq)
    )
    if since_seq is not None:
        query = query.where(DeploymentGeneration.seq > since_seq)
    query = query.limit(limit)
    rows = (await db.execute(query)).all()
    return [
        {
            "generation_id": row.id,
            "seq": row.seq,
            "status": row.status,
            "job_id": row.job_id,
            "mode": row.mode,
            "settlement_cohort": row.settlement_cohort,
            "digest": row.digest,
            "stream_revisions": row.stream_revisions,
            "source_push_seq": row.source_push_seq,
            "created_at": iso_z(row.created_at),
            "updated_at": iso_z(row.updated_at),
        }
        for row in rows
    ]


class DevicePatch(BaseModel):
    nso_instance: str | None = None
    nso_device_name: str | None = None


@router.patch(
    "/{device_id}",
    dependencies=[Depends(verify_token)],
    response_model=DeviceOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
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


@router.delete(
    "/{device_id}",
    status_code=204,
    dependencies=[Depends(verify_token)],
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
async def offboard_device(device_id: int, db: AsyncSession = Depends(get_db)):
    from nso_adapter.core.claim import ClaimUnavailableError
    from nso_adapter.core.onboarding import offboard_device as _offboard

    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    try:
        await _offboard(db, device)
    except ClaimUnavailableError:
        # Something is working on this device. Tearing it down from under a runner is the
        # one thing the claim exists to prevent; the operator retries.
        raise api_error(
            409,
            "conflict",
            "The device is busy with another operation; retry",
            {"reason": "device_claimed"},
        ) from None
