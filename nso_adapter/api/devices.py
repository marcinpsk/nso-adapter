# SPDX-License-Identifier: Apache-2.0
"""Devices API — list, onboard, get, re-key, offboard."""

from __future__ import annotations

from typing import Annotated, Never
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import (
    RESP_401,
    RESP_404,
    RESP_404_DEVICE,
    RESP_409,
    RESP_422_VALIDATION,
    RESP_500_INTERNAL,
    api_error,
)
from nso_adapter.api.pagination import DEFAULT_PAGE, LIMIT_MAX, LIMIT_MIN, validate_page_limit
from nso_adapter.api.timestamps import iso_z
from nso_adapter.store.models import (
    DbInterface,
    DeploymentApplyAttempt,
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
logger = structlog.get_logger(__name__)


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


_DEPLOYMENT_EVIDENCE_ATTEMPT_LIMIT = 100


class DeploymentEvidenceIn(BaseModel):
    apply_attempt_ids: list[UUID] = Field(max_length=_DEPLOYMENT_EVIDENCE_ATTEMPT_LIMIT)

    @field_validator("apply_attempt_ids", mode="before")
    @classmethod
    def deduplicate_attempt_ids(cls, value):
        if not isinstance(value, list):
            return value
        unique = []
        identities = set()
        for item in value:
            try:
                identity = UUID(item) if isinstance(item, str) else item
                hash(identity)
            except (TypeError, ValueError):
                unique.append(item)
                if len(unique) > _DEPLOYMENT_EVIDENCE_ATTEMPT_LIMIT:
                    return value
                continue
            if identity in identities:
                continue
            unique.append(item)
            identities.add(identity)
            if len(unique) > _DEPLOYMENT_EVIDENCE_ATTEMPT_LIMIT:
                return value
        return unique


class DeploymentEvidenceGenerationOut(BaseModel):
    generation_id: int
    seq: int
    status: GenerationStatus
    sections: list[str]
    source_push_seq: dict[str, int | None] = Field(description="Plugin X-Push-Seq keyed by intent stream.")
    carrier_job_id: int | None
    carrier_job_status: str | None
    carrier_job_result: dict | None
    carrier_job_error: dict | None
    updated_at: str


class DeploymentEvidenceAttemptOut(BaseModel):
    apply_attempt_id: UUID
    admission_state: str
    http_status: int
    response: dict
    generations: list[DeploymentEvidenceGenerationOut]


class DeploymentEvidenceHeadOut(BaseModel):
    generation_id: int
    seq: int
    status: GenerationStatus
    mode: GenerationMode
    settlement_cohort: int | None
    sections: list[str]
    source_push_seq: dict[str, int | None] = Field(description="Plugin X-Push-Seq keyed by intent stream.")
    apply_attempt_id: UUID | None
    carrier_job_id: int | None
    carrier_job_status: str | None
    carrier_job_result: dict | None
    carrier_job_error: dict | None
    created_at: str
    updated_at: str


class DeviceDeploymentEvidenceOut(BaseModel):
    device_id: int
    head: DeploymentEvidenceHeadOut | None
    blocked: bool
    write_work_pending: bool
    held_jobs: list[int]
    pending_generations: int
    attempts: list[DeploymentEvidenceAttemptOut]
    unknown_apply_attempt_ids: list[UUID]


class DeploymentEvidenceInvariantError(RuntimeError):
    """The durable Apply response disagrees with its stamped generations."""


def _raise_attempt_generation_evidence_invariant(
    attempt: DeploymentApplyAttempt,
    *,
    response_generation_ids: list | None,
    stamped_generation_ids: list[int],
    message: str,
) -> Never:
    logger.error(
        "deployment_evidence.invariant_violation",
        apply_attempt_id=attempt.id,
        response_generation_ids=response_generation_ids,
        stamped_generation_ids=stamped_generation_ids,
    )
    raise DeploymentEvidenceInvariantError(message)


def _validate_attempt_generation_evidence(
    attempt: DeploymentApplyAttempt,
    generations: list[DeploymentGeneration],
) -> None:
    stamped_ids = [generation.id for generation in generations]
    response_generations = attempt.response.get("generations")
    if response_generations is None:
        if generations:
            _raise_attempt_generation_evidence_invariant(
                attempt,
                response_generation_ids=None,
                stamped_generation_ids=stamped_ids,
                message=f"Apply attempt {attempt.id} replay body has no generation list for its stamped generations",
            )
        return
    if not isinstance(response_generations, list):
        _raise_attempt_generation_evidence_invariant(
            attempt,
            response_generation_ids=None,
            stamped_generation_ids=stamped_ids,
            message=f"Apply attempt {attempt.id} replay body has an invalid generation list",
        )

    response_ids = [item.get("generation_id") if isinstance(item, dict) else None for item in response_generations]
    for generation_id in response_ids:
        if not isinstance(generation_id, int) or isinstance(generation_id, bool):
            _raise_attempt_generation_evidence_invariant(
                attempt,
                response_generation_ids=response_ids,
                stamped_generation_ids=stamped_ids,
                message=f"Apply attempt {attempt.id} replay body has an invalid generation ID",
            )

    if len(response_ids) != len(set(response_ids)) or set(response_ids) != set(stamped_ids):
        _raise_attempt_generation_evidence_invariant(
            attempt,
            response_generation_ids=response_ids,
            stamped_generation_ids=stamped_ids,
            message=f"Apply attempt {attempt.id} replay body does not match its stamped generations",
        )


def _generation_sections(generation: DeploymentGeneration) -> list[str]:
    from nso_adapter.core.projection import stream_section

    return sorted({stream_section(stream) for stream in (generation.stream_revisions or {})})


def _generation_source_push_seq(generation: DeploymentGeneration) -> dict[str, int | None]:
    return dict(sorted((generation.source_push_seq or {}).items()))


def _attempt_generation_out(generation: DeploymentGeneration) -> dict:
    return {
        "generation_id": generation.id,
        "seq": generation.seq,
        "status": generation.status,
        "sections": _generation_sections(generation),
        "source_push_seq": _generation_source_push_seq(generation),
        "carrier_job_id": generation.carrier_job_id,
        "carrier_job_status": generation.carrier_job_status,
        "carrier_job_result": generation.carrier_job_result,
        "carrier_job_error": generation.carrier_job_error,
        "updated_at": iso_z(generation.updated_at),
    }


def _evidence_head_out(generation: DeploymentGeneration | None) -> dict | None:
    if generation is None:
        return None
    return {
        **_attempt_generation_out(generation),
        "mode": generation.mode,
        "settlement_cohort": generation.settlement_cohort,
        "apply_attempt_id": generation.apply_attempt_id,
        "created_at": iso_z(generation.created_at),
    }


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
    since_seq: Annotated[int | None, Query(ge=-(2**63), le=2**63 - 1)] = None,
    limit: Annotated[int, Query(ge=LIMIT_MIN, le=LIMIT_MAX)] = DEFAULT_PAGE,
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


@router.post(
    "/{device_id}/deployment-evidence",
    dependencies=[Depends(verify_token)],
    response_model=DeviceDeploymentEvidenceOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION, **RESP_500_INTERNAL},
)
async def deployment_evidence(
    device_id: int,
    body: DeploymentEvidenceIn,
    db: AsyncSession = Depends(get_read_db),
):
    """Return durable deployment facts for the requested Apply attempts.

    An ID listed under ``unknown_apply_attempt_ids`` is NON-ACTIONABLE. The caller must
    re-submit the identical Apply request and must not settle or roll back local intent from
    an unknown ID alone. A deterministic rejection can omit ``generations`` or set it to null
    when the attempt stamped none. Any other mismatch between a stored response and its stamped
    generations is corrupt and NON-ACTIONABLE. It returns the internal invariant error envelope.
    """
    from nso_adapter.core.generation import (
        BLOCKED_STATUSES,
        CROSSABLE_STATUSES,
        DEVICE_WRITING_JOB_TYPES,
        LIVE_JOB_STATUSES,
        executable_head,
        job_admissible,
    )

    if not await db.get(Device, device_id):
        raise api_error(404, "not_found", "Device not found")

    head = await executable_head(db, device_id)
    live_jobs = (
        await db.execute(
            select(Job.id, Job.status)
            .where(
                Job.device_id == device_id,
                Job.job_type.in_(DEVICE_WRITING_JOB_TYPES),
                Job.status.in_(LIVE_JOB_STATUSES),
            )
            .order_by(Job.created_at, Job.id)
        )
    ).all()
    write_work_pending = False
    held_jobs: list[int] = []
    for job in live_jobs:
        admissible = await job_admissible(db, job.id, device_id)
        write_work_pending = write_work_pending or admissible
        if job.status.value == "queued" and not admissible:
            held_jobs.append(job.id)

    pending_generations = 0
    if head is not None:
        pending_generations = await db.scalar(
            select(func.count())
            .select_from(DeploymentGeneration)
            .where(
                DeploymentGeneration.device_id == device_id,
                DeploymentGeneration.seq > head.seq,
                DeploymentGeneration.status.not_in(CROSSABLE_STATUSES),
            )
        )

    requested_ids = body.apply_attempt_ids
    attempts_by_id: dict[UUID, DeploymentApplyAttempt] = {}
    generations_by_attempt: dict[UUID, list[DeploymentGeneration]] = {}
    if requested_ids:
        attempts_by_id = {
            attempt.id: attempt
            for attempt in (
                (
                    await db.execute(
                        select(DeploymentApplyAttempt).where(
                            DeploymentApplyAttempt.device_id == device_id,
                            DeploymentApplyAttempt.id.in_(requested_ids),
                        )
                    )
                )
                .scalars()
                .all()
            )
        }
        generation_rows = (
            (
                await db.execute(
                    select(DeploymentGeneration)
                    .where(
                        DeploymentGeneration.device_id == device_id,
                        DeploymentGeneration.apply_attempt_id.in_(requested_ids),
                    )
                    .order_by(DeploymentGeneration.seq)
                )
            )
            .scalars()
            .all()
        )
        for generation in generation_rows:
            generations_by_attempt.setdefault(generation.apply_attempt_id, []).append(generation)

        for attempt in attempts_by_id.values():
            _validate_attempt_generation_evidence(
                attempt,
                generations_by_attempt.get(attempt.id, []),
            )

    return {
        "device_id": device_id,
        "head": _evidence_head_out(head),
        "blocked": head is not None and head.status in BLOCKED_STATUSES,
        "write_work_pending": write_work_pending,
        "held_jobs": held_jobs,
        "pending_generations": pending_generations,
        "attempts": [
            {
                "apply_attempt_id": attempt.id,
                "admission_state": attempt.admission_state,
                "http_status": attempt.http_status,
                "response": attempt.response,
                "generations": [
                    _attempt_generation_out(generation) for generation in generations_by_attempt.get(attempt.id, [])
                ],
            }
            for attempt_id in requested_ids
            if (attempt := attempts_by_id.get(attempt_id)) is not None
        ],
        "unknown_apply_attempt_ids": [attempt_id for attempt_id in requested_ids if attempt_id not in attempts_by_id],
    }


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
