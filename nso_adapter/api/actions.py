# SPDX-License-Identifier: Apache-2.0
"""Actions API — async device actions (sync, check-sync_state, connect, apply, sync-notify).

All actions return 202 with {job_id}.
409 is returned if a job is already queued/running for the device.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import (
    RESP_400,
    RESP_401,
    RESP_404_DEVICE,
    RESP_409,
    RESP_409_ACTIVE_JOB,
    RESP_422_VALIDATION,
    api_error,
)
from nso_adapter.core.jobs import enqueue_job
from nso_adapter.store.models import DeploymentGeneration, Device, JobType

router = APIRouter(prefix="/api/v1/devices", tags=["actions"])

# All action endpoints emit 401 (token) + 422 (device_id path); the responses fragments
# below add the ones each endpoint actually raises. The trigger POSTs go through
# _trigger (404 + 409-active-job); force-removal / apply-diff raise 400 bad_request.
_TRIGGER_ERRORS = {**RESP_401, **RESP_404_DEVICE, **RESP_409_ACTIVE_JOB, **RESP_422_VALIDATION}


class JobTriggerOut(BaseModel):
    """The async-action envelope: the id of the enqueued job (202)."""

    job_id: int


class ApplyDiffOut(BaseModel):
    """apply-diff preview — {scope: native_delta} for scopes with a non-empty change."""

    device_id: int
    outformat: str
    diffs: dict[str, str]


async def _trigger(
    device_id: int,
    job_type: JobType,
    db: AsyncSession,
) -> dict:
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    job, created = await enqueue_job(device_id, job_type, db)
    if not created:
        raise api_error(409, "conflict", "A job is already running for this device", {"job_id": job.id})
    return {"job_id": job.id}


class ForceRemovalBody(BaseModel):
    scope: str
    interfaces: list[str] | None = None


@router.post(
    "/{device_id}/actions/force-removal",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=JobTriggerOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_400, **RESP_422_VALIDATION},
)
async def action_force_removal(
    device_id: int,
    body: ForceRemovalBody,
    db: AsyncSession = Depends(get_db),
):
    """Re-run a scope's removal with the collateral guard DISABLED.

    The operator override for a ``removal_blocked_collateral`` failure: after
    reviewing the blocked job's orphan list + dry-run preview, this deliberately
    flushes the orphaned service rows (PUT-replace with only the remaining intent).

    ``interface_config`` is per-instance (interface-reconciler is keyed by
    ``(device, interface-name)``), so its removal job flushes exactly the interfaces named
    in *interfaces* — with none, ``_replace_interface_config`` iterates an empty list and
    the job succeeds having pushed NOTHING, telling the operator their orphaned addresses
    were flushed while the config is still live on the device. Reject that rather than
    succeed at nothing.

    It PROMOTES NOTHING (#1522 §G2). The flush re-deploys state an earlier push already
    authorized, with the guard off, so ``enqueue_removal`` gives it a reissue generation.
    Treating it as an operator write instead authorized every stream of the family, and its
    settlement then certified them applied — the sibling lane's un-promoted store-only state
    included, on interfaces this job never sends.
    """
    from nso_adapter.core.removal import VALID_REMOVAL_SCOPES, enqueue_removal

    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    if body.scope not in VALID_REMOVAL_SCOPES:
        raise api_error(400, "bad_request", f"Unknown removal scope {body.scope!r}")
    if body.scope == "interface_config" and not body.interfaces:
        raise api_error(
            400,
            "bad_request",
            "force-removal of interface_config requires 'interfaces': the interface-reconciler "
            "is keyed per interface, so with none named the job would flush nothing.",
        )
    job = await enqueue_removal(db, device_id, body.scope, interfaces=body.interfaces, force=True)
    await db.commit()
    return {"job_id": job.id}


@router.post(
    "/{device_id}/actions/sync",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=JobTriggerOut,
    responses=_TRIGGER_ERRORS,
)
async def action_sync(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    # Operator Sync-Now = READSEM grain c: the mirror fan-out reads ONE atomic
    # device-state-read build. The plugin's sync-notify below stays grain b (automatic,
    # frequent - the record-served projection).
    return await _trigger(device_id, JobType.sync_now, db)


@router.post(
    "/{device_id}/actions/sync-from-nso",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=JobTriggerOut,
    responses=_TRIGGER_ERRORS,
)
async def action_sync_from_nso(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    # Operator "Sync from NSO" (S5a): comprehensive CDB-only mirror read — every surface
    # from ONE atomic device-state-read, NO device round-trip (Sync Now above runs the
    # device sync-from first; this re-reads what NSO already knows).
    return await _trigger(device_id, JobType.sync_from_nso, db)


@router.post(
    "/{device_id}/actions/detect-drift",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=JobTriggerOut,
    responses=_TRIGGER_ERRORS,
)
async def action_detect_drift(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    return await _trigger(device_id, JobType.detect_drift, db)


@router.post(
    "/{device_id}/actions/connect",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=JobTriggerOut,
    responses=_TRIGGER_ERRORS,
)
async def action_connect(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    return await _trigger(device_id, JobType.connect, db)


@router.post(
    "/{device_id}/sync-notify",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=JobTriggerOut,
    responses=_TRIGGER_ERRORS,
)
async def sync_notify(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Handle the NetBox plugin's notification that scope or intent changed for this device.

    Triggers an immediate sync job. If a job is already running, returns 409 with
    the existing job_id so the plugin can poll for the result.
    """
    return await _trigger(device_id, JobType.sync, db)


@router.post(
    "/{device_id}/actions/apply",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=JobTriggerOut,
    responses=_TRIGGER_ERRORS,
)
async def action_apply(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Phase 2 — push accepted NetBox intent to NSO via reconcile-commit service."""
    from nso_adapter.core.request_flags import STORE_ONLY

    if STORE_ONLY.get():
        raise api_error(422, "validation_error", "store_only is not valid for the Apply action")
    return await _trigger(device_id, JobType.apply, db)


class GenerationOut(BaseModel):
    """The blocked head a retry or an abandon acted on."""

    generation_id: int
    seq: int
    job_id: int | None = None


#: What both barrier exits report when the head moved under them.
_HEAD_ALREADY_ACTED_ON = "This device's blocked deployment generation was already acted on"


async def _blocked_head(db: AsyncSession, device_id: int) -> DeploymentGeneration:
    """Return the device's blocked head, or 404/409 explaining why there is nothing to do.

    The head is read UNDER the device's projection lock, held to this request's commit. Both
    exits act on "the current head", so two operator requests that read it unlocked can each
    decide it is theirs: two retries duplicate the removal, and a retry racing an abandon
    leaves a queued job executing a generation the operator gave up on. The lock also orders
    these against the intent pushes that create successors, which is the same lock every
    accepted projection write already takes.

    An offboard committing inside that lock raises ``DeviceProjectionGone``, which the app's
    own handler turns into this same 404 envelope for every route that can hit it.
    """
    from nso_adapter.core.generation import BLOCKED_STATUSES, executable_head, lock_projection

    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    await lock_projection(db, device_id)
    head = await executable_head(db, device_id)
    if head is None or head.status not in BLOCKED_STATUSES:
        raise api_error(
            409,
            "conflict",
            "This device has no blocked deployment generation",
            {"head_status": head.status.value if head is not None else None},
        )
    return head


@router.post(
    "/{device_id}/actions/retry-generation",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=GenerationOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
async def action_retry_generation(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Re-admit this device's blocked deployment generation (#1522 §H2).

    One of the two explicit exits from the success barrier. The head is re-queued with the
    SAME document, mode and digest — it is re-sent, never rebuilt — and its successors go
    back to waiting behind it. Use it after fixing whatever the device refused.
    """
    from nso_adapter.core.generation import GenerationNotBlocked, retry_generation

    head = await _blocked_head(db, device_id)
    try:
        job = await retry_generation(db, head.id)
    except GenerationNotBlocked:
        # The compare-and-set behind the lock: unreachable while this request holds it, and
        # kept as the guarantee's second half for any caller that does not.
        await db.rollback()
        raise api_error(409, "conflict", _HEAD_ALREADY_ACTED_ON) from None
    await db.commit()
    return {"generation_id": head.id, "seq": head.seq, "job_id": job.id if job else None}


@router.post(
    "/{device_id}/actions/abandon-generation",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=GenerationOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
async def action_abandon_generation(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Give up on this device's blocked generation so its successors may run (#1522 §H2).

    The other exit. Deliberately destructive of intent: this deployment is recorded as never
    delivered and the chain moves past it, so the operator is asserting that the device state
    it was meant to establish is either already there or no longer wanted.
    """
    from nso_adapter.core.generation import GenerationNotBlocked, reconcile_generation

    head = await _blocked_head(db, device_id)
    try:
        await reconcile_generation(db, head.id)
    except GenerationNotBlocked:
        await db.rollback()
        raise api_error(409, "conflict", _HEAD_ALREADY_ACTED_ON) from None
    await db.commit()
    return {"generation_id": head.id, "seq": head.seq, "job_id": head.job_id}


@router.get(
    "/{device_id}/actions/apply-diff",
    dependencies=[Depends(verify_token)],
    response_model=ApplyDiffOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_400, **RESP_422_VALIDATION},
)
async def action_apply_diff(
    device_id: int,
    outformat: str = "native",
    db: AsyncSession = Depends(get_db),
):
    """Preview the per-scope diff the next Apply would push (NSO dry-run, no commit).

    ``outformat=native`` (default): device-native rendering (CLI lines for cli NEDs,
    edit-config XML for netconf NEDs). ``outformat=cli``: NSO's NED-uniform ``+``/``-``
    tree diff — the "diff -u" style the preview panel renders.
    """
    from nso_adapter.core.apply import collect_apply_diff

    if outformat not in ("native", "cli"):
        raise api_error(400, "bad_request", f"Unknown outformat {outformat!r} (native|cli)")
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    diffs = await collect_apply_diff(db, device_id, outformat=outformat)
    return {"device_id": device_id, "outformat": outformat, "diffs": diffs}
