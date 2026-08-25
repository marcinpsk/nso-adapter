# SPDX-License-Identifier: Apache-2.0
"""Actions API: async device actions and deployment-generation barrier exits.

The device claim permits one executing device job. Admission is endpoint-specific:
ordinary triggers reject queued jobs of the requested type, Apply checks all queued
and running jobs after it finds promotable work, and barrier actions ignore unrelated jobs.
"""

from __future__ import annotations

import json
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import (
    RESP_400,
    RESP_401,
    RESP_404_DEVICE,
    RESP_409,
    RESP_409_APPLY_CONFLICT,
    RESP_409_QUEUED_ACTION,
    RESP_422_VALIDATION,
    RESP_500_INTERNAL,
    api_error,
)
from nso_adapter.core.jobs import enqueue_job
from nso_adapter.core.request_flags import MAX_PUSH_SEQ, MIN_PUSH_SEQ
from nso_adapter.store.models import DeploymentGeneration, Device, JobType

router = APIRouter(prefix="/api/v1/devices", tags=["actions"])

SelectedPushSequence = Annotated[
    int,
    Field(strict=True, ge=MIN_PUSH_SEQ, le=MAX_PUSH_SEQ),
]

# Trigger endpoints add their queued same-type conflict to the common action errors.
_ACTION_ERRORS = {**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION}
_TRIGGER_ERRORS = {**_ACTION_ERRORS, **RESP_409_QUEUED_ACTION}


class JobTriggerOut(BaseModel):
    """The async-action envelope: the id of the enqueued job (202)."""

    job_id: int


class ApplyDiffOut(BaseModel):
    """apply-diff preview — {scope: native_delta} for scopes with a non-empty change."""

    device_id: int
    outformat: str
    diffs: dict[str, str]


class ActionApplyIn(BaseModel):
    """The exact push sequences this manual Apply is allowed to promote."""

    model_config = ConfigDict(extra="forbid")

    apply_attempt_id: UUID
    selected: dict[str, SelectedPushSequence]

    @field_validator("selected")
    @classmethod
    def _validate_selected(cls, selected: dict[str, int]) -> dict[str, int]:
        from nso_adapter.core.projection import projection_streams
        from nso_adapter.store.apply_attempt_store import canonical_selected

        unknown = set(selected) - projection_streams()
        if unknown:
            raise ValueError(f"unknown projection streams: {sorted(unknown)}")
        return canonical_selected(selected)


class ActionApplyGenerationOut(BaseModel):
    generation_id: int
    seq: int
    job_id: int
    mode: Literal["networked", "detach"]
    source_push_seq: dict[str, int | None]
    stream_revisions: dict[str, int]
    digest: str


class ActionApplySkippedDetailOut(BaseModel):
    generation_id: int
    seq: int
    status: Literal["pending", "running", "failed", "outcome_unknown", "abandoned"]


class ActionApplyOut(BaseModel):
    device_id: int
    outcome: Literal["promoted", "no_op"]
    job_id: int | None = Field(default=None, exclude_if=lambda value: value is None)
    selected: dict[str, int]
    skipped: dict[
        str,
        Literal[
            "superseded",
            "already_applied",
            "already_authorized",
            "no_receipt",
            "backfill_only",
            "revision_mismatch",
        ],
    ]
    skipped_detail: dict[str, ActionApplySkippedDetailOut] | None
    generations: list[ActionApplyGenerationOut]


def _apply_http_response(status_code: int, body: dict) -> Response:
    """Serialize first delivery and replay with one byte-stable representation."""
    content = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return Response(content=content, status_code=status_code, media_type="application/json")


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
        raise api_error(
            409,
            "conflict",
            "A job of the requested type is already queued for this device",
            {"job_id": job.id},
        )
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
    job = await enqueue_removal(
        db,
        device_id,
        body.scope,
        marking=None,
        defer_retract=False,
        promotes=(),
        interfaces=body.interfaces,
        force=True,
    )
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

    A queued sync job returns 409 with its job id. A running sync job permits a queued
    successor, and jobs of other types do not refuse the notification.
    """
    return await _trigger(device_id, JobType.sync, db)


@router.post(
    "/{device_id}/actions/apply",
    status_code=202,
    dependencies=[Depends(verify_token)],
    response_model=ActionApplyOut,
    responses={
        200: {"model": ActionApplyOut, "description": "No selected stream required a job"},
        **_ACTION_ERRORS,
        **RESP_409_APPLY_CONFLICT,
        **RESP_500_INTERNAL,
    },
)
async def action_apply(
    device_id: int,
    body: ActionApplyIn,
    db: AsyncSession = Depends(get_db),
):
    """Promote selected pushes.

    Check queued and running jobs only when the selection contains promotable work.
    """
    from nso_adapter.core.generation import ApplyJobConflict, ApplyUnexecutable, create_action_apply, lock_projection
    from nso_adapter.core.request_flags import STORE_ONLY
    from nso_adapter.store.apply_attempt_store import (
        ApplyAttemptIdentityMismatch,
        begin_apply_attempt,
        complete_apply_attempt,
        replay_apply_attempt,
    )

    if STORE_ONLY.get():
        raise api_error(422, "validation_error", "store_only is not valid for the Apply action")

    # The UUID identity outranks the device lookup: an existing attempt POSTed at any
    # other device (even a nonexistent one) is an identity conflict, not a 404.
    try:
        stored = await replay_apply_attempt(db, body.apply_attempt_id, device_id, body.selected)
    except ApplyAttemptIdentityMismatch as exc:
        await db.rollback()
        raise api_error(
            409,
            "conflict",
            "Apply attempt UUID belongs to a different request identity",
            {"mismatch": exc.mismatch},
        ) from None
    if stored is not None:
        await db.rollback()
        return _apply_http_response(stored.http_status, stored.response)
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    await lock_projection(db, device_id)
    try:
        stored = await begin_apply_attempt(db, body.apply_attempt_id, device_id, body.selected)
    except ApplyAttemptIdentityMismatch as exc:
        await db.rollback()
        raise api_error(
            409,
            "conflict",
            "Apply attempt UUID belongs to a different request identity",
            {"mismatch": exc.mismatch},
        ) from None
    if stored is not None:
        await db.rollback()
        return _apply_http_response(stored.http_status, stored.response)
    try:
        async with db.begin_nested():
            apply_result = await create_action_apply(db, device_id, body.selected, body.apply_attempt_id)
    except ApplyJobConflict as exc:
        result = {
            "error": {
                "code": "conflict",
                "message": "A job is already queued or running for this device",
                "detail": {"job_id": exc.job_id},
            }
        }
        await complete_apply_attempt(
            db,
            body.apply_attempt_id,
            admission_state="rejected",
            http_status=409,
            response=result,
        )
        await db.commit()
        return _apply_http_response(409, result)
    except ApplyUnexecutable as exc:
        streams = sorted(exc.reasons)
        result = {
            "error": {
                "code": "apply_unexecutable",
                "message": f"Selected stream(s) cannot be applied faithfully: {', '.join(streams)}",
                "detail": {"streams": exc.reasons},
            }
        }
        await complete_apply_attempt(
            db,
            body.apply_attempt_id,
            admission_state="rejected",
            http_status=409,
            response=result,
        )
        await db.commit()
        return _apply_http_response(409, result)
    generations = [
        {
            "generation_id": generation.id,
            "seq": generation.seq,
            "job_id": generation.job_id,
            "mode": generation.mode.value,
            "source_push_seq": generation.source_push_seq,
            "stream_revisions": generation.stream_revisions,
            "digest": generation.digest,
        }
        for generation in apply_result.generations
    ]
    status_code = 202 if generations else 200
    job_id = generations[0]["job_id"] if generations else None
    if generations and job_id is None:
        raise api_error(500, "internal", "The promoted generation chain has no executable head job")
    result = {
        "device_id": device_id,
        "outcome": "promoted" if generations else "no_op",
        "selected": body.selected,
        "skipped": apply_result.skipped,
        "skipped_detail": apply_result.skipped_detail or None,
        "generations": generations,
    }
    if job_id is not None:
        result["job_id"] = job_id
    await complete_apply_attempt(
        db,
        body.apply_attempt_id,
        admission_state="admitted",
        http_status=status_code,
        response=result,
    )
    await db.commit()
    return _apply_http_response(status_code, result)


class BarrierActionIn(BaseModel):
    generation_id: int


class BarrierActionOut(BaseModel):
    """The job admitted after retrying or abandoning a blocked head."""

    generation_id: int
    seq: int
    job_id: int | None


#: What both barrier exits report when the head moved under them.
_HEAD_ALREADY_ACTED_ON = "This device's blocked deployment generation was already acted on"


async def _blocked_head(
    db: AsyncSession,
    device_id: int,
    expected_generation_id: int,
) -> DeploymentGeneration:
    """Return the device's blocked head, or 404/409 explaining why there is nothing to do.

    The head is read UNDER the device's projection lock, held to this request's commit. Both
    exits compare the current head with the generation named by the operator. The lock also
    orders these requests against intent pushes that create successors. It is the same lock
    every accepted projection write already takes.

    An offboard committing inside that lock raises ``DeviceProjectionGone``, which the app's
    own handler turns into this same 404 envelope for every route that can hit it.
    """
    from nso_adapter.core.generation import BLOCKED_STATUSES, executable_head, lock_projection

    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    await lock_projection(db, device_id)
    head = await executable_head(db, device_id)
    if head is not None and head.id != expected_generation_id:
        raise api_error(
            409,
            "conflict",
            "This request names a generation that is not the device's current head",
            {"head_generation_id": head.id, "head_status": head.status.value},
        )
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
    response_model=BarrierActionOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
async def action_retry_generation(
    device_id: int,
    body: BarrierActionIn,
    db: AsyncSession = Depends(get_db),
):
    """Re-admit this device's blocked deployment generation (#1522 §H2).

    One of the two explicit exits from the success barrier. The head is re-queued with the
    SAME document, mode and digest — it is re-sent, never rebuilt — and its successors go
    back to waiting behind it. Use it after fixing whatever the device refused.

    The required body names the generation to retry. A request naming a generation
    that is not the current head returns 409 with ``error.detail.head_generation_id``
    naming the head. A request with no blocked head returns 409 with
    ``error.detail.head_status``.
    """
    from nso_adapter.core.generation import GenerationNotBlocked, retry_generation

    head = await _blocked_head(db, device_id, body.generation_id)
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
    response_model=BarrierActionOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
async def action_abandon_generation(
    device_id: int,
    body: BarrierActionIn,
    db: AsyncSession = Depends(get_db),
):
    """Give up on this device's blocked generation so its successors may run (#1522 §H2).

    The other exit. Deliberately destructive of intent: this deployment is recorded as never
    delivered and the chain moves past it, so the operator is asserting that the device state
    it was meant to establish is either already there or no longer wanted. The response
    ``generation_id`` and ``seq`` identify the abandoned head. ``job_id`` identifies the
    successor carrier this action released, or is ``null`` when no successor is executable.

    The required body names the generation to abandon. A request naming a generation
    that is not the current head returns 409 with ``error.detail.head_generation_id``
    naming the head. A request with no blocked head returns 409 with
    ``error.detail.head_status``.
    """
    from nso_adapter.core.generation import GenerationNotBlocked, reconcile_generation

    head = await _blocked_head(db, device_id, body.generation_id)
    try:
        successor = await reconcile_generation(db, head.id)
    except GenerationNotBlocked:
        await db.rollback()
        raise api_error(409, "conflict", _HEAD_ALREADY_ACTED_ON) from None
    await db.commit()
    return {
        "generation_id": head.id,
        "seq": head.seq,
        "job_id": successor.id if successor else None,
    }


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
