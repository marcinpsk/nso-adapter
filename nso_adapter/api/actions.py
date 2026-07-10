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
from nso_adapter.api.errors import api_error
from nso_adapter.core.jobs import enqueue_job
from nso_adapter.store.models import Device, JobType

router = APIRouter(prefix="/api/v1/devices", tags=["actions"])


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


@router.post("/{device_id}/actions/force-removal", status_code=202, dependencies=[Depends(verify_token)])
async def action_force_removal(
    device_id: int,
    body: ForceRemovalBody,
    db: AsyncSession = Depends(get_db),
):
    """Re-run a scope's removal with the collateral guard DISABLED.

    The operator override for a ``removal_blocked_collateral`` failure: after
    reviewing the blocked job's orphan list + dry-run preview, this deliberately
    flushes the orphaned service rows (PUT-replace with only the remaining intent).
    """
    from nso_adapter.core.removal import VALID_REMOVAL_SCOPES, enqueue_removal

    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    if body.scope not in VALID_REMOVAL_SCOPES:
        raise api_error(400, "bad_request", f"Unknown removal scope {body.scope!r}")
    job = await enqueue_removal(db, device_id, body.scope, force=True)
    await db.commit()
    return {"job_id": job.id}


@router.post("/{device_id}/actions/sync", status_code=202, dependencies=[Depends(verify_token)])
async def action_sync(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    return await _trigger(device_id, JobType.sync, db)


@router.post("/{device_id}/actions/detect-drift", status_code=202, dependencies=[Depends(verify_token)])
async def action_detect_drift(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    return await _trigger(device_id, JobType.detect_drift, db)


@router.post("/{device_id}/actions/connect", status_code=202, dependencies=[Depends(verify_token)])
async def action_connect(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    return await _trigger(device_id, JobType.connect, db)


@router.post("/{device_id}/sync-notify", status_code=202, dependencies=[Depends(verify_token)])
async def sync_notify(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Handle the NetBox plugin's notification that scope or intent changed for this device.

    Triggers an immediate sync job. If a job is already running, returns 409 with
    the existing job_id so the plugin can poll for the result.
    """
    return await _trigger(device_id, JobType.sync, db)


@router.post("/{device_id}/actions/apply", status_code=202, dependencies=[Depends(verify_token)])
async def action_apply(
    device_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Phase 2 — push accepted NetBox intent to NSO via reconcile-commit service."""
    return await _trigger(device_id, JobType.apply, db)


@router.get("/{device_id}/actions/apply-diff", dependencies=[Depends(verify_token)])
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
