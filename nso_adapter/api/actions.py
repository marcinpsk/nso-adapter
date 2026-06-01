# SPDX-License-Identifier: Apache-2.0
"""Actions API — async device actions (sync, check-sync_state, connect, apply, sync-notify).

All actions return 202 with {job_id}.
409 is returned if a job is already queued/running for the device.
"""

from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends
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
    background_tasks: BackgroundTasks,
) -> dict:
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    job, created = await enqueue_job(device_id, job_type, db, background_tasks)
    if not created:
        raise api_error(409, "conflict", "A job is already running for this device", {"job_id": job.id})
    return {"job_id": job.id}


@router.post("/{device_id}/actions/sync", status_code=202, dependencies=[Depends(verify_token)])
async def action_sync(
    device_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    return await _trigger(device_id, JobType.sync, db, background_tasks)


@router.post("/{device_id}/actions/detect-drift", status_code=202, dependencies=[Depends(verify_token)])
async def action_detect_drift(
    device_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    return await _trigger(device_id, JobType.detect_drift, db, background_tasks)


@router.post("/{device_id}/actions/connect", status_code=202, dependencies=[Depends(verify_token)])
async def action_connect(
    device_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    return await _trigger(device_id, JobType.connect, db, background_tasks)


@router.post("/{device_id}/sync-notify", status_code=202, dependencies=[Depends(verify_token)])
async def sync_notify(
    device_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Called by the NetBox plugin when scope or intent changes for this device.

    Triggers an immediate sync job. If a job is already running, returns 409 with
    the existing job_id so the plugin can poll for the result.
    """
    return await _trigger(device_id, JobType.sync, db, background_tasks)


@router.post("/{device_id}/actions/apply", status_code=202, dependencies=[Depends(verify_token)])
async def action_apply(
    device_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Phase 2 — push accepted NetBox intent to NSO via reconcile-commit service."""
    return await _trigger(device_id, JobType.apply, db, background_tasks)
