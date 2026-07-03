# SPDX-License-Identifier: Apache-2.0
"""Jobs API — list and get job status."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Job, JobStatus

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


def _job_out(j: Job) -> dict:
    return {
        "id": j.id,
        "type": j.job_type.value,
        "device_id": j.device_id,
        "status": j.status.value,
        "result": j.result,
        "error": j.error,
        "created_at": j.created_at.isoformat() + "Z",
        "updated_at": j.updated_at.isoformat() + "Z",
        "started_at": j.started_at.isoformat() + "Z" if j.started_at else None,
        "heartbeat_at": j.heartbeat_at.isoformat() + "Z" if j.heartbeat_at else None,
    }


@router.get("", dependencies=[Depends(verify_token)])
async def list_jobs(
    device_id: int | None = None,
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(Job).order_by(Job.created_at.desc()).limit(100)
    if device_id is not None:
        query = query.where(Job.device_id == device_id)
    if status is not None:
        try:
            js = JobStatus(status)
        except ValueError:
            raise api_error(422, "validation_error", f"Invalid job status: {status!r}")
        query = query.where(Job.status == js)
    result = await db.execute(query)
    return [_job_out(j) for j in result.scalars().all()]


@router.get("/{job_id}", dependencies=[Depends(verify_token)])
async def get_job(job_id: int, db: AsyncSession = Depends(get_db)):
    job = await db.get(Job, job_id)
    if not job:
        raise api_error(404, "not_found", "Job not found")
    return _job_out(job)
