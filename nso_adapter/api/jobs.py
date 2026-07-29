# SPDX-License-Identifier: Apache-2.0
"""Jobs API — list and get job status."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404, RESP_422_VALIDATION, api_error
from nso_adapter.api.timestamps import iso_z
from nso_adapter.store.models import Job, JobStatus

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


class JobOut(BaseModel):
    """EMIT-NULL job shape — every key always present, nullables emitted as null."""

    id: int
    type: str
    device_id: int | None
    status: str
    result: dict | None
    error: dict | None
    context: dict | None
    created_at: str
    updated_at: str
    started_at: str | None
    heartbeat_at: str | None


def _job_out(j: Job) -> dict:
    return {
        "id": j.id,
        "type": j.job_type.value,
        "device_id": j.device_id,
        "status": j.status.value,
        "result": j.result,
        "error": j.error,
        "context": j.context,
        "created_at": iso_z(j.created_at),
        "updated_at": iso_z(j.updated_at),
        "started_at": iso_z(j.started_at),
        "heartbeat_at": iso_z(j.heartbeat_at),
    }


@router.get(
    "",
    dependencies=[Depends(verify_token)],
    response_model=list[JobOut],
    responses={**RESP_401, **RESP_422_VALIDATION},
)
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


@router.get(
    "/{job_id}",
    dependencies=[Depends(verify_token)],
    response_model=JobOut,
    responses={**RESP_401, **RESP_404, **RESP_422_VALIDATION},
)
async def get_job(job_id: int, db: AsyncSession = Depends(get_db)):
    job = await db.get(Job, job_id)
    if not job:
        raise api_error(404, "not_found", "Job not found")
    return _job_out(job)
