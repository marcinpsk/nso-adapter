# SPDX-License-Identifier: Apache-2.0
"""Jobs API — list and get job status.

``GET /api/v1/jobs`` serves two readings of the same rows. The default page is the job
list the UI and the plugin's existing consumers read: the newest 100, ``created_at DESC,
id DESC``. Adding ``order=asc`` turns it into the settlement feed (Appendix S §3.4), which
walks ONE device's terminal jobs in the order they became true — ``settle_seq`` ascending
from a caller-held cursor.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404, RESP_422_VALIDATION, api_error
from nso_adapter.api.timestamps import iso_z
from nso_adapter.store.meta import get_store_incarnation
from nso_adapter.store.models import Job, JobStatus

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])

#: The default page size, unchanged from before the feed existed (S3.4).
DEFAULT_PAGE = 100
LIMIT_MIN = 1
LIMIT_MAX = 500

STORE_INCARNATION_HEADER = "X-Store-Incarnation"


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
    # NULL until the job settles: a queued or running job has no place in the feed yet.
    settle_seq: int | None


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
        "settle_seq": j.settle_seq,
    }


def stamp_store_incarnation(response: Response) -> None:
    """Carry the live store incarnation on the response, not on the rows (§3.4, r2-M5).

    A consumer resets its cursor when the store it belongs to is gone, and the page that
    proves it is gone is an EMPTY one — a cursor past the end of a restarted sequence
    returns no rows at all. A per-row field would therefore say nothing in the one state
    where the decision is made, and an envelope would change the body the existing
    consumers read. The value is a process-cached read, so it costs no query.
    """
    response.headers[STORE_INCARNATION_HEADER] = get_store_incarnation()[0]


_RESP_200_INCARNATION = {
    200: {
        "headers": {
            STORE_INCARNATION_HEADER: {
                "description": "The live store incarnation; a change means the cursor belongs to a dead store.",
                "schema": {"type": "string"},
            }
        }
    }
}


@router.get(
    "",
    dependencies=[Depends(verify_token), Depends(stamp_store_incarnation)],
    response_model=list[JobOut],
    responses={**_RESP_200_INCARNATION, **RESP_401, **RESP_422_VALIDATION},
)
async def list_jobs(
    device_id: int | None = None,
    status: str | None = None,
    order: Literal["asc", "desc"] = "desc",
    after_settle_seq: int | None = None,
    limit: int = DEFAULT_PAGE,
    db: AsyncSession = Depends(get_db),
):
    if (order == "asc" or after_settle_seq is not None) and device_id is None:
        # Sequences are allocated per device, so an unscoped ascending page would
        # interleave two devices' independent sequences into one order that is wrong for
        # both. Reject rather than coerce a scope the caller did not choose.
        raise api_error(
            422,
            "validation_error",
            "order=asc and after_settle_seq require device_id: the settlement sequence is per device",
        )
    if not LIMIT_MIN <= limit <= LIMIT_MAX:
        # Not clamped: a caller that asked for 5000 and silently received 500 believes it
        # holds the whole page, and advances its cursor as if it did.
        raise api_error(422, "validation_error", f"limit must be between {LIMIT_MIN} and {LIMIT_MAX}: {limit}")

    query = select(Job).limit(limit)
    if device_id is not None:
        query = query.where(Job.device_id == device_id)
    if status is not None:
        try:
            js = JobStatus(status)
        except ValueError:
            raise api_error(422, "validation_error", f"Invalid job status: {status!r}")
        query = query.where(Job.status == js)
    # An ascending page always walks from a cursor; absent, that cursor is the start of the
    # sequence. The predicate is also the visibility rule: `settle_seq > :cursor` is
    # NULL-false, so a queued or running job cannot be paged over and consumed as a result
    # it does not have (P0.6). No `id` tiebreak — `(device_id, settle_seq)` is unique.
    cursor = 0 if (after_settle_seq is None and order == "asc") else after_settle_seq
    if cursor is not None:
        query = query.where(Job.settle_seq > cursor)
    if order == "asc":
        query = query.order_by(Job.settle_seq.asc())
    else:
        # `created_at` is transaction time, so a single tick can hold several jobs and
        # ordering on it alone leaves them in whatever order the plan happens to emit — two
        # consumers polling the same device can then see different orders. The `id` tiebreak
        # makes the existing descending page deterministic; it changes no row or direction.
        query = query.order_by(Job.created_at.desc(), Job.id.desc())
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
