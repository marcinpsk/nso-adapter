# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
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
from nso_adapter.api.errors import RESP_401, RESP_404, RESP_422_VALIDATION, ResponseSpec, api_error
from nso_adapter.api.execution import (
    AttributedRefusalOut,
    EvidenceEnvelope,
    RowOutcomeOut,
    evidence_envelope,
    evidence_projection,
    validate_execution_result,
)
from nso_adapter.api.pagination import DEFAULT_PAGE, validate_page_limit
from nso_adapter.store.meta import get_store_incarnation
from nso_adapter.store.models import Job, JobStatus

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])

STORE_INCARNATION_HEADER = "X-Store-Incarnation"


class JobOut(BaseModel):
    """Emit every job field and retain null values."""

    model_config = {"extra": "forbid"}

    id: int
    type: str
    device_id: int | None
    status: str
    result: dict | None
    row_outcomes: list[RowOutcomeOut] | None
    refusals: list[AttributedRefusalOut] | None
    error: dict | None
    context: dict | None
    created_at: str
    updated_at: str
    started_at: str | None
    heartbeat_at: str | None
    # NULL until the job settles: a queued or running job has no place in the feed yet.
    settle_seq: int | None


class JobIdentityOut(BaseModel):
    model_config = {"extra": "forbid"}

    id: int
    device_id: int | None
    settle_seq: int | None


class JobEvidenceOut(EvidenceEnvelope[JobIdentityOut, JobOut]):
    pass


def _job_out(j: Job) -> dict:
    identity = {"id": j.id, "device_id": j.device_id, "settle_seq": j.settle_seq}
    candidate = {
        "id": j.id,
        "type": j.job_type.value,
        "device_id": j.device_id,
        "status": j.status.value,
        "result": j.result,
        "error": j.error,
        "context": j.context,
        "created_at": j.created_at,
        "updated_at": j.updated_at,
        "started_at": j.started_at,
        "heartbeat_at": j.heartbeat_at,
        "settle_seq": j.settle_seq,
    }

    return evidence_envelope(
        identity,
        candidate,
        JobOut,
        construct=lambda value: value.update(evidence_projection(j.result)),
        validate=lambda value: (
            validate_execution_result(
                value["result"],
                required=j.job_type.value in ("apply", "removal") and j.status.value in ("succeeded", "failed"),
            )
            if j.job_type.value in ("apply", "removal")
            else None
        ),
    )


def stamp_store_incarnation(response: Response) -> None:
    """Carry the store incarnation on every response, including empty pages.

    A cursor past the end of a rebuilt store can return no rows. The header lets the
    consumer detect that rebuild and reset its cursor. Startup caches this store identity.
    """
    response.headers[STORE_INCARNATION_HEADER] = get_store_incarnation()[0]


_RESP_200_INCARNATION: ResponseSpec = {
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
    response_model=list[JobEvidenceOut],
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
    if after_settle_seq is not None and order != "asc":
        # The cursor names a position in the settlement order; the descending page is in
        # creation order. Serving the mix lets a consumer skip or repeat settlements.
        raise api_error(
            422,
            "validation_error",
            "after_settle_seq requires order=asc: the cursor walks the settlement order",
        )
    if status is not None and order == "asc":
        # A thinned ascending page still advances the cursor over the filtered-out rows,
        # making those settlements permanently invisible to that cursor.
        raise api_error(
            422,
            "validation_error",
            "status cannot filter the ascending settlement feed; use the descending list",
        )
    query = select(Job).limit(validate_page_limit(limit))
    if device_id is not None:
        query = query.where(Job.device_id == device_id)
    if status is not None:
        invalid = None
        try:
            js = JobStatus(status)
        except ValueError:
            # Built in the handler, raised after it: a raise inside attaches the caught exception.
            invalid = api_error(422, "validation_error", "Invalid job status")
        if invalid is not None:
            raise invalid
        query = query.where(Job.status == js)
    # An ascending page always walks from a cursor; absent, that cursor is the start of the
    # sequence. The predicate is also the visibility rule: `settle_seq > :cursor` is
    # NULL-false, so a queued or running job cannot be paged over and consumed as a result
    # it does not have (P0.6). No `id` tiebreak — `(device_id, settle_seq)` is unique.
    if order == "asc":
        query = query.where(Job.settle_seq > (after_settle_seq if after_settle_seq is not None else 0))
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
    response_model=JobEvidenceOut,
    responses={**RESP_401, **RESP_404, **RESP_422_VALIDATION},
)
async def get_job(job_id: int, db: AsyncSession = Depends(get_db)):
    job = await db.get(Job, job_id)
    if not job:
        raise api_error(404, "not_found", "Job not found")
    return _job_out(job)
