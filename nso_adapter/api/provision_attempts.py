# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Provision attempts API: the evidence of one plugin provision attempt."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404, RESP_422_VALIDATION, api_error
from nso_adapter.core.jobs import get_provision_attempt_job
from nso_adapter.core.provision_attempt import ProvisionAttemptEvidence

router = APIRouter(prefix="/api/v1/provision-attempts", tags=["devices"])


@router.get(
    "/{provision_attempt_id}",
    dependencies=[Depends(verify_token)],
    response_model=ProvisionAttemptEvidence,
    responses={**RESP_401, **RESP_404, **RESP_422_VALIDATION},
)
async def get_provision_attempt(provision_attempt_id: UUID, db: AsyncSession = Depends(get_db)):
    """Return the attempt's status, job, result and error; ``404`` when no job carries the id."""
    job = await get_provision_attempt_job(provision_attempt_id, db)
    if job is None:
        raise api_error(404, "not_found", "Provision attempt not found")
    return ProvisionAttemptEvidence.from_job(job)
