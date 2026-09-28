# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The provision-attempt evidence document.

The plugin reads this one document from two places: the attempt poll
(``GET /api/v1/provision-attempts/{id}``) and the ``provision-complete`` callback body.
Both sides build it here, so the two cannot drift.
"""

from __future__ import annotations

import uuid

from pydantic import BaseModel

from nso_adapter.store.models import Job, JobStatus


class ProvisionResult(BaseModel):
    """What ``core.onboarding.provision_nso_device`` returns for a job that ran."""

    ok: bool
    # NULL when the request carried no netbox_device_id: then no adapter mapping row exists.
    device_id: int | None
    steps: list[dict]


class ProvisionAttemptEvidence(BaseModel):
    """EMIT-NULL attempt shape: every key always present."""

    provision_attempt_id: uuid.UUID
    status: JobStatus
    job_id: int
    result: ProvisionResult | None
    error: dict | None

    @classmethod
    def from_job(cls, job: Job) -> ProvisionAttemptEvidence:
        """Build the evidence of one attempt from the provision job that carries it."""
        if job.provision_attempt_id is None:
            raise ValueError(f"job {job.id} carries no provision attempt")
        return cls(
            provision_attempt_id=job.provision_attempt_id,
            status=job.status,
            job_id=job.id,
            result=ProvisionResult.model_validate(job.result) if job.result is not None else None,
            error=job.error,
        )


class ProvisionAttemptConflict(Exception):
    """An admission the store refused; *job* is the provision that holds the key."""

    def __init__(self, reason: str, job: Job) -> None:
        super().__init__(reason)
        self.reason = reason
        self.job_id = job.id
        self.provision_attempt_id = job.provision_attempt_id
