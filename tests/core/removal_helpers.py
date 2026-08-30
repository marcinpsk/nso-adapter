# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Shared removal-job seed helpers."""

from __future__ import annotations

from nso_adapter.core.generation import attach_to_job, create_reissue_generation
from nso_adapter.store.models import GenerationMode, Job, JobStatus, JobType
from tests.conftest import session


async def seed_removal_job(device_id: int, context: dict) -> int:
    """Seed a started removal job carrying the REISSUE generation enqueue_removal would give it.

    A removal job with no generation at all is not a state production can reach (#1522 §G1),
    and both runners now refuse it. A reissue promotes no projection (``stream_revisions``
    empty), so the classification below still runs against the live store, which is what
    these cases are about.
    """
    full_context = {"scope": "static_route", **context}
    async with session() as db:
        generation = await create_reissue_generation(
            db,
            device_id,
            mode=GenerationMode.detach if full_context.get("detach") else GenerationMode.networked,
            removal_context=full_context,
            allowed_removal_keys=full_context.get("removed") or {},
        )
        # Started, at attempt 1: see seed_apply_job in test_static_route_put.
        job = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.running,
            coalescible=False,
            run_attempt=1,
            context=full_context,
        )
        db.add(job)
        await db.flush()
        await attach_to_job(db, generation, job)
        await db.commit()
        return job.id
