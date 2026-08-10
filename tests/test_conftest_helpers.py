# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Contract tests for the shared seeding helpers in ``tests/conftest.py``.

The helpers are used by hundreds of tests, so a helper that only works under the store's
CURRENT session settings turns one settings change into a suite-wide failure with no
bearing on the behaviour under test.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.store import db as store_db
from nso_adapter.store.models import Job, JobStatus, JobType
from tests.conftest import session, start_job


async def test_start_job_returns_the_attempt_under_an_expiring_session(store_engine, monkeypatch):
    """``start_job`` must not read the Job row after its own commit.

    The store factory sets ``expire_on_commit=False`` today. With expiry ON (SQLAlchemy's
    default), the committed instance is expired and reading ``run_attempt`` afterwards
    starts a lazy load outside the greenlet: ``MissingGreenlet``, not an attempt.
    """
    async with session() as db:
        job = Job(job_type=JobType.provision, device_id=None, status=JobStatus.queued, context={})
        db.add(job)
        await db.commit()
        job_id = job.id

    monkeypatch.setattr(store_db, "_session_factory", async_sessionmaker(store_engine, expire_on_commit=True))
    assert await start_job(job_id) == 1

    async with session() as db:
        started = await db.get(Job, job_id)
        assert started.status is JobStatus.running and started.run_attempt == 1
