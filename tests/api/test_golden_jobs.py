# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — the jobs router (list / get).

EMIT-NULL shape: ``_job_out`` always emits all 11 keys, nullables (device_id,
result, error, context, started_at, heartbeat_at) as null. created_at/updated_at
are always-present "<iso>Z" strings; started_at/heartbeat_at are "<iso>Z" or null.
The goldens pin the full key set (before + after typing).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
TS_Z = "2026-06-01T10:00:00Z"


async def _seed_job(device_id: int, **overrides) -> int:
    from nso_adapter.store.models import Job, JobStatus, JobType

    async with session() as db:
        job = Job(
            job_type=JobType.apply,
            device_id=device_id,
            status=JobStatus.succeeded,
            result={"ok": True},
            error=None,
            context={"scope": "bgp"},
            created_at=TS,
            updated_at=TS,
            started_at=TS,
            heartbeat_at=TS,
            **overrides,
        )
        db.add(job)
        await db.commit()
        return job.id
    raise AssertionError("unreachable")


@pytest.mark.anyio
async def test_get_job_maximal_golden(adapter_client):
    device_id = await seed_device(nso_device_name="job-dev", netbox_device_id=201)
    job_id = await _seed_job(device_id)

    body = (await adapter_client.get(f"/api/v1/jobs/{job_id}", headers=AUTH)).json()

    assert body == {
        "id": job_id,
        "type": "apply",
        "device_id": device_id,
        "status": "succeeded",
        "result": {"ok": True},
        "error": None,
        "context": {"scope": "bgp"},
        "created_at": TS_Z,
        "updated_at": TS_Z,
        "started_at": TS_Z,
        "heartbeat_at": TS_Z,
    }


@pytest.mark.anyio
async def test_list_jobs_nullable_golden(adapter_client):
    """A queued job with no device / result / started_at → those keys present as null."""
    from nso_adapter.store.models import Job, JobStatus, JobType

    async with session() as db:
        job = Job(job_type=JobType.provision, status=JobStatus.queued, created_at=TS, updated_at=TS)
        db.add(job)
        await db.commit()
        job_id = job.id

    body = (await adapter_client.get("/api/v1/jobs", headers=AUTH)).json()

    assert body == [
        {
            "id": job_id,
            "type": "provision",
            "device_id": None,
            "status": "queued",
            "result": None,
            "error": None,
            "context": None,
            "created_at": TS_Z,
            "updated_at": TS_Z,
            "started_at": None,
            "heartbeat_at": None,
        }
    ]
