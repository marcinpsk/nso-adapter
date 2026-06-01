# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for jobs.py endpoint functions."""

from __future__ import annotations

import pytest

from nso_adapter.api.errors import ApiError
from nso_adapter.api.jobs import get_job, list_jobs
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, Job, JobStatus, JobType


async def _seed_device_and_job(nso_device_name: str, netbox_id: int, status: JobStatus = JobStatus.succeeded):
    """Seed a device and a completed job; return (device_id, job_id)."""
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.flush()
        j = Job(device_id=d.id, job_type=JobType.sync, status=status)
        db.add(j)
        await db.commit()
        await db.refresh(j)
        return d.id, j.id
    raise RuntimeError("no DB session")


# ── list_jobs ─────────────────────────────────────────────────────────────────


async def test_list_jobs_all(adapter_client):
    """list_jobs() returns all jobs, most recent first."""
    device_id, job_id = await _seed_device_and_job("jobs-list-01", 1400)
    async for db in get_session():
        result = await list_jobs(db=db)
        assert any(r["id"] == job_id for r in result)
        break


async def test_list_jobs_filter_by_device_id(adapter_client):
    """list_jobs(device_id=X) returns only jobs for that device."""
    device_id, job_id = await _seed_device_and_job("jobs-list-02", 1410)
    async for db in get_session():
        result = await list_jobs(device_id=device_id, db=db)
        assert all(r["device_id"] == device_id for r in result)
        assert any(r["id"] == job_id for r in result)
        break


async def test_list_jobs_filter_by_status(adapter_client):
    """list_jobs(status='succeeded') returns only succeeded jobs."""
    device_id, job_id = await _seed_device_and_job("jobs-list-03", 1420, status=JobStatus.succeeded)
    async for db in get_session():
        result = await list_jobs(status="succeeded", db=db)
        assert all(r["status"] == "succeeded" for r in result)
        break


async def test_list_jobs_filter_invalid_status_raises_422(adapter_client):
    """list_jobs() raises 422 on unrecognised status string."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await list_jobs(status="bad-status", db=db)
        assert exc_info.value.status_code == 422
        break


# ── get_job ───────────────────────────────────────────────────────────────────


async def test_get_job_not_found(adapter_client):
    """get_job() raises 404 for unknown job_id."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await get_job(job_id=99988, db=db)
        assert exc_info.value.status_code == 404
        break


async def test_get_job_found(adapter_client):
    """get_job() returns job dict with expected fields."""
    device_id, job_id = await _seed_device_and_job("jobs-get-01", 1430)
    async for db in get_session():
        result = await get_job(job_id=job_id, db=db)
        assert result["id"] == job_id
        assert result["type"] == "sync"
        assert result["device_id"] == device_id
        assert result["status"] == "succeeded"
        assert "created_at" in result
        assert "updated_at" in result
        break
