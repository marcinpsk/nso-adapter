# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/jobs.py — enqueue_job, _run_with_db, _run_connect, runners."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import BackgroundTasks

from nso_adapter.core.jobs import (
    _run_apply,
    _run_connect,
    _run_detect_drift,
    _run_sync,
    _run_with_db,
    enqueue_job,
    get_active_job,
)
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, Job, JobStatus, JobType


async def _seed_device(nso_device_name: str = "test-rtr", netbox_id: int = 1) -> int:
    """Insert a device and return its id."""
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id
    raise RuntimeError("no session")


async def _seed_job(device_id: int, status: JobStatus = JobStatus.queued) -> int:
    """Insert a job and return its id."""
    async for db in get_session():
        j = Job(job_type=JobType.sync, device_id=device_id, status=status)
        db.add(j)
        await db.commit()
        await db.refresh(j)
        return j.id
    raise RuntimeError("no session")


# ── get_active_job ────────────────────────────────────────────────────────────


async def test_get_active_job_returns_queued(adapter_client):
    """Returns queued job for device."""
    device_id = await _seed_device("rtr-01", 11)
    job_id = await _seed_job(device_id, JobStatus.queued)

    async for db in get_session():
        result = await get_active_job(device_id, db)
        assert result is not None
        assert result.id == job_id
        break


async def test_get_active_job_returns_none_when_succeeded(adapter_client):
    """Returns None when all jobs are in terminal states."""
    device_id = await _seed_device("rtr-02", 12)
    await _seed_job(device_id, JobStatus.succeeded)

    async for db in get_session():
        result = await get_active_job(device_id, db)
        assert result is None
        break


async def test_get_active_job_returns_running_job(adapter_client):
    """Returns running job (not just queued)."""
    device_id = await _seed_device("rtr-03", 13)
    await _seed_job(device_id, JobStatus.running)

    async for db in get_session():
        result = await get_active_job(device_id, db)
        assert result is not None
        assert result.status == JobStatus.running
        break


# ── enqueue_job ───────────────────────────────────────────────────────────────


async def test_enqueue_job_creates_new_job(adapter_client):
    """enqueue_job creates a new job and schedules it."""
    device_id = await _seed_device("rtr-04", 14)

    bg = MagicMock(spec=BackgroundTasks)
    async for db in get_session():
        with patch("nso_adapter.core.jobs._run_sync"):
            job, created = await enqueue_job(device_id, JobType.sync, db, bg)
            assert created is True
            assert job.status == JobStatus.queued
            bg.add_task.assert_called_once()
        break


async def test_enqueue_job_returns_existing_when_active(adapter_client):
    """enqueue_job returns existing active job with created=False."""
    device_id = await _seed_device("rtr-05", 15)
    existing_id = await _seed_job(device_id, JobStatus.queued)

    bg = MagicMock(spec=BackgroundTasks)
    async for db in get_session():
        job, created = await enqueue_job(device_id, JobType.sync, db, bg)
        assert created is False
        assert job.id == existing_id
        bg.add_task.assert_not_called()
        break


async def test_enqueue_job_raises_on_unknown_type(adapter_client):
    """enqueue_job raises ValueError for unregistered job type."""
    device_id = await _seed_device("rtr-06", 16)
    bg = MagicMock(spec=BackgroundTasks)

    async for db in get_session():
        # Patch _JOB_RUNNERS to return None for any job type
        with patch("nso_adapter.core.jobs._JOB_RUNNERS", {}):
            with pytest.raises(ValueError, match="No runner registered"):
                await enqueue_job(device_id, JobType.sync, db, bg)
        break


# ── _run_with_db ──────────────────────────────────────────────────────────────


async def test_run_with_db_success(adapter_client):
    """_run_with_db marks job succeeded and stores result."""
    device_id = await _seed_device("rtr-10", 20)
    job_id = await _seed_job(device_id, JobStatus.queued)

    async def success_factory(dev_id, db):
        return {"outcome": "ok"}

    await _run_with_db(job_id, device_id, success_factory)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result == {"outcome": "ok"}
        break


async def test_run_with_db_failure(adapter_client):
    """_run_with_db marks job failed on exception."""
    device_id = await _seed_device("rtr-11", 21)
    job_id = await _seed_job(device_id, JobStatus.queued)

    async def fail_factory(dev_id, db):
        raise RuntimeError("something broke")

    await _run_with_db(job_id, device_id, fail_factory)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert "something broke" in job.error["message"]
        break


async def test_run_with_db_timeout(adapter_client):
    """_run_with_db marks job failed on timeout."""
    device_id = await _seed_device("rtr-12", 22)
    job_id = await _seed_job(device_id, JobStatus.queued)

    async def slow_factory(dev_id, db):
        await asyncio.sleep(9999)

    # Patch asyncio.wait_for in the module namespace to raise TimeoutError
    async def mock_wait_for(coro, timeout):
        coro.close()
        raise TimeoutError

    with patch("asyncio.wait_for", side_effect=mock_wait_for):
        await _run_with_db(job_id, device_id, slow_factory)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "timeout"
        break


async def test_run_with_db_job_not_found(adapter_client):
    """_run_with_db returns early when job doesn't exist."""
    device_id = await _seed_device("rtr-13", 23)

    async def success_factory(dev_id, db):
        return {"outcome": "ok"}

    # Should not raise — just return early
    await _run_with_db(99999, device_id, success_factory)


# ── _run_sync and _run_detect_drift ───────────────────────────────────────


async def test_run_sync_calls_run_with_db(adapter_client):
    """_run_sync delegates to _run_with_db with sync_device."""
    device_id = await _seed_device("rtr-20", 30)
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.jobs._run_with_db", new_callable=AsyncMock) as mock_run:
        await _run_sync(job_id, device_id)
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == job_id
        assert mock_run.call_args[0][1] == device_id


async def test_run_detect_drift_calls_run_with_db(adapter_client):
    """_run_detect_drift delegates to _run_with_db."""
    device_id = await _seed_device("rtr-21", 31)
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.jobs._run_with_db", new_callable=AsyncMock) as mock_run:
        await _run_detect_drift(job_id, device_id)
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == job_id


# ── _run_connect ──────────────────────────────────────────────────────────────


async def test_run_connect_success(adapter_client):
    """_run_connect marks job succeeded after NSO connect call."""
    device_id = await _seed_device("rtr-30", 40)
    job_id = await _seed_job(device_id)

    mock_client = MagicMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.actions.connect", new_callable=AsyncMock, return_value={"status": "ok"}),
    ):
        await _run_connect(job_id, device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break


async def test_run_connect_device_not_found(adapter_client):
    """_run_connect marks job failed when get_nso_client raises."""
    device_id = await _seed_device("rtr-31", 41)
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.importer.get_nso_client", side_effect=KeyError("nso-dev not found")):
        await _run_connect(job_id, device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        break


async def test_run_connect_device_not_in_db(adapter_client):
    """_run_connect marks job failed when device_id doesn't exist in DB."""
    # Seed a device just to have the job FK work, then use non-existent device_id
    device_id = await _seed_device("rtr-34", 44)
    job_id = await _seed_job(device_id)
    non_existent_device_id = 99998

    await _run_connect(job_id, non_existent_device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert "not found" in job.error["message"]
        break
    """_run_connect returns early when job id doesn't exist in DB."""
    device_id = await _seed_device("rtr-32", 42)
    # Don't seed a job — use a non-existent job_id
    await _run_connect(99999, device_id)  # should not raise


async def test_run_connect_timeout(adapter_client):
    """_run_connect marks job failed on timeout."""
    device_id = await _seed_device("rtr-33", 43)
    job_id = await _seed_job(device_id)

    async def mock_wait_for(coro, timeout):
        coro.close()
        raise TimeoutError

    mock_client = MagicMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("asyncio.wait_for", side_effect=mock_wait_for),
    ):
        await _run_connect(job_id, device_id)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "timeout"
        break


# ── _run_apply ────────────────────────────────────────────────────────────────


async def test_run_apply_calls_run_apply(adapter_client):
    """_run_apply delegates to core.apply.run_apply."""
    device_id = await _seed_device("rtr-40", 50)
    job_id = await _seed_job(device_id)

    with patch("nso_adapter.core.apply.run_apply", new_callable=AsyncMock) as mock_run:
        await _run_apply(job_id, device_id)
        mock_run.assert_called_once_with(job_id, device_id, force=True)
