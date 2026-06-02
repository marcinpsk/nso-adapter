# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/worker.py — the durable job worker (Layer B)."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from nso_adapter.core import worker
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, Job, JobStatus, JobType


async def _seed_device(nso_device_name: str = "wrk-rtr", netbox_id: int = 700) -> int:
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id
    raise RuntimeError("no session")


async def _seed_job(device_id: int, job_type: JobType, status: JobStatus) -> int:
    async for db in get_session():
        j = Job(job_type=job_type, device_id=device_id, status=status)
        db.add(j)
        await db.commit()
        await db.refresh(j)
        return j.id
    raise RuntimeError("no session")


async def _get_job(job_id: int) -> Job:
    async for db in get_session():
        return await db.get(Job, job_id)
    raise RuntimeError("no session")


# ── _claim_next_job ─────────────────────────────────────────────────────────────


async def test_claim_next_job_claims_oldest_queued(adapter_client):
    """_claim_next_job flips the oldest queued job to running and stamps started/heartbeat."""
    device_id = await _seed_device("wrk-claim", 701)
    first = await _seed_job(device_id, JobType.sync, JobStatus.queued)
    # Different device so dedup doesn't matter; just need a second, newer queued job.
    device2 = await _seed_device("wrk-claim2", 702)
    await _seed_job(device2, JobType.sync, JobStatus.queued)

    claimed = await worker._claim_next_job()
    assert claimed is not None
    job_id, claimed_device, job_type = claimed
    assert job_id == first  # oldest first
    assert claimed_device == device_id
    assert job_type == JobType.sync

    job = await _get_job(first)
    assert job.status == JobStatus.running
    assert job.started_at is not None
    assert job.heartbeat_at is not None


async def test_claim_next_job_returns_none_when_empty(adapter_client):
    """_claim_next_job returns None when there are no queued jobs."""
    device_id = await _seed_device("wrk-empty", 703)
    await _seed_job(device_id, JobType.sync, JobStatus.succeeded)
    assert await worker._claim_next_job() is None


# ── requeue_orphaned_jobs ───────────────────────────────────────────────────────


async def test_requeue_orphaned_requeues_idempotent(adapter_client):
    """Running sync/detect_drift/connect jobs are requeued; started/heartbeat cleared."""
    device_id = await _seed_device("wrk-orphan", 704)
    sync_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    dd_device = await _seed_device("wrk-orphan-dd", 705)
    dd_id = await _seed_job(dd_device, JobType.detect_drift, JobStatus.running)

    await worker.requeue_orphaned_jobs()

    for jid in (sync_id, dd_id):
        job = await _get_job(jid)
        assert job.status == JobStatus.queued
        assert job.started_at is None
        assert job.heartbeat_at is None


async def test_requeue_orphaned_fails_interrupted_apply(adapter_client):
    """A running apply is failed (never silently re-pushed), not requeued."""
    device_id = await _seed_device("wrk-orphan-apply", 706)
    apply_id = await _seed_job(device_id, JobType.apply, JobStatus.running)

    await worker.requeue_orphaned_jobs()

    job = await _get_job(apply_id)
    assert job.status == JobStatus.failed
    assert job.error["code"] == "orphaned"


async def test_requeue_orphaned_leaves_terminal_and_queued(adapter_client):
    """Queued jobs stay queued; terminal jobs are untouched."""
    device_id = await _seed_device("wrk-orphan-noop", 707)
    queued_id = await _seed_job(device_id, JobType.sync, JobStatus.queued)
    done_device = await _seed_device("wrk-orphan-done", 708)
    done_id = await _seed_job(done_device, JobType.sync, JobStatus.succeeded)

    await worker.requeue_orphaned_jobs()

    assert (await _get_job(queued_id)).status == JobStatus.queued
    assert (await _get_job(done_id)).status == JobStatus.succeeded


# ── _mark_failed ────────────────────────────────────────────────────────────────


async def test_mark_failed_sets_error(adapter_client):
    """_mark_failed flips a job to failed with the given error code."""
    device_id = await _seed_device("wrk-fail", 709)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)

    await worker._mark_failed(job_id, "boom", "it broke")

    job = await _get_job(job_id)
    assert job.status == JobStatus.failed
    assert job.error == {"code": "boom", "message": "it broke", "detail": {}}


async def test_mark_failed_missing_job_is_noop(adapter_client):
    """_mark_failed silently returns when the job doesn't exist."""
    await worker._mark_failed(99999, "x", "y")  # should not raise


# ── worker loop ─────────────────────────────────────────────────────────────────


async def test_worker_loop_drains_queue(adapter_client):
    """A worker claims a queued job and invokes the registered runner."""
    device_id = await _seed_device("wrk-loop", 710)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.queued)

    ran = asyncio.Event()
    seen: dict = {}

    async def fake_runner(jid: int, did: int) -> None:
        seen["job_id"] = jid
        seen["device_id"] = did
        # Runner owns the terminal status, mirroring the real runners.
        async for db in get_session():
            job = await db.get(Job, jid)
            job.status = JobStatus.succeeded
            await db.commit()
        ran.set()

    stop = asyncio.Event()
    with patch.dict("nso_adapter.core.jobs._JOB_RUNNERS", {JobType.sync: fake_runner}):
        task = asyncio.create_task(worker._worker_loop(0, stop))
        await asyncio.wait_for(ran.wait(), timeout=5.0)
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    assert seen == {"job_id": job_id, "device_id": device_id}
    assert (await _get_job(job_id)).status == JobStatus.succeeded


async def test_worker_loop_marks_failed_when_runner_raises(adapter_client):
    """If a runner raises, the worker fails the job rather than stranding it."""
    device_id = await _seed_device("wrk-loop-raise", 711)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.queued)

    raised = asyncio.Event()

    async def boom_runner(jid: int, did: int) -> None:
        raised.set()
        raise RuntimeError("kaboom")

    stop = asyncio.Event()
    with patch.dict("nso_adapter.core.jobs._JOB_RUNNERS", {JobType.sync: boom_runner}):
        task = asyncio.create_task(worker._worker_loop(0, stop))
        await asyncio.wait_for(raised.wait(), timeout=5.0)
        # Give the finally/_mark_failed a moment to commit.
        for _ in range(50):
            if (await _get_job(job_id)).status == JobStatus.failed:
                break
            await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    job = await _get_job(job_id)
    assert job.status == JobStatus.failed
    assert job.error["code"] == "internal"


async def test_start_and_stop_workers(adapter_client):
    """start_workers requeues orphans + launches the pool; stop_workers tears it down."""
    device_id = await _seed_device("wrk-pool", 712)
    orphan = await _seed_job(device_id, JobType.detect_drift, JobStatus.running)

    # No-op runner so nothing actually executes against NSO.
    async def noop_runner(jid: int, did: int) -> None:
        async for db in get_session():
            job = await db.get(Job, jid)
            job.status = JobStatus.succeeded
            await db.commit()

    with patch.dict(
        "nso_adapter.core.jobs._JOB_RUNNERS",
        {JobType.sync: noop_runner, JobType.detect_drift: noop_runner},
    ):
        await worker.start_workers(concurrency=2)
        assert len(worker._workers) == 2
        # The orphaned detect_drift was requeued and should drain to succeeded.
        for _ in range(100):
            if (await _get_job(orphan)).status == JobStatus.succeeded:
                break
            await asyncio.sleep(0.05)
        await worker.stop_workers()

    assert worker._workers == []
    assert (await _get_job(orphan)).status == JobStatus.succeeded
