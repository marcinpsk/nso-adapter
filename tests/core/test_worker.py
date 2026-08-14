# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/worker.py — the durable job worker (Layer B)."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from nso_adapter.core import worker
from nso_adapter.store.device_settle import create_counter
from nso_adapter.store.models import Device, Job, JobStatus, JobType
from tests.conftest import session


async def _seed_device(nso_device_name: str = "wrk-rtr", netbox_id: int = 700) -> int:
    async with session() as db:
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.flush()
        await create_counter(db, d.id)
        await db.commit()
        await db.refresh(d)
        return d.id


async def _seed_job(device_id: int, job_type: JobType, status: JobStatus) -> int:
    async with session() as db:
        j = Job(job_type=job_type, device_id=device_id, status=status)
        db.add(j)
        await db.commit()
        await db.refresh(j)
        return j.id


async def _get_job(job_id: int) -> Job:
    async with session() as db:
        return await db.get(Job, job_id)


async def test_generation_advancement_retries_transient_failures(monkeypatch):
    """A transient failure does not strand a pending successor until restart."""
    from nso_adapter.core import generation

    calls = 0
    delays: list[float] = []

    async def advance(_device_id: int) -> None:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("transient database failure")

    async def record_delay(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(generation, "advance_device_generations", advance)
    monkeypatch.setattr(worker.asyncio, "sleep", record_delay)

    await worker._advance_generations(17)

    assert calls == 3
    assert delays == [0.5, 1.0]


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
    job_id, claimed_device, job_type, reg = claimed
    assert job_id == first  # oldest first
    assert claimed_device == device_id
    assert job_type == JobType.sync

    job = await _get_job(first)
    assert job.status == JobStatus.running
    assert job.started_at is not None
    assert job.heartbeat_at is not None

    # The device is now CLAIMED, and the claim is associated with the job it started — in
    # the same transaction as the running transition, so no window exists where a running
    # job has an unassociated claim.
    assert reg.registered
    async with session() as db:
        from nso_adapter.store.models import DeviceClaim

        claim = await db.get(DeviceClaim, device_id)
        assert claim is not None
        assert claim.claim_token == reg.token
        assert claim.job_id == first


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


async def _set_heartbeat(job_id: int, hb) -> None:
    async with session() as db:
        job = await db.get(Job, job_id)
        job.heartbeat_at = hb
        await db.commit()


async def test_requeue_orphaned_leaves_live_heartbeating_job(adapter_client):
    """A running job with a FRESH heartbeat belongs to a live worker (rolling restart /
    two-process overlap) — startup recovery must NOT steal it (s3-4)."""
    from datetime import UTC, datetime

    device_id = await _seed_device("wrk-live-sync", 720)
    sync_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    apply_device = await _seed_device("wrk-live-apply", 721)
    apply_id = await _seed_job(apply_device, JobType.apply, JobStatus.running)
    now = datetime.now(UTC)
    await _set_heartbeat(sync_id, now)
    await _set_heartbeat(apply_id, now)

    await worker.requeue_orphaned_jobs()

    # Both are actively heartbeating → left running, not requeued/failed.
    assert (await _get_job(sync_id)).status == JobStatus.running
    assert (await _get_job(apply_id)).status == JobStatus.running


async def test_requeue_orphaned_recovers_stale_heartbeat(adapter_client):
    """A running job that NO claim covers, stale past the claimless cutoff, is recovered.

    The cutoff is the claimless one now, not 60s: a claimed job recovers through the claim
    scan instead, on one clock. Two clocks would let a still-valid holder overwrite the
    disposition a shorter clock had just written.
    """
    from datetime import UTC, datetime, timedelta

    from nso_adapter.core.claim import PROVISION_STALE_AFTER

    device_id = await _seed_device("wrk-stale-sync", 722)
    sync_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    apply_device = await _seed_device("wrk-stale-apply", 723)
    apply_id = await _seed_job(apply_device, JobType.apply, JobStatus.running)
    stale = datetime.now(UTC) - timedelta(seconds=PROVISION_STALE_AFTER + 60)
    await _set_heartbeat(sync_id, stale)
    await _set_heartbeat(apply_id, stale)

    await worker.requeue_orphaned_jobs()

    assert (await _get_job(sync_id)).status == JobStatus.queued
    apply_job = await _get_job(apply_id)
    assert apply_job.status == JobStatus.failed
    assert apply_job.error["code"] == "orphaned"


async def test_requeue_orphaned_supersedes_behind_a_queued_successor(adapter_client):
    """The claimless reaper hits the same uniqueness slot: a queued same-type successor
    already occupies (device, type), so requeueing the stale row would raise and abort
    startup recovery wholesale. It must land failed/superseded instead."""
    from datetime import UTC, datetime, timedelta

    from nso_adapter.core.claim import PROVISION_STALE_AFTER

    device_id = await _seed_device("wrk-succ-sync", 724)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    successor_id = await _seed_job(device_id, JobType.sync, JobStatus.queued)
    await _set_heartbeat(job_id, datetime.now(UTC) - timedelta(seconds=PROVISION_STALE_AFTER + 60))

    await worker.requeue_orphaned_jobs()

    superseded = await _get_job(job_id)
    assert superseded.status == JobStatus.failed
    assert superseded.error["code"] == "superseded"
    assert (await _get_job(successor_id)).status == JobStatus.queued


async def test_periodic_reap_recovers_stale_orphan_but_spares_live(adapter_client):
    """The PERIODIC scheduler tick (not just startup) reaps a stale orphan while leaving a live
    heartbeating job untouched — proving the reaper is safe to run concurrently with the worker
    pool. This closes the 'orphan blocks the device forever' gap: without a periodic tick, a job
    stranded 'running' in a long-lived process (worker task killed mid-run, no restart) would make
    the queued-type dedupe treat it as in-flight and 409 every future job of that type until the next
    restart — the same failure the plugin's reconcile enqueue once had."""
    from datetime import UTC, datetime, timedelta

    from nso_adapter.core import scheduler

    live_device = await _seed_device("reap-live", 740)
    live_id = await _seed_job(live_device, JobType.sync, JobStatus.running)
    stale_device = await _seed_device("reap-stale", 741)
    stale_id = await _seed_job(stale_device, JobType.sync, JobStatus.running)
    apply_device = await _seed_device("reap-stale-apply", 742)
    apply_id = await _seed_job(apply_device, JobType.apply, JobStatus.running)

    now = datetime.now(UTC)
    from nso_adapter.core.claim import PROVISION_STALE_AFTER

    old = now - timedelta(seconds=PROVISION_STALE_AFTER + 60)
    await _set_heartbeat(live_id, now)  # fresh heartbeat → a live worker owns it
    await _set_heartbeat(stale_id, old)  # stale past the claimless cutoff → orphaned
    await _set_heartbeat(apply_id, old)

    await scheduler._scheduled_orphan_reap()

    assert (await _get_job(live_id)).status == JobStatus.running  # spared — never steal a live job
    assert (await _get_job(stale_id)).status == JobStatus.queued  # idempotent orphan → requeued
    apply_job = await _get_job(apply_id)
    assert apply_job.status == JobStatus.failed  # apply orphan → failed (never silently re-push)
    assert apply_job.error["code"] == "orphaned"


async def test_heartbeat_survives_transient_db_error(adapter_client, monkeypatch):
    """A transient DB error on one heartbeat tick must not silently kill the heartbeat task —
    otherwise the heartbeat goes stale under a live job and the reaper (s3-4) later steals it,
    breaking the module's 'hung job detectable via stale heartbeat' guarantee (s3-16)."""
    device_id = await _seed_device("hb-rtr", 730)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)

    monkeypatch.setattr(worker, "_HEARTBEAT_INTERVAL", 0.001)
    real_get_session = worker.get_session
    calls = {"n": 0}

    def flaky_get_session():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient db blip")  # first tick fails
        return real_get_session()

    monkeypatch.setattr(worker, "get_session", flaky_get_session)

    task = asyncio.create_task(worker._heartbeat(job_id))
    await asyncio.sleep(0.05)  # several ticks at the 1ms interval
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # The loop survived the failed first tick and stamped heartbeat_at on a later one.
    assert calls["n"] >= 2
    assert (await _get_job(job_id)).heartbeat_at is not None


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

    async def fake_runner(jid: int, did: int, _reg=None) -> None:
        seen["job_id"] = jid
        seen["device_id"] = did
        # Runner owns the terminal status, mirroring the real runners.
        async with session() as db:
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

    async def boom_runner(jid: int, did: int, _reg=None) -> None:
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
    async def noop_runner(jid: int, did: int, _reg=None) -> None:
        async with session() as db:
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


async def test_cancelled_worker_requeues_its_running_claim(adapter_client):
    """A2 (S5a, codex R2-F7 residual): a graceful-shutdown cancel mid-run returns the
    worker's own whitelisted claim to the queue, so the device isn't 409-blocked until
    the periodic reap tick's staleness window passes."""
    device_id = await _seed_device("wrk-cancel", 713)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.queued)

    started = asyncio.Event()

    async def hanging_runner(jid: int, did: int, _reg=None) -> None:
        started.set()
        await asyncio.sleep(60)

    stop = asyncio.Event()
    with patch.dict("nso_adapter.core.jobs._JOB_RUNNERS", {JobType.sync: hanging_runner}):
        task = asyncio.create_task(worker._worker_loop(0, stop))
        await asyncio.wait_for(started.wait(), timeout=5.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    job = await _get_job(job_id)
    assert job.status == JobStatus.queued
    assert job.started_at is None
    assert job.heartbeat_at is None


async def test_cancel_after_success_never_requeues(adapter_client):
    """A2 guard (codex R4-4): a commit can succeed and deliver the CancelledError before
    the await returns — the requeue must be status-guarded so a finished job never
    re-runs after restart."""
    device_id = await _seed_device("wrk-cancel-done", 714)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.queued)

    done = asyncio.Event()

    async def succeed_then_hang(jid: int, did: int, _reg=None) -> None:
        async with session() as db:
            job = await db.get(Job, jid)
            job.status = JobStatus.succeeded
            await db.commit()
        done.set()
        await asyncio.sleep(60)

    stop = asyncio.Event()
    with patch.dict("nso_adapter.core.jobs._JOB_RUNNERS", {JobType.sync: succeed_then_hang}):
        task = asyncio.create_task(worker._worker_loop(0, stop))
        await asyncio.wait_for(done.wait(), timeout=5.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert (await _get_job(job_id)).status == JobStatus.succeeded


async def test_ensure_workers_respawns_dead_worker(adapter_client):
    """A2 (codex R3-6): the periodic reap requeues stale jobs, but nothing drains them if
    the (unsupervised, created-once) pool has no live task — ensure_workers respawns."""

    async def noop_runner(jid: int, did: int, _reg=None) -> None:
        async with session() as db:
            job = await db.get(Job, jid)
            job.status = JobStatus.succeeded
            await db.commit()

    with patch.dict("nso_adapter.core.jobs._JOB_RUNNERS", {JobType.sync: noop_runner}):
        await worker.start_workers(concurrency=1)
        try:
            worker._workers[0].cancel()
            await asyncio.gather(worker._workers[0], return_exceptions=True)

            device_id = await _seed_device("wrk-respawn", 715)
            job_id = await _seed_job(device_id, JobType.sync, JobStatus.queued)

            worker.ensure_workers()

            for _ in range(100):
                if (await _get_job(job_id)).status == JobStatus.succeeded:
                    break
                await asyncio.sleep(0.05)
            assert (await _get_job(job_id)).status == JobStatus.succeeded
        finally:
            await worker.stop_workers()


async def test_ensure_workers_noops_once_stopping(adapter_client):
    """A2 (codex R4 note): shutdown sets the stop event before yielding — a reap tick
    landing mid-shutdown must not respawn a stray worker into a disposing process."""
    await worker.start_workers(concurrency=1)
    try:
        worker._stop.set()  # simulate: shutdown began, tasks not yet reaped
        worker._workers[0].cancel()
        await asyncio.gather(worker._workers[0], return_exceptions=True)

        worker.ensure_workers()

        assert worker._workers[0].done(), "no respawn once the stop event is set"
    finally:
        await worker.stop_workers()


async def test_orphan_reap_tick_ensures_workers(adapter_client):
    """A2 wiring: the periodic reap tick is the liveness driver — it must call
    ensure_workers so requeued orphans always have a drainer."""
    from unittest.mock import AsyncMock

    from nso_adapter.core.scheduler import _scheduled_orphan_reap

    with (
        patch("nso_adapter.core.worker.requeue_orphaned_jobs", new_callable=AsyncMock),
        patch("nso_adapter.core.worker.ensure_workers") as ew,
    ):
        await _scheduled_orphan_reap()

    ew.assert_called_once()


def test_sync_now_is_requeue_safe_and_runnable():
    """READSEM S3 B7 (codex R1-F10): a process death mid-sync_now must not block the device
    forever — the grain-c refresh is an idempotent read, so recovery requeues it; and the
    runner registry knows the type (enqueue_job rejects unknowns).

    The policy now has ONE expression, ``disposition_for``; the worker's duplicate set is gone.
    """
    from nso_adapter.core.claim import disposition_for
    from nso_adapter.core.jobs import _JOB_RUNNERS
    from nso_adapter.store.models import JobStatus, JobType

    assert disposition_for(JobType.sync_now) is JobStatus.queued
    assert JobType.sync_now in _JOB_RUNNERS
    # S5a B: same guarantees for the comprehensive CDB-only read (idempotent mirror job).
    assert disposition_for(JobType.sync_from_nso) is JobStatus.queued
    assert JobType.sync_from_nso in _JOB_RUNNERS
