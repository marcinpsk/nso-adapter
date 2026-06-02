# SPDX-License-Identifier: Apache-2.0
"""Durable in-process job worker (Layer B).

Drains the ``jobs`` table: claims ``queued`` jobs atomically, runs the
registered runner, and keeps a heartbeat so a crashed or hung job can be
recovered on the next startup.

This replaces the previous fire-and-forget execution model (FastAPI
``BackgroundTasks`` for API actions, ``asyncio.create_task`` in the scheduler).
That model never drained apply jobs created by :func:`core.apply.enqueue_apply`
(auto-apply) and left interrupted jobs stranded in ``running`` forever.  A single
queue drainer fixes both: every ``queued`` job is executed regardless of how it
was created, and orphaned jobs are reconciled at startup.

Concurrency defaults to 1 (serial).  Per-device dedup (``get_active_job``) means
a higher count only adds *cross-device* parallelism, at the cost of more
concurrent NSO/NetBox load.

Note: the runners themselves own the terminal status transition (they set
``running`` on entry and ``succeeded``/``failed`` on exit, each wrapped in a
600s timeout).  The worker's job is to *claim* (so two workers never grab the
same row) and *heartbeat*; the runner's redundant ``running`` set is harmless.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy import update as sa_update

from nso_adapter.store.db import get_session
from nso_adapter.store.models import Job, JobStatus, JobType

logger = structlog.get_logger(__name__)

# Seconds between heartbeat refreshes while a job runs.
_HEARTBEAT_INTERVAL = 15.0
# Seconds a worker sleeps when the queue is empty before polling again.
_EMPTY_POLL_INTERVAL = 2.0
# Job types safe to auto-requeue after an orphaning restart: read-only or
# idempotent.  An interrupted ``apply`` is *not* requeued — never silently
# re-push device config.
_REQUEUE_ON_RESTART = {JobType.sync, JobType.detect_drift, JobType.connect}

_workers: list[asyncio.Task] = []
_stop: asyncio.Event | None = None


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


async def _claim_next_job() -> tuple[int, int | None, JobType] | None:
    """Atomically claim the oldest queued job.

    Returns ``(job_id, device_id, job_type)`` or ``None`` if the queue is empty.
    ``SELECT ... FOR UPDATE SKIP LOCKED`` ensures two workers never claim the
    same row.
    """
    async for db in get_session():
        result = await db.execute(
            select(Job)
            .where(Job.status == JobStatus.queued)
            .order_by(Job.created_at, Job.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        job = result.scalar_one_or_none()
        if job is None:
            return None
        # Capture identity before commit so an expire-on-commit refresh can't
        # trigger a lazy load outside the greenlet context.
        claimed = (job.id, job.device_id, job.job_type)
        now = _now()
        job.status = JobStatus.running
        job.started_at = now
        job.heartbeat_at = now
        await db.commit()
        return claimed
    return None


async def _mark_failed(job_id: int, code: str, message: str) -> None:
    """Fallback terminal failure when a runner raises (runners normally self-manage)."""
    async for db in get_session():
        job = await db.get(Job, job_id)
        if job is None:
            return
        job.status = JobStatus.failed
        job.error = {"code": code, "message": message, "detail": {}}
        await db.commit()


async def _heartbeat(job_id: int) -> None:
    """Refresh ``heartbeat_at`` every interval until cancelled."""
    try:
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            async for db in get_session():
                await db.execute(sa_update(Job).where(Job.id == job_id).values(heartbeat_at=_now()))
                await db.commit()
    except asyncio.CancelledError:
        return


async def _worker_loop(worker_id: int, stop: asyncio.Event) -> None:
    from nso_adapter.core.jobs import _JOB_RUNNERS

    logger.info("worker.started", worker_id=worker_id)
    while not stop.is_set():
        try:
            claimed = await _claim_next_job()
        except Exception as exc:  # pragma: no cover - defensive: keep the loop alive
            logger.exception("worker.claim_error", worker_id=worker_id, error=repr(exc))
            claimed = None

        if claimed is None:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=_EMPTY_POLL_INTERVAL)
            continue

        job_id, device_id, job_type = claimed
        runner = _JOB_RUNNERS.get(job_type)
        if runner is None:
            logger.error("worker.no_runner", job_id=job_id, job_type=str(job_type))
            await _mark_failed(job_id, "no_runner", f"No runner for job type {job_type}")
            continue

        hb = asyncio.create_task(_heartbeat(job_id))
        try:
            logger.info(
                "worker.job_start",
                worker_id=worker_id,
                job_id=job_id,
                job_type=str(job_type),
                device_id=device_id,
            )
            await runner(job_id, device_id)
        except Exception as exc:
            # Runners normally catch their own errors; this is a last resort so a
            # runner bug can't strand the job in ``running``.
            logger.exception("worker.job_crashed", worker_id=worker_id, job_id=job_id, error=repr(exc))
            await _mark_failed(job_id, "internal", repr(exc))
        finally:
            hb.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await hb

    logger.info("worker.stopped", worker_id=worker_id)


async def requeue_orphaned_jobs() -> None:
    """Recover jobs left non-terminal by a previous process.

    Run once at startup, before workers begin draining:
      * ``running`` idempotent jobs (sync/detect_drift/connect) → ``queued``.
      * ``running`` ``apply`` jobs → ``failed`` (never silently re-push config).
      * ``queued`` jobs are left as-is; a worker will pick them up.
    """
    async for db in get_session():
        requeued = await db.execute(
            sa_update(Job)
            .where(Job.status == JobStatus.running, Job.job_type.in_(_REQUEUE_ON_RESTART))
            .values(status=JobStatus.queued, started_at=None, heartbeat_at=None)
        )
        failed = await db.execute(
            sa_update(Job)
            .where(Job.status == JobStatus.running, Job.job_type == JobType.apply)
            .values(
                status=JobStatus.failed,
                error={
                    "code": "orphaned",
                    "message": "Adapter restarted while apply was running",
                    "detail": {},
                },
            )
        )
        await db.commit()
        if requeued.rowcount:
            logger.warning("worker.requeued_orphaned", count=requeued.rowcount)
        if failed.rowcount:
            logger.warning("worker.failed_orphaned_apply", count=failed.rowcount)


async def start_workers(concurrency: int = 1) -> None:
    """Reconcile orphaned jobs, then start the worker pool."""
    global _stop, _workers
    await requeue_orphaned_jobs()
    _stop = asyncio.Event()
    _workers = [asyncio.create_task(_worker_loop(i, _stop)) for i in range(max(1, concurrency))]
    logger.info("worker.pool_started", concurrency=len(_workers))


async def stop_workers() -> None:
    """Signal all workers to stop and await their exit."""
    global _workers
    if _stop is not None:
        _stop.set()
    for task in _workers:
        task.cancel()
    for task in _workers:
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, timeout=5.0)
    _workers = []
