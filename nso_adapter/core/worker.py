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
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import or_, select
from sqlalchemy import update as sa_update

from nso_adapter.core.claim import ClaimLostError, ClaimRegistration, lock_claim
from nso_adapter.store.db import get_session
from nso_adapter.store.models import DeviceClaim, Job, JobStatus, JobType

logger = structlog.get_logger(__name__)

# Seconds between heartbeat refreshes while a job runs.
_HEARTBEAT_INTERVAL = 15.0
# A running job is only "orphaned" (recoverable at startup) once its heartbeat has gone
# stale by this many seconds — several heartbeat intervals of margin. A job heartbeating
# more recently than this belongs to a LIVE worker (rolling restart / two-process overlap)
# and must not be stolen out from under it.
_ORPHAN_STALE_AFTER = 60.0
# Seconds a worker sleeps when the queue is empty before polling again.
_EMPTY_POLL_INTERVAL = 2.0
# Job types safe to auto-requeue after an orphaning restart: read-only or
# idempotent.  An interrupted ``apply`` is *not* requeued — never silently
# re-push operator intent that may have changed.  A ``removal`` IS requeued: it
# re-reads the CURRENT accepted rows and PUT-replaces, so re-running only
# re-asserts the already-decided desired state (idempotent), and dropping it
# would leave orphaned device config behind.
_REQUEUE_ON_RESTART = {
    JobType.sync,
    JobType.sync_now,  # idempotent read (grain-c atomic mirror refresh)
    JobType.sync_from_nso,  # idempotent read (S5a comprehensive CDB-only mirror refresh)
    JobType.detect_drift,
    JobType.connect,
    JobType.removal,
    JobType.provision,
}

_workers: list[asyncio.Task] = []
_stop: asyncio.Event | None = None


def _now() -> datetime:
    return datetime.now(UTC)


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


async def _mark_failed(job_id: int, code: str, message: str, reg: ClaimRegistration | None = None) -> None:
    """Last-resort terminal failure for a WORKER-MACHINERY fault.

    Not the runner's error path: an ordinary runner exception is consumed by the
    result-specific branch and routed to the claim-bearing terminal writer. What reaches
    here is a fault outside result-specific handling — the drain plumbing raising, the
    claim-token plumbing raising before a branch is chosen — plus direct invocation.

    It still writes a terminal status on a claimed device, so it takes the claim row lock
    first when *reg* is supplied: otherwise it can overwrite the disposition recovery
    already made, or a fresh worker's ``running``.
    """
    async for db in get_session():
        if reg is not None:
            try:
                await lock_claim(db, reg)
            except ClaimLostError:
                logger.warning("worker.mark_failed_claim_lost", job_id=job_id, device_id=reg.device_id)
                return
        job = await db.get(Job, job_id)
        if job is None:
            return
        job.status = JobStatus.failed
        job.error = {"code": code, "message": message, "detail": {}}
        await db.commit()


async def _heartbeat(job_id: int, reg: ClaimRegistration | None = None) -> None:
    """Refresh ``heartbeat_at`` every interval until cancelled.

    Two lanes, chosen LIVE on every tick from *reg* — never from a value captured when the
    task was created. A provision acquires its claim mid-run and has to switch lanes on the
    very next tick; reading a stale ``None`` would leave its claim un-heartbeated and let
    the reaper revoke a healthy run.

    * registered: lock the claim row FIRST, then refresh both ``device_claim`` and ``jobs``.
      Claim-before-job is the §3.9 order and is not cosmetic — recovery holds the claim and
      reaches for the job, so appending the claim update after the job write would invert
      the two and deadlock. The lock doubles as the guard: zero rows means the claim was
      revoked, so stop heartbeating and let the runner's next guard raise ``ClaimLostError``.
      Never re-insert the row — resurrecting a revoked claim would hand the device back to a
      holder recovery has already replaced.
    * unregistered: refresh ``jobs`` only, exactly as before.

    A transient DB error on one tick must NOT kill the heartbeat: if it did, the heartbeat
    would go stale under a still-running job and the reaper would eventually steal it. Log
    and keep looping; only cancellation or a revoked claim stops it.
    """
    while True:
        try:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            live = reg if (reg is not None and reg.registered) else None
            revoked = False
            async for db in get_session():
                now = _now()
                if live is not None:
                    held = await db.scalar(
                        select(DeviceClaim.claim_token)
                        .where(
                            DeviceClaim.device_id == live.device_id,
                            DeviceClaim.claim_token == live.token,
                        )
                        .with_for_update()
                    )
                    if held is None:
                        revoked = True
                    else:
                        await db.execute(
                            sa_update(DeviceClaim)
                            .where(DeviceClaim.device_id == live.device_id, DeviceClaim.claim_token == live.token)
                            .values(heartbeat_at=now)
                        )
                if not revoked:
                    await db.execute(sa_update(Job).where(Job.id == job_id).values(heartbeat_at=now))
                    await db.commit()
            if revoked:
                logger.warning("worker.heartbeat_claim_revoked", job_id=job_id, device_id=reg.device_id)
                return
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("worker.heartbeat_error", job_id=job_id, error=repr(exc))


async def reap_stale_claims() -> None:
    """Revoke every claim whose heartbeat has gone silent (Q7).

    Purpose- and job-independent by design: an ``intent_put`` or ``teardown`` claim has no
    job at all, and an apply's claim deliberately outlives its terminal job through the
    post-apply refresh, so a reaper keyed on ``Job.status = 'running'`` would miss all of
    them. Revocation leaves NO claim — reissuing one at startup, before any worker exists,
    would strand the requeued job behind the worker's live-claim skip.
    """
    from nso_adapter.core.claim import revoke_stale_claims

    revoked = await revoke_stale_claims()
    if revoked:
        logger.warning(
            "worker.claims_revoked",
            count=len(revoked),
            devices=[entry.device_id for entry in revoked],
        )


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
        except asyncio.CancelledError:
            # Graceful shutdown cancelled a mid-run claim (S5a A2, codex R2-F7): return it
            # to the queue now instead of leaving the device 409-blocked until the periodic
            # reap's staleness window passes on the next restart/tick.
            await _requeue_own_claim(job_id, job_type)
            raise
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


async def _requeue_own_claim(job_id: int, job_type: JobType) -> None:
    """Best-effort: return a cancelled mid-run claim to the queue (S5a A2).

    Status-guarded (codex R4-4): the runner may have committed a terminal status and been
    cancelled at the very next await — ``WHERE status = 'running'`` never re-runs a
    finished job. Only whitelisted (idempotent) types are requeued; an interrupted apply
    keeps its existing never-requeue semantics. Best-effort: a second cancel landing
    mid-requeue leaves recovery to the periodic reap instead.
    """
    if job_type not in _REQUEUE_ON_RESTART:
        return
    try:
        async for db in get_session():
            res = await db.execute(
                sa_update(Job)
                .where(Job.id == job_id, Job.status == JobStatus.running)
                .values(status=JobStatus.queued, started_at=None, heartbeat_at=None)
            )
            await db.commit()
            if res.rowcount:
                logger.warning("worker.requeued_cancelled_claim", job_id=job_id)
    except Exception as exc:
        logger.warning("worker.cancel_requeue_failed", job_id=job_id, error=repr(exc))


def ensure_workers() -> None:
    """Respawn dead worker tasks (S5a A2, codex R3-6).

    The periodic reap can requeue a stale job, but the pool is created once and
    unsupervised — a dead sole worker would leave requeued jobs queued forever while
    ``get_active_job`` 409s every new job for those devices. Called from the periodic
    orphan-reap tick. No-ops once shutdown has set the stop event (it is set before the
    event loop yields to teardown, so a mid-shutdown tick cannot respawn a stray worker).
    """
    if _stop is None or _stop.is_set():
        return
    respawned = 0
    for i, task in enumerate(_workers):
        if task.done():
            _workers[i] = asyncio.create_task(_worker_loop(i, _stop))
            respawned += 1
    if respawned:
        logger.warning("worker.respawned", count=respawned)


async def requeue_orphaned_jobs() -> None:
    """Recover jobs left non-terminal by a previous process.

    Run once at startup, before workers begin draining. Only jobs whose heartbeat has
    gone STALE (or was never stamped) are recovered — a job heartbeating within
    ``_ORPHAN_STALE_AFTER`` is being actively run by a live worker (e.g. during a rolling
    restart or an accidental two-process overlap) and is left untouched, so we never
    double-run a sync/removal or falsely fail a live apply:
      * stale ``running`` idempotent jobs (sync/detect_drift/connect/removal/provision) → ``queued``.
      * stale ``running`` ``apply`` jobs → ``failed`` (never silently re-push config).
      * ``queued`` jobs are left as-is; a worker will pick them up.
    """
    async for db in get_session():
        cutoff = _now() - timedelta(seconds=_ORPHAN_STALE_AFTER)
        # NULL heartbeat = claimed by a prior process that never stamped one → treat as stale.
        stale = or_(Job.heartbeat_at.is_(None), Job.heartbeat_at < cutoff)
        requeued = await db.execute(
            sa_update(Job)
            .where(Job.status == JobStatus.running, Job.job_type.in_(_REQUEUE_ON_RESTART), stale)
            .values(status=JobStatus.queued, started_at=None, heartbeat_at=None)
        )
        failed = await db.execute(
            sa_update(Job)
            .where(Job.status == JobStatus.running, Job.job_type == JobType.apply, stale)
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
    # Before any worker exists, so a revoked device is immediately claimable again.
    await reap_stale_claims()
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
