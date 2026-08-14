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

Concurrency defaults to 1 (serial). A higher count only adds *cross-device*
parallelism: the per-device claim serializes execution on any one device, whatever
the worker count, at the cost of more concurrent NSO/NetBox load.

Note: the ``queued -> running`` transition happens HERE and nowhere else — the claimed
head and the claimless head are the only two sites, and each bumps ``jobs.run_attempt``
in the same UPDATE (Appendix S §3.1). The runners own only the terminal transition, and
they take that through ``core.claim.terminalize``, naming the attempt they were started
at. A runner that re-wrote ``running`` would be a third, unguarded bump site.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
import time
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import func, or_, select
from sqlalchemy import update as sa_update

from nso_adapter.core.claim import (
    JOB_CANCEL_DRAIN,
    JOB_CLEANUP_BOUND,
    JOB_EXECUTION_BUDGET,
    PROVISION_STALE_AFTER,
    BookkeepingOutcomeUnknown,
    ClaimLostError,
    ClaimOutcome,
    ClaimRegistration,
    abandon_claim_to_staleness,
    acquire_claim,
    dispose_cancelled,
    disposition_for,
    error_envelope,
    lock_claim,
    mark_failed_and_release,
    release_claim,
    terminalize,
    terminalize_running,
)
from nso_adapter.store.db import get_session
from nso_adapter.store.models import DeviceClaim, Job, JobStatus, JobType

logger = structlog.get_logger(__name__)

# Seconds between heartbeat refreshes while a job runs.
_HEARTBEAT_INTERVAL = 15.0
# Seconds a worker sleeps when the queue is empty before polling again.
_EMPTY_POLL_INTERVAL = 2.0

_workers: list[asyncio.Task] = []
_stop: asyncio.Event | None = None

# Exactly ONE drain task per runner task. Both entry points — execution-budget expiry and
# worker cancellation — go through _drain_handle, so the runner is cancelled once.
_drains: dict[asyncio.Task, asyncio.Task] = {}

# Distinct from 1 so a supervisor's logs separate "the adapter fail-stopped" from an
# ordinary crash.
_FAILSTOP_EXIT_CODE = 70

# How many candidate devices one poll may consider. More than one is the cross-device
# progress guarantee: a device whose head cannot be locked is skipped in favour of the next
# candidate within the SAME poll, so sustained traffic on one busy device cannot starve the
# rest. Re-polling from the global oldest row would rediscover the busy device forever.
_CANDIDATE_BATCH = 10


def _now() -> datetime:
    return datetime.now(UTC)


async def _discover_candidates() -> list[int | None]:
    """Devices with queued work and no live claim, oldest work first. NON-LOCKING.

    A BATCH, not a single row, and that is the cross-device progress guarantee: a device
    whose head cannot be locked must be skipped in favour of the next candidate within the
    same poll. Re-polling from the global oldest row instead would rediscover the same busy
    device every time, and sustained traffic on it would starve every other device.

    No ``FOR UPDATE`` here on purpose — this only picks a device. Taking a job row before the
    claim would invert the lock order against recovery, which holds the claim and reaches for
    the job.
    """
    async for db in get_session():
        rows = (
            await db.execute(
                select(Job.device_id, func.min(Job.created_at).label("oldest"))
                .outerjoin(DeviceClaim, DeviceClaim.device_id == Job.device_id)
                .where(Job.status == JobStatus.queued, DeviceClaim.device_id.is_(None))
                .group_by(Job.device_id)
                .order_by(func.min(Job.created_at))
                .limit(_CANDIDATE_BATCH)
            )
        ).all()
        return [row[0] for row in rows]
    return []


async def _start_claimless_head(device_id_is_null_head: Job, db) -> tuple[int, None, JobType, int] | None:
    """Start a claimless (provision) job that has already been locked.

    One of the two ``queued -> running`` transitions, so one of the two sites that bump
    the run-attempt token. The row is held ``FOR UPDATE``, so read-then-increment is
    serialized by the lock rather than by the arithmetic.
    """
    job = device_id_is_null_head
    now = _now()
    job.status = JobStatus.running
    job.started_at = now
    job.heartbeat_at = now
    job.run_attempt = job.run_attempt + 1
    claimed = (job.id, None, job.job_type, job.run_attempt)
    await db.commit()
    return claimed


async def _claim_next_job() -> tuple[int, int | None, JobType, ClaimRegistration] | None:
    """Claim a device, then its exact queued head. CLAIM FIRST, never job first.

    Returns ``(job_id, device_id, job_type, registration)`` or ``None``.

    The order is the whole point. Taking the head job row and then acquiring the claim
    inverts against recovery — which holds the claim and reaches for the job — and the two
    deadlock under exactly the barriers the claim tests install. So: pick a device without
    locking anything, acquire its claim, and only then lock the head *under* that claim.

    Three further rules that are not obvious:

    * the claim is inserted with ``job_id = NULL``. It is a real FK, and PostgreSQL validates
      an inserted FK by locking the referenced row ``FOR KEY SHARE`` — which conflicts with the
      ``FOR UPDATE`` an endpoint holds on a queued winner. The worker would block inside the FK
      check before its ``SKIP LOCKED`` ever ran, stalling on one busy device;
    * the head is re-derived under the claim and then locked BY EXACT ID. An
      ``ORDER BY … LIMIT 1 FOR UPDATE SKIP LOCKED`` would skip a locked head and hand back a
      LATER job on the same device, breaking the per-device FIFO the removal-before-apply
      ordering depends on;
    * zero rows means the head is locked or gone: release the claim and move to the next
      candidate device. Never inspect a later job on that device.
    """
    for device_id in await _discover_candidates():
        if device_id is None:
            claimed = await _claim_next_claimless_job()
            if claimed is not None:
                job_id, _none, job_type, attempt = claimed
                return (job_id, None, job_type, ClaimRegistration(run_attempt=attempt))
            continue

        reg = await acquire_claim(device_id, "job")
        if reg is None:
            continue  # someone acquired it between discovery and now

        started = await _start_head_under_claim(device_id, reg)
        if started is not None:
            return (*started, reg)

        # Nothing runnable here: give the device back so it is not skipped next poll.
        await release_claim(reg)
    return None


async def _start_head_under_claim(device_id: int, reg: ClaimRegistration) -> tuple[int, int, JobType] | None:
    """Lock this device's first ADMISSIBLE queued job under the claim and start it.

    One transaction. The candidates are walked in per-device FIFO order and the first one the
    success barrier admits is started — stopping at an inadmissible head instead would wedge
    the device for good: a retried generation cannot always take over a queued successor's
    job (a removal may not take over an apply, or the reverse), so its own job is created
    LATER and sits behind the very successor its failure blocks. Nothing is reordered by
    this: a generation-carrying job is admissible only when every predecessor generation has
    settled or been abandoned, so an admissible successor is one the barrier already cleared.

    Each candidate is still locked BY EXACT ID with ``SKIP LOCKED``, and a skipped lock ends
    the whole attempt rather than moving on — an endpoint holding a queued winner means its
    intent is not visible yet, and running a later job in that window is the FIFO break the
    lock exists to prevent.
    """
    from nso_adapter.core.generation import job_admissible, mark_job_generations_running

    async for db in get_session():
        await lock_claim(db, reg)  # claim -> jobs, per the global lock order

        candidates = (
            (
                await db.execute(
                    select(Job.id)
                    .where(Job.device_id == device_id, Job.status == JobStatus.queued)
                    .order_by(Job.created_at, Job.id)
                )
            )
            .scalars()
            .all()
        )

        job = None
        for candidate in candidates:
            # By EXACT id, so a locked head is never silently replaced by a later job.
            locked = await db.scalar(
                select(Job).where(Job.id == candidate, Job.status == JobStatus.queued).with_for_update(skip_locked=True)
            )
            if locked is None:
                await db.rollback()
                return None
            # The success barrier (#1522 §H2). Selecting a queued job is NOT enough on its
            # own: a job carrying generation N+1 must not run while N is failed or its
            # outcome unknown, or the successor deploys over a state nobody established.
            if await job_admissible(db, locked.id, device_id):
                job = locked
                break
            logger.debug("worker.skipped_inadmissible", device_id=device_id, job_id=locked.id)
        if job is None:
            await db.rollback()
            return None
        await mark_job_generations_running(db, job.id)

        claimed = (job.id, device_id, job.job_type)
        now = _now()
        job.status = JobStatus.running
        job.started_at = now
        job.heartbeat_at = now
        # The run-attempt bump rides the SAME UPDATE as the transition (Appendix S §3.1),
        # under the row lock taken above. The registration carries it to every terminal
        # writer, so each can name the execution it belongs to.
        job.run_attempt = job.run_attempt + 1
        reg.run_attempt = job.run_attempt
        # Association and the running transition in the SAME guarded transaction: a crash
        # between them would leave a queued job plus a job-less claim, which recovery handles,
        # but a released claim with a running job would be unrecoverable.
        await db.execute(
            sa_update(DeviceClaim)
            .where(DeviceClaim.device_id == device_id, DeviceClaim.claim_token == reg.token)
            .values(job_id=job.id)
        )
        await db.commit()
        return claimed
    return None


async def _claim_next_claimless_job() -> tuple[int, None, JobType, int] | None:
    """Start the oldest queued ``device_id IS NULL`` job. Provision only.

    Any OTHER type with no device is a corrupt row: the worker would dispatch it with
    ``device_id=None`` against a device that no longer exists, and it is invisible to both the
    claim machinery and the per-device FIFO. Teardown is specified never to create one, so
    this is a belt against a bug, not a routine path.
    """
    async for db in get_session():
        job = await db.scalar(
            select(Job)
            .where(Job.device_id.is_(None), Job.status == JobStatus.queued)
            .order_by(Job.created_at, Job.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if job is None:
            await db.rollback()
            return None
        if job.job_type is not JobType.provision:
            logger.error("worker.orphaned_claimless", job_id=job.id, job_type=str(job.job_type))
            # Queued-sourced: there is no execution to name, so no token. The row is held
            # FOR UPDATE and its status was proven `queued` by the same SELECT.
            await terminalize(
                db,
                job.id,
                status=JobStatus.failed,
                expect=JobStatus.queued,
                error={
                    "code": "orphaned_claimless",
                    "message": f"{job.job_type} job has no device_id; only provision may be claimless",
                    "detail": {},
                },
            )
            await db.commit()
            return None
        return await _start_claimless_head(job, db)
    return None


async def _mark_failed(job_id: int, code: str, message: str, reg: ClaimRegistration | None = None) -> None:
    """Last-resort terminal failure for a WORKER-MACHINERY fault.

    Not the runner's error path: an ordinary runner exception is consumed by the
    result-specific branch and routed to the claim-bearing terminal writer. What reaches
    here is a fault outside result-specific handling — the drain plumbing raising, the
    claim-token plumbing raising before a branch is chosen — plus direct invocation.

    It still writes a terminal status on a claimed device, so it takes the claim row lock
    first when *reg* is supplied: otherwise it can overwrite the disposition recovery
    already made, or a fresh worker's ``running``. The attempt on *reg* is the second half
    of that guard, and it covers the claimless lane too, which has no claim to lock.
    """
    async for db in get_session():
        if reg is not None:
            try:
                await lock_claim(db, reg)
            except ClaimLostError:
                logger.warning("worker.mark_failed_claim_lost", job_id=job_id, device_id=reg.device_id)
                return
        await terminalize(
            db,
            job_id,
            status=JobStatus.failed,
            expect=JobStatus.running,
            run_attempt=reg.run_attempt if reg is not None else None,
            error={"code": code, "message": message, "detail": {}},
        )
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


def _failstop(event: str, **fields) -> None:
    """Log, flush, and kill the process. Does NOT return.

    Reached only when a cancellation drain or a terminal-write cleanup exceeded a bound that
    the server-enforced statement and lock timeouts, plus the absorb/drain margin, say is
    impossible. That means a bound is missing or wrong — a bug, not load — and the process is
    already in a state its own invariants forbid. Continuing risks a device write under
    ownership the adapter can no longer prove it holds, which is the single failure the claim
    design exists to prevent.

    ``os._exit`` deliberately skips atexit hooks and finalizers: a graceful shutdown path
    could block on the very thing that refused to drain. Process exit closes every DB
    connection, which ABORTS the in-flight transaction — uncommitted work is discarded
    cleanly, nothing is left half-written — and kills any in-flight RESTCONF socket, so the
    device write cannot continue behind the adapter's back.

    Recovery is NOT immediate: the exit leaves a claim whose heartbeat was just refreshed, and
    the reaper revokes only claims older than the stale cutoff. Both adapter compose services
    carry ``restart: unless-stopped`` for exactly this reason — without a supervisor this exit
    leaves the adapter down for good.
    """
    logger.error(event, **fields)
    for handler in logging.getLogger().handlers:
        with contextlib.suppress(Exception):
            handler.flush()
    sys.stderr.flush()
    os._exit(_FAILSTOP_EXIT_CODE)


class _CancelSeen:
    """Shared flag: a parent cancellation was absorbed and still owes a re-raise.

    Shared rather than returned, because the cancel can land in any of several awaits and the
    re-raise must happen once, at the very end, after all cleanup.
    """

    __slots__ = ("hit",)

    def __init__(self) -> None:
        self.hit = False


async def _absorb(awaitable, seen: _CancelSeen):
    """Await *awaitable*, absorbing EVERY parent cancellation until it resolves.

    Never re-raises the parent cancel — not even when the inner awaitable has already
    finished. It records the cancel on *seen*, which the worker body re-raises from only after
    every cleanup step has run. Re-raising here would skip the disposition, the release and
    the drain bookkeeping, which is how a shutdown mid-cleanup used to strand a job.
    """
    fut = asyncio.ensure_future(awaitable)
    while True:
        try:
            return await asyncio.shield(fut)
        except asyncio.CancelledError:
            seen.hit = True


async def _cancel_and_drain(task: asyncio.Task, *, job_id: int, device_id: int | None, job_type: str) -> str:
    """Cancel the runner once and wait a separately-timed drain. Fail-stops on expiry.

    ``asyncio.wait`` neither cancels on timeout nor waits for a cancellation to finish, which
    is why the two phases can be bounded independently. ``asyncio.wait_for`` cannot do this:
    at its timeout it cancels the child and then waits for that cancellation to complete,
    which is unbounded against a span that absorbs cancels — and this repository deliberately
    contains such spans.
    """
    started = time.monotonic()
    task.cancel()  # idempotent by construction: only ever called here, once per runner
    _done, pending = await asyncio.wait({task}, timeout=JOB_CANCEL_DRAIN)
    if pending:
        _failstop(
            "worker.drain_expired_failstop",
            job_id=job_id,
            device_id=device_id,
            job_type=job_type,
            elapsed=round(time.monotonic() - started, 3),
            drain_bound=JOB_CANCEL_DRAIN,
        )
    return "drained"


def _drain_handle(task: asyncio.Task, *, job_id: int, device_id: int | None, job_type: str) -> asyncio.Task:
    """Exactly ONE drain per runner, whichever entry point asks for it.

    Budget expiry and worker cancellation both arrive here. Without the shared handle the
    second entry point issues a second ``task.cancel()``, and a repeated cancel turns
    ``await_uncancellable``'s bounded drain into an abandoned live task — its drain phase
    treats a second cancel as "still pending" and abandons the child immediately.
    """
    existing = _drains.get(task)
    if existing is None:
        existing = asyncio.create_task(_cancel_and_drain(task, job_id=job_id, device_id=device_id, job_type=job_type))
        _drains[task] = existing
    return existing


async def _bounded_cleanup(awaitable, seen: _CancelSeen, *, job_id: int, device_id: int | None):
    """``_absorb`` plus a hard wall-clock bound on the whole cleanup future.

    Server statement and lock timeouts bound individual SQL statements. They do NOT bound
    connection checkout, a stalled client socket, a rollback, or waiting for a COMMIT
    acknowledgement that never arrives — and without a bound on those, a cleanup future can
    stay pending forever while ``_absorb`` dutifully swallows every shutdown cancel.

    The watchdog is its own TASK with a FIXED deadline, awaited THROUGH ``_absorb``. Awaiting
    ``asyncio.wait`` directly would put the watchdog on the cancellable path: a second
    shutdown cancel would raise out of that wait, destroy the watchdog, and leave the
    disposition running detached with no expiry at all. Creating the task once means a cancel
    is absorbed by the shield while the deadline keeps running on its original clock.
    """
    fut = asyncio.ensure_future(awaitable)

    async def _watchdog():
        _done, pending = await asyncio.wait({fut}, timeout=JOB_CLEANUP_BOUND)
        if pending:
            _failstop(
                "worker.cleanup_expired_failstop",
                job_id=job_id,
                device_id=device_id,
                cleanup_bound=JOB_CLEANUP_BOUND,
            )
        return fut.result()

    return await _absorb(asyncio.create_task(_watchdog()), seen)


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

        job_id, device_id, job_type, reg = claimed
        runner = _JOB_RUNNERS.get(job_type)
        if runner is None:
            logger.error("worker.no_runner", job_id=job_id, job_type=str(job_type))
            await _mark_failed(job_id, "no_runner", f"No runner for job type {job_type}", reg)
            # Give the device back: we claimed it and are not going to run anything.
            await release_claim(reg)
            continue

        await _run_one_job(worker_id, job_id, device_id, job_type, runner, reg)

    logger.info("worker.stopped", worker_id=worker_id)


async def _run_one_job(
    worker_id: int,
    job_id: int,
    device_id: int | None,
    job_type: JobType,
    runner,
    reg: ClaimRegistration | None = None,
) -> None:
    """Drive one runner as an explicit task, with execution and drain bounded separately.

    The runner is a task, not an await, because the two phases have different deadlines and
    ``asyncio.wait_for`` cannot express that: at its timeout it cancels the child and then
    waits for the cancellation to COMPLETE, which is unbounded against a span that absorbs
    cancels. ``asyncio.wait`` neither cancels nor waits for cancellation, so each phase gets
    its own bound.

    Both ways out — the execution budget expiring, and the worker task itself being cancelled
    by shutdown — go through the SAME drain handle. Cancelling a task that is awaiting
    ``asyncio.wait({runner})`` does not cancel the runner: the waiter raises while the runner
    keeps going. Without the except arm, shutdown would bypass every branch below and release
    ownership while the runner was still writing.
    """
    # The claim identity for this run. The caller supplies it once the worker acquires
    # claims; an unregistered one is the claimless lane, which behaves exactly as before.
    # It is an OBJECT, not a token: a run that acquires its claim mid-run registers into this
    # same instance, and the heartbeat and the terminal writers read it live.
    if reg is None:
        reg = ClaimRegistration(device_id, None)
    # The SAME object reaches the runner and the heartbeat. A run that acquires its claim
    # mid-run — provision, at its mapping — registers into it, and both readers see that
    # immediately; a token captured here would still be None for the rest of the run.
    task = asyncio.create_task(runner(job_id, device_id, reg))
    hb = asyncio.create_task(_heartbeat(job_id, reg))
    seen = _CancelSeen()
    ownership = "drained"
    outcome = ClaimOutcome.ABORT_KNOWN
    drain_kwargs = {"job_id": job_id, "device_id": device_id, "job_type": str(job_type)}

    logger.info(
        "worker.job_start",
        worker_id=worker_id,
        job_id=job_id,
        job_type=str(job_type),
        device_id=device_id,
    )
    try:
        try:
            _done, pending = await asyncio.wait({task}, timeout=JOB_EXECUTION_BUDGET)
            if pending:
                logger.error("worker.execution_budget_expired", budget=JOB_EXECUTION_BUDGET, **drain_kwargs)
                ownership = await _absorb(_drain_handle(task, **drain_kwargs), seen)
        except asyncio.CancelledError:
            seen.hit = True
            ownership = await _absorb(_drain_handle(task, **drain_kwargs), seen)

        # ALWAYS observe the finished runner: asyncio.wait returns tasks in `done` WITHOUT
        # propagating their exceptions, so a runner that raised before writing a terminal
        # status used to look like clean completion — claim released, job stranded `running`,
        # and invisible to a reaper that scans claims.
        #
        # The lane is read from the registration HERE, at terminal time, never from a value
        # captured at task creation: a run that acquired a claim mid-run must take the claimed
        # branch, or it leaks the claim or writes unguarded.
        try:
            task.result()
        except ClaimLostError:
            # Recovery already owns this job's disposition; writing anything would clobber it.
            logger.warning("worker.claim_lost", worker_id=worker_id, **drain_kwargs)
            raise
        except BookkeepingOutcomeUnknown:
            # R2 §4.6: the runner's terminal COMMIT was neither acknowledged nor provably
            # aborted. Same shape as a lost claim and for the same reason — a second
            # terminal write could flip a job whose CAS and results already landed. Write
            # nothing, release nothing; `outcome` stays non-acknowledged so the finally
            # abandons the claim to staleness and stops the heartbeat, and recovery
            # re-dispositions only if the job is still `running`. NOT re-raised: the worker
            # loop has more jobs to run and this run's disposition is now recovery's.
            logger.error("worker.bookkeeping_outcome_unknown", worker_id=worker_id, **drain_kwargs)
        except asyncio.CancelledError:
            outcome = await _bounded_cleanup(_dispose(job_id, job_type, reg), seen, job_id=job_id, device_id=device_id)
        except Exception as exc:
            logger.exception("worker.job_crashed", worker_id=worker_id, job_id=job_id, error=repr(exc))
            outcome = await _bounded_cleanup(
                _fail_and_release(job_id, exc, reg), seen, job_id=job_id, device_id=device_id
            )
        else:
            outcome = await _bounded_cleanup(_release(reg), seen, job_id=job_id, device_id=device_id)
    finally:
        # NEVER a standalone release after a disposition that did not commit: a rolled-back
        # status write with a deleted claim is invisible to the reaper forever. This branch
        # only logs and leaves the row — the result-specific branches above own release.
        if outcome is not ClaimOutcome.COMMIT_ACKNOWLEDGED and ownership != "transferred":
            abandon_claim_to_staleness(reg, outcome)
        # UNCONDITIONAL: every exit stops the heartbeat. A run that simply SUCCEEDS would
        # otherwise leave its heartbeat looping forever, and a heartbeated claim never goes
        # stale, so the reaper would never see it.
        hb.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hb
        _drains.pop(task, None)

    if device_id is not None and not seen.hit:
        # This run settled (or blocked) its generations; the NEXT one may now be startable
        # and, if its own admission could not attach it to a job, has none yet. Own
        # transaction on purpose — a job insert inside the terminal transaction would put
        # the ``jobs -> devices`` FK edge back into it. Skipped on a cancellation: shutdown
        # must not open one more DB round trip, and startup recovery advances the chain.
        await _advance_generations(device_id)

    if seen.hit:
        # Re-raised only now, after every cleanup step. Recorded rather than propagated at the
        # point of the cancel, because propagating there skipped the disposition and release.
        raise asyncio.CancelledError()


async def _advance_generations(device_id: int) -> None:
    """Hand the device's next executable generation a job. Never fails the finished run."""
    from nso_adapter.core.generation import advance_device_generations

    for attempt in range(1, 4):
        try:
            await advance_device_generations(device_id)
            return
        except Exception as exc:  # noqa: BLE001 — the finished run must stay terminal
            if attempt == 3:
                logger.error(
                    "worker.generation_advance_failed",
                    device_id=device_id,
                    attempts=attempt,
                    error=repr(exc),
                )
                return
            logger.warning(
                "worker.generation_advance_retry",
                device_id=device_id,
                attempt=attempt,
                error=repr(exc),
            )
            await asyncio.sleep(0.5 * attempt)


async def _dispose(job_id: int, job_type: JobType, reg: ClaimRegistration) -> ClaimOutcome:
    """Disposition a cancelled run, claimed or claimless depending on the live registration."""
    if reg.registered:
        return await dispose_cancelled(job_id, job_type, reg)
    # The claimless lane: status only, nothing to release. The registration is unregistered
    # here but still carries this run's attempt, which is the only ownership proof available.
    if disposition_for(job_type) is JobStatus.queued:
        await _requeue_own_claim(job_id, job_type, run_attempt=reg.run_attempt)
    else:
        await _mark_failed(job_id, "cancelled", "Worker cancelled the run", reg)
    return ClaimOutcome.COMMIT_ACKNOWLEDGED


async def _fail_and_release(job_id: int, exc: BaseException, reg: ClaimRegistration) -> ClaimOutcome:
    """Terminal failure: status AND release in one transaction when a claim is held."""
    error = error_envelope(exc)
    if reg.registered:
        return await mark_failed_and_release(job_id, error["code"], error["message"], reg)
    await _mark_failed(job_id, error["code"], error["message"], reg)
    return ClaimOutcome.COMMIT_ACKNOWLEDGED


async def _release(reg: ClaimRegistration) -> ClaimOutcome:
    """Normal-success release. A no-op on the claimless lane."""
    return await release_claim(reg)


async def _requeue_own_claim(job_id: int, job_type: JobType, *, run_attempt: int | None = None) -> None:
    """Best-effort: return a cancelled mid-run claim to the queue (S5a A2).

    Status-guarded (codex R4-4): the runner may have committed a terminal status and been
    cancelled at the very next await — ``WHERE status = 'running'`` never re-runs a
    finished job. *run_attempt* is the rest of that guard: this disposition can arrive
    after recovery already requeued the job and a SUCCESSOR re-entered ``running``, and a
    status-only predicate would return that successor's execution to the queue. Only
    whitelisted (idempotent) types are requeued; an interrupted apply keeps its existing
    never-requeue semantics. Best-effort: a second cancel landing mid-requeue leaves
    recovery to the periodic reap instead.
    """
    if disposition_for(job_type) is not JobStatus.queued:
        return
    try:
        async for db in get_session():
            landed = await terminalize_running(db, job_id, status=JobStatus.queued, expected_attempt=run_attempt)
            await db.commit()
            if landed is not None:
                logger.warning("worker.requeued_cancelled_claim", job_id=job_id, landed=landed.value)
    except Exception as exc:
        logger.warning("worker.cancel_requeue_failed", job_id=job_id, error=repr(exc))


def ensure_workers() -> None:
    """Respawn dead worker tasks (S5a A2, codex R3-6).

    The periodic reap can requeue a stale job, but the pool is created once and
    unsupervised — a dead sole worker would leave requeued jobs queued forever while
    the queued-type dedupe 409s every new job of that type. Called from the periodic
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
    """Recover running jobs that NO claim covers — the claimless lane only.

    Every claimed job now has exactly ONE recovery clock: the claim scan
    (:func:`reap_stale_claims`), which revokes the stale claim and re-dispositions its job in
    the same transaction. Keeping a second, shorter job-status clock alongside it was the
    contradiction the single clock removes — re-dispositioning a job while its holder's token
    is still valid lets the old runner overwrite the disposition, and the row lock the holder
    takes is what makes revocation safe in the first place.

    What remains is the lane with no claim to scan: a provision that has not registered a
    token, plus any job left ``running`` by a process that predates the claim table. Their
    cutoff is the claimless one, sized against the OUTER job lifecycle rather than provision's
    own inner timeout — a heartbeat that merely stopped must not let this requeue a still
    running onboarding and run it twice.

    ``apply`` still ends ``failed`` rather than requeued: never silently re-push operator
    intent that may have changed since.
    """
    async for db in get_session():
        cutoff = _now() - timedelta(seconds=PROVISION_STALE_AFTER)
        # NULL heartbeat = started by a prior process that never stamped one → treat as stale.
        stale = or_(Job.heartbeat_at.is_(None), Job.heartbeat_at < cutoff)
        # "No claim covers it": a claimed job is the claim scan's business, not ours.
        uncovered = ~select(DeviceClaim.device_id).where(DeviceClaim.job_id == Job.id).exists()

        # The attempt rides the candidate SELECT: this lane holds no claim by construction
        # and takes no row lock, so the attempt observed HERE is the only thing that ties
        # the UPDATE below to the execution this pass judged stale.
        rows = (
            await db.execute(
                select(Job.id, Job.job_type, Job.run_attempt).where(Job.status == JobStatus.running, stale, uncovered)
            )
        ).all()

        requeued = failed = 0
        for job_id, job_type, run_attempt in rows:
            target = disposition_for(job_type)
            error = None
            if target is JobStatus.failed:
                error = {
                    "code": "orphaned",
                    "message": "Adapter restarted while the job was running",
                    "detail": {},
                }
            landed = await terminalize_running(db, job_id, status=target, error=error, expected_attempt=run_attempt)
            if landed is JobStatus.queued:
                requeued += 1
            elif landed is not None:
                failed += 1
        await db.commit()
        if requeued:
            logger.warning("worker.requeued_orphaned", count=requeued)
        if failed:
            logger.warning("worker.failed_orphaned_apply", count=failed)


async def start_workers(concurrency: int = 1) -> None:
    """Reconcile orphaned jobs, then start the worker pool."""
    global _stop, _workers
    from nso_adapter.core.generation import recover_generations
    from nso_adapter.core.tombstone_sweep import sweep_tombstones
    from nso_adapter.store.device_settle import ensure_settle_counters

    # BEFORE the reaper, not after (Appendix S §3.3). Recovery terminalizes, and a
    # terminalization that finds no counter row raises — appended after the reaper, this
    # repair could be aborted out of the lifespan by the very state it exists to fix.
    await ensure_settle_counters()
    await requeue_orphaned_jobs()
    # After the job recovery, so a generation whose job was just requeued is still covered,
    # and before the pool starts: a generation the dead process left ``running`` has an
    # unknown outcome and must block its successors from the first poll onwards (#1522 §H2).
    await recover_generations()
    # Before any worker exists, so a revoked device is immediately claimable again.
    await reap_stale_claims()
    # And before the pool drains anything: a deletion whose removal job was lost with the
    # process has no other carrier, and the sweep needs the device unclaimed to act.
    await sweep_tombstones()
    _stop = asyncio.Event()
    _workers = [asyncio.create_task(_worker_loop(i, _stop)) for i in range(max(1, concurrency))]
    logger.info("worker.pool_started", concurrency=len(_workers))


# Shutdown must outlast a full cancellation drain plus the terminal-write cleanup that
# follows it. The old 5s was BELOW await_uncancellable's own absorb (5s) + drain (2s), so a
# graceful shutdown could return while a span was still draining — and returning is what
# releases ownership, so it could leave a live child behind a released claim.
_SHUTDOWN_TASK_WAIT = JOB_CANCEL_DRAIN + JOB_CLEANUP_BOUND + 10.0


async def stop_workers() -> None:
    """Signal all workers to stop and await their exit.

    The wait per task must exceed the drain plus cleanup bounds; anything shorter means
    shutdown can return with a runner still writing. Note this issues the SECOND cancel each
    worker sees — ``task.cancel()`` here, then the ``wait_for`` expiry would cancel again —
    which is why the drain handle is idempotent.
    """
    global _workers
    if _stop is not None:
        _stop.set()
    for task in _workers:
        task.cancel()
    for task in _workers:
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, timeout=_SHUTDOWN_TASK_WAIT)
    _workers = []
