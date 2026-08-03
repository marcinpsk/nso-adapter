# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The exclusive per-device execution claim.

One ``device_claim`` row per device, with a token minted fresh on every acquisition and
validated by a ROW LOCK — not a predicate — before any effect performed on its behalf.

Why a row lock and not `EXISTS (SELECT 1 FROM device_claim WHERE …)`: a predicate proves
the token was valid at statement time, and nothing makes it valid at COMMIT time. A holder
could issue a guarded write while its token was good, pause, have the claim revoked and
taken over, and then commit. Holding ``SELECT … FOR UPDATE`` to commit makes the ordering a
database serialization property instead: a takeover's DELETE physically blocks until the
in-flight transaction commits or aborts, and once the DELETE commits no later transaction
can re-acquire the lock, because the row is gone.

The rule this module exists to enforce, and it is purpose-independent — ``job``,
``intent_put``, ``teardown``, ``sweep`` and ``failover`` all acquire the same claim:

    every transaction containing an effect performed on behalf of a claim must, before
    its first effectful statement, take that claim's row lock, and hold it to commit.

Lock order for every transaction built here, and for every caller (§3.9):

    device_claim -> devices -> intent/tombstone rows -> jobs

Retrying on deadlock does NOT repair an inverted order; it only covers lock upgrades and
PostgreSQL's own internal ordering. Any new transaction that takes two or more of these
must take them in that sequence.
"""

from __future__ import annotations

import asyncio
import enum
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import delete, select, text
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.store.models import DeviceClaim, Job, JobStatus, JobType

logger = structlog.get_logger(__name__)

# The five holders. A closed set, mirrored by ck_device_claim_purpose.
PURPOSES = frozenset({"job", "intent_put", "teardown", "sweep", "failover"})

# ── timing (the derivation the claim cutoff depends on) ──────────────────────
#
# These live together because the cutoff is DERIVED from the budgets and the two can
# never be changed independently: a cutoff shorter than the job lifecycle revokes a live
# runner, and that runner's own writes then race its successor's.
#
# The execution budget bounds the runner's execution phase and must clear the largest
# existing inner timeout (900s for the comprehensive sync runners) plus post-apply refresh
# headroom, because apply and removal are called with no wait_for at all.
JOB_EXECUTION_BUDGET = 1200.0
# The drain phase after cancellation is issued. `await_uncancellable` absorbs cancels for
# 5s and then observes its span for 2s, so a drain must clear that plus the margin the
# server-enforced statement/lock timeouts imply.
JOB_CANCEL_DRAIN = 30.0
# A wall-clock bound on the whole disposition/release future. Server timeouts bound
# individual statements; they do NOT bound connection checkout, a stalled socket, a
# rollback, or waiting for a lost COMMIT ack.
JOB_CLEANUP_BOUND = 60.0
# The reaper's scheduling margin. In a live process the periodic tick is the ONLY thing
# that looks at staleness, and it cannot fire more often than its configured interval
# (SchedulerConfig.orphan_reap_interval, 5 minutes by default). The margin therefore has to
# clear a whole tick plus slack: sized below the interval, a claim becomes revocable before
# anything has scanned for it, so the first scan after the cutoff can revoke a holder that
# is still inside its own lifecycle. A test pins this against the configured default, so
# raising that default fails loudly here instead of silently shrinking the margin.
_REAP_INTERVAL_DEFAULT_S = 5 * 60.0
REAPER_MARGIN = _REAP_INTERVAL_DEFAULT_S + 120.0

# The four terms the cutoff must clear.
_LIFECYCLE_TOTAL = JOB_EXECUTION_BUDGET + JOB_CANCEL_DRAIN + JOB_CLEANUP_BOUND + REAPER_MARGIN
# STRICTLY greater, never equal. At equality a runner that used every last second of its
# budget, drain and cleanup is revocable at the exact instant it may still be committing —
# which is the one state the whole token/row-lock design exists to make impossible.
STALE_SLACK = 60.0

CLAIM_STALE_AFTER = _LIFECYCLE_TOTAL + STALE_SLACK
# The claimless lane's cutoff. Sized against the OUTER lifecycle, not provision's inner
# 600s timeout: a heartbeat that stopped would otherwise let the reaper requeue a still
# running onboarding and run it twice.
PROVISION_STALE_AFTER = _LIFECYCLE_TOTAL + STALE_SLACK

# How often a waiter re-attempts acquisition. Short enough that the handoff from a
# releasing holder costs the next waiter almost nothing, long enough not to spin.
CLAIM_WAIT_POLL_INTERVAL_S = 0.05
# Monotonic: a wall-clock budget would stretch or collapse if the host clock steps.
_monotonic = time.monotonic


def claim_stale_cutoff() -> float:
    """Seconds of heartbeat silence after which a claim may be revoked."""
    return CLAIM_STALE_AFTER


class ClaimLostError(Exception):
    """The claim this transaction acts for is gone, or now belongs to someone else.

    Deliberately OUTSIDE the app's error hierarchy so no ``api_error`` mapping converts
    it, and re-raised explicitly through every broad ``except Exception`` on a claimed
    path: swallowing it turns a revocation into a benign-looking outcome and lets the
    revoked holder carry on writing under ownership it no longer has.
    """


class ClaimUnavailableError(Exception):
    """The claim was held by someone else for the whole wait budget.

    Raised by :func:`acquire_claim_or_refuse` (and so by :func:`held_claim`) rather than
    returned, so a caller with an answer for "busy" — the intent PUT's and the offboard's
    409 — gives it instead of blocking indefinitely.
    """


class ClaimOutcome(enum.Enum):
    """What a claim-bearing COMMIT is known to have done.

    Never a boolean: a lost COMMIT ack makes "did it commit" genuinely unknowable, and
    collapsing that into False drives callers into the wrong repair.
    """

    COMMIT_ACKNOWLEDGED = "commit_acknowledged"
    ABORT_KNOWN = "abort_known"
    OUTCOME_UNKNOWN = "outcome_unknown"


class ClaimRegistration:
    """The LIVE claim identity for one run. Never captured by value.

    The worker owns it; the runner and the heartbeat hold the SAME object. A provision
    acquires its claim mid-run, so anything that reads the token at task-creation time
    would keep seeing ``None`` and either leak the claim or write unguarded.
    """

    __slots__ = ("device_id", "token")

    def __init__(self, device_id: int | None = None, token: str | None = None) -> None:
        self.device_id, self.token = device_id, token

    @property
    def registered(self) -> bool:
        return self.token is not None

    def register(self, device_id: int, token: str) -> None:
        if self.token is not None:
            raise RuntimeError("a run registers at most once")
        self.device_id, self.token = device_id, token

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        state = "registered" if self.registered else "unregistered"
        return f"ClaimRegistration(device_id={self.device_id}, {state})"


@dataclass(frozen=True)
class RevokedClaim:
    """One claim the reaper removed, and the job it had to re-disposition (if any)."""

    device_id: int
    claim_token: str
    job_id: int | None
    purpose: str


def disposition_for(job_type: JobType) -> JobStatus:
    """Where a re-dispositioned job lands.

    An interrupted ``apply`` is never silently re-pushed — operator intent may have
    changed since. Every other type re-reads current state and re-asserts an
    already-decided outcome, so requeueing only repeats idempotent work.
    """
    return JobStatus.failed if job_type == JobType.apply else JobStatus.queued


@asynccontextmanager
async def claim_session(db: AsyncSession | None) -> AsyncIterator[AsyncSession]:
    """Use the caller's session, or open one that closes deterministically.

    Every primitive here owns its own transaction by default — acquisition, release and
    revocation are each one committed transaction — but callers that already hold a
    session (the endpoint, a rival engine in tests) must be able to supply it.
    """
    if db is not None:
        yield db
        return
    from nso_adapter.store.db import get_session

    gen = get_session()
    own = await anext(gen)
    try:
        yield own
    finally:
        await gen.aclose()


async def _commit_outcome(db: AsyncSession) -> ClaimOutcome:
    """Commit and classify, per the three-state contract.

    A failure raised by COMMIT itself is UNKNOWN by construction — the server may or may
    not have applied it and the client cannot tell. Anything failing earlier has provably
    not committed, so the caller can rely on the claim still being there.
    """
    try:
        await db.commit()
    except Exception as exc:  # noqa: BLE001 - classification IS the handling here
        logger.error("claim.commit_outcome_unknown", error=repr(exc))
        return ClaimOutcome.OUTCOME_UNKNOWN
    return ClaimOutcome.COMMIT_ACKNOWLEDGED


async def _set_lock_timeout(db: AsyncSession, lock_timeout_ms: int | None) -> None:
    """Bound this transaction's lock waits without touching the pooled session.

    SET LOCAL is scoped to the transaction and needs no reset before the connection goes
    back to the pool; a session-level value would, and would also kill the capability
    refresh that legitimately holds one transaction across a 120s NSO action.
    """
    if lock_timeout_ms is not None:
        await db.execute(text(f"SET LOCAL lock_timeout = '{int(lock_timeout_ms)}ms'"))


async def acquire_claim(
    device_id: int,
    purpose: str,
    *,
    job_id: int | None = None,
    db: AsyncSession | None = None,
) -> ClaimRegistration | None:
    """Take the device's claim, or return None if someone else holds it.

    ``INSERT … ON CONFLICT (device_id) DO NOTHING RETURNING`` in its own committed
    transaction. The primary key does the mutual exclusion at the database, across
    connections and processes — no application check is involved, and none would be sound.

    *job_id* stays NULL for the worker's queued-head acquisition: it is a real FK, and
    PostgreSQL validates an inserted FK by locking the referenced row FOR KEY SHARE, which
    conflicts with the FOR UPDATE an endpoint holds on the queued winner. The worker would
    block inside the FK check before its SKIP LOCKED ever ran, stalling on one busy device.
    Provision is the exception and passes it: its job is already running, is its own, and
    carries no winner lock, so the check contends with nothing — and a revocation with no
    job recorded could not re-disposition it.
    """
    if purpose not in PURPOSES:
        raise ValueError(f"unknown claim purpose {purpose!r}")
    token = uuid.uuid4().hex
    async with claim_session(db) as conn:
        stmt = (
            pg_insert(DeviceClaim)
            .values(device_id=device_id, claim_token=token, purpose=purpose, job_id=job_id)
            .on_conflict_do_nothing(index_elements=["device_id"])
            .returning(DeviceClaim.claim_token)
        )
        try:
            granted = await conn.scalar(stmt)
            await conn.commit()
        except IntegrityError as exc:
            await conn.rollback()
            # The device vanished between discovery and acquisition (teardown won the
            # race). "Cannot claim" is the honest answer; raising would abort a whole
            # sweep — at startup, the whole lifespan. Scoped to THIS constraint so a
            # bad job_id still surfaces.
            if getattr(getattr(exc.orig, "__cause__", None), "constraint_name", None) == "device_claim_device_id_fkey":
                logger.info("claim.device_vanished", device_id=device_id, purpose=purpose)
                return None
            raise
    if granted is None:
        logger.debug("claim.acquire_conflict", device_id=device_id, purpose=purpose)
        return None
    logger.info("claim.acquired", device_id=device_id, purpose=purpose, job_id=job_id)
    return ClaimRegistration(device_id, granted)


async def acquire_claim_waiting(
    device_id: int,
    purpose: str,
    *,
    timeout_s: float,
    job_id: int | None = None,
    poll_interval_s: float = CLAIM_WAIT_POLL_INTERVAL_S,
) -> ClaimRegistration | None:
    """Retry :func:`acquire_claim` until *timeout_s* elapses; None if it never won.

    Polling, deliberately NOT a blocking database lock: the wait budget is an application
    policy that has to hold across processes and outlive any one transaction, and a lock
    wait would additionally be capped by whatever ``lock_timeout`` the deployment sets.
    Each attempt is its own committed transaction, so a waiter holds nothing while it waits.
    """
    deadline = _monotonic() + timeout_s
    while True:
        reg = await acquire_claim(device_id, purpose, job_id=job_id)
        if reg is not None:
            return reg
        if _monotonic() >= deadline:
            return None
        await asyncio.sleep(min(poll_interval_s, max(0.0, deadline - _monotonic())))


async def acquire_claim_or_refuse(
    device_id: int,
    purpose: str,
    *,
    timeout_s: float,
    job_id: int | None = None,
) -> ClaimRegistration:
    """Wait for the claim, or raise :class:`ClaimUnavailableError`.

    Every timeout is logged: on the plugin's push path the 409 is swallowed by
    ``_push_changed``, so this log line is the only signal the contention happened.
    """
    reg = await acquire_claim_waiting(device_id, purpose, timeout_s=timeout_s, job_id=job_id)
    if reg is None:
        logger.warning("claim.wait_timeout", device_id=device_id, purpose=purpose, waited_s=timeout_s)
        raise ClaimUnavailableError(f"device {device_id} is claimed by another operation")
    return reg


@asynccontextmanager
async def held_claim(
    device_id: int,
    purpose: str,
    *,
    timeout_s: float,
    job_id: int | None = None,
    guard_db: AsyncSession | None = None,
) -> AsyncIterator[ClaimRegistration]:
    """Hold the device claim for the body, and release it on EVERY exit.

    A refusal raised after acquisition — a store-dependent 422, a fence read, a DB error —
    must not leave the device claimed until the reaper notices; the release lives here so
    no call site can forget it.

    *guard_db* is the session the body guard-locks the claim in, and it MUST be passed
    when there is one: a body that dies after ``lock_claim`` leaves its FOR UPDATE pending
    in that session, and the standalone release would wait on our own lock — forever where
    no lock timeout is set, or abort and leave the claim to the reaper where one is. It is
    rolled back before the release (a no-op after the body's own commit).
    """
    reg = await acquire_claim_or_refuse(device_id, purpose, timeout_s=timeout_s, job_id=job_id)
    try:
        yield reg
    finally:
        if guard_db is not None:
            with suppress(Exception):
                await guard_db.rollback()
        await release_claim(reg)


async def lock_claim(db: AsyncSession, reg: ClaimRegistration) -> None:
    """Take the claim's row lock; raise ClaimLostError if it is not ours any more.

    Call this before the FIRST effectful statement of the transaction and let the lock ride
    to commit. A no-op while *reg* is unregistered: the claimless lane (a provision that
    has not reached mapping) has nothing to validate, which is what lets guarded code call
    this unconditionally instead of branching at every site.
    """
    if not reg.registered:
        return
    held = await db.scalar(
        select(DeviceClaim.claim_token)
        .where(DeviceClaim.device_id == reg.device_id, DeviceClaim.claim_token == reg.token)
        .with_for_update()
    )
    if held is None:
        logger.warning("claim.lost", device_id=reg.device_id)
        raise ClaimLostError(f"claim for device {reg.device_id} is no longer held")


async def release_claim(
    reg: ClaimRegistration,
    *,
    db: AsyncSession | None = None,
    lock_timeout_ms: int | None = None,
) -> ClaimOutcome:
    """Delete our own claim row. Token-scoped, so it can never remove a successor's.

    The normal-success path. On anything other than COMMIT_ACKNOWLEDGED the caller must
    abandon the claim to staleness and NEVER retry the delete — see
    :func:`abandon_claim_to_staleness`.
    """
    if not reg.registered:
        return ClaimOutcome.COMMIT_ACKNOWLEDGED
    async with claim_session(db) as conn:
        try:
            await _set_lock_timeout(conn, lock_timeout_ms)
            # Row lock first: the delete is itself an effect on behalf of this claim, and
            # taking the lock is what makes a concurrent revoke serialize against it.
            await lock_claim(conn, reg)
            await conn.execute(
                delete(DeviceClaim).where(
                    DeviceClaim.device_id == reg.device_id,
                    DeviceClaim.claim_token == reg.token,
                )
            )
        except ClaimLostError:
            # Already revoked and taken over: nothing of ours remains to delete.
            await conn.rollback()
            return ClaimOutcome.ABORT_KNOWN
        except Exception as exc:  # noqa: BLE001 - provably before COMMIT, so provably aborted
            logger.warning("claim.release_aborted", device_id=reg.device_id, error=repr(exc))
            await conn.rollback()
            return ClaimOutcome.ABORT_KNOWN
        return await _commit_outcome(conn)


def abandon_claim_to_staleness(reg: ClaimRegistration, outcome: ClaimOutcome) -> None:
    """Leave the claim row exactly as it is and let the reaper take it.

    Never issue a second DELETE. Not because it could hit a successor — tokens are
    per-acquisition and deletes are token-scoped, so it cannot — but because under
    ABORT_KNOWN the job is still running with its ORIGINAL claim, and deleting that claim
    strands it: unchanged status, no claim, invisible to the reaper forever.

    The caller must stop the heartbeat unconditionally; that is the whole mechanism, since
    a heartbeated claim never goes stale and the reaper would never see it.
    """
    logger.error(
        "claim.abandoned_to_staleness",
        device_id=reg.device_id,
        outcome=outcome.value,
    )


async def terminalize_running(
    db: AsyncSession,
    job_id: int,
    *,
    status: JobStatus,
    error: dict | None = None,
) -> JobStatus | None:
    """Move a still-running job to *status*; return what was written, None if not running.

    The status guard matters: a runner can commit its own terminal status and be cancelled
    at the very next await, and rewriting that would re-run finished work.

    A requeue coalesces with a queued same-type successor: admission deliberately lets a
    running job's successor queue up, so the (device, type) uniqueness slot may already be
    occupied — writing ``queued`` would violate ``uq_job_queued_per_device_type``. The job
    lands ``failed``/``superseded`` instead; the successor re-runs the same idempotent
    work. Removals are exempt from the index (one job per scope, all must run) and always
    requeue.
    """
    if status == JobStatus.queued:
        row = (
            await db.execute(
                select(Job.device_id, Job.job_type).where(Job.id == job_id, Job.status == JobStatus.running)
            )
        ).one_or_none()
        if row is not None and row.device_id is not None and row.job_type != JobType.removal:
            successor_id = await db.scalar(
                select(Job.id)
                .where(
                    Job.device_id == row.device_id,
                    Job.job_type == row.job_type,
                    Job.status == JobStatus.queued,
                )
                .limit(1)
            )
            if successor_id is not None:
                status = JobStatus.failed
                error = {
                    "code": "superseded",
                    "message": "Interrupted; an equivalent queued job covers the re-run",
                    "detail": {"queued_successor_id": successor_id},
                }
    values: dict = {"status": status}
    if error is not None:
        values["error"] = error
    if status == JobStatus.queued:
        values["started_at"] = None
        values["heartbeat_at"] = None
    result = await db.execute(sa_update(Job).where(Job.id == job_id, Job.status == JobStatus.running).values(**values))
    return status if result.rowcount else None


async def mark_failed_and_release(
    job_id: int,
    code: str,
    message: str,
    reg: ClaimRegistration,
    *,
    db: AsyncSession | None = None,
    lock_timeout_ms: int | None = None,
) -> ClaimOutcome:
    """Write the terminal failure AND delete the claim in ONE transaction.

    Not a failure write followed by a separate release: that shape can leave a terminal
    status with a live claim, or a released claim with the job stranded ``running`` and
    therefore invisible to the reaper, which scans claims.
    """
    async with claim_session(db) as conn:
        try:
            await _set_lock_timeout(conn, lock_timeout_ms)
            await lock_claim(conn, reg)  # claim -> jobs, per §3.9
            await terminalize_running(
                conn,
                job_id,
                status=JobStatus.failed,
                error={"code": code, "message": message, "detail": {}},
            )
            await _delete_own_claim(conn, reg)
        except ClaimLostError:
            # Recovery already re-dispositioned this job; writing now would clobber it.
            logger.warning("claim.mark_failed_claim_lost", job_id=job_id, device_id=reg.device_id)
            await conn.rollback()
            return ClaimOutcome.ABORT_KNOWN
        except Exception as exc:  # noqa: BLE001 - before COMMIT, so provably aborted
            logger.warning("claim.mark_failed_aborted", job_id=job_id, error=repr(exc))
            await conn.rollback()
            return ClaimOutcome.ABORT_KNOWN
        return await _commit_outcome(conn)


async def dispose_cancelled(
    job_id: int,
    job_type: JobType,
    reg: ClaimRegistration,
    *,
    db: AsyncSession | None = None,
    lock_timeout_ms: int | None = None,
) -> ClaimOutcome:
    """Disposition a cancelled run AND delete the claim in ONE transaction.

    Replaces the older best-effort requeue, which swallowed every failure and released
    ownership independently of the status write.
    """
    async with claim_session(db) as conn:
        try:
            await _set_lock_timeout(conn, lock_timeout_ms)
            await lock_claim(conn, reg)
            status = disposition_for(job_type)
            error = (
                {"code": "cancelled", "message": "Worker cancelled the run", "detail": {}}
                if status == JobStatus.failed
                else None
            )
            await terminalize_running(conn, job_id, status=status, error=error)
            await _delete_own_claim(conn, reg)
        except ClaimLostError:
            logger.warning("claim.dispose_claim_lost", job_id=job_id, device_id=reg.device_id)
            await conn.rollback()
            return ClaimOutcome.ABORT_KNOWN
        except Exception as exc:  # noqa: BLE001 - before COMMIT, so provably aborted
            logger.warning("claim.dispose_aborted", job_id=job_id, error=repr(exc))
            await conn.rollback()
            return ClaimOutcome.ABORT_KNOWN
        return await _commit_outcome(conn)


async def _delete_own_claim(db: AsyncSession, reg: ClaimRegistration) -> None:
    if not reg.registered:
        return
    await db.execute(
        delete(DeviceClaim).where(
            DeviceClaim.device_id == reg.device_id,
            DeviceClaim.claim_token == reg.token,
        )
    )


async def revoke_stale_claims(
    *,
    db: AsyncSession | None = None,
    cutoff_seconds: float | None = None,
    lock_timeout_ms: int | None = None,
) -> list[RevokedClaim]:
    """Revoke every claim whose heartbeat has gone silent, and re-disposition its job.

    Revocation NEVER reissues a claim. Startup recovery runs before any worker exists, so a
    reissued claim would have no holder, and the worker's live-claim skip would strand the
    requeued job forever. A worker mints a fresh token when it later claims the head.

    The scan is over ``device_claim`` alone, independent of job status and purpose: an
    ``intent_put`` or ``teardown`` claim has no job by construction, and an apply's claim
    deliberately outlives its terminal job through the post-apply refresh. A reaper keyed on
    ``Job.status = 'running'`` cannot see any of them.

    Each DELETE blocks on any in-flight effectful transaction holding that row, so
    re-disposition can never race a runner that is still committing.

    *lock_timeout_ms* bounds that wait. A reaper that blocks indefinitely behind a wedged
    holder stops reaping every other device too, and the next tick will retry anyway.
    """
    cutoff_at = datetime.now(UTC) - timedelta(
        seconds=cutoff_seconds if cutoff_seconds is not None else CLAIM_STALE_AFTER
    )
    revoked: list[RevokedClaim] = []
    async with claim_session(db) as conn:
        await _set_lock_timeout(conn, lock_timeout_ms)
        rows = (
            await conn.execute(
                delete(DeviceClaim)
                .where(DeviceClaim.heartbeat_at < cutoff_at)
                .returning(
                    DeviceClaim.device_id,
                    DeviceClaim.claim_token,
                    DeviceClaim.job_id,
                    DeviceClaim.purpose,
                )
            )
        ).all()
        for device_id, token, job_id, purpose in rows:
            revoked.append(RevokedClaim(device_id, token, job_id, purpose))
            if job_id is None:
                continue
            job_type = await conn.scalar(select(Job.job_type).where(Job.id == job_id))
            if job_type is None:
                continue
            await terminalize_running(
                conn,
                job_id,
                status=disposition_for(job_type),
                error=(
                    {"code": "orphaned", "message": "Claim revoked after heartbeat loss", "detail": {}}
                    if disposition_for(job_type) == JobStatus.failed
                    else None
                ),
            )
        await conn.commit()
    for entry in revoked:
        logger.warning(
            "claim.revoked",
            device_id=entry.device_id,
            job_id=entry.job_id,
            purpose=entry.purpose,
        )
    return revoked
