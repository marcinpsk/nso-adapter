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

    device_claim -> devices -> intent/tombstone rows -> jobs -> device_settle_counter

The counter comes AFTER jobs (Appendix S §3.3), and that edge is real: a terminal
transaction takes the job row and then the counter row, and never reaches back to
``devices``. It can only stay that way while the counter row is pre-created — a lazy insert
would validate its FK by taking ``FOR KEY SHARE`` on ``devices``, against offboard which
holds ``devices FOR UPDATE`` and then reaches for ``jobs``.

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
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.store.device_settle import MissingSettleCounter, allocate_settle_seq
from nso_adapter.store.models import DeviceClaim, Job, JobStatus, JobType

logger = structlog.get_logger(__name__)

# The five holders. A closed set, mirrored by ck_device_claim_purpose.
PURPOSES = frozenset({"job", "intent_put", "teardown", "sweep", "failover"})

# PostgreSQL's lock_not_available. Read from the SQLSTATE rather than matched in the
# message, which is locale- and version-dependent.
_LOCK_NOT_AVAILABLE = "55P03"

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


class BookkeepingOutcomeUnknown(Exception):
    """A terminal bookkeeping transaction did not complete, after its effect was performed.

    Two causes, one rule. The COMMIT was neither acknowledged nor provably aborted (R2 §4.6);
    or the settlement allocation inside that transaction aborted — deadlock, lock timeout, or
    a counter row a concurrent offboard cascaded away (Appendix S §3.3).

    Raised INSTEAD of letting the caller fall back to a second terminal write. That fallback
    is the bug: if PostgreSQL applied the commit, the job is already terminal with its CAS,
    per-route results and status intact, and a second write would flip it to ``failed`` over
    a landed consumption; and if the allocation aborted, the second write reports ``failed``
    for a device change that really happened, with the result discarded. The runner therefore
    stops here, the worker skips the terminal write AND the release, the heartbeat stops, and
    claim recovery re-dispositions only a job still ``running`` — which is exactly the
    distinction that tells the cases apart (G38).
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

    *run_attempt* is Appendix S's execution token: the value ``jobs.run_attempt`` was
    bumped to when this run was started. It reaches every runner through this same
    object, which is why every terminal writer can name the execution it belongs to.
    It is ``None`` only on the claimless lanes that have no execution to name.
    """

    __slots__ = ("device_id", "run_attempt", "token")

    def __init__(
        self,
        device_id: int | None = None,
        token: str | None = None,
        run_attempt: int | None = None,
    ) -> None:
        self.device_id, self.token, self.run_attempt = device_id, token, run_attempt

    @property
    def registered(self) -> bool:
        return self.token is not None

    def register(self, device_id: int, token: str) -> None:
        if self.token is not None:
            raise RuntimeError("a run registers at most once")
        self.device_id, self.token = device_id, token

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        state = "registered" if self.registered else "unregistered"
        return f"ClaimRegistration(device_id={self.device_id}, attempt={self.run_attempt}, {state})"


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
    token: str | None = None,
    lock_timeout_ms: int | None = None,
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

    *token* is for :func:`acquire_claim_resolving` alone, which has to know the token BEFORE
    the attempt so it can read the durable outcome of an in-doubt COMMIT. It must be freshly
    minted and never reused: a token that outlives its acquisition lets a revoked holder's
    write validate against its successor's claim (ABA). Every other caller lets this mint.

    *lock_timeout_ms* bounds the one wait this statement can genuinely make: ``ON CONFLICT
    DO NOTHING`` blocks on a rival's UNCOMMITTED speculative insertion until that
    transaction resolves, which is not bounded by any application-side budget.
    """
    if purpose not in PURPOSES:
        raise ValueError(f"unknown claim purpose {purpose!r}")
    token = token or uuid.uuid4().hex
    async with claim_session(db) as conn:
        stmt = (
            pg_insert(DeviceClaim)
            .values(device_id=device_id, claim_token=token, purpose=purpose, job_id=job_id)
            .on_conflict_do_nothing(index_elements=["device_id"])
            .returning(DeviceClaim.claim_token)
        )
        try:
            await _set_lock_timeout(conn, lock_timeout_ms)
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
        except DBAPIError as exc:
            await conn.rollback()
            # A lock timeout resolved BEFORE the commit, so nothing of ours landed and the
            # holder we waited on still has it: that is a conflict, not a failure.
            if getattr(exc, "orig", None) is not None and getattr(exc.orig, "sqlstate", None) == _LOCK_NOT_AVAILABLE:
                logger.debug("claim.acquire_lock_timeout", device_id=device_id, purpose=purpose)
                return None
            raise
    if granted is None:
        logger.debug("claim.acquire_conflict", device_id=device_id, purpose=purpose)
        return None
    logger.info("claim.acquired", device_id=device_id, purpose=purpose, job_id=job_id)
    return ClaimRegistration(device_id, granted)


async def acquire_claim_resolving(
    device_id: int,
    purpose: str,
    *,
    job_id: int | None = None,
    lock_timeout_ms: int | None = None,
    adopt: ClaimRegistration | None = None,
) -> ClaimRegistration | None:
    """One acquisition attempt whose in-doubt COMMIT is RESOLVED, never guessed.

    §3.4's three-state contract covers disposition and release; acquisition needs it too. A
    definite conflict, a vanished device or a lock timeout are all unambiguous "not ours".
    But a connection lost AROUND the COMMIT — or a CANCELLATION delivered at that await —
    can leave the row (and, for a caller that inserts a ``Device`` in the same transaction,
    that too) durably committed while this call sees an exception. The token is minted here
    so the durable answer can be read back.

    *adopt* is the caller's live registration, and it exists for the cancellation case
    alone: the cancel MUST propagate (swallowing one breaks the worker's drain), so a claim
    resolved as ours cannot be handed back through the return value. Registering it there
    instead is what makes the worker's claimed terminal path own the release, rather than
    the run dying while a durable claim looks unowned.
    """
    token = uuid.uuid4().hex
    try:
        return await acquire_claim(device_id, purpose, job_id=job_id, token=token, lock_timeout_ms=lock_timeout_ms)
    except BaseException as exc:
        resolved = await resolve_claim_by_token(token)
        if resolved is None:
            raise
        if isinstance(exc, Exception):
            return resolved
        if adopt is not None and not adopt.registered:
            adopt.register(resolved.device_id, resolved.token)
        raise


async def resolve_claim_by_token(token: str, *, timeout_s: float = JOB_CLEANUP_BOUND) -> ClaimRegistration | None:
    """Read the durable outcome of an in-doubt acquisition, by its unique token.

    The token is unique, so the answer is unambiguous if it can be read at all.

    Fail-stops when it cannot be read within the cleanup bound: a run that cannot determine
    whether it owns a device must not continue AND must not release. The bound is observed
    with a NON-cancelling ``asyncio.wait`` for the same reason the worker's cleanup is —
    ``wait_for`` cancels at expiry and then waits for that cancellation to complete, which
    is exactly what a stalled socket will not do. A parent cancellation arriving here is the
    same condition: ownership is still unknown, so it fail-stops rather than unwinding.
    """

    async def _read() -> int | None:
        async with claim_session(None) as conn:
            try:
                return await conn.scalar(select(DeviceClaim.device_id).where(DeviceClaim.claim_token == token))
            finally:
                await conn.rollback()

    def _unresolvable(reason: str) -> None:
        from nso_adapter.core.worker import _failstop

        _failstop("claim.acquisition_unresolvable_failstop", error=reason, cleanup_bound=timeout_s)

    fut = asyncio.ensure_future(_read())
    try:
        done, _pending = await asyncio.wait({fut}, timeout=timeout_s)
    except asyncio.CancelledError:
        _unresolvable("cancelled while resolving")
        raise
    if not done:
        _unresolvable("timed out")
    try:
        device_id = fut.result()
    except Exception as exc:  # noqa: BLE001 - unresolvable ownership is the fail-stop condition
        _unresolvable(repr(exc))
        raise  # unreachable: _failstop does not return
    if device_id is None:
        return None
    logger.warning("claim.acquisition_resolved_committed", device_id=device_id)
    return ClaimRegistration(device_id, token)


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


# ── the ONE terminal writer (Appendix S §3.2) ────────────────────────────────
#
# Sixteen physical terminal writes across six modules was the failure mode: a
# seventeenth that forgets the compare-and-set produces a job whose result no consumer
# can order or attribute, silently. Every per-job status write now runs through
# :func:`_cas_job_status` here, and ``tests/test_no_direct_terminal_write.py`` fails the
# build on any module outside this one that assigns a terminal status.

# "leave device_id unchanged". NOT None, which is a legal value the provision success path
# is the only caller ever to pass, and which an omission must never be read as. Public
# because the one caller that conditionally sets a device has to name it.
UNSET: object = object()

# The two members that end a job, and the two that allocate a settlement sequence. A
# non-terminal status reaching :func:`terminalize` is a caller bug, not a variant to serve:
# it would take a device's sequence for a run that has not finished.
_TERMINAL_STATUSES = frozenset({JobStatus.succeeded, JobStatus.failed})


@dataclass(frozen=True)
class TerminalWrite:
    """What a terminal compare-and-set actually wrote.

    *device_id* is read back from the same statement, so a provision that acquires its
    device in the terminal UPDATE reports the device it just created, not the pre-write
    NULL — and the sequence is allocated against that device rather than against the
    pre-write null.

    *settle_seq* is None exactly when *device_id* is: a provision that failed before
    acquiring a device, the claimless-corruption failure, and anything offboard has already
    detached. A device-scoped cursor can never reach any of them.
    """

    job_id: int
    status: JobStatus
    device_id: int | None
    settle_seq: int | None = None


async def _cas_job_status(
    db: AsyncSession,
    job_id: int,
    *,
    status: JobStatus,
    expect: JobStatus,
    run_attempt: int | None,
    values: dict | None = None,
):
    """Issue the single physical ``jobs.status`` UPDATE; return the row, None on a miss.

    The predicate proves ownership: the status the caller observed AND, for anything with
    an execution to name, the attempt that execution was started at. *run_attempt* is None
    only for the queued-sourced writers (offboard, the claimless-corruption failure),
    which have no execution.
    """
    where = [Job.id == job_id, Job.status == expect]
    if run_attempt is not None:
        where.append(Job.run_attempt == run_attempt)
    stmt = (
        sa_update(Job)
        .where(*where)
        .values(status=status, **(values or {}))
        .returning(Job.device_id)
        .execution_options(synchronize_session=False)
    )
    return (await db.execute(stmt)).one_or_none()


def internal_error(exc: BaseException, *, code: str = "internal", detail: dict | None = None) -> dict:
    """Build the persisted envelope for an unexpected crash: the type only, never the text.

    Exception text can carry credentials — a RESTCONF error echoes the request, an httpx
    error its headers. The full detail belongs in the server log, never in the store.
    """
    return {
        "code": code,
        "message": f"internal error ({type(exc).__name__}); see the server log",
        "detail": detail or {},
    }


class JobError(Exception):
    """Fail the job deliberately: *code* and *message* are author-controlled, safe to persist.

    The blanket crash handlers persist :func:`internal_error` (the exception type only).
    Raise this where the failure is a known condition the operator must read verbatim.
    """

    def __init__(self, code: str, message: str, detail: dict | None = None) -> None:
        super().__init__(message)
        self.error = {"code": code, "message": message, "detail": detail or {}}


def error_envelope(exc: BaseException, *, code: str = "internal", detail: dict | None = None) -> dict:
    """Map an exception to its persisted envelope: a JobError verbatim, anything else type-only."""
    if isinstance(exc, JobError):
        return {**exc.error, "detail": {**(detail or {}), **exc.error["detail"]}}
    return internal_error(exc, code=code, detail=detail)


async def terminalize(
    db: AsyncSession,
    job_id: int,
    *,
    status: JobStatus,
    expect: JobStatus,
    run_attempt: int | None = None,
    result: dict | None = None,
    error: dict | None = None,
    set_device_id: object = UNSET,
) -> TerminalWrite | None:
    """Write one job's terminal status under its ownership predicate. Caller commits.

    Returns None when the predicate matched no row — another execution owns this job, and
    the caller must treat that as such, never as success. Nothing is written in that case,
    so the row is left byte-identical for whoever does own it.

    *set_device_id* takes an explicit sentinel rather than None: only the provision
    success path ever sets one, and omitting the argument must mean "leave it attached".

    A device-bound write also allocates the device's next settlement sequence (Appendix S
    §3.3), in this same transaction and in this order: the status CAS locks the job row and
    proves ownership, then the counter UPDATE locks the counter row, then the sequence lands
    on the job row this transaction already holds. A refused CAS returns before any of it,
    so a doomed write burns no sequence and takes no device-wide lock.

    An allocation that ABORTS — deadlock, lock timeout, or a counter row a concurrent
    offboard cascaded away — takes the whole terminal transaction down with it, including the
    result and any carrier consumption it carried. That is the three-state contract's case,
    not a job failure: it raises :class:`BookkeepingOutcomeUnknown` so no caller writes a
    second terminal status for a device change that already happened, and recovery
    re-dispositions the job while it is still ``running``.
    """
    if status not in _TERMINAL_STATUSES:
        raise ValueError(f"terminalize is for terminal statuses only, not {status.value}")
    values: dict = {}
    if result is not None:
        values["result"] = result
    if error is not None:
        values["error"] = error
    if set_device_id is not UNSET:
        values["device_id"] = set_device_id
    row = await _cas_job_status(db, job_id, status=status, expect=expect, run_attempt=run_attempt, values=values)
    if row is None:
        logger.error(
            "job.terminal_write_refused",
            job_id=job_id,
            requested=status.value,
            expected_status=expect.value,
            expected_attempt=run_attempt,
        )
        return None
    settle_seq: int | None = None
    if row.device_id is not None:
        try:
            settle_seq = await allocate_settle_seq(db, row.device_id)
            await db.execute(
                sa_update(Job)
                .where(Job.id == job_id)
                .values(settle_seq=settle_seq)
                .execution_options(synchronize_session=False)
            )
        except (MissingSettleCounter, DBAPIError) as exc:
            logger.error("job.settle_allocation_failed", job_id=job_id, device_id=row.device_id, error=repr(exc))
            raise BookkeepingOutcomeUnknown(f"job {job_id}: settlement allocation aborted") from exc
    return TerminalWrite(job_id, status, row.device_id, settle_seq)


async def terminalize_queued_bulk(db: AsyncSession, device_id: int, *, error: dict) -> int:
    """Terminalize EVERY queued job of a device in one statement. Caller commits.

    Offboard's writer. A per-job helper cannot express it: the row set is unbounded and
    the transaction that runs it goes on to detach every job of the device, terminal ones
    included. There is no execution to name, so the predicate is the queued status alone.
    """
    result = await db.execute(
        sa_update(Job)
        .where(Job.device_id == device_id, Job.status == JobStatus.queued)
        .values(status=JobStatus.failed, error=error)
        .execution_options(synchronize_session=False)
    )
    return result.rowcount


async def _queued_successor_id(db: AsyncSession, device_id: int, job_type: JobType) -> int | None:
    return await db.scalar(
        select(Job.id)
        .where(Job.device_id == device_id, Job.job_type == job_type, Job.status == JobStatus.queued)
        .limit(1)
    )


def _superseded_error(successor_id: int | None) -> dict:
    return {
        "code": "superseded",
        "message": "Interrupted; an equivalent queued job covers the re-run",
        "detail": {"queued_successor_id": successor_id},
    }


async def terminalize_running(
    db: AsyncSession,
    job_id: int,
    *,
    status: JobStatus,
    error: dict | None = None,
    expected_attempt: int | None = None,
) -> JobStatus | None:
    """Recovery's re-disposition of a still-running job; returns what was WRITTEN.

    The status guard matters: a runner can commit its own terminal status and be cancelled
    at the very next await, and rewriting that would re-run finished work. *expected_attempt*
    is the second half of the predicate and closes the ABA the status alone leaves open —
    ``requeue_orphaned_jobs`` reads its candidates without a row lock and terminalizes them
    statements later, with no claim barrier, so a status-only CAS lands on whatever is
    ``running`` when the UPDATE runs rather than on the run recovery judged stale.

    A requeue coalesces with a queued same-type successor: admission deliberately lets a
    running job's successor queue up, so the (device, type) uniqueness slot may already be
    occupied — writing ``queued`` would violate ``uq_job_queued_per_device_type``. The job
    lands ``failed``/``superseded`` instead; the successor re-runs the same idempotent
    work. Removals are exempt from the index (one job per scope, all must run) and always
    requeue.

    The coalescing decision spans two statements, and admission can commit a successor
    between them, so the requeue UPDATE runs in a SAVEPOINT: the resulting ``IntegrityError``
    would otherwise abort the CALLER's whole transaction — a whole recovery batch — instead
    of returning the ``failed``/``superseded`` outcome this docstring already promises.
    """
    coalescible: tuple[int, JobType] | None = None
    if status == JobStatus.queued:
        row = (
            await db.execute(
                select(Job.device_id, Job.job_type).where(Job.id == job_id, Job.status == JobStatus.running)
            )
        ).one_or_none()
        if row is not None and row.device_id is not None and row.job_type != JobType.removal:
            coalescible = (row.device_id, row.job_type)
            successor_id = await _queued_successor_id(db, *coalescible)
            if successor_id is not None:
                status, error = JobStatus.failed, _superseded_error(successor_id)

    if status == JobStatus.queued:
        values: dict = {"started_at": None, "heartbeat_at": None}
        if error is not None:
            values["error"] = error
        try:
            async with db.begin_nested():
                landed = await _cas_job_status(
                    db,
                    job_id,
                    status=status,
                    expect=JobStatus.running,
                    run_attempt=expected_attempt,
                    values=values,
                )
        except IntegrityError:
            # A successor was admitted between the lookup and the UPDATE. The savepoint
            # absorbed it; re-read and re-issue as the coalesced failure.
            successor_id = await _queued_successor_id(db, *coalescible) if coalescible else None
            logger.warning("claim.requeue_raced_a_successor", job_id=job_id, queued_successor_id=successor_id)
            status, error = JobStatus.failed, _superseded_error(successor_id)
        else:
            return JobStatus.queued if landed is not None else None

    write = await terminalize(
        db,
        job_id,
        status=status,
        expect=JobStatus.running,
        run_attempt=expected_attempt,
        error=error,
    )
    return write.status if write is not None else None


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
                expected_attempt=reg.run_attempt,
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
            await terminalize_running(conn, job_id, status=status, error=error, expected_attempt=reg.run_attempt)
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
            observed = (await conn.execute(select(Job.job_type, Job.run_attempt).where(Job.id == job_id))).one_or_none()
            if observed is None:
                continue
            job_type, run_attempt = observed
            await terminalize_running(
                conn,
                job_id,
                status=disposition_for(job_type),
                error=(
                    {"code": "orphaned", "message": "Claim revoked after heartbeat loss", "detail": {}}
                    if disposition_for(job_type) == JobStatus.failed
                    else None
                ),
                # Carried for uniformity, not for safety: this path's own DELETE holds the
                # device_claim primary-key row lock to COMMIT, and re-acquisition inserts
                # that same key, so no successor can re-enter `running` before it lands.
                expected_attempt=run_attempt,
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
