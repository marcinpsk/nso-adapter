# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The per-device execution claim: exclusivity, the row-lock guard, revocation.

Every concurrency case forces the disputed ordering with an explicit barrier and a
SECOND engine on the same database. "Start two coroutines and gather" lets
correct-looking-but-broken code pass by serial scheduling, and an asyncio lock would
prove nothing about a claim that has to hold across processes.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.core.claim import (
    ClaimLostError,
    ClaimOutcome,
    ClaimRegistration,
    acquire_claim,
    claim_stale_cutoff,
    dispose_cancelled,
    lock_claim,
    mark_failed_and_release,
    release_claim,
    revoke_stale_claims,
)
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio


async def _seed_job(device_id: int | None, job_type, status):
    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        job = Job(
            job_type=job_type,
            device_id=device_id,
            status=status,
            coalescible=job_type not in (JobType.removal, JobType.provision),
            context={},
        )
        db.add(job)
        await db.commit()
        return job.id


async def _job_status(job_id: int):
    from nso_adapter.store.models import Job

    async with session() as db:
        return (await db.get(Job, job_id)).status


async def _claim_row(device_id: int):
    from nso_adapter.store.models import DeviceClaim

    async with session() as db:
        return await db.get(DeviceClaim, device_id)


async def _backdate_heartbeat(device_id: int, *, seconds: float) -> None:
    """Age a claim past the reaper cutoff. Encodes the real recovery delay."""
    from nso_adapter.store.models import DeviceClaim

    async with session() as db:
        await db.execute(
            sa.update(DeviceClaim)
            .where(DeviceClaim.device_id == device_id)
            .values(heartbeat_at=datetime.now(UTC) - timedelta(seconds=seconds))
        )
        await db.commit()


# ── acquisition exclusivity (M6.1) ───────────────────────────────────────────


async def test_acquire_returns_a_registration_and_writes_the_row(adapter_client):
    device_id = await seed_device(nso_device_name="cl-acq", netbox_device_id=9900)

    reg = await acquire_claim(device_id, "job")
    assert reg is not None
    assert reg.registered
    assert reg.device_id == device_id

    row = await _claim_row(device_id)
    assert row.claim_token == reg.token
    assert row.purpose == "job"
    # Never an FK-bearing job reference at the worker's queued-head acquisition.
    assert row.job_id is None


async def test_second_acquisition_loses_at_the_database(adapter_client, rival_engine):
    """Two engines, two committed transactions — the exclusion is PostgreSQL's, not ours."""
    device_id = await seed_device(nso_device_name="cl-excl", netbox_device_id=9901)

    first = await acquire_claim(device_id, "job")
    assert first is not None

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as db:
        second = await acquire_claim(device_id, "intent_put", db=db)
    assert second is None, "two holders acquired the same device"

    assert (await _claim_row(device_id)).claim_token == first.token


async def test_rival_blocks_on_an_uncommitted_acquisition_then_loses(adapter_client, rival_engine):
    """Forced contention, not `asyncio.gather`.

    An unsynchronized gather can serialize by scheduling and prove nothing. Here the first
    INSERT is deliberately left UNCOMMITTED: PostgreSQL makes the rival's
    `ON CONFLICT DO NOTHING` wait on the speculative insertion until the first transaction
    resolves, so the rival genuinely contends and is then told it lost.
    """
    from nso_adapter.store.models import DeviceClaim

    device_id = await seed_device(nso_device_name="cl-race", netbox_device_id=9902)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async def _rival_attempt():
        async with rival() as db:
            return await acquire_claim(device_id, "sweep", db=db)

    async with session() as holder:
        # Visible as contention, not yet durable.
        await holder.execute(
            sa.insert(DeviceClaim).values(
                device_id=device_id, claim_token="uncommitted-token", purpose="job", job_id=None
            )
        )
        await holder.flush()

        attempt = asyncio.create_task(_rival_attempt())
        await asyncio.sleep(0.3)
        assert not attempt.done(), "the rival did not block on the uncommitted insert"

        await holder.commit()

    assert await asyncio.wait_for(attempt, timeout=10) is None
    assert (await _claim_row(device_id)).claim_token == "uncommitted-token"


async def test_provision_acquisition_sets_job_id(adapter_client):
    """Its job is already running, is its own, and carries no winner lock — so the FK
    check contends with nothing, and without job_id a revocation could not re-disposition it."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-prov", netbox_device_id=9903)
    job_id = await _seed_job(None, JobType.provision, JobStatus.running)

    reg = await acquire_claim(device_id, "job", job_id=job_id)
    assert (await _claim_row(device_id)).job_id == job_id
    assert reg.token


async def test_each_acquisition_mints_a_fresh_token(adapter_client):
    """M6.8 — per-acquisition, never a process identity: token reuse would let a revoked
    holder's write validate against its successor's claim."""
    device_id = await seed_device(nso_device_name="cl-aba", netbox_device_id=9904)

    tokens = set()
    for _ in range(3):
        reg = await acquire_claim(device_id, "job")
        tokens.add(reg.token)
        assert await release_claim(reg) is ClaimOutcome.COMMIT_ACKNOWLEDGED
    assert len(tokens) == 3


# ── the in-doubt acquisition COMMIT ──────────────────────────────────────────


async def test_a_committed_acquisition_is_resolvable_by_its_token(adapter_client):
    """The resolving acquisition knows its token BEFORE the attempt, so a lost COMMIT ack is
    answerable — and still mints a fresh one per acquisition, so nothing reuses a token."""
    from nso_adapter.core.claim import acquire_claim_resolving, resolve_claim_by_token

    device_id = await seed_device(nso_device_name="cl-indoubt", netbox_device_id=9908)

    reg = await acquire_claim_resolving(device_id, "job")
    assert reg is not None

    resolved = await resolve_claim_by_token(reg.token)
    assert resolved is not None
    assert (resolved.device_id, resolved.token) == (device_id, reg.token)

    tokens = {reg.token}
    for _ in range(2):
        assert await release_claim(reg) is ClaimOutcome.COMMIT_ACKNOWLEDGED
        reg = await acquire_claim_resolving(device_id, "job")
        tokens.add(reg.token)
    assert len(tokens) == 3, "a token was reused across acquisitions"


async def test_a_resolving_acquisition_bounds_its_own_conflict_wait(adapter_client, rival_engine):
    """``ON CONFLICT DO NOTHING`` waits on an UNCOMMITTED rival insertion, and no
    application-side budget bounds that wait — only a server-side lock timeout does.

    Without the bound this blocks until the rival's transaction ends, however long that is.
    """
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from nso_adapter.core.claim import acquire_claim_resolving
    from nso_adapter.store.models import DeviceClaim

    device_id = await seed_device(nso_device_name="cl-stalled", netbox_device_id=9909)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with rival() as stalled:
        # Visible as contention and deliberately never resolved while the contender runs.
        await stalled.execute(
            sa.insert(DeviceClaim).values(device_id=device_id, claim_token="stalled", purpose="job", job_id=None)
        )
        await stalled.flush()

        contender = asyncio.create_task(acquire_claim_resolving(device_id, "job", lock_timeout_ms=400))
        assert await asyncio.wait_for(contender, timeout=10) is None, "it waited past its bound"

        await stalled.rollback()


async def test_an_uncommitted_acquisition_resolves_to_nothing(adapter_client):
    from nso_adapter.core.claim import resolve_claim_by_token

    assert await resolve_claim_by_token("never-written") is None


async def test_an_unresolvable_acquisition_fail_stops(adapter_client, monkeypatch):
    """A run that cannot determine whether it owns a device must not continue OR release.

    Both alternatives are corruption: continuing writes under ownership it cannot prove,
    and releasing frees a device it may still hold.
    """
    from nso_adapter.core import claim as claim_mod
    from nso_adapter.core import worker as worker_mod

    killed: dict = {}

    def _record(event, **fields):
        killed["event"] = event
        raise SystemExit(70)

    def _unreadable(_db):
        raise ConnectionError("the store is unreachable")

    monkeypatch.setattr(worker_mod, "_failstop", _record)
    monkeypatch.setattr(claim_mod, "claim_session", _unreadable)

    with pytest.raises(SystemExit):
        await claim_mod.resolve_claim_by_token("whatever")
    assert killed["event"] == "claim.acquisition_unresolvable_failstop"


# ── the row-lock guard ───────────────────────────────────────────────────────


async def test_acquire_claim_skips_a_device_deleted_between_discovery_and_now(adapter_client):
    """Teardown can delete the device after a sweeper/worker discovery listed it. The FK
    violation must read as "cannot claim" (None), not an exception — at startup that
    exception would propagate through the sweep and fail the whole lifespan."""
    assert await acquire_claim(986000, "sweep") is None


async def test_lock_claim_passes_for_the_holder(adapter_client):
    device_id = await seed_device(nso_device_name="cl-lock-ok", netbox_device_id=9905)
    reg = await acquire_claim(device_id, "job")
    async with session() as db:
        await lock_claim(db, reg)  # must not raise
        await db.rollback()


async def test_lock_claim_raises_for_a_stale_token(adapter_client):
    device_id = await seed_device(nso_device_name="cl-lock-stale", netbox_device_id=9906)
    reg = await acquire_claim(device_id, "job")
    await release_claim(reg)
    successor = await acquire_claim(device_id, "job")
    assert successor.token != reg.token

    async with session() as db:
        with pytest.raises(ClaimLostError):
            await lock_claim(db, reg)


async def test_lock_claim_is_a_noop_while_unregistered(adapter_client):
    """The claimless lane: an unregistered provision has nothing to validate, so guarded
    code can call the guard unconditionally."""
    async with session() as db:
        await lock_claim(db, ClaimRegistration())  # must not raise


async def test_revoked_holder_write_raises_and_commits_nothing(adapter_client, rival_engine):
    """M6.6 — a timestamp-only steal would let A's own write land afterwards."""
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="cl-revoked", netbox_device_id=9907)
    reg = await acquire_claim(device_id, "job")

    await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as db:
        revoked = await revoke_stale_claims(db=db)
    assert [r.device_id for r in revoked] == [device_id]

    async with session() as db:
        with pytest.raises(ClaimLostError):
            await lock_claim(db, reg)
            device = await db.get(Device, device_id)
            device.nso_device_name = "written-under-revoked-claim"
            await db.commit()

    async with session() as db:
        assert (await db.get(Device, device_id)).nso_device_name == "cl-revoked"


async def test_write_before_takeover_cannot_commit_after(adapter_client, rival_engine):
    """M6.10 — the case a predicate-based guard silently permits.

    A takes the row lock, writes, and pauses BEFORE committing. The revoke's DELETE then
    physically blocks on A's row lock. Exactly one order may be observable: A commits and
    then revocation lands, or revocation wins and A's commit fails. What must never
    happen is "revocation committed, then A's write committed".

    The blocking is proven POSITIVELY, by PostgreSQL: the rival revoke runs with a short
    ``lock_timeout`` and must fail waiting on the claim row. Timing alone cannot prove it —
    an unfinished task is equally consistent with a slow connection checkout, so a
    predicate-guarded build would pass a "not done yet" assertion.
    """
    from sqlalchemy.exc import DBAPIError

    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="cl-linear", netbox_device_id=9908)
    reg = await acquire_claim(device_id, "job")
    await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with session() as db:
        await lock_claim(db, reg)
        device = await db.get(Device, device_id)
        device.nso_device_name = "written-by-a"
        await db.flush()

        # The revoke's DELETE must WAIT on A's row lock — proven by the wait expiring.
        with pytest.raises(DBAPIError) as blocked:
            async with rival() as rival_db:
                await revoke_stale_claims(db=rival_db, lock_timeout_ms=400)
        assert "lock timeout" in str(blocked.value).lower() or "canceling statement" in str(blocked.value).lower()

        assert (await _claim_row(device_id)).claim_token == reg.token, "the claim was revoked mid-write"
        await db.commit()

    # Only now can the revoke proceed.
    async with rival() as rival_db:
        revoked = await revoke_stale_claims(db=rival_db)

    async with session() as db:
        # A committed first, so its write stands and the claim went afterwards.
        assert (await db.get(Device, device_id)).nso_device_name == "written-by-a"
    assert await _claim_row(device_id) is None
    assert [r.device_id for r in revoked] == [device_id]


# ── revocation (M6.7, M6.14) ─────────────────────────────────────────────────


async def test_revoke_leaves_no_claim_and_redispositions_the_job(adapter_client):
    """M6.7/M6.13 — recovery must not reissue a claim: startup runs before any worker
    exists, so a reissued claim has no holder and the worker's live-claim skip would
    strand the requeued job forever."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-revoke", netbox_device_id=9909)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    reg = await acquire_claim(device_id, "job", job_id=job_id)
    assert reg is not None
    await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)

    revoked = await revoke_stale_claims()
    assert [r.job_id for r in revoked] == [job_id]

    assert await _claim_row(device_id) is None
    assert await _job_status(job_id) is JobStatus.queued


async def test_revoke_fails_an_apply_instead_of_requeueing_it(adapter_client):
    """An interrupted apply is never silently re-pushed."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-revoke-apply", netbox_device_id=9910)
    job_id = await _seed_job(device_id, JobType.apply, JobStatus.running)
    await acquire_claim(device_id, "job", job_id=job_id)
    await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)

    await revoke_stale_claims()
    assert await _job_status(job_id) is JobStatus.failed


async def test_revoke_scans_claims_independently_of_job_and_purpose(adapter_client):
    """M6.14 — an intent_put or teardown claim has no job by construction, and an apply's
    claim deliberately outlives its terminal job. A reaper starting from
    Job.status='running' cannot see any of them."""
    ids = {}
    for index, purpose in enumerate(("intent_put", "teardown", "failover", "sweep")):
        device_id = await seed_device(nso_device_name=f"cl-nojob-{purpose}", netbox_device_id=9920 + index)
        await acquire_claim(device_id, purpose)
        await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)
        ids[purpose] = device_id

    revoked = await revoke_stale_claims()
    assert {r.device_id for r in revoked} == set(ids.values())
    for device_id in ids.values():
        assert await _claim_row(device_id) is None


async def test_revoke_leaves_a_fresh_claim_alone(adapter_client):
    """A claim heartbeating inside the cutoff belongs to a LIVE holder."""
    device_id = await seed_device(nso_device_name="cl-fresh", netbox_device_id=9930)
    reg = await acquire_claim(device_id, "job")

    assert await revoke_stale_claims() == []
    assert (await _claim_row(device_id)).claim_token == reg.token


async def test_revoke_does_not_touch_a_terminal_job(adapter_client):
    """An apply's claim outlives its terminal job through post-refresh; revoking the claim
    must not rewrite a status the runner already committed."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-terminal", netbox_device_id=9931)
    job_id = await _seed_job(device_id, JobType.apply, JobStatus.succeeded)
    await acquire_claim(device_id, "job", job_id=job_id)
    await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)

    await revoke_stale_claims()
    assert await _claim_row(device_id) is None
    assert await _job_status(job_id) is JobStatus.succeeded


# ── terminal writers: one transaction, three-state outcome ───────────────────


async def test_mark_failed_and_release_is_one_transaction(adapter_client):
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-mfr", netbox_device_id=9940)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    outcome = await mark_failed_and_release(job_id, "internal", "boom", reg)
    assert outcome is ClaimOutcome.COMMIT_ACKNOWLEDGED
    assert await _job_status(job_id) is JobStatus.failed
    assert await _claim_row(device_id) is None


async def test_mark_failed_and_release_writes_nothing_for_a_stale_token(adapter_client):
    """M9.11's shape: a revoked runner's terminal write must not overwrite the
    disposition recovery already made."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-mfr-stale", netbox_device_id=9941)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    reg = await acquire_claim(device_id, "job", job_id=job_id)
    await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)
    await revoke_stale_claims()
    assert await _job_status(job_id) is JobStatus.queued

    outcome = await mark_failed_and_release(job_id, "internal", "boom", reg)
    assert outcome is ClaimOutcome.ABORT_KNOWN
    # recovery's disposition survives
    assert await _job_status(job_id) is JobStatus.queued


@pytest.mark.parametrize(
    ("job_type_name", "expected"),
    [("apply", "failed"), ("sync", "queued")],
)
async def test_dispose_cancelled_dispositions_per_job_type(adapter_client, job_type_name, expected):
    """M6.9e — apply ends failed (never silently re-pushed); an idempotent type requeues."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name=f"cl-disp-{job_type_name}", netbox_device_id=9950)
    job_type = getattr(JobType, job_type_name)
    job_id = await _seed_job(device_id, job_type, JobStatus.running)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    outcome = await dispose_cancelled(job_id, job_type, reg)
    assert outcome is ClaimOutcome.COMMIT_ACKNOWLEDGED
    assert (await _job_status(job_id)).value == expected
    assert await _claim_row(device_id) is None


async def _job_error(job_id: int):
    from nso_adapter.store.models import Job

    async with session() as db:
        return (await db.get(Job, job_id)).error


async def test_revoke_supersedes_instead_of_colliding_with_a_queued_successor(adapter_client):
    """Admission deliberately lets a running job's same-type successor queue up, so the
    (device, type) uniqueness slot may already be taken when recovery wants to requeue.
    An unguarded requeue violates uq_job_queued_per_device_type — and aborts the reaper's
    whole single-transaction batch, every tick, blocking the device lane forever. The
    interrupted job must land failed/superseded instead: the successor re-runs the same
    idempotent read."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-succ-revoke", netbox_device_id=9960)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    successor_id = await _seed_job(device_id, JobType.sync, JobStatus.queued)
    await acquire_claim(device_id, "job", job_id=job_id)
    await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)

    revoked = await revoke_stale_claims()

    assert [r.job_id for r in revoked] == [job_id]
    assert await _claim_row(device_id) is None
    assert await _job_status(job_id) is JobStatus.failed
    assert (await _job_error(job_id))["code"] == "superseded"
    assert await _job_status(successor_id) is JobStatus.queued


async def test_dispose_cancelled_supersedes_instead_of_colliding(adapter_client):
    """Same collision through graceful cancellation: dispose must still commit (claim
    deleted, job terminal) rather than abort and abandon the claim to staleness."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-succ-dispose", netbox_device_id=9961)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    successor_id = await _seed_job(device_id, JobType.sync, JobStatus.queued)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    outcome = await dispose_cancelled(job_id, JobType.sync, reg)

    assert outcome is ClaimOutcome.COMMIT_ACKNOWLEDGED
    assert await _claim_row(device_id) is None
    assert await _job_status(job_id) is JobStatus.failed
    assert (await _job_error(job_id))["code"] == "superseded"
    assert await _job_status(successor_id) is JobStatus.queued


async def test_removal_requeue_is_exempt_from_supersession(adapter_client):
    """Removals are exempt from the uniqueness index (one job per scope, all must run),
    so their requeue can never collide and must NOT be coalesced away."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-succ-removal", netbox_device_id=9962)
    job_id = await _seed_job(device_id, JobType.removal, JobStatus.running)
    other_id = await _seed_job(device_id, JobType.removal, JobStatus.queued)
    await acquire_claim(device_id, "job", job_id=job_id)
    await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)

    await revoke_stale_claims()

    assert await _job_status(job_id) is JobStatus.queued
    assert await _job_status(other_id) is JobStatus.queued


async def test_dispose_cancelled_leaves_a_terminal_job_alone(adapter_client):
    """The runner may have committed a terminal status and been cancelled at the very
    next await — the status guard means a finished job is never re-run."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-disp-terminal", netbox_device_id=9951)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.succeeded)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    assert await dispose_cancelled(job_id, JobType.sync, reg) is ClaimOutcome.COMMIT_ACKNOWLEDGED
    assert await _job_status(job_id) is JobStatus.succeeded
    assert await _claim_row(device_id) is None


async def test_release_claim_is_token_scoped(adapter_client):
    """A revoked holder can never delete its successor's row."""
    device_id = await seed_device(nso_device_name="cl-rel-scope", netbox_device_id=9960)
    first = await acquire_claim(device_id, "job")
    await release_claim(first)
    second = await acquire_claim(device_id, "job")

    assert await release_claim(first) is ClaimOutcome.ABORT_KNOWN
    assert (await _claim_row(device_id)).claim_token == second.token


async def test_release_claim_is_a_noop_while_unregistered(adapter_client):
    assert await release_claim(ClaimRegistration()) is ClaimOutcome.COMMIT_ACKNOWLEDGED


async def test_lock_contention_reports_abort_known(adapter_client, rival_engine):
    """M6.9i/M6.9k(b) — a lock timeout resolves BEFORE the commit, so the abort is KNOWN
    and the claim provably remains. The caller must then abandon it to staleness rather
    than retry the delete, because retrying would strand a still-running job."""
    device_id = await seed_device(nso_device_name="cl-abort", netbox_device_id=9970)
    reg = await acquire_claim(device_id, "job")

    from nso_adapter.store.models import DeviceClaim

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as blocker:
        # Hold the claim row so the release's own FOR UPDATE cannot get it.
        await blocker.execute(sa.select(DeviceClaim).where(DeviceClaim.device_id == device_id).with_for_update())
        outcome = await release_claim(reg, lock_timeout_ms=250)
        assert outcome is ClaimOutcome.ABORT_KNOWN
        await blocker.rollback()

    # provably rolled back: the claim is still there
    assert (await _claim_row(device_id)).claim_token == reg.token


class _LostAckSession:
    """A REAL session whose COMMIT lands and whose acknowledgement is then lost.

    This is the one failure the three-state contract exists for and the one that cannot be
    reproduced from SQL: the server applied the transaction, the client never learned it.
    Terminating the backend does not reproduce it — PostgreSQL aborts the open transaction,
    which is an ABORT_KNOWN and a different contract. So the loss is injected at the driver
    boundary, after a genuine commit, by delegation rather than by a mock: every other
    attribute is the real session's.
    """

    def __init__(self, inner) -> None:
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def commit(self) -> None:
        await self._inner.commit()  # really commits
        raise ConnectionResetError("acknowledgement lost after COMMIT")


async def _assert_two_atomic_end_states(device_id: int, job_id: int, original, token: str) -> None:
    """The job status and the claim must agree. A torn pair is the only forbidden result."""
    from nso_adapter.store.models import JobStatus

    status = await _job_status(job_id)
    row = await _claim_row(device_id)
    terminal = status in {JobStatus.failed, JobStatus.queued, JobStatus.succeeded} and status is not original
    if terminal:
        assert row is None, f"TORN: job moved to {status} but the claim was retained"
    else:
        assert status is original, f"job moved to {status} without the transition being intended"
        assert row is not None and row.claim_token == token, "TORN: job unchanged but the claim is gone"


@pytest.mark.parametrize("helper", ["dispose_cancelled", "mark_failed_and_release"])
async def test_terminal_helper_aborting_before_commit_leaves_the_pair_intact(adapter_client, rival_engine, helper):
    """M6.9i(i) — a deterministic pre-COMMIT abort: (original status, retained claim).

    Catches the two-transaction shape: an implementation that commits the job transition
    and releases the claim separately reaches (terminal, retained claim) here, which the
    end-state assertion rejects.
    """
    from nso_adapter.store.models import DeviceClaim, JobStatus, JobType

    device_id = await seed_device(nso_device_name=f"cl-abort-{helper}", netbox_device_id=9972)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as blocker:
        # Hold the claim row so the helper's own FOR UPDATE cannot get it.
        await blocker.execute(sa.select(DeviceClaim).where(DeviceClaim.device_id == device_id).with_for_update())

        if helper == "dispose_cancelled":
            outcome = await dispose_cancelled(job_id, JobType.sync, reg, lock_timeout_ms=250)
        else:
            outcome = await mark_failed_and_release(job_id, "internal", "boom", reg, lock_timeout_ms=250)
        assert outcome is ClaimOutcome.ABORT_KNOWN
        await blocker.rollback()

    await _assert_two_atomic_end_states(device_id, job_id, JobStatus.running, reg.token)


@pytest.mark.parametrize("helper", ["dispose_cancelled", "mark_failed_and_release"])
async def test_terminal_helper_losing_the_ack_leaves_the_pair_intact(adapter_client, helper):
    """M6.9i(ii) — a genuine acknowledgement loss after a real commit.

    The safe outcome here is (terminal, no claim): both halves were in the SAME
    transaction, so the commit that landed carried both. An implementation using two
    transactions would land only the first and be caught by the torn-pair assertion.
    """
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name=f"cl-lostack-{helper}", netbox_device_id=9973)
    job_id = await _seed_job(device_id, JobType.sync, JobStatus.running)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    async with session() as real:
        wrapped = _LostAckSession(real)
        if helper == "dispose_cancelled":
            outcome = await dispose_cancelled(job_id, JobType.sync, reg, db=wrapped)
        else:
            outcome = await mark_failed_and_release(job_id, "internal", "boom", reg, db=wrapped)
        assert outcome is ClaimOutcome.OUTCOME_UNKNOWN

    await _assert_two_atomic_end_states(device_id, job_id, JobStatus.running, reg.token)


# ── constants ────────────────────────────────────────────────────────────────


def test_both_cutoffs_strictly_exceed_all_four_lifecycle_terms():
    """The relation, never the literals — and STRICTLY, with the margin included.

    An earlier version omitted REAPER_MARGIN from the right-hand side, which is exactly why
    it could not see that the cutoff had been set EQUAL to the four-term total. At equality
    a runner that used every last second of budget, drain and cleanup is revocable at the
    instant it may still be committing.
    """
    from nso_adapter.core import claim

    four_terms = claim.JOB_EXECUTION_BUDGET + claim.JOB_CANCEL_DRAIN + claim.JOB_CLEANUP_BOUND + claim.REAPER_MARGIN
    assert claim.CLAIM_STALE_AFTER > four_terms
    assert claim.PROVISION_STALE_AFTER > four_terms
    # The 60s orphan window recovers nothing once claims exist — it would re-disposition a
    # job whose holder's token is still valid.
    assert claim.CLAIM_STALE_AFTER > 60.0


def test_reaper_margin_clears_the_configured_reap_interval():
    """The periodic tick is the only thing that scans staleness in a live process.

    Sized below its interval, a claim becomes revocable before anything has looked, so the
    first scan past the cutoff can revoke a holder still inside its own lifecycle. Pinned
    against the configured default so raising that default fails here rather than silently
    shrinking the margin.
    """
    from nso_adapter.config import SchedulerConfig
    from nso_adapter.core import claim

    interval_seconds = SchedulerConfig().orphan_reap_interval * 60.0
    assert interval_seconds > 0
    assert claim.REAPER_MARGIN > interval_seconds


def test_disposition_for_is_the_only_requeue_policy():
    """The record of a completed transition.

    There were two expressions of one rule: ``_REQUEUE_ON_RESTART`` in the worker, used by the
    legacy job-status reaper, and ``disposition_for`` here. They agreed, but a job type added
    to one and forgotten in the other would have diverged silently.

    The legacy reaper is gone — every claimed job now recovers through the claim scan, on one
    clock — so the set went with it and this is the surviving definition. Pinned both ways: the
    rule still holds for every member of ``JobType``, and the duplicate must not come back.
    """
    from nso_adapter.core import worker as worker_mod
    from nso_adapter.core.claim import disposition_for
    from nso_adapter.store.models import JobStatus, JobType

    assert not hasattr(worker_mod, "_REQUEUE_ON_RESTART"), "the duplicate requeue policy came back"
    assert not hasattr(worker_mod, "_ORPHAN_STALE_AFTER"), "the second recovery clock came back"

    for job_type in JobType:
        expected = JobStatus.failed if job_type is JobType.apply else JobStatus.queued
        assert disposition_for(job_type) is expected, job_type
