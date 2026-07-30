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
    from nso_adapter.store.models import Job

    async with session() as db:
        job = Job(job_type=job_type, device_id=device_id, status=status, context={})
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


async def test_concurrent_acquisitions_produce_exactly_one_holder(adapter_client, rival_engine):
    """Both INSERTs in flight at once against real PostgreSQL."""
    device_id = await seed_device(nso_device_name="cl-race", netbox_device_id=9902)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async def _rival_attempt():
        async with rival() as db:
            return await acquire_claim(device_id, "sweep", db=db)

    mine, theirs = await asyncio.gather(acquire_claim(device_id, "job"), _rival_attempt())
    winners = [r for r in (mine, theirs) if r is not None]
    assert len(winners) == 1
    assert (await _claim_row(device_id)).claim_token == winners[0].token


async def test_provision_acquisition_sets_job_id(adapter_client):
    """Its job is already running, is its own, and carries no winner lock — so the FK
    check contends with nothing, and without job_id a revocation could not re-disposition it."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="cl-prov", netbox_device_id=9903)
    job_id = await _seed_job(device_id, JobType.provision, JobStatus.running)

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


# ── the row-lock guard ───────────────────────────────────────────────────────


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
    """
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="cl-linear", netbox_device_id=9908)
    reg = await acquire_claim(device_id, "job")
    await _backdate_heartbeat(device_id, seconds=claim_stale_cutoff() + 60)

    revoke_started = asyncio.Event()
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    revoked: list = []

    async def _revoke():
        revoke_started.set()
        async with rival() as db:
            revoked.extend(await revoke_stale_claims(db=db))

    async with session() as db:
        await lock_claim(db, reg)
        device = await db.get(Device, device_id)
        device.nso_device_name = "written-by-a"
        await db.flush()

        revoker = asyncio.create_task(_revoke())
        await revoke_started.wait()
        # The revoke's DELETE must be blocked on A's row lock, not already done.
        await asyncio.sleep(0.3)
        assert not revoker.done(), "the revoke did not block on the holder's row lock"

        await db.commit()

    await asyncio.wait_for(revoker, timeout=10)

    async with session() as db:
        # A committed first, so its write stands and the claim is gone afterwards.
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


async def test_killed_connection_around_commit_reports_outcome_unknown(adapter_client, rival_engine):
    """M6.9i/M6.9k(c) — a genuinely lost COMMIT ack must classify as UNKNOWN, not as a
    known abort: the server may have applied it and the client cannot tell.

    The backend is terminated from a SECOND connection while the claim-bearing transaction
    is open, so the failure surfaces on the COMMIT itself — which is the only way to reach
    this branch honestly. Killing it from inside the same session makes an earlier
    statement raise instead, and that is an ABORT_KNOWN, a different contract.
    """
    from nso_adapter.core.claim import _commit_outcome
    from nso_adapter.store.models import DeviceClaim

    device_id = await seed_device(nso_device_name="cl-unknown", netbox_device_id=9971)
    reg = await acquire_claim(device_id, "job")

    async with session() as db:
        await lock_claim(db, reg)
        await db.execute(sa.delete(DeviceClaim).where(DeviceClaim.device_id == device_id))
        pid = await db.scalar(sa.text("SELECT pg_backend_pid()"))

        async with rival_engine.connect() as killer:
            await killer.execute(sa.text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            await killer.commit()

        assert await _commit_outcome(db) is ClaimOutcome.OUTCOME_UNKNOWN

    row = await _claim_row(device_id)
    # Either end state is legal here; the contract forbids only a TORN one.
    assert row is None or row.claim_token == reg.token


# ── constants ────────────────────────────────────────────────────────────────


def test_claim_cutoff_exceeds_the_whole_job_lifecycle():
    """The relation, never the literals: a cutoff shorter than the lifecycle can revoke a
    live runner, and then its own writes race the successor's."""
    from nso_adapter.core import claim

    lifecycle = claim.JOB_EXECUTION_BUDGET + claim.JOB_CANCEL_DRAIN + claim.JOB_CLEANUP_BOUND
    assert claim.CLAIM_STALE_AFTER > lifecycle
    assert claim.PROVISION_STALE_AFTER > lifecycle
    # The 60s orphan window recovers nothing once claims exist — it would re-disposition a
    # job whose holder's token is still valid.
    assert claim.CLAIM_STALE_AFTER > 60.0
