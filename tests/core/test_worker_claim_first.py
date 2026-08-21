# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Q5: the worker claims the DEVICE first, then that device's exact queued head.

Claim-before-job is not a style choice. Recovery holds the claim and reaches for the job, so
a worker that took the job row first would invert the order and the two deadlock. And once
the claim is held, the head must be locked BY EXACT ID: an ordered ``SKIP LOCKED`` silently
hands back a later job on the same device, which breaks the per-device FIFO that
removal-before-apply depends on.
"""

from __future__ import annotations

import asyncio

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.core import worker as worker_mod
from nso_adapter.core.claim import acquire_claim
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio


async def _queue(device_id: int | None, job_type, *, context=None):
    from nso_adapter.store.models import Job, JobStatus, JobType

    async with session() as db:
        job = Job(
            job_type=job_type,
            device_id=device_id,
            status=JobStatus.queued,
            coalescible=job_type not in (JobType.removal, JobType.provision),
            context=context or {},
        )
        db.add(job)
        await db.commit()
        return job.id


async def _status(job_id: int):
    from nso_adapter.store.models import Job

    async with session() as db:
        return (await db.get(Job, job_id)).status


# ── M6.24: FIFO survives a locked head ──────────────────────────────────────


async def test_a_locked_head_does_not_release_a_later_job(adapter_client, rival_engine):
    """M6.24 — J1 locked, J2 newer on the SAME device: J2 must not start.

    Fails against the tempting adaptation of the old query
    (``ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED``), which skips the locked head
    and returns J2 — breaking the binding per-device FIFO.
    """
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q5-fifo", netbox_device_id=9860)
    j1 = await _queue(device_id, JobType.removal, context={"scope": "static_route"})
    j2 = await _queue(device_id, JobType.apply)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as holder:
        # An endpoint's winner lock on the head.
        await holder.execute(sa.select(Job).where(Job.id == j1).with_for_update())

        claimed = await worker_mod._claim_next_job()
        assert claimed is None, f"the worker started a job behind a locked head: {claimed}"
        await holder.rollback()

    assert await _status(j1) is JobStatus.queued
    assert await _status(j2) is JobStatus.queued, "J2 ran ahead of the locked head J1"


async def test_the_device_is_released_when_its_head_cannot_be_locked(adapter_client, rival_engine):
    """Skipping a device must hand its claim back, or one locked head strands it until the
    reaper — the opposite of the cross-device progress the batch exists to provide."""
    from nso_adapter.store.models import DeviceClaim, Job, JobType

    device_id = await seed_device(nso_device_name="q5-release", netbox_device_id=9861)
    j1 = await _queue(device_id, JobType.sync)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as holder:
        await holder.execute(sa.select(Job).where(Job.id == j1).with_for_update())
        assert await worker_mod._claim_next_job() is None
        await holder.rollback()

    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None, "the skipped device stayed claimed"


# ── M6.25: cross-device progress ────────────────────────────────────────────


async def test_a_locked_device_does_not_starve_the_next(adapter_client, rival_engine):
    """M6.25 — D1's head is locked, D2 has runnable work: D2 must start in the SAME poll.

    Fails against "release and re-poll from the global oldest row": the worker rediscovers D1
    every time, and sustained traffic on D1 starves D2 indefinitely.
    """
    from nso_adapter.store.models import Job, JobStatus, JobType

    d1 = await seed_device(nso_device_name="q5-d1", netbox_device_id=9862)
    d2 = await seed_device(nso_device_name="q5-d2", netbox_device_id=9863)
    blocked = await _queue(d1, JobType.sync)  # older, so discovered first
    runnable = await _queue(d2, JobType.sync)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as holder:
        await holder.execute(sa.select(Job).where(Job.id == blocked).with_for_update())

        claimed = await worker_mod._claim_next_job()
        assert claimed is not None, "D2 was starved by D1's locked head"
        assert claimed[0] == runnable
        assert claimed[1] == d2
        await holder.rollback()

    assert await _status(blocked) is JobStatus.queued
    assert await _status(runnable) is JobStatus.running


async def test_a_claimed_device_is_skipped_for_another(adapter_client):
    """A device with a live claim is not a candidate at all."""
    from nso_adapter.store.models import JobType

    d1 = await seed_device(nso_device_name="q5-held", netbox_device_id=9864)
    d2 = await seed_device(nso_device_name="q5-free", netbox_device_id=9865)
    await _queue(d1, JobType.sync)
    free_job = await _queue(d2, JobType.sync)
    await acquire_claim(d1, "intent_put")  # another holder owns D1

    claimed = await worker_mod._claim_next_job()
    assert claimed is not None
    assert claimed[0] == free_job
    assert claimed[1] == d2


# ── the claimless lane ──────────────────────────────────────────────────────


async def test_provision_runs_without_a_claim(adapter_client):
    """``device_id IS NULL`` bypasses the claim: provision has no device yet."""
    from nso_adapter.store.models import JobStatus, JobType

    job_id = await _queue(None, JobType.provision, context={"nso_instance": "nso-dev", "device_name": "x"})

    claimed = await worker_mod._claim_next_job()
    assert claimed is not None
    job, device_id, job_type, reg = claimed
    assert job == job_id
    assert device_id is None
    assert job_type is JobType.provision
    assert not reg.registered, "provision must start on the claimless lane"
    assert await _status(job_id) is JobStatus.running


# ck_job_detached_non_provision_terminal makes the old persisted corruption shape unreachable.


# ── the association is atomic with the running transition ───────────────────


async def test_claim_is_associated_in_the_same_transaction_as_running(adapter_client):
    """A running job with an unassociated claim would be unrecoverable: the reaper needs
    ``job_id`` to re-disposition it."""
    from nso_adapter.store.models import DeviceClaim, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q5-assoc", netbox_device_id=9866)
    job_id = await _queue(device_id, JobType.sync)

    claimed = await worker_mod._claim_next_job()
    assert claimed is not None

    async with session() as db:
        claim = await db.get(DeviceClaim, device_id)
        assert claim.job_id == job_id
        assert await _status(job_id) is JobStatus.running


async def test_acquisition_inserts_no_job_reference(adapter_client):
    """The R7-1 rule, asserted structurally.

    A claim inserted with ``job_id`` set makes PostgreSQL validate the FK by locking that job
    ``FOR KEY SHARE``, which conflicts with an endpoint's ``FOR UPDATE`` winner lock — the
    worker would stall inside the INSERT, before its ``SKIP LOCKED`` ever ran.
    """
    from nso_adapter.store.models import DeviceClaim

    device_id = await seed_device(nso_device_name="q5-nofk", netbox_device_id=9867)
    reg = await acquire_claim(device_id, "job")

    async with session() as db:
        assert (await db.get(DeviceClaim, device_id)).job_id is None
    assert reg.registered


async def test_worker_does_not_block_on_a_held_winner_lock(adapter_client, rival_engine):
    """M6.26 — acquisition must not wait on a locked head; it acquires, then skips.

    Asserted with a BOUND, not "eventually": the failure mode is a stall inside the INSERT.
    """
    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="q5-nostall", netbox_device_id=9868)
    head = await _queue(device_id, JobType.sync)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as holder:
        await holder.execute(sa.select(Job).where(Job.id == head).with_for_update())
        # If acquisition carried an FK reference this would block until the holder released.
        result = await asyncio.wait_for(worker_mod._claim_next_job(), timeout=5)
        assert result is None
        await holder.rollback()


# ── M9: a revoked claim must not have its disposition overwritten ────────────


async def test_a_revoked_run_keeps_recoverys_disposition(adapter_client):
    """M9 at the worker boundary, now reachable because runners really hold claims.

    Recovery revokes the stale claim and requeues the job. The original runner then finishes
    (or fails) and its terminal write must NOT land: recovery owns the disposition. The
    terminal writers are claim-guarded, so the write is refused rather than clobbering.

    Wiring ``lock_claim`` into each runner's OWN inner commits — so a revocation aborts a run
    mid-flight rather than only at its terminal write — is the remaining half of that audit.
    """
    from nso_adapter.core.claim import claim_stale_cutoff, revoke_stale_claims
    from nso_adapter.store.models import DeviceClaim, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q5-m9", netbox_device_id=9869)
    job_id = await _queue(device_id, JobType.sync)

    claimed = await worker_mod._claim_next_job()
    assert claimed is not None
    _job, _dev, job_type, reg = claimed

    # Age the claim and let recovery take it, exactly as the periodic reaper would.
    from datetime import UTC, datetime, timedelta

    async with session() as db:
        await db.execute(
            sa.update(DeviceClaim)
            .where(DeviceClaim.device_id == device_id)
            .values(heartbeat_at=datetime.now(UTC) - timedelta(seconds=claim_stale_cutoff() + 60))
        )
        await db.commit()
    assert [r.device_id for r in await revoke_stale_claims()] == [device_id]
    assert await _status(job_id) is JobStatus.queued  # recovery's disposition

    async def _boom(_job_id, _device_id, _reg=None):
        raise RuntimeError("finished under a revoked claim")

    task = asyncio.create_task(worker_mod._run_one_job(0, job_id, device_id, job_type, _boom, reg))
    await asyncio.wait_for(task, timeout=20)

    assert await _status(job_id) is JobStatus.queued, "the revoked run overwrote recovery"


async def test_a_claim_covered_job_is_left_to_the_claim_scan(adapter_client):
    """ONE recovery clock per job. The claimless reaper must not touch a claimed job.

    Two clocks was the contradiction: re-dispositioning a job on a shorter job-status clock
    while its holder's token is still valid lets the old runner overwrite that disposition.
    Recovery for a claimed job goes through revoke-and-disposition, which blocks on the
    holder's row lock and so cannot race it.
    """
    from datetime import UTC, datetime, timedelta

    from nso_adapter.core.claim import PROVISION_STALE_AFTER
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q5-onefold", netbox_device_id=9870)
    job_id = await _queue(device_id, JobType.sync)

    claimed = await worker_mod._claim_next_job()
    assert claimed is not None

    # Age the JOB well past the claimless cutoff, but leave its claim fresh.
    async with session() as db:
        await db.execute(
            sa.update(Job)
            .where(Job.id == job_id)
            .values(heartbeat_at=datetime.now(UTC) - timedelta(seconds=PROVISION_STALE_AFTER + 600))
        )
        await db.commit()

    await worker_mod.requeue_orphaned_jobs()

    assert await _status(job_id) is JobStatus.running, "the claimless reaper stole a claimed job"
