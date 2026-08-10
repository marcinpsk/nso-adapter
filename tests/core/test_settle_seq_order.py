# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Appendix S, chunk S2: the settlement sequence orders terminal jobs by COMMIT order.

The consumer walks one device's terminal jobs ascending from a cursor, so the order it
walks has to be the order the results BECAME TRUE — the commit order. Nothing already on the
row expresses that: admission inserts under a SAVEPOINT so insert order is unusable,
``created_at`` is transaction time, and a bare sequence hands values out in allocation order
while the transactions commit in whatever order they finish.

A counter row whose lock is held to COMMIT is what converts one into the other, and these
pins drive it against real PostgreSQL through two independent connections.
"""

from __future__ import annotations

import asyncio

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.core import jobs as jobs_mod
from nso_adapter.core.claim import terminalize
from nso_adapter.store.models import DeviceSettleCounter, Job, JobStatus, JobType
from tests.conftest import seed_device, session
from tests.core.test_settle_token import _recover, _start_run

pytestmark = pytest.mark.anyio

# Long enough that a rival which does NOT block would have finished many times over.
_BLOCKED_FOR = 0.5


async def _running_job(device_id: int, job_type: JobType = JobType.removal) -> int:
    """A job as the worker head leaves it: started, at attempt 1.

    ``removal`` by default: it is the one type exempt from the queued-per-(device, type)
    uniqueness index, so a device can hold two of them at once.
    """
    async with session() as db:
        job = Job(job_type=job_type, device_id=device_id, status=JobStatus.running, run_attempt=1)
        db.add(job)
        await db.commit()
        return job.id


async def _settle_seq(job_id: int) -> int | None:
    async with session() as db:
        return await db.scalar(sa.select(Job.settle_seq).where(Job.id == job_id))


async def _last_seq(device_id: int) -> int:
    async with session() as db:
        return await db.scalar(
            sa.select(DeviceSettleCounter.last_seq).where(DeviceSettleCounter.device_id == device_id)
        )


async def _terminalize_and_commit(db, job_id: int) -> None:
    await terminalize(db, job_id, status=JobStatus.succeeded, expect=JobStatus.running, run_attempt=1)
    await db.commit()


# ── S2.1 (P0.4): allocation order == commit order, per device ────────────────


async def test_allocation_order_equals_commit_order_per_device(adapter_client, rival_engine):
    """S2.1 — the rival blocks on the counter row until the holder commits.

    Forbidden: the second transaction taking a HIGHER sequence and committing FIRST, which
    is exactly what a plain sequence does — and it would hand the consumer the two results
    in the reverse of the order they became true.
    """
    device_id = await seed_device(nso_device_name="seq-order", netbox_device_id=8101)
    slow_job = await _running_job(device_id)
    quick_job = await _running_job(device_id)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with session() as holder, rival() as other:
        # Allocates, and holds the counter row: no COMMIT yet.
        assert (
            await terminalize(holder, slow_job, status=JobStatus.succeeded, expect=JobStatus.running, run_attempt=1)
            is not None
        )

        racer = asyncio.create_task(_terminalize_and_commit(other, quick_job))
        await asyncio.sleep(_BLOCKED_FOR)
        assert not racer.done(), "the rival allocated without waiting for the holder's COMMIT"

        await holder.commit()
        await asyncio.wait_for(racer, timeout=30)

    assert await _settle_seq(slow_job) == 1, "the first COMMIT did not take the first sequence"
    assert await _settle_seq(quick_job) == 2


async def test_cross_device_pairs_may_interleave(adapter_client, rival_engine):
    """S2.1 — the counter is PER DEVICE, so two devices never serialize against each other.

    Forbidden: a device-wide (or store-wide) allocator that makes every terminal write on
    the fleet queue behind one uncommitted transaction.
    """
    held_device = await seed_device(nso_device_name="seq-cross-a", netbox_device_id=8102)
    free_device = await seed_device(nso_device_name="seq-cross-b", netbox_device_id=8103)
    held_job = await _running_job(held_device)
    free_job = await _running_job(free_device)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with session() as holder, rival() as other:
        assert (
            await terminalize(holder, held_job, status=JobStatus.succeeded, expect=JobStatus.running, run_attempt=1)
            is not None
        )

        # Must complete while the OTHER device's counter is still held uncommitted.
        await asyncio.wait_for(_terminalize_and_commit(other, free_job), timeout=10)
        assert await _settle_seq(free_job) == 1

        await holder.commit()

    assert await _settle_seq(held_job) == 1


# ── S2.2 (P0.5): a late-committed INSERT still lands ahead of the cursor ─────


async def test_a_late_committed_insert_gets_a_higher_seq(adapter_client, rival_engine):
    """S2.2 — job 11 is inserted first and committed last; it must not land behind the cursor.

    Forbidden: 11 receiving a sequence below 12's, which an insert-ordered (or id-ordered)
    feed would give it — a consumer already past 12 would then never see 11 at all.
    """
    device_id = await seed_device(nso_device_name="seq-late-insert", netbox_device_id=8104)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as pending:
        early = Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.running, run_attempt=1)
        pending.add(early)
        await pending.flush()  # the id is taken; the row is NOT visible yet
        early_id = early.id

        # A later row is admitted, runs and goes terminal while the earlier insert is open.
        late_id = await _running_job(device_id)
        assert late_id > early_id, "the setup no longer models A19: the later job must have the higher id"
        async with session() as db:
            await _terminalize_and_commit(db, late_id)

        await pending.commit()

    async with session() as db:
        await _terminalize_and_commit(db, early_id)

    assert await _settle_seq(late_id) == 1
    assert await _settle_seq(early_id) == 2, "the late-committed insert landed behind the cursor"


# ── S2.7: a refused write burns no sequence ─────────────────────────────────


async def test_a_rejected_cas_burns_no_sequence(adapter_client, monkeypatch):
    """S2.7 — the abandoned runner of S1.1 is refused, so the counter must not move.

    Forbidden: allocating before (or despite) the compare-and-set, which spends a device's
    sequence on a write that never landed and takes the counter's device-wide lock for a
    doomed transaction.
    """
    from tests.core.test_settle_token import _ok_sync

    device_id = await seed_device(nso_device_name="seq-refused", netbox_device_id=8105)
    async with session() as db:
        job = Job(job_type=JobType.sync, device_id=device_id, status=JobStatus.queued)
        db.add(job)
        await db.commit()
        job_id = job.id

    _jid, _dev, _jt, abandoned = await _start_run(device_id, job_id)
    await _recover(device_id)  # recovery requeued it — a requeue is not terminal

    monkeypatch.setattr("nso_adapter.core.importer.sync_device", _ok_sync)
    await jobs_mod._run_sync(job_id, device_id, abandoned)

    assert await _last_seq(device_id) == 0, "a refused terminal write took a sequence"
    assert await _settle_seq(job_id) is None
