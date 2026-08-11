# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Q2/Q3: atomic, handoff-safe job admission.

Check-then-insert has two failure modes the DB has to close instead. A conflict raised as
an IntegrityError poisons the CALLER's transaction — and every one of the fifteen
auto-apply endpoints calls admission with intent rows already mutated and uncommitted, so
poisoning it loses the intent write too. And an admission that merely returns "blocked"
hands the worker a queued job it may start before the caller's intent mutation is even
visible.

So: one statement with ON CONFLICT DO NOTHING inside a savepoint, and on a conflict the
exact queued winner is row-locked and that lock is held through the caller's outer commit.
"""

from __future__ import annotations

import asyncio

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from tests.conftest import note_projection_write, seed_device, session

pytestmark = pytest.mark.anyio


async def _add_job(device_id: int, job_type, status, *, context=None):
    from nso_adapter.store.models import Job

    async with session() as db:
        job = Job(job_type=job_type, device_id=device_id, status=status, context=context or {})
        db.add(job)
        await db.commit()
        return job.id


async def _queued_of_type(device_id: int, job_type) -> list[int]:
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        rows = (
            (
                await db.execute(
                    sa.select(Job.id)
                    .where(Job.device_id == device_id, Job.job_type == job_type, Job.status == JobStatus.queued)
                    .order_by(Job.id)
                )
            )
            .scalars()
            .all()
        )
        return list(rows)


# ── M8.5: the conflict target must be valid SQL ──────────────────────────────


async def test_admission_conflict_returns_empty_not_error(adapter_client):
    """M8.5 — one line, and it fails loudly against the wrong conflict target.

    The dedupe object is a partial INDEX, not a constraint. ``ON CONFLICT ON CONSTRAINT
    uq_job_queued_per_device_type`` raises InvalidObjectDefinition, which is exactly the 500
    atomic admission exists to prevent. Conflict INFERENCE — index columns plus a matching
    predicate — returns an empty result instead.
    """
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q2-infer", netbox_device_id=9800)
    existing = await _add_job(device_id, JobType.sync, JobStatus.queued)

    async with session() as db:
        created, winner = await admit_queued_job(db, device_id, JobType.sync)
        assert created is None
        assert winner is not None and winner.id == existing
        await db.rollback()


async def test_admission_inserts_when_nothing_is_queued(adapter_client):
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="q2-fresh", netbox_device_id=9801)

    async with session() as db:
        created, winner = await admit_queued_job(db, device_id, JobType.sync)
        assert winner is None
        assert created is not None
        await db.commit()

    assert await _queued_of_type(device_id, JobType.sync) == [created.id]


async def test_admission_ignores_a_running_job_of_the_same_type(adapter_client):
    """A running job must not refuse its successor — the successor carries newer intent."""
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q2-running", netbox_device_id=9802)
    await _add_job(device_id, JobType.apply, JobStatus.running)

    async with session() as db:
        created, winner = await admit_queued_job(db, device_id, JobType.apply)
        assert winner is None and created is not None
        await db.commit()


async def test_removals_are_never_deduped(adapter_client):
    """Exempt by design: one per scope, and every one must run."""
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="q2-removal", netbox_device_id=9803)

    async with session() as db:
        first, _ = await admit_queued_job(db, device_id, JobType.removal, context={"scope": "bgp"})
        second, winner = await admit_queued_job(db, device_id, JobType.removal, context={"scope": "isis"})
        await db.commit()
    assert first is not None and second is not None and winner is None


# ── the savepoint: a conflict must not poison the caller ─────────────────────


async def test_conflict_leaves_the_callers_intent_mutation_committable(adapter_client):
    """The reason admission is wrapped in a savepoint.

    Mirrors the canonical endpoint shape: mutate intent, then admit. A conflict must leave
    the caller free to commit its own rows.
    """
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import BfdIntent, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q2-savepoint", netbox_device_id=9804)
    await _add_job(device_id, JobType.apply, JobStatus.queued)

    async with session() as db:
        db.add(BfdIntent(device_id=device_id, interface_name="ge-0/0/0", min_tx=300))
        await db.flush()

        created, winner = await admit_queued_job(db, device_id, JobType.apply)
        assert created is None and winner is not None

        await db.commit()  # must not raise

    async with session() as db:
        rows = (await db.execute(sa.select(BfdIntent).where(BfdIntent.device_id == device_id))).scalars().all()
        assert len(rows) == 1, "the conflict poisoned the caller's transaction"


# ── the winner lock (F6 handoff safety) ─────────────────────────────────────


async def test_conflict_holds_the_winner_row_lock_until_the_caller_commits(adapter_client, rival_engine):
    """The worker must not be able to start the winner before the caller's mutation lands.

    Proven positively: a rival that tries to lock the winner with a short lock_timeout must
    FAIL while the caller still holds it.
    """
    from sqlalchemy.exc import DBAPIError

    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q2-winlock", netbox_device_id=9805)
    winner_id = await _add_job(device_id, JobType.apply, JobStatus.queued)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with session() as db:
        created, winner = await admit_queued_job(db, device_id, JobType.apply)
        assert created is None and winner.id == winner_id

        with pytest.raises(DBAPIError) as blocked:
            async with rival() as other:
                await other.execute(sa.text("SET LOCAL lock_timeout = '400ms'"))
                await other.execute(sa.select(Job).where(Job.id == winner_id).with_for_update())
        assert "lock" in str(blocked.value).lower() or "canceling statement" in str(blocked.value).lower()

        await db.commit()

    # Once the caller commits, the row is claimable.
    async with rival() as other:
        locked = await other.scalar(sa.select(Job.id).where(Job.id == winner_id).with_for_update())
        assert locked == winner_id
        await other.rollback()


async def test_winner_gone_running_creates_a_successor(adapter_client, rival_engine):
    """M8.3 — the retry branch. The loser must never get ``None``.

    If the winner starts running between the failed insert and the winner lookup, the
    correct answer is a NEW queued successor, not "blocked": the caller's intent is newer
    than the running job's snapshot.
    """
    from nso_adapter.core import jobs as jobs_mod
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q2-succ", netbox_device_id=9806)
    winner_id = await _add_job(device_id, JobType.apply, JobStatus.queued)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    original = jobs_mod._lock_queued_winner
    fired = {"n": 0}

    async def _steal_then_lock(db, dev_id, job_type):
        # Exactly once, between the failed insert and the lookup, the winner starts.
        fired["n"] += 1
        if fired["n"] == 1:
            async with rival() as other:
                await other.execute(sa.update(Job).where(Job.id == winner_id).values(status=JobStatus.running))
                await other.commit()
        return await original(db, dev_id, job_type)

    jobs_mod._lock_queued_winner = _steal_then_lock
    try:
        async with session() as db:
            created, winner = await jobs_mod.admit_queued_job(db, device_id, JobType.apply)
            await db.commit()
    finally:
        jobs_mod._lock_queued_winner = original

    assert winner is None, "returned the running job instead of queueing a successor"
    assert created is not None and created.id != winner_id
    assert await _queued_of_type(device_id, JobType.apply) == [created.id]


# ── M8.1/M8.2: two engines, both intent mutations survive ───────────────────


async def test_two_concurrent_auto_apply_puts_both_commit(adapter_client, rival_engine):
    """M8.1 — two DIFFERENT intent families racing one device.

    Both intent mutations must commit, exactly one apply may be queued, and neither request
    may 500. Against check-then-insert one caller's whole transaction is poisoned by the
    unique violation and its intent rows are lost.
    """
    from nso_adapter.core.apply import enqueue_apply
    from nso_adapter.store.models import BfdIntent, InterfaceMtuIntent, JobType

    device_id = await seed_device(nso_device_name="q2-m81", netbox_device_id=9807)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    first_admitted = asyncio.Event()
    second_done = asyncio.Event()

    async def _second() -> None:
        await first_admitted.wait()
        async with rival() as db:
            db.add(InterfaceMtuIntent(device_id=device_id, interface_name="ge-0/0/1", mtu=9000))
            await db.flush()
            await note_projection_write(db, device_id, "interface_mtu")
            await enqueue_apply(db, device_id, force=True, stream="interface_mtu")
            await db.commit()
        second_done.set()

    task = asyncio.create_task(_second())
    async with session() as db:
        db.add(BfdIntent(device_id=device_id, interface_name="ge-0/0/0", min_tx=300))
        await db.flush()
        await note_projection_write(db, device_id, "bfd")
        await enqueue_apply(db, device_id, force=True, stream="bfd")
        first_admitted.set()
        # The rival is now contending; the winner lock means it waits for this commit.
        await asyncio.sleep(0.2)
        await db.commit()

    await asyncio.wait_for(task, timeout=20)
    assert second_done.is_set()

    async with session() as db:
        bfd = (await db.execute(sa.select(BfdIntent).where(BfdIntent.device_id == device_id))).scalars().all()
        mtu = (
            (await db.execute(sa.select(InterfaceMtuIntent).where(InterfaceMtuIntent.device_id == device_id)))
            .scalars()
            .all()
        )
    assert len(bfd) == 1, "the first caller's intent was lost"
    assert len(mtu) == 1, "the second caller's intent was lost"
    assert len(await _queued_of_type(device_id, JobType.apply)) == 1


async def test_two_concurrent_static_route_puts_both_commit(adapter_client, rival_engine):
    """M8.2 — same shape, both requests the same family."""
    from nso_adapter.core.apply import enqueue_apply
    from nso_adapter.store.models import JobType, StaticRouteIntent

    device_id = await seed_device(nso_device_name="q2-m82", netbox_device_id=9808)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    first_admitted = asyncio.Event()

    async def _second() -> None:
        await first_admitted.wait()
        async with rival() as db:
            db.add(
                StaticRouteIntent(device_id=device_id, vrf="", prefix="10.9.0.0/24", next_hop="192.0.2.9", route_id=91)
            )
            await db.flush()
            await note_projection_write(db, device_id, "static_route")
            await enqueue_apply(db, device_id, force=True, stream="static_route")
            await db.commit()

    task = asyncio.create_task(_second())
    async with session() as db:
        db.add(StaticRouteIntent(device_id=device_id, vrf="", prefix="10.8.0.0/24", next_hop="192.0.2.8", route_id=90))
        await db.flush()
        await note_projection_write(db, device_id, "static_route")
        await enqueue_apply(db, device_id, force=True, stream="static_route")
        first_admitted.set()
        await asyncio.sleep(0.2)
        await db.commit()

    await asyncio.wait_for(task, timeout=20)

    async with session() as db:
        rows = (
            (await db.execute(sa.select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
    assert len(rows) == 2
    assert len(await _queued_of_type(device_id, JobType.apply)) == 1


# ── enqueue_job keeps its outer-commit ownership (Q3) ───────────────────────


async def test_enqueue_job_still_commits_its_own_work(adapter_client):
    """Q3 — its API and scheduler callers depend on it committing; get_db does not."""
    from nso_adapter.core.jobs import enqueue_job
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q3-commit", netbox_device_id=9810)

    async with session() as db:
        job, created = await enqueue_job(device_id, JobType.sync, db)
        assert created is True

    async with session() as fresh:
        row = await fresh.get(Job, job.id)
        assert row is not None and row.status is JobStatus.queued


async def test_enqueue_job_conflict_returns_the_winner_not_none(adapter_client):
    from nso_adapter.core.jobs import enqueue_job
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q3-conflict", netbox_device_id=9811)
    existing = await _add_job(device_id, JobType.sync, JobStatus.queued)

    async with session() as db:
        job, created = await enqueue_job(device_id, JobType.sync, db)
        assert created is False
        assert job is not None and job.id == existing


# ── M6.9t: provision admission is atomic per (nso_instance, device_name) ─────

_PROVISION = {
    "nso_instance": "nso-dev",
    "device_name": "adm-rtr",
    "ned_id": "cisco-ios-cli-6.114:cisco-ios-cli-6.114",
    "authgroup": "network",
}


async def _active_provisions(device_name: str) -> list[int]:
    from nso_adapter.core.jobs import _PROVISION_DEDUPE_PREDICATE, _PROVISION_PAIR_ELEMENTS
    from nso_adapter.store.models import Job

    async with session() as db:
        rows = (
            (
                await db.execute(
                    sa.select(Job.id)
                    .where(_PROVISION_DEDUPE_PREDICATE, _PROVISION_PAIR_ELEMENTS[1] == device_name)
                    .order_by(Job.id)
                )
            )
            .scalars()
            .all()
        )
        return list(rows)


async def test_two_concurrent_provisions_admit_exactly_one(adapter_client, rival_engine):
    """M6.9t — the loser must lose at the INDEX, not at a lookup it can race.

    The two requests differ in ``address``, so an index inferred on the wrong column would
    admit both and this test would still fail. The contention is forced rather than
    scheduled: the winner's INSERT is left uncommitted while the rival runs, so PostgreSQL
    makes the rival wait on the speculative insertion — which is exactly the window where
    check-then-insert let both callers find nothing and both admit.
    """
    from nso_adapter.core.jobs import enqueue_provision_job

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    winner_inserted = asyncio.Event()
    loser: dict = {}

    async def _rival() -> None:
        await winner_inserted.wait()
        async with rival() as db:
            loser["job"], loser["created"] = await enqueue_provision_job({**_PROVISION, "address": "10.0.0.2"}, db)

    task = asyncio.create_task(_rival())
    async with session() as db:
        real_commit = db.commit

        async def _commit_after_the_rival_contends() -> None:
            # Fires between the speculative INSERT and its COMMIT — the whole window.
            winner_inserted.set()
            await asyncio.sleep(0.4)
            assert not task.done(), "the rival never contended: it finished before the winner committed"
            await real_commit()

        db.commit = _commit_after_the_rival_contends
        winner, created = await enqueue_provision_job({**_PROVISION, "address": "10.0.0.1"}, db)

    await asyncio.wait_for(task, timeout=20)

    assert created is True
    assert loser["created"] is False, "both callers admitted a provision for the same node"
    assert loser["job"].id == winner.id
    assert await _active_provisions("adm-rtr") == [winner.id]


async def test_a_terminal_provision_does_not_block_a_new_one(adapter_client):
    """The index covers queued and running only — a finished onboarding must be repeatable."""
    from nso_adapter.core.jobs import enqueue_provision_job
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        first, created = await enqueue_provision_job({**_PROVISION, "address": "10.0.0.1"}, db)
        assert created is True

    async with session() as db:
        await db.execute(sa.update(Job).where(Job.id == first.id).values(status=JobStatus.succeeded))
        await db.commit()

    async with session() as db:
        second, created = await enqueue_provision_job({**_PROVISION, "address": "10.0.0.1"}, db)
    assert created is True and second.id != first.id


async def test_a_running_provision_still_refuses_a_second_one(adapter_client):
    """Unlike the per-device queued dedupe: a provision has no successor semantics.

    Re-admitting one mid-flight repeats the NSO node creation and the sync-from against a
    node another runner is already onboarding.
    """
    from nso_adapter.core.jobs import enqueue_provision_job
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        first, _ = await enqueue_provision_job({**_PROVISION, "address": "10.0.0.1"}, db)

    async with session() as db:
        await db.execute(sa.update(Job).where(Job.id == first.id).values(status=JobStatus.running))
        await db.commit()

    async with session() as db:
        second, created = await enqueue_provision_job({**_PROVISION, "address": "10.0.0.1"}, db)
    assert created is False and second.id == first.id


async def test_provision_admission_retries_when_the_winner_finishes(adapter_client, rival_engine):
    """Zero rows plus no active job is a finished winner, not "blocked" — admit a fresh one."""
    from nso_adapter.core import jobs as jobs_mod
    from nso_adapter.store.models import Job, JobStatus

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with session() as db:
        first, _ = await jobs_mod.enqueue_provision_job({**_PROVISION, "address": "10.0.0.1"}, db)

    original = jobs_mod.get_active_provision_job
    fired = {"n": 0}

    async def _finish_then_look(instance, name, db):
        # Exactly once, between the conflicting insert and the lookup, the winner completes.
        fired["n"] += 1
        if fired["n"] == 1:
            async with rival() as other:
                await other.execute(sa.update(Job).where(Job.id == first.id).values(status=JobStatus.succeeded))
                await other.commit()
        return await original(instance, name, db)

    jobs_mod.get_active_provision_job = _finish_then_look
    try:
        async with session() as db:
            second, created = await jobs_mod.enqueue_provision_job({**_PROVISION, "address": "10.0.0.1"}, db)
    finally:
        jobs_mod.get_active_provision_job = original

    assert created is True and second.id != first.id


async def test_a_failing_insert_does_not_poison_the_caller(adapter_client):
    """What the SAVEPOINT is actually for.

    A dedupe conflict raises nothing — ON CONFLICT DO NOTHING returns empty — so the
    savepoint is not exercised by contention. It earns its place on an insert that genuinely
    raises: here an FK violation from an unknown device. The caller's own rows must still be
    committable, because those rows are operator intent and losing them is silent data loss.
    """
    from sqlalchemy.exc import IntegrityError

    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import BfdIntent, JobType

    device_id = await seed_device(nso_device_name="q2-fkfail", netbox_device_id=9812)

    async with session() as db:
        db.add(BfdIntent(device_id=device_id, interface_name="ge-0/0/9", min_tx=300))
        await db.flush()

        with pytest.raises(IntegrityError):
            await admit_queued_job(db, 10_000_000, JobType.sync)  # no such device

        await db.commit()  # the savepoint rolled back only the insert

    async with session() as db:
        rows = (await db.execute(sa.select(BfdIntent).where(BfdIntent.device_id == device_id))).scalars().all()
        assert len(rows) == 1, "a failing admission poisoned the caller's transaction"
