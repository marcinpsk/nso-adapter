# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R1b chunk 3 / Q12+Q13: the tombstone sweeper and the snapshotted delete (M5).

The sweeper is the recovery path for a deletion whose removal job was never committed —
the intent row it described is already gone, so nothing else records it. Exclusion is the
ordinary per-device claim: every rival (another sweeper, a teardown, an intent PUT) loses
at acquisition rather than getting a subset of the work.
"""

from __future__ import annotations

import asyncio

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.core.claim import ClaimLostError, acquire_claim, release_claim
from nso_adapter.core.tombstone_sweep import sweep_tombstones
from tests.conftest import seed_device, session

A = ("", "10.0.0.0/24", "192.0.2.1")
B = ("", "10.0.1.0/24", "192.0.2.2")
C = ("", "10.0.2.0/24", "192.0.2.3")


async def _seed_tombstone(
    device_id: int,
    triple: tuple[str, str, str] = A,
    *,
    route_id: int = 7,
    marking: str = "detach",
    deployed_key: tuple[str, str, str] | None = None,
    job_id: int | None = None,
    tombstone_id: int | None = None,
) -> int:
    from nso_adapter.store.models import StaticRouteTombstone

    vrf, prefix, next_hop = triple
    async with session() as db:
        row = StaticRouteTombstone(
            device_id=device_id,
            route_id=route_id,
            vrf=vrf,
            prefix=prefix,
            next_hop=next_hop,
            deployed_key=list(deployed_key or triple),
            marking=marking,
            job_id=job_id,
        )
        if tombstone_id is not None:
            row.id = tombstone_id
        db.add(row)
        await db.commit()
        return row.id


async def _seed_job(device_id: int, status, job_type=None) -> int:
    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        effective_type = job_type or JobType.removal
        job = Job(
            job_type=effective_type,
            device_id=device_id,
            status=status,
            coalescible=effective_type not in (JobType.removal, JobType.provision),
            context={},
        )
        db.add(job)
        await db.commit()
        return job.id


async def _removal_jobs(device_id: int) -> list[dict]:
    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        rows = (
            (
                await db.execute(
                    sa.select(Job)
                    .where(Job.device_id == device_id, Job.job_type == JobType.removal)
                    .order_by(Job.created_at, Job.id)
                )
            )
            .scalars()
            .all()
        )
        return [{"id": r.id, "context": r.context, "status": r.status} for r in rows]


async def _tombstones(device_id: int) -> list[tuple[int, int | None]]:
    from nso_adapter.store.models import StaticRouteTombstone

    async with session() as db:
        rows = (
            (
                await db.execute(
                    sa.select(StaticRouteTombstone)
                    .where(StaticRouteTombstone.device_id == device_id)
                    .order_by(StaticRouteTombstone.id)
                )
            )
            .scalars()
            .all()
        )
        return [(r.id, r.job_id) for r in rows]


# ── M5.1-M5.3, M5.8: the predicate ───────────────────────────────────────────


async def test_an_unowned_tombstone_gets_a_job_and_the_stamp(adapter_client):
    """M5.1 — the whole point: a deletion with no carrier job acquires one."""
    device_id = await seed_device(nso_device_name="sw-m5-1", netbox_device_id=9600)
    tombstone_id = await _seed_tombstone(device_id)

    assert await sweep_tombstones() == 1

    jobs = await _removal_jobs(device_id)
    assert len(jobs) == 1
    assert await _tombstones(device_id) == [(tombstone_id, jobs[0]["id"])]


async def test_a_sweep_rejects_a_carrier_that_cannot_take_its_generation(adapter_client, monkeypatch):
    import nso_adapter.core.tombstone_sweep as sweep_mod
    from nso_adapter.core.generation import (
        GenerationCarrierCorruption,
        attach_to_job,
        create_reissue_generation,
    )
    from nso_adapter.core.jobs import create_dedicated_job
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode, Job, JobType, StaticRouteTombstone

    device_id = await seed_device(nso_device_name="sw-m5-rejected-carrier", netbox_device_id=9642)
    tombstone_id = await _seed_tombstone(device_id)
    context = {
        "scope": "static_route",
        "removed": {"route": [list(A)]},
        "detach": True,
        "tombstone_ids": [tombstone_id],
    }
    async with session() as db:
        first = await create_reissue_generation(
            db,
            device_id,
            mode=GenerationMode.detach,
            removal_context=context,
            allowed_removal_keys=context["removed"],
        )
        carrier = await create_dedicated_job(db, device_id, JobType.removal, context=context)
        assert await attach_to_job(db, first, carrier)
        carrier_id = carrier.id
        await db.commit()

    async def _occupied_carrier(db, _device_id, _job_type, *, context=None):
        carrier = await db.get(Job, carrier_id)
        assert carrier is not None
        return carrier

    monkeypatch.setattr(sweep_mod, "create_dedicated_job", _occupied_carrier)

    with pytest.raises(
        GenerationCarrierCorruption,
        match=rf"dedicated carrier {carrier_id} rejected generation \d+",
    ):
        await sweep_tombstones()

    async with session() as db:
        generations = (
            await db.scalars(
                sa.select(DeploymentGeneration)
                .where(DeploymentGeneration.device_id == device_id)
                .order_by(DeploymentGeneration.seq)
            )
        ).all()
        tombstone = await db.get(StaticRouteTombstone, tombstone_id)
    assert [(generation.seq, generation.job_id) for generation in generations] == [(1, carrier_id)]
    assert tombstone is not None and tombstone.job_id is None


async def test_a_failed_owner_is_swept_again(adapter_client):
    """M5.2 — a failed removal did not consume the tombstone."""
    from nso_adapter.store.models import JobStatus

    device_id = await seed_device(nso_device_name="sw-m5-2", netbox_device_id=9601)
    failed_id = await _seed_job(device_id, JobStatus.failed)
    tombstone_id = await _seed_tombstone(device_id, job_id=failed_id)

    assert await sweep_tombstones() == 1

    jobs = await _removal_jobs(device_id)
    assert [j["id"] for j in jobs] != [failed_id]
    reissued = next(j["id"] for j in jobs if j["id"] != failed_id)
    assert await _tombstones(device_id) == [(tombstone_id, reissued)]


@pytest.mark.parametrize("marking", ["detach", "delete_origin"])
async def test_the_reissued_job_runs_with_the_marking_the_tombstone_recorded(adapter_client, marking, monkeypatch):
    """M5.2's other half — assert the WIRE behavior of the re-issued job, not its context.

    A sweeper that called ``enqueue_removal`` would produce a detach for BOTH markings
    (the request-scoped ``DELETE_ORIGIN`` ContextVar is unset outside a request), turning a
    failed delete-origin retract into a no-op no-networking retry.
    """
    from unittest.mock import AsyncMock

    from nso_adapter.store.models import Job, JobStatus
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client, wire

    device_name = f"sw-m5-2-{marking}"
    device_id = await seed_device(nso_device_name=device_name, netbox_device_id=9630)
    failed_id = await _seed_job(device_id, JobStatus.failed)
    await _seed_tombstone(device_id, B, route_id=8, marking=marking, job_id=failed_id)

    assert await sweep_tombstones() == 1
    reissued = next(j["id"] for j in await _removal_jobs(device_id) if j["id"] != failed_id)

    fake = SrFake(device_name, service=[wire(A), wire(B)])
    sync_from = AsyncMock(return_value={})
    job = await run_removal_job(device_id, reissued, sr_client(fake), sync_from=sync_from)

    assert job.status == JobStatus.succeeded
    assert fake.writes[-1]["no_networking"] is (marking == "detach")
    assert fake.service_keys == {A}, "the un-owned/deleted key leaves the SERVICE either way"
    if marking == "detach":
        # No networking, and CDB is re-aligned with device truth afterwards.
        assert job.result["detach"] is True
        assert job.result["residue_check"] == "skipped_detach"
        assert fake.device_keys == {A, B}, "the device keeps it — that is what an un-own means"
        sync_from.assert_awaited()
    else:
        assert "detach" not in job.result
        # R2 now ENFORCES this rather than merely recording it (§4.4).
        assert job.result["residue_check"] == "clean"
        assert fake.device_keys == {A}
        sync_from.assert_not_awaited()
    async with session() as db:
        assert await db.get(Job, reissued) is not None


async def test_a_failed_sweep_retry_removes_the_tombstone_and_deployed_keys(adapter_client):
    from nso_adapter.core.generation import retry_generation
    from nso_adapter.store.models import DeploymentGeneration, JobStatus, StaticRouteTombstone
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client, wire

    device_name = "sw-m5-retry-divergent"
    device_id = await seed_device(nso_device_name=device_name, netbox_device_id=9631)
    await _seed_tombstone(device_id, B, marking="delete_origin", deployed_key=A)

    assert await sweep_tombstones() == 1
    swept_id = (await _removal_jobs(device_id))[0]["id"]
    failing = SrFake(device_name, service=[wire(A), wire(B), wire(C)], dry_run_status=500)
    failed = await run_removal_job(device_id, swept_id, sr_client(failing))
    assert failed.status is JobStatus.failed

    async with session() as db:
        generation = await db.scalar(sa.select(DeploymentGeneration).where(DeploymentGeneration.job_id == swept_id))
        retried = await retry_generation(db, generation.id)
        await db.commit()
        retried_id = retried.id

    fake = SrFake(device_name, service=[wire(A), wire(B), wire(C)])
    succeeded = await run_removal_job(device_id, retried_id, sr_client(fake))

    assert succeeded.status is JobStatus.succeeded
    assert fake.sent_keys() == {C}, "the retry kept a key recorded only as deployed_key"
    async with session() as db:
        remaining = (
            await db.scalars(sa.select(StaticRouteTombstone).where(StaticRouteTombstone.device_id == device_id))
        ).all()
    assert remaining == [], "the proven retry left its deletion carrier behind"


@pytest.mark.parametrize("status_name", ["queued", "running", "succeeded"])
async def test_a_live_or_succeeded_owner_is_not_swept(adapter_client, status_name):
    """M5.3 + M5.8 — no duplicate job, and R2's handoff set is not garbage-collected."""
    from nso_adapter.store.models import JobStatus

    device_id = await seed_device(nso_device_name=f"sw-m5-{status_name}", netbox_device_id=9610)
    owner = await _seed_job(device_id, JobStatus[status_name])
    tombstone_id = await _seed_tombstone(device_id, job_id=owner)

    assert await sweep_tombstones() == 0

    assert [j["id"] for j in await _removal_jobs(device_id)] == [owner]
    assert await _tombstones(device_id) == [(tombstone_id, owner)]


async def test_the_swept_job_context_comes_from_the_tombstone(adapter_client):
    """Q12 — never from ambient request state: the sweeper has no originating request."""
    device_id = await seed_device(nso_device_name="sw-m5-ctx", netbox_device_id=9602)
    tombstone_id = await _seed_tombstone(device_id, B, marking="delete_origin")

    assert await sweep_tombstones() == 1

    context = (await _removal_jobs(device_id))[0]["context"]
    assert context["scope"] == "static_route"
    assert context["removed"] == {"route": [list(B)]}
    assert context["tombstone_ids"] == [tombstone_id]
    # The flip this guards: with an unset DELETE_ORIGIN ContextVar, a delete-origin
    # retract would silently become a no-networking detach retry.
    assert context["detach"] is False


async def test_a_detach_tombstone_keeps_its_no_networking_marking(adapter_client):
    device_id = await seed_device(nso_device_name="sw-m5-detach", netbox_device_id=9603)
    await _seed_tombstone(device_id, marking="detach")

    assert await sweep_tombstones() == 1
    assert (await _removal_jobs(device_id))[0]["context"]["detach"] is True


# ── M5.4/M5.5: contention ────────────────────────────────────────────────────


async def test_a_rival_sweeper_loses_at_claim_acquisition(adapter_client, rival_engine):
    """M5.4 — zero jobs for the rival, and it never reaches the guarded transaction.

    Sequential double-sweep is not a substitute: removals are exempt from the queued-type
    uniqueness index, so nothing else would stop two jobs.
    """
    device_id = await seed_device(nso_device_name="sw-m5-4", netbox_device_id=9604)
    tombstone_id = await _seed_tombstone(device_id)

    holder = await acquire_claim(device_id, "sweep")
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as db:
        assert await sweep_tombstones(db=db) == 0
    assert await _removal_jobs(device_id) == []

    await release_claim(holder)

    assert await sweep_tombstones() == 1
    jobs = await _removal_jobs(device_id)
    assert len(jobs) == 1
    assert await _tombstones(device_id) == [(tombstone_id, jobs[0]["id"])]


async def test_multiple_tombstones_are_swept_in_id_order(adapter_client, rival_engine):
    """M5.5 — ordering comes from an explicit sorted(), never from scan order.

    The ids are assigned so that insertion order differs from id order: an implementation
    that follows the scan produces 3, 1, 2.
    """
    device_id = await seed_device(nso_device_name="sw-m5-5", netbox_device_id=9605)
    await _seed_tombstone(device_id, C, route_id=9, tombstone_id=3003)
    await _seed_tombstone(device_id, A, route_id=7, tombstone_id=3001)
    await _seed_tombstone(device_id, B, route_id=8, tombstone_id=3002)

    holder = await acquire_claim(device_id, "sweep")
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as db:
        assert await sweep_tombstones(db=db) == 0
    await release_claim(holder)

    assert await sweep_tombstones() == 3

    jobs = await _removal_jobs(device_id)
    assert len(jobs) == 3
    # Job order (created_at, id) must follow tombstone id order.
    assert [j["context"]["removed"]["route"][0] for j in jobs] == [list(A), list(B), list(C)]
    assert await _tombstones(device_id) == [(3001, jobs[0]["id"]), (3002, jobs[1]["id"]), (3003, jobs[2]["id"])]


@pytest.mark.parametrize("purpose", ["teardown", "intent_put", "job", "failover"])
async def test_any_other_holder_makes_the_sweep_skip_the_device(adapter_client, purpose):
    """M5.9's claim-exclusion half — an `intent_put` holder takes no `devices` lock at all,
    so a device-lock-only sweeper would not skip it."""
    device_id = await seed_device(nso_device_name=f"sw-m5-9-{purpose}", netbox_device_id=9620)
    await _seed_tombstone(device_id)

    holder = await acquire_claim(device_id, purpose)
    try:
        assert await sweep_tombstones() == 0
    finally:
        await release_claim(holder)
    assert await _removal_jobs(device_id) == []


async def test_a_claim_committed_after_discovery_still_excludes_the_sweep(adapter_client, monkeypatch):
    """M5.10 — the READ COMMITTED phantom, forced.

    Discovery happens with no claim present; an intent PUT then acquires and COMMITS its
    claim; only then does the per-device sweep run. Against a `NOT EXISTS (device_claim)`
    predicate the sweep's snapshot never sees that row and it enqueues anyway.
    """
    import nso_adapter.core.tombstone_sweep as sweep_mod

    device_id = await seed_device(nso_device_name="sw-m5-10", netbox_device_id=9606)
    await _seed_tombstone(device_id)

    gate = asyncio.Event()
    original = sweep_mod._devices_with_eligible_tombstones

    async def _discover_then_wait(db=None):
        found = await original(db)
        await gate.wait()
        return found

    monkeypatch.setattr(sweep_mod, "_devices_with_eligible_tombstones", _discover_then_wait)

    task = asyncio.create_task(sweep_tombstones())
    await asyncio.sleep(0.2)
    holder = await acquire_claim(device_id, "intent_put")
    gate.set()
    try:
        assert await asyncio.wait_for(task, timeout=10) == 0
    finally:
        await release_claim(holder)

    assert await _removal_jobs(device_id) == []


async def test_the_sweep_never_holds_a_claim_it_could_not_use(adapter_client):
    """A device with nothing eligible must not be left claimed until the reaper."""
    from nso_adapter.core.tombstone_sweep import sweep_one_device
    from nso_adapter.store.models import DeviceClaim

    device_id = await seed_device(nso_device_name="sw-m5-empty", netbox_device_id=9607)

    assert await sweep_one_device(device_id) == 0
    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None


# ── wiring: startup and the periodic tick ────────────────────────────────────


async def test_startup_sweeps_before_the_pool_drains_anything(adapter_client, monkeypatch):
    """M5.1's entry point — the deletion carrier must not wait for the next tick."""
    import nso_adapter.core.worker as worker_mod

    device_id = await seed_device(nso_device_name="sw-m5-start", netbox_device_id=9640)
    tombstone_id = await _seed_tombstone(device_id)

    async def _idle_loop(_worker_id, stop):
        await stop.wait()

    monkeypatch.setattr(worker_mod, "_worker_loop", _idle_loop)
    await worker_mod.start_workers(concurrency=1)
    try:
        jobs = await _removal_jobs(device_id)
        assert len(jobs) == 1
        assert await _tombstones(device_id) == [(tombstone_id, jobs[0]["id"])]
    finally:
        await worker_mod.stop_workers()


async def test_the_periodic_reap_tick_also_sweeps(adapter_client, monkeypatch):
    """A deletion orphaned in a LIVE process has no restart to wait for."""
    import nso_adapter.core.scheduler as scheduler_mod
    import nso_adapter.core.worker as worker_mod

    device_id = await seed_device(nso_device_name="sw-m5-tick", netbox_device_id=9641)
    await _seed_tombstone(device_id)
    monkeypatch.setattr(worker_mod, "ensure_workers", lambda: None)

    await scheduler_mod._scheduled_orphan_reap()

    assert len(await _removal_jobs(device_id)) == 1


# ── M5.6/M5.7: the snapshotted delete ────────────────────────────────────────


async def test_delete_tombstones_deletes_exactly_the_snapshotted_ids(adapter_client):
    """M5.6 + M5.7 — both halves: the listed ones go, a later insert survives."""
    from nso_adapter.store.tombstone_store import delete_tombstones

    device_id = await seed_device(nso_device_name="sw-m5-6", netbox_device_id=9608)
    first = await _seed_tombstone(device_id, A, route_id=7)
    second = await _seed_tombstone(device_id, B, route_id=8)
    third = await _seed_tombstone(device_id, C, route_id=9)

    reg = await acquire_claim(device_id, "sweep")
    snapshot = [first, second]
    # Inserted AFTER the snapshot: nothing has proven anything about it.
    fourth = await _seed_tombstone(device_id, ("", "10.9.0.0/24", "192.0.2.9"), route_id=10)

    async with session() as db:
        assert await delete_tombstones(db, snapshot, device_id=device_id, claim_token=reg.token) == 2
        await db.commit()
    await release_claim(reg)

    assert [row_id for row_id, _job in await _tombstones(device_id)] == sorted([third, fourth])


async def test_delete_tombstones_with_an_empty_snapshot_deletes_nothing(adapter_client):
    """An empty proof set is not a licence to delete the device's other carriers."""
    from nso_adapter.store.tombstone_store import delete_tombstones

    device_id = await seed_device(nso_device_name="sw-m5-empty-ids", netbox_device_id=9612)
    first = await _seed_tombstone(device_id, A, route_id=7)

    reg = await acquire_claim(device_id, "sweep")
    try:
        async with session() as db:
            assert await delete_tombstones(db, [], device_id=device_id, claim_token=reg.token) == 0
            await db.commit()
    finally:
        await release_claim(reg)

    assert [row_id for row_id, _job in await _tombstones(device_id)] == [first]


async def test_delete_tombstones_refuses_a_stale_token(adapter_client):
    """M5.7's other half — a caller whose claim was revoked cannot commit the delete."""
    from nso_adapter.store.tombstone_store import delete_tombstones

    device_id = await seed_device(nso_device_name="sw-m5-7", netbox_device_id=9609)
    first = await _seed_tombstone(device_id, A, route_id=7)

    async with session() as db:
        with pytest.raises(ClaimLostError):
            await delete_tombstones(db, [first], device_id=device_id, claim_token="not-a-live-token")
        await db.rollback()

    assert [row_id for row_id, _job in await _tombstones(device_id)] == [first]
