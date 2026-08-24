# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R2 chunk C4 — the R1→R2 activation reclaimer (C4.22, C4.23, §4.10).

R1's sweeper leaves a tombstone whose owner SUCCEEDED alone (G17), so those rows have no
retry path at all until something proves them. That "something" is here. Every case drives
the real :func:`reclaim_succeeded_tombstones` against a real PostgreSQL clone and a real
per-device claim; only the RESTCONF boundary is faked, by the same stateful substrate the
removal pins use.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from nso_adapter.core.claim import acquire_claim, release_claim
from nso_adapter.store.models import Job, JobStatus, JobType, StaticRouteTombstone
from tests.conftest import seed_device, session
from tests.core.test_static_route_put import A, B, wire
from tests.core.test_static_route_removal import SrFake, seed_tomb, sr_client, tombstone_ids

pytestmark = pytest.mark.anyio


async def seed_succeeded_owner(device_id: int) -> int:
    async with session() as db:
        job = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.succeeded,
            context={"scope": "static_route"},
            result={"scope": "static_route"},
        )
        db.add(job)
        await db.commit()
        return job.id


async def queued_removals(device_id: int) -> list[Job]:
    async with session() as db:
        return list(
            (
                await db.execute(
                    select(Job)
                    .where(Job.device_id == device_id, Job.job_type == JobType.removal, Job.status == JobStatus.queued)
                    .order_by(Job.id)
                )
            )
            .scalars()
            .all()
        )


async def owners(device_id: int) -> dict[int, int | None]:
    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(StaticRouteTombstone)
                    .where(StaticRouteTombstone.device_id == device_id)
                    .order_by(StaticRouteTombstone.id)
                )
            )
            .scalars()
            .all()
        )
        return {r.id: r.job_id for r in rows}


async def run_reclaim(client, *, budget: int | None = None):
    from nso_adapter.core.static_route_reclaim import reclaim_succeeded_tombstones, reset_cursor

    reset_cursor()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.nso.actions.sync_from", new=AsyncMock(return_value={})),
    ):
        return await reclaim_succeeded_tombstones(budget=budget)


# ── C4.22 — the four dispositions ────────────────────────────────────────────


async def test_c4_22_i_delete_origin_with_device_residue_is_reissued(adapter_client):
    """(i) the route is still ON the device: consuming here would strand it forever."""
    device_id = await seed_device(nso_device_name="rc-i", netbox_device_id=8101)
    owner = await seed_succeeded_owner(device_id)
    tomb = await seed_tomb(device_id, A, job_id=owner, route_id=1)
    fake = SrFake("rc-i", service=[wire(B)], device=[wire(A), wire(B)])

    consumed, reissued = await run_reclaim(sr_client(fake))

    assert (consumed, reissued) == (0, 1)
    assert await tombstone_ids(device_id) == [tomb]
    jobs = await queued_removals(device_id)
    assert len(jobs) == 1
    assert jobs[0].id != owner, "a FRESH job id — the succeeded owner is what made it unsweepable"
    assert (await owners(device_id))[tomb] == jobs[0].id
    assert jobs[0].context == {
        "scope": "static_route",
        "removed": {"route": [list(A)]},
        "detach": False,
        "tombstone_ids": [tomb],
    }


async def test_c4_22_ii_delete_origin_with_a_clean_device_is_consumed(adapter_client):
    """(ii) the deletion really landed — R1 just never had a way to say so."""
    device_id = await seed_device(nso_device_name="rc-ii", netbox_device_id=8102)
    owner = await seed_succeeded_owner(device_id)
    await seed_tomb(device_id, A, job_id=owner, route_id=1)
    fake = SrFake("rc-ii", service=[wire(B)], device=[wire(B)])

    consumed, reissued = await run_reclaim(sr_client(fake))

    assert (consumed, reissued) == (1, 0)
    assert await tombstone_ids(device_id) == []
    assert await queued_removals(device_id) == []


async def test_c4_22_iii_detach_with_the_key_only_on_the_device_is_consumed(adapter_client):
    """(iii) device presence is the EXPECTED state after an un-own — never a failure.

    Re-issuing here would push a fresh detach for a row already un-owned, forever.
    """
    device_id = await seed_device(nso_device_name="rc-iii", netbox_device_id=8103)
    owner = await seed_succeeded_owner(device_id)
    await seed_tomb(device_id, A, job_id=owner, route_id=1, marking="detach")
    fake = SrFake("rc-iii", service=None, device=[wire(A), wire(B)])

    consumed, reissued = await run_reclaim(sr_client(fake))

    assert (consumed, reissued) == (1, 0)
    assert await tombstone_ids(device_id) == []
    assert await queued_removals(device_id) == []


async def test_c4_22_iv_detach_whose_service_still_owns_the_key_is_reissued(adapter_client):
    """(iv) the service still owns it, so the un-own never happened."""
    device_id = await seed_device(nso_device_name="rc-iv", netbox_device_id=8104)
    owner = await seed_succeeded_owner(device_id)
    tomb = await seed_tomb(device_id, A, job_id=owner, route_id=1, marking="detach")
    fake = SrFake("rc-iv", service=[wire(A), wire(B)])

    consumed, reissued = await run_reclaim(sr_client(fake))

    assert (consumed, reissued) == (0, 1)
    assert await tombstone_ids(device_id) == [tomb]
    jobs = await queued_removals(device_id)
    assert jobs[0].context["detach"] is True


async def test_c4_22_r1_sweep_predicate_is_unchanged(adapter_client):
    """The reclaimer is a separate reader: R1's sweeper must still ignore a succeeded owner."""
    from nso_adapter.core.tombstone_sweep import sweep_tombstones

    device_id = await seed_device(nso_device_name="rc-pred", netbox_device_id=8105)
    owner = await seed_succeeded_owner(device_id)
    await seed_tomb(device_id, A, job_id=owner, route_id=1)

    assert await sweep_tombstones() == 0


async def test_c4_22_an_inconclusive_service_read_reissues_rather_than_consumes(adapter_client):
    """An uncertified read proves nothing — and "proves nothing" must never mean "consume"."""
    device_id = await seed_device(nso_device_name="rc-incon", netbox_device_id=8106)
    owner = await seed_succeeded_owner(device_id)
    tomb = await seed_tomb(device_id, A, job_id=owner, route_id=1, marking="detach")
    fake = SrFake("rc-incon", service=[wire(A)], service_status="inconclusive")

    consumed, reissued = await run_reclaim(sr_client(fake))

    assert (consumed, reissued) == (0, 1)
    assert await tombstone_ids(device_id) == [tomb]


# ── C4.23 — a drain, atomic, and off the critical path ──────────────────────


async def test_c4_23_a_claimed_device_is_skipped_and_reclaimed_on_a_later_tick(adapter_client):
    """C4.23 — a startup-only pass would abandon this device forever (G17 never revisits it)."""
    device_id = await seed_device(nso_device_name="rc-rival", netbox_device_id=8107)
    owner = await seed_succeeded_owner(device_id)
    tomb = await seed_tomb(device_id, A, job_id=owner, route_id=1)
    fake = SrFake("rc-rival", service=[wire(B)], device=[wire(B)])

    rival = await acquire_claim(device_id, "teardown")
    assert rival is not None
    assert await run_reclaim(sr_client(fake)) == (0, 0)
    assert await tombstone_ids(device_id) == [tomb], "skipped, not consumed and not re-issued"

    await release_claim(rival)
    assert await run_reclaim(sr_client(fake)) == (1, 0)
    assert await tombstone_ids(device_id) == []


async def test_c4_23_a_killed_reissue_leaves_no_ownerless_job(adapter_client):
    """C4.23 — a split transaction leaves a job whose tombstone points elsewhere.

    Such a job falls back to ``context["removed"]`` and silently loses the ``deployed_key``
    half of its authorization, so the re-issue must be all-or-nothing.
    """
    from nso_adapter.core import tombstone_sweep as sweep_mod

    device_id = await seed_device(nso_device_name="rc-atomic", netbox_device_id=8108)
    owner = await seed_succeeded_owner(device_id)
    first = await seed_tomb(device_id, A, job_id=owner, route_id=1)
    second = await seed_tomb(device_id, B, job_id=owner, route_id=2)
    fake = SrFake("rc-atomic", service=[wire(A), wire(B)], device=[wire(A), wire(B)])

    real = sweep_mod._removal_context
    seen: list[int] = []

    def _boom(row):
        seen.append(row.id)
        if len(seen) == 2:
            raise RuntimeError("killed mid-reissue")
        return real(row)

    with patch.object(sweep_mod, "_removal_context", _boom):
        assert await run_reclaim(sr_client(fake)) == (0, 0)

    assert await queued_removals(device_id) == [], "no ownerless job may survive the rollback"
    assert await owners(device_id) == {first: owner, second: owner}


async def test_c4_23_the_reclaimer_is_its_own_bounded_scheduled_job(adapter_client):
    """C4.23 — it must block neither the sequential reap tick nor startup readiness.

    ``_scheduled_orphan_reap`` ends in ``ensure_workers()`` (G30) and ``start_workers`` is
    awaited before any worker exists, so a slow reclaim in either would delay stale-claim
    reaping, the R1 sweep and worker repair.
    """
    from nso_adapter.core import scheduler, worker

    specs = {spec.job_id: spec for spec in scheduler._JOB_SPECS}
    assert "static_route_reclaim" in specs
    spec = specs["static_route_reclaim"]
    assert spec.fn is scheduler._scheduled_static_route_reclaim
    assert spec.interval_attr == "static_route_reclaim_interval"
    assert spec.gate_on_interval is True, "an interval of 0 must be able to disable it"

    for fn in (scheduler._scheduled_orphan_reap, worker.start_workers):
        assert "reclaim" not in inspect.getsource(fn), f"{fn.__name__} must not run the reclaimer inline"

    # max_instances=1 comes from the scheduler's job defaults, and the startup kick is a
    # fire-and-forget "date" job rather than anything the readiness path awaits.
    captured: dict = {}

    class _FakeScheduler:
        def __init__(self, **kwargs):
            captured["defaults"] = kwargs["job_defaults"]
            captured["jobs"] = []

        def add_job(self, fn, trigger, **kwargs):
            captured["jobs"].append((fn, trigger, kwargs.get("id"), kwargs))

        def start(self):
            captured["started"] = True

    with patch.object(scheduler, "AsyncIOScheduler", _FakeScheduler):
        scheduler.start_scheduler()
    scheduler._scheduler = None

    assert captured["defaults"]["max_instances"] == 1
    # ONE registration, not an interval job plus a separate startup "date" job: APScheduler
    # enforces max_instances per job ID, so two ids let a slow startup pass overlap the first
    # recurring tick and race the drain cursor. The immediate first run rides the same id.
    ours = [job for job in captured["jobs"] if job[2].startswith("static_route_reclaim")]
    assert [job[2] for job in ours] == ["static_route_reclaim"]
    assert ours[0][1] == "interval"
    assert ours[0][3].get("next_run_time") is not None, "the activation pass must still fire at startup"


async def test_c4_23_the_per_tick_budget_bounds_the_drain(adapter_client):
    """C4.23 — one wedged device must not monopolize the drain; the next tick resumes after it."""
    from nso_adapter.core.static_route_reclaim import reclaim_succeeded_tombstones, reset_cursor

    devices = []
    for index in range(3):
        device_id = await seed_device(nso_device_name=f"rc-budget-{index}", netbox_device_id=8200 + index)
        owner = await seed_succeeded_owner(device_id)
        await seed_tomb(device_id, A, job_id=owner, route_id=1)
        devices.append(device_id)
    fake = SrFake("rc-budget", service=[wire(B)], device=[wire(B)])

    reset_cursor()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=sr_client(fake)),
        patch("nso_adapter.nso.actions.sync_from", new=AsyncMock(return_value={})),
    ):
        first = await reclaim_succeeded_tombstones(budget=2)
        second = await reclaim_succeeded_tombstones(budget=2)

    assert first == (2, 0)
    assert second == (1, 0), "the second tick resumes after the first batch instead of redoing it"
    for device_id in devices:
        assert await tombstone_ids(device_id) == []
