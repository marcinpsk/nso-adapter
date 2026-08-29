# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1503 Appendix-O chunk O2: one removal job carries ONE deletion marking.

``?delete_origin`` marks the whole request today, so every removal is homogeneous by
accident. Appendix O §4.5 marks static routes PER OBJECT, and one job cannot then hold both
provenances: ``detach`` is a job-wide dispatch switch (``DETACH_REPLACE`` decides whether the
whole PUT commits with no-networking), so a mixed job would either leave a delete-origin
retraction off the device or play an un-own's reverse diff against it (#106).

Every case drives the real enqueue seam and the real ``run_removal`` against a real
PostgreSQL clone; the only fake is the RESTCONF boundary, and it is the stateful one from
``test_static_route_removal``, so what survives on the device is an observed fact here.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from nso_adapter.store.models import Job, JobStatus, JobType, StaticRouteIntent, StaticRouteTombstone
from tests.conftest import note_projection_write, seed_device, session
from tests.core.test_static_route_put import A, B, C, D, seed_rows, wire
from tests.core.test_static_route_removal import SrFake, run_removal_job, seed_removal_job, sr_client

pytestmark = pytest.mark.anyio


async def _tombstone(db, device_id: int, triple, *, marking: str, route_id: int) -> StaticRouteTombstone:
    vrf, prefix, next_hop = triple
    tomb = StaticRouteTombstone(
        device_id=device_id,
        route_id=route_id,
        vrf=vrf,
        prefix=prefix,
        next_hop=next_hop,
        deployed_key=None,
        marking=marking,
    )
    db.add(tomb)
    return tomb


async def _enqueue_split(device_id: int, *, removed: dict[str, list], retract: bool = False) -> dict:
    """Drive the real seam: carriers for every removed key, then the marking-homogeneous jobs.

    Returns ``{"jobs": [job ids in creation order], "tombstones": {triple: (marking, job_id)}}``.
    """
    from nso_adapter.core.removal import enqueue_static_route_removals

    async with session() as db:
        await note_projection_write(db, device_id, "static_route")
        route_id = 100
        tombstones = []
        for marking, triples in removed.items():
            for triple in triples:
                route_id += 1
                tombstones.append(await _tombstone(db, device_id, triple, marking=marking, route_id=route_id))
        await db.flush()
        jobs = await enqueue_static_route_removals(
            db,
            device_id,
            promotes=("static_route",),
            removed=removed,
            tombstones=tombstones,
            retract=retract,
        )
        await db.commit()
        return {
            "jobs": [job.id for job in jobs],
            "tombstones": {(t.vrf, t.prefix, t.next_hop): (t.marking, t.job_id) for t in tombstones},
        }


async def _contexts(device_id: int) -> list[dict]:
    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(Job)
                    .where(Job.device_id == device_id, Job.job_type == JobType.removal)
                    .order_by(Job.created_at, Job.id)
                )
            )
            .scalars()
            .all()
        )
        return [row.context for row in rows]


async def _generations(device_id: int) -> list[tuple[int, str, int | None]]:
    from nso_adapter.store.models import DeploymentGeneration

    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(DeploymentGeneration)
                    .where(DeploymentGeneration.device_id == device_id)
                    .order_by(DeploymentGeneration.seq)
                )
            )
            .scalars()
            .all()
        )
        return [(r.seq, r.mode.value, r.job_id) for r in rows]


async def _tombstone_rows(device_id: int) -> dict[tuple, tuple[str, int | None]]:
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
        return {(r.vrf, r.prefix, r.next_hop): (r.marking, r.job_id) for r in rows}


# ── O2.1: two markings, two jobs ────────────────────────────────────────────


async def test_o2_1_both_markings_produce_two_jobs_each_owning_its_own_carriers(adapter_client):
    """O2.1. Forbidden: one job with one job-wide ``detach``.

    Required: TWO jobs, each tombstone stamped with its OWN marking's job, and each job's
    authority naming only its own keys.
    """
    device_id = await seed_device(nso_device_name="sr-o21", netbox_device_id=9870)

    out = await _enqueue_split(device_id, removed={"delete_origin": [A, B], "detach": [C]})

    contexts = await _contexts(device_id)
    assert len(contexts) == 2, "a removal carrying both markings must not become one job"
    assert set(contexts[0]) == {"scope", "removed"}, "the marked job retracts for real"
    assert sorted(contexts[0]["removed"]["route"]) == sorted([list(A), list(B)])
    assert contexts[1] == {"scope": "static_route", "removed": {"route": [list(C)]}, "detach": True}
    marked, unmarked = out["jobs"]
    assert out["tombstones"] == {
        A: ("delete_origin", marked),
        B: ("delete_origin", marked),
        C: ("detach", unmarked),
    }
    # The networked retraction is ordered first: the detach commits no-networking and then
    # runs sync-from, whose failure would block the device write behind it (#1522 §H2).
    assert await _generations(device_id) == [(1, "networked", marked), (2, "detach", unmarked)]


async def test_o2_1_a_homogeneous_removal_still_produces_exactly_one_job(adapter_client):
    """O2.1 control: the split must not manufacture a second job for one marking."""
    device_id = await seed_device(nso_device_name="sr-o21-ctl", netbox_device_id=9871)

    out = await _enqueue_split(device_id, removed={"detach": [A, B]})

    assert len(out["jobs"]) == 1
    contexts = await _contexts(device_id)
    assert set(contexts[0]) == {"scope", "removed", "detach"}
    assert sorted(contexts[0]["removed"]["route"]) == sorted([list(A), list(B)])
    assert await _generations(device_id) == [(1, "detach", out["jobs"][0])]


async def test_a_carrier_without_a_matching_removal_job_is_rejected(adapter_client):
    from nso_adapter.core.removal import enqueue_static_route_removals

    device_id = await seed_device(nso_device_name="sr-unmatched-marking", netbox_device_id=9880)
    async with session() as db:
        await note_projection_write(db, device_id, "static_route")
        tombstone = await _tombstone(db, device_id, B, marking="detach", route_id=101)
        await db.flush()

        with pytest.raises(RuntimeError, match="carrier marked 'detach' has no job"):
            await enqueue_static_route_removals(
                db,
                device_id,
                promotes=("static_route",),
                removed={"delete_origin": [A]},
                tombstones=[tombstone],
            )


# ── O2.2: run to execution ──────────────────────────────────────────────────


async def test_o2_2_each_job_subtracts_only_its_own_keys_and_reads_current_fresh(adapter_client):
    """O2.2. Forbidden: either job's ``authorized`` set containing the other's keys.

    The second job's body is built from the service as the FIRST job left it, so the split
    cannot resurrect what the first one dropped.
    """
    device_id = await seed_device(nso_device_name="sr-o22", netbox_device_id=9872)
    await seed_rows(device_id, [{"triple": D, "route_id": 4, "deployed_key": list(D)}])
    out = await _enqueue_split(device_id, removed={"delete_origin": [A, B], "detach": [C]})
    marked, unmarked = out["jobs"]
    fake = SrFake("sr-o22", service=[wire(A), wire(B), wire(C), wire(D)])

    first = await run_removal_job(device_id, marked, sr_client(fake))
    second = await run_removal_job(device_id, unmarked, sr_client(fake))

    assert (first.status, second.status) == (JobStatus.succeeded, JobStatus.succeeded)
    assert first.result["authorized"] == [list(A), list(B)]
    assert second.result["authorized"] == [list(C)]
    assert fake.sent_keys(0) == {C, D}, "the networked job may drop only its own two keys"
    assert fake.sent_keys(1) == {D}, "the detach read `current` fresh, so A and B are already gone"
    assert fake.writes[0]["no_networking"] is False
    assert fake.writes[1]["no_networking"] is True
    # A and B were retracted from the device; C was only un-owned, so it stays as brownfield.
    assert fake.device_keys == {C, D}
    assert await _tombstone_rows(device_id) == {}, "each job consumed its own carriers"


# ── O2.3: the clear a mixed removal carries ─────────────────────────────────


async def test_o2_3_a_mixed_removal_defers_its_clear_exactly_as_today(adapter_client):
    """O2.3: the whole-request ``has_detach`` fact reaches the networked job."""
    device_id = await seed_device(nso_device_name="sr-o23a", netbox_device_id=9873)

    await _enqueue_split(device_id, removed={"delete_origin": [A], "detach": [B]}, retract=True)

    assert await _contexts(device_id) == [
        {"scope": "static_route", "removed": {"route": [list(A)]}, "retract_deferred": True},
        {"scope": "static_route", "removed": {"route": [list(B)]}, "detach": True},
    ]


async def test_o2_3_a_marked_only_removal_with_a_clear_still_delivers_it(adapter_client):
    """O2.3 control: with nothing un-owned there is no deferral, so the clear rides out."""
    device_id = await seed_device(nso_device_name="sr-o23c", netbox_device_id=9874)

    await _enqueue_split(device_id, removed={"delete_origin": [A]}, retract=True)

    assert await _contexts(device_id) == [{"scope": "static_route", "removed": {"route": [list(A)]}}]


async def test_o2_3_b_a_deferred_retract_delivers_no_clear_at_execution(adapter_client):
    """O2.3. Forbidden: the networked job delivering a clear today's code defers.

    A deferred retract is recorded on a NETWORKED job as soon as the markings split, so the
    deferral has to be honoured where the body is built, not merely implied by ``detach``.
    """
    device_id = await seed_device(nso_device_name="sr-o23b", netbox_device_id=9875)
    await seed_rows(
        device_id,
        [{"triple": B, "route_id": 2, "deployed_key": list(B), "pending_clear": {"authorized": ["metric"]}}],
    )
    fake = SrFake("sr-o23b", service=[wire(A), wire(B, metric=10)])
    job_id = await seed_removal_job(device_id, {"removed": {"route": [list(A)]}, "retract_deferred": True})

    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status == JobStatus.succeeded
    sent = {(e.get("vrf") or "", e["prefix"], e["next-hop"]): e for e in fake.sent_routes()}
    assert sent.keys() == {B}
    assert sent[B].get("metric") == 10, "the deferred clear must not ride out on this push"
    async with session() as db:
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.pending_clear == {"authorized": ["metric"]}, "the carrier holds the deferred clear"


# ── O2.5: removal before apply, with two removals ───────────────────────────


async def test_o2_5_both_removals_carry_the_lower_queue_key_than_the_apply(adapter_client):
    """O2.5. Forbidden: the apply admitted ahead of either removal."""
    from nso_adapter.core.apply import enqueue_apply
    from nso_adapter.core.removal import enqueue_static_route_removals

    device_id = await seed_device(nso_device_name="sr-o25", netbox_device_id=9876)
    async with session() as db:
        await note_projection_write(db, device_id, "static_route")
        removals = await enqueue_static_route_removals(
            db,
            device_id,
            promotes=("static_route",),
            removed={"delete_origin": [A], "detach": [B]},
        )
        apply_job = await enqueue_apply(db, device_id, force=True, stream="static_route")
        await db.commit()
        removal_ids = [job.id for job in removals]
        apply_id = apply_job.id

    async with session() as db:
        ordered = (
            (await db.execute(select(Job).where(Job.device_id == device_id).order_by(Job.created_at, Job.id)))
            .scalars()
            .all()
        )
    assert [job.id for job in ordered] == [*removal_ids, apply_id]
    assert [job.job_type for job in ordered] == [JobType.removal, JobType.removal, JobType.apply]
    assert await _generations(device_id) == [
        (1, "networked", removal_ids[0]),
        (2, "detach", removal_ids[1]),
        (3, "networked", apply_id),
    ]


# ── O2.6: one job fails, the other succeeds ─────────────────────────────────


async def test_o2_6_the_sweeper_reissues_each_carrier_at_its_own_marking(adapter_client):
    """O2.6. Forbidden: the sweeper re-issuing the wrong marking; cross-job consumption."""
    from nso_adapter.core.tombstone_sweep import sweep_one_device

    device_id = await seed_device(nso_device_name="sr-o26", netbox_device_id=9877)
    await seed_rows(device_id, [{"triple": D, "route_id": 4, "deployed_key": list(D)}])
    out = await _enqueue_split(device_id, removed={"delete_origin": [A], "detach": [C]})
    marked, unmarked = out["jobs"]

    fake = SrFake("sr-o26", service=[wire(A), wire(C), wire(D)])
    first = await run_removal_job(device_id, marked, sr_client(fake))
    # The detach cannot certify the live service, so it fails with its carrier intact.
    blind = SrFake("sr-o26", service=[wire(C), wire(D)], service_status="inconclusive")
    second = await run_removal_job(device_id, unmarked, sr_client(blind))

    assert (first.status, second.status) == (JobStatus.succeeded, JobStatus.failed)
    assert await _tombstone_rows(device_id) == {C: ("detach", unmarked)}, "consumption stays job-scoped"
    async with session() as db:
        tombstone_id = await db.scalar(
            select(StaticRouteTombstone.id).where(
                StaticRouteTombstone.device_id == device_id,
                StaticRouteTombstone.prefix == C[1],
            )
        )

    assert await sweep_one_device(device_id) == 1
    contexts = await _contexts(device_id)
    assert contexts[2] == {
        "scope": "static_route",
        "removed": {"route": [list(C)]},
        "detach": True,
        "tombstone_ids": [tombstone_id],
    }
    assert len(contexts) == 3, "the succeeded job's keys must not be re-issued"


async def test_a_split_revision_is_not_applied_when_one_marking_job_fails(adapter_client):
    """One promoted revision is applied only when both marking-specific jobs land."""
    from nso_adapter.store.models import DeviceProjectionStream

    device_id = await seed_device(nso_device_name="sr-split-settle-fail", netbox_device_id=9880)
    out = await _enqueue_split(device_id, removed={"delete_origin": [A], "detach": [C]})
    marked, unmarked = out["jobs"]

    fake = SrFake("sr-split-settle-fail", service=[wire(A), wire(C)])
    first = await run_removal_job(device_id, marked, sr_client(fake))
    blind = SrFake("sr-split-settle-fail", service=[wire(C)], service_status="inconclusive")
    second = await run_removal_job(device_id, unmarked, sr_client(blind))

    assert (first.status, second.status) == (JobStatus.succeeded, JobStatus.failed)
    async with session() as db:
        stream = await db.scalar(
            select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "static_route",
            )
        )
    assert (stream.desired_revision, stream.authorized_revision) == (1, 1)
    assert stream.applied_revision == 0, "the successful half certified a revision its failed sibling did not land"


async def test_a_split_revision_is_applied_when_both_marking_jobs_succeed(adapter_client):
    """The shared revision is certified once both marking-specific jobs land."""
    from nso_adapter.store.models import DeviceProjectionStream

    device_id = await seed_device(nso_device_name="sr-split-settle-success", netbox_device_id=9881)
    out = await _enqueue_split(device_id, removed={"delete_origin": [A], "detach": [C]})
    marked, unmarked = out["jobs"]

    fake = SrFake("sr-split-settle-success", service=[wire(A), wire(C)])
    first = await run_removal_job(device_id, marked, sr_client(fake))
    second = await run_removal_job(device_id, unmarked, sr_client(fake))

    assert (first.status, second.status) == (JobStatus.succeeded, JobStatus.succeeded)
    async with session() as db:
        stream = await db.scalar(
            select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "static_route",
            )
        )
    assert (stream.desired_revision, stream.authorized_revision, stream.applied_revision) == (1, 1, 1)


async def test_a_split_revision_is_applied_when_the_failed_sibling_is_abandoned(adapter_client):
    """A settled cohort member certifies the revision after its failed sibling is abandoned."""
    from nso_adapter.core.generation import reconcile_generation
    from nso_adapter.store.models import DeploymentGeneration, DeviceProjectionStream

    device_id = await seed_device(nso_device_name="sr-split-settle-abandon", netbox_device_id=9882)
    out = await _enqueue_split(device_id, removed={"delete_origin": [A], "detach": [C]})
    marked, unmarked = out["jobs"]

    fake = SrFake("sr-split-settle-abandon", service=[wire(A), wire(C)])
    first = await run_removal_job(device_id, marked, sr_client(fake))
    blind = SrFake("sr-split-settle-abandon", service=[wire(C)], service_status="inconclusive")
    second = await run_removal_job(device_id, unmarked, sr_client(blind))
    assert (first.status, second.status) == (JobStatus.succeeded, JobStatus.failed)

    async with session() as db:
        failed_generation = await db.scalar(select(DeploymentGeneration).where(DeploymentGeneration.job_id == unmarked))
        assert await reconcile_generation(db, failed_generation.id) is None
        await db.commit()

    async with session() as db:
        stream = await db.scalar(
            select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "static_route",
            )
        )
        assert stream.applied_revision == 1


async def test_a_homogeneous_removal_still_applies_its_revision(adapter_client):
    """The ordinary one-generation settlement keeps its existing revision behavior."""
    from nso_adapter.store.models import DeviceProjectionStream

    device_id = await seed_device(nso_device_name="sr-single-settle", netbox_device_id=9882)
    out = await _enqueue_split(device_id, removed={"delete_origin": [A]})
    (job_id,) = out["jobs"]

    fake = SrFake("sr-single-settle", service=[wire(A)])
    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status is JobStatus.succeeded
    async with session() as db:
        stream = await db.scalar(
            select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "static_route",
            )
        )
    assert (stream.desired_revision, stream.authorized_revision, stream.applied_revision) == (1, 1, 1)


# ── O2.7: two enqueues in one request ───────────────────────────────────────


async def test_o2_7_the_marking_is_an_argument_not_the_requests_query_flag(adapter_client):
    """O2.7. Forbidden: both jobs reading one request-scoped ``DELETE_ORIGIN``.

    The request here is UNMARKED, and the delete-origin job must still be networked.
    """
    from nso_adapter.core.removal import enqueue_removal
    from nso_adapter.core.request_flags import DELETE_ORIGIN, DELETE_ORIGIN_MARKING, DETACH_MARKING

    device_id = await seed_device(nso_device_name="sr-o27", netbox_device_id=9878)
    assert DELETE_ORIGIN.get() is False
    async with session() as db:
        await note_projection_write(db, device_id, "static_route")
        marked = await enqueue_removal(
            db,
            device_id,
            "static_route",
            marking=DELETE_ORIGIN_MARKING,
            defer_retract=False,
            promotes=("static_route",),
            removed={"route": [list(A)]},
            shrank=True,
        )
        unmarked = await enqueue_removal(
            db,
            device_id,
            "static_route",
            marking=DETACH_MARKING,
            defer_retract=False,
            promotes=("static_route",),
            removed={"route": [list(B)]},
            shrank=True,
        )
        await db.commit()
        marked_context, unmarked_context = marked.context, unmarked.context

    assert "detach" not in marked_context, "the marked job is networked even on an unmarked request"
    assert unmarked_context["detach"] is True


async def test_o2_7_the_deferred_retract_is_an_argument_too(adapter_client):
    """O2.7: one job cannot derive the sibling's un-own from its own rows."""
    from nso_adapter.core.removal import enqueue_removal
    from nso_adapter.core.request_flags import DELETE_ORIGIN_MARKING

    device_id = await seed_device(nso_device_name="sr-o27b", netbox_device_id=9879)
    async with session() as db:
        await note_projection_write(db, device_id, "static_route")
        job = await enqueue_removal(
            db,
            device_id,
            "static_route",
            marking=DELETE_ORIGIN_MARKING,
            defer_retract=True,
            promotes=("static_route",),
            removed={"route": [list(A)]},
            retract=True,
            shrank=True,
        )
        await db.commit()
        context = job.context

    assert context["retract_deferred"] is True
    assert "detach" not in context
