# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R1b chunk 3 / Q9 + M7: the removal job joins the endpoint's transaction.

The static-route PUT used to commit its deletes and only then ask
``replace_on_removal`` to create the removal job in a second transaction. That put the
apply job ahead of the removal in the queue (M7.1) and left every tombstone with
``job_id IS NULL`` — indistinguishable from a tombstone whose job never got created,
which is the sweeper's whole trigger.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from tests.api.test_static_route_identity import (
    A,
    B,
    C,
    enable_auto_apply,
    entry,
    put_intent,
    read_intent,
    read_jobs,
    read_tombstones,
    seed_intent,
)
from tests.conftest import seed_device, session


async def _jobs_ordered(device_id: int) -> list[tuple[int, str]]:
    """(id, job_type) in queue order — the (created_at, id) key Q5's head claim uses."""
    from sqlalchemy import select

    from nso_adapter.store.models import Job

    async with session() as db:
        rows = (
            (await db.execute(select(Job).where(Job.device_id == device_id).order_by(Job.created_at, Job.id)))
            .scalars()
            .all()
        )
        return [(r.id, r.job_type.value) for r in rows]


async def _run_apply_job(device_id: int, job_id: int, client):
    """Run the apply job the endpoint enqueued, as the worker would."""
    from nso_adapter.core.apply import run_apply
    from nso_adapter.core.claim import acquire_claim, release_claim
    from nso_adapter.store.models import Job
    from tests.conftest import start_job

    attempt = await start_job(job_id)
    reg = await acquire_claim(device_id, "job", job_id=job_id)
    if reg.run_attempt is None:
        reg.run_attempt = attempt
    try:
        with (
            patch("nso_adapter.core.importer.get_nso_client", return_value=client),
            patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()),
        ):
            await run_apply(job_id=job_id, device_id=device_id, force=True, reg=reg)
    finally:
        await release_claim(reg)
    async with session() as db:
        return await db.get(Job, job_id)


async def _projection_revisions(device_id: int) -> tuple[int, int, int]:
    from sqlalchemy import select

    from nso_adapter.store.models import DeviceProjectionStream

    async with session() as db:
        stream = await db.scalar(
            select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "static_route",
            )
        )
        return stream.desired_revision, stream.authorized_revision, stream.applied_revision


async def _settlement_cohorts(device_id: int) -> list[int | None]:
    from sqlalchemy import select

    from nso_adapter.store.models import DeploymentGeneration

    async with session() as db:
        generations = (
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
        return [generation.settlement_cohort for generation in generations]


async def _cohorts_by_job(device_id: int) -> dict[int | None, int | None]:
    from sqlalchemy import select

    from nso_adapter.store.models import DeploymentGeneration

    async with session() as db:
        generations = (
            (await db.execute(select(DeploymentGeneration).where(DeploymentGeneration.device_id == device_id)))
            .scalars()
            .all()
        )
        return {generation.job_id: generation.settlement_cohort for generation in generations}


async def _generation_document_sections(device_id: int) -> list[set[str]]:
    from sqlalchemy import select

    from nso_adapter.store.models import DeploymentGeneration

    async with session() as db:
        documents = (
            await db.scalars(
                select(DeploymentGeneration.document)
                .where(DeploymentGeneration.device_id == device_id)
                .order_by(DeploymentGeneration.seq)
            )
        ).all()
        return [set(document) for document in documents]


async def test_removal_precedes_apply_at_the_endpoint(adapter_client):
    """M7.1 — asserted before any worker runs; the endpoint's own ordering is the contract."""
    device_id = await seed_device(nso_device_name="sr-m7-1", netbox_device_id=9780)
    await enable_auto_apply(device_id)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 7, "deployed_key": list(A)},
            {"triple": C, "route_id": 8, "deployed_key": list(C)},
        ],
    )

    resp = await put_intent(adapter_client, device_id, [entry(B, route_id=7)])
    assert resp.status_code == 200

    ordered = await _jobs_ordered(device_id)
    assert [kind for _id, kind in ordered] == ["removal", "apply"]
    removal_id, apply_id = ordered[0][0], ordered[1][0]
    assert removal_id < apply_id


async def test_detach_cannot_certify_the_revision_when_its_companion_apply_fails(adapter_client):
    """One PUT's removal and apply must settle the promoted revision together."""
    from nso_adapter.store.models import JobStatus
    from tests.core.test_static_route_put import wire
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client

    device_id = await seed_device(nso_device_name="sr-cohort-fail", netbox_device_id=9786)
    await enable_auto_apply(device_id)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 7, "deployed_key": list(A)},
            {"triple": B, "route_id": 8, "deployed_key": list(B)},
        ],
    )

    response = await put_intent(adapter_client, device_id, [entry(B, route_id=8, metric=20)])
    assert response.status_code == 200
    ordered = await _jobs_ordered(device_id)
    assert [kind for _id, kind in ordered] == ["removal", "apply"]
    removal_id, apply_id = (job_id for job_id, _kind in ordered)

    fake = SrFake("sr-cohort-fail", service=[wire(A), wire(B)])
    removal = await run_removal_job(device_id, removal_id, sr_client(fake))
    assert removal.status is JobStatus.succeeded

    fake.dry_run_status = 400
    apply = await _run_apply_job(device_id, apply_id, sr_client(fake))
    assert apply.status is JobStatus.failed

    assert (await _projection_revisions(device_id))[2] == 0, (
        "the detach certified a revision its companion apply did not land"
    )


async def test_detach_and_companion_apply_stamp_the_revision_after_both_succeed(adapter_client):
    """The second success stamps the shared revision exactly once."""
    from nso_adapter.store.models import JobStatus
    from tests.core.test_static_route_put import wire
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client

    device_id = await seed_device(nso_device_name="sr-cohort-success", netbox_device_id=9787)
    await enable_auto_apply(device_id)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 7, "deployed_key": list(A)},
            {"triple": B, "route_id": 8, "deployed_key": list(B)},
        ],
    )

    response = await put_intent(adapter_client, device_id, [entry(B, route_id=8, metric=20)])
    assert response.status_code == 200
    ordered = await _jobs_ordered(device_id)
    assert [kind for _id, kind in ordered] == ["removal", "apply"]
    removal_id, apply_id = (job_id for job_id, _kind in ordered)
    cohorts = await _settlement_cohorts(device_id)
    assert cohorts[0] is not None
    assert cohorts == [cohorts[0], cohorts[0]]
    by_job = await _cohorts_by_job(device_id)
    assert by_job[apply_id] == by_job[removal_id] == cohorts[0]

    fake = SrFake("sr-cohort-success", service=[wire(A), wire(B)])
    removal = await run_removal_job(device_id, removal_id, sr_client(fake))
    assert removal.status is JobStatus.succeeded
    assert (await _projection_revisions(device_id))[2] == 0

    apply = await _run_apply_job(device_id, apply_id, sr_client(fake))
    assert apply.status is JobStatus.succeeded
    assert await _projection_revisions(device_id) == (1, 1, 1)


async def test_an_apply_only_put_keeps_independent_settlement(adapter_client):
    """A single-stream generation executes without inventing absent document sections."""
    from nso_adapter.store.models import JobStatus
    from tests.core.test_static_route_put import wire
    from tests.core.test_static_route_removal import SrFake, sr_client

    device_id = await seed_device(nso_device_name="sr-single-apply", netbox_device_id=9788)
    await enable_auto_apply(device_id)
    await seed_intent(device_id, [{"triple": B, "route_id": 8, "deployed_key": list(B)}])

    response = await put_intent(adapter_client, device_id, [entry(B, route_id=8, metric=20)])
    assert response.status_code == 200
    ordered = await _jobs_ordered(device_id)
    assert [kind for _id, kind in ordered] == ["apply"]
    assert await _settlement_cohorts(device_id) == [None]
    assert await _generation_document_sections(device_id) == [{"static_route"}]

    fake = SrFake("sr-single-apply", service=[wire(B)])
    apply = await _run_apply_job(device_id, ordered[0][0], sr_client(fake))
    assert apply.status is JobStatus.succeeded
    assert await _projection_revisions(device_id) == (1, 1, 1)


async def test_the_tombstone_carries_its_removal_job_id(adapter_client):
    """The stamp the sweeper's predicate depends on: an owned tombstone is not orphaned."""
    device_id = await seed_device(nso_device_name="sr-stamp", netbox_device_id=9781)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    resp = await put_intent(
        adapter_client,
        device_id,
        [],
        deleted_routes=[{"route_id": 7, "triples": [entry(A)], "unverified": False}],
    )
    assert resp.status_code == 200

    jobs = await read_jobs(device_id)
    assert [j["job_type"] for j in jobs] == ["removal"]
    tombstones = await read_tombstones(device_id)
    assert [t["job_id"] for t in tombstones] == [jobs[0]["id"]]
    assert [t["marking"] for t in tombstones] == ["delete_origin"]


async def test_two_deleted_rows_share_the_one_removal_job(adapter_client):
    """One PUT-replace covers the whole scope, so both carriers point at the same job."""
    device_id = await seed_device(nso_device_name="sr-stamp-2", netbox_device_id=9782)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 7, "deployed_key": list(A)},
            {"triple": B, "route_id": 8, "deployed_key": list(B)},
        ],
    )

    assert (await put_intent(adapter_client, device_id, [])).status_code == 200

    jobs = await read_jobs(device_id)
    assert len(jobs) == 1
    assert {t["job_id"] for t in await read_tombstones(device_id)} == {jobs[0]["id"]}


async def test_the_removal_job_and_the_delete_roll_back_together(adapter_client, monkeypatch):
    """M7.3 — the delete, the tombstone and the removal job are one transaction.

    Against the pre-fold endpoint the deletes and the tombstone commit first and the job is
    created afterwards, so a failure in between leaves the row gone with no carrier job —
    exactly the lost deletion the tombstone exists to prevent.
    """
    device_id = await seed_device(nso_device_name="sr-m7-3", netbox_device_id=9783)
    ids = await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])
    seen_removals = []

    async def _explode(*args, **kwargs):
        seen_removals.append((kwargs["marking"], kwargs["removed"]))
        raise RuntimeError("forced failure at the removal enqueue")

    monkeypatch.setattr("nso_adapter.core.removal.enqueue_removal", _explode)

    resp = await put_intent(
        adapter_client,
        device_id,
        [],
        deleted_routes=[{"route_id": 7, "triples": [entry(A)], "unverified": False}],
    )
    assert resp.status_code == 500, resp.text
    assert resp.json() == {"error": {"code": "internal", "message": "Internal server error", "detail": {}}}
    assert seen_removals == [("delete_origin", {"route": [A]})]

    assert [(r["id"], r["triple"]) for r in await read_intent(device_id)] == [(ids[A], A)]
    assert await read_tombstones(device_id) == []
    assert await read_jobs(device_id) == []


async def test_the_removal_job_keeps_its_scope_and_removed_keys(adapter_client):
    """The context the collateral guard reads must survive the move into the transaction."""
    device_id = await seed_device(nso_device_name="sr-ctx", netbox_device_id=9784)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    assert (await put_intent(adapter_client, device_id, [])).status_code == 200

    jobs = await read_jobs(device_id)
    assert len(jobs) == 1
    context = jobs[0]["context"]
    assert context["scope"] == "static_route"
    assert context["removed"] == {"route": [list(A)]}
    # Unmarked shrink = un-own, so the device is deliberately not touched.
    assert context["detach"] is True


async def test_a_store_only_shrink_still_creates_no_job_and_no_tombstone(adapter_client):
    """M4.1 — the fold must not smuggle a device write into the resync path."""
    device_id = await seed_device(nso_device_name="sr-so-fold", netbox_device_id=9785)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    assert (await put_intent(adapter_client, device_id, [], query="?store_only=true")).status_code == 200

    assert await read_intent(device_id) == []
    assert await read_tombstones(device_id) == []
    assert await read_jobs(device_id) == []
