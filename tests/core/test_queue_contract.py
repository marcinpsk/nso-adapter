# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Q1/Q4: multiplicity-safe active-job lookups and same-type queued dedupe.

The old single ``get_active_job`` asked "is anything active for this device?" with
``scalar_one_or_none()``, which is only sound if at most one row can ever match. Removal
jobs are deliberately exempt from queued uniqueness — ``enqueue_removal`` queues one per
scope and every one must run — so two scope pushes on one device already put two queued
rows in the table and the next lookup raised.

Splitting it three ways also makes the 409 answer correct: the job that caused a same-type
refusal is the only job the caller can usefully be told about.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _add_job(device_id: int, job_type, status, *, context=None):
    from nso_adapter.store.models import Job

    async with session() as db:
        job = Job(job_type=job_type, device_id=device_id, status=status, context=context or {})
        db.add(job)
        await db.commit()
        return job.id


# ── Q1: the three helpers ────────────────────────────────────────────────────


async def test_two_queued_removals_do_not_break_admission(adapter_client):
    """Q1's headline pin. Today ``get_active_job`` raises MultipleResultsFound here.

    Two scope pushes on one device legitimately queue two removals, and every caller of the
    old helper then blew up — the API 409 path, the failover gate and the apply admission.
    """
    from nso_adapter.core.jobs import get_head_queued_job, get_queued_job_of_type, has_any_active_job
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q1-two-removals", netbox_device_id=9700)
    first = await _add_job(device_id, JobType.removal, JobStatus.queued, context={"scope": "static_route"})
    second = await _add_job(device_id, JobType.removal, JobStatus.queued, context={"scope": "bgp"})

    async with session() as db:
        assert await has_any_active_job(device_id, db) is True
        # No exception, and the FIFO head is the older of the two.
        assert (await get_head_queued_job(device_id, db)).id == first
        # No queued *sync* exists, so a sync may be admitted.
        assert await get_queued_job_of_type(device_id, JobType.sync, db) is None
    assert second  # both really are in the table


async def test_queued_job_of_type_ignores_other_types(adapter_client):
    from nso_adapter.core.jobs import get_queued_job_of_type
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q1-of-type", netbox_device_id=9701)
    await _add_job(device_id, JobType.removal, JobStatus.queued)
    sync_id = await _add_job(device_id, JobType.sync, JobStatus.queued)

    async with session() as db:
        assert (await get_queued_job_of_type(device_id, JobType.sync, db)).id == sync_id
        assert await get_queued_job_of_type(device_id, JobType.detect_drift, db) is None


async def test_queued_job_of_type_ignores_running_and_terminal(adapter_client):
    """Only a QUEUED job blocks a same-type enqueue; the index predicate says so too."""
    from nso_adapter.core.jobs import get_queued_job_of_type, has_any_active_job
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q1-running", netbox_device_id=9702)
    await _add_job(device_id, JobType.sync, JobStatus.running)
    await _add_job(device_id, JobType.apply, JobStatus.succeeded)

    async with session() as db:
        assert await get_queued_job_of_type(device_id, JobType.sync, db) is None
        # ...but the device is still busy, which is what the cheap pre-filter reports.
        assert await has_any_active_job(device_id, db) is True


async def test_has_any_active_job_is_false_when_only_terminal(adapter_client):
    from nso_adapter.core.jobs import has_any_active_job
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q1-idle", netbox_device_id=9703)
    await _add_job(device_id, JobType.sync, JobStatus.succeeded)
    await _add_job(device_id, JobType.apply, JobStatus.failed)

    async with session() as db:
        assert await has_any_active_job(device_id, db) is False


async def test_head_queued_job_is_fifo(adapter_client):
    from nso_adapter.core.jobs import get_head_queued_job
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q1-fifo", netbox_device_id=9704)
    first = await _add_job(device_id, JobType.removal, JobStatus.queued)
    await _add_job(device_id, JobType.sync, JobStatus.queued)

    async with session() as db:
        assert (await get_head_queued_job(device_id, db)).id == first


async def test_get_active_job_is_gone(adapter_client):
    """The unsafe helper must not survive as a tempting shortcut."""
    from nso_adapter.core import jobs

    assert not hasattr(jobs, "get_active_job")


# ── the 409 surface ──────────────────────────────────────────────────────────


async def test_409_names_the_conflicting_job_of_that_type(adapter_client):
    """B7 — with an older queued removal present, a duplicate sync must name the SYNC.

    Today the lookup either raises or reports whichever single row it happened to find.
    """
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q1-409", netbox_device_id=9705)
    # POST /actions/sync enqueues sync_now, so THAT is the type the 409 must name.
    removal_id = await _add_job(device_id, JobType.removal, JobStatus.queued, context={"scope": "bgp"})
    sync_now_id = await _add_job(device_id, JobType.sync_now, JobStatus.queued)

    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/sync", headers=AUTH)
    assert resp.status_code == 409, resp.text
    detail = resp.json()["error"]["detail"]
    assert detail["job_id"] == sync_now_id, f"named {detail['job_id']}, not the queued sync_now {sync_now_id}"
    assert detail["job_id"] != removal_id


async def test_a_different_type_is_admitted_alongside_a_queued_removal(adapter_client):
    """A queued removal must not 409 an unrelated operator action."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q1-admit", netbox_device_id=9706)
    await _add_job(device_id, JobType.removal, JobStatus.queued, context={"scope": "bgp"})

    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/sync", headers=AUTH)
    assert resp.status_code == 202, resp.text


# ── Q2's admission narrowing ─────────────────────────────────────────────────


async def test_apply_admitted_while_removal_queued(adapter_client):
    """Q2 — today ``enqueue_apply`` rejects on ANY active job, removals included.

    That is the bug the queue contract exists to remove: a removal is enqueued BEFORE its
    apply by design, so letting it block the apply drops the apply entirely.
    """
    from nso_adapter.core.apply import enqueue_apply
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q2-apply", netbox_device_id=9710)
    await _add_job(device_id, JobType.removal, JobStatus.queued, context={"scope": "static_route"})

    async with session() as db:
        job = await enqueue_apply(db, device_id, force=True)
        await db.commit()
    assert job is not None, "a queued removal blocked the apply"


async def test_second_queued_apply_is_refused(adapter_client):
    from nso_adapter.core.apply import enqueue_apply
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q2-dupe", netbox_device_id=9711)
    await _add_job(device_id, JobType.apply, JobStatus.queued)

    async with session() as db:
        assert await enqueue_apply(db, device_id, force=True) is None


# ── Q4: the index enforces it at the database ────────────────────────────────


async def test_two_queued_applies_are_rejected_by_the_database(adapter_client):
    """The uniqueness is the DB's, not the application's check-then-insert."""
    from sqlalchemy.exc import IntegrityError

    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q4-two-applies", netbox_device_id=9720)
    await _add_job(device_id, JobType.apply, JobStatus.queued)

    async with session() as db:
        db.add(Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.queued, context={}))
        try:
            await db.commit()
        except IntegrityError:
            return
    raise AssertionError("two queued applies coexisted for one device")


async def test_two_queued_removals_are_admitted_by_the_database(adapter_client):
    """Removals stay exempt: one per scope, and every one must run. FIFO ordering comes
    from the worker's head claim, not from uniqueness."""
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q4-two-removals", netbox_device_id=9721)
    async with session() as db:
        db.add_all(
            [
                Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.queued, context={"scope": "bgp"}),
                Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.queued, context={"scope": "isis"}),
            ]
        )
        await db.commit()  # must not raise


async def test_a_queued_apply_coexists_with_a_running_one(adapter_client):
    """The predicate is ``status = 'queued'``: a successor may queue while one runs, and
    execution is serialized by the device claim rather than by admission."""
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="q4-succ", netbox_device_id=9722)
    await _add_job(device_id, JobType.apply, JobStatus.running)

    async with session() as db:
        db.add(Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.queued, context={}))
        await db.commit()  # must not raise


# ── the failover gate keeps working (N2) ─────────────────────────────────────


async def test_failover_gate_survives_multiple_removals(adapter_client):
    """N2 — the scheduler's gate called the multiplicity-unsafe helper directly.

    Only the boolean pre-filter belongs there; the full claim-based gate arrives with the
    failover restructure.
    """
    from nso_adapter.core.jobs import has_any_active_job
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="q1-failover", netbox_device_id=9730)
    await _add_job(device_id, JobType.removal, JobStatus.queued, context={"scope": "bgp"})
    await _add_job(device_id, JobType.removal, JobStatus.queued, context={"scope": "isis"})

    async with session() as db:
        assert await has_any_active_job(device_id, db) is True
