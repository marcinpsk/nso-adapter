# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The heartbeat's two lanes, and the staleness reaper's entry points.

The lane is chosen LIVE on every tick. A provision acquires its claim mid-run, so a
heartbeat that captured the token when its task was created would keep using the
job-only lane and let the reaper revoke a perfectly healthy run.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa

from nso_adapter.core import worker as worker_mod
from nso_adapter.core.claim import ClaimRegistration, acquire_claim, claim_stale_cutoff
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio


async def _seed_running_job(device_id: int | None, job_type):
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        job = Job(job_type=job_type, device_id=device_id, status=JobStatus.running, context={})
        db.add(job)
        await db.commit()
        return job.id


async def _stamps(device_id: int, job_id: int):
    from nso_adapter.store.models import DeviceClaim, Job

    async with session() as db:
        claim = await db.get(DeviceClaim, device_id)
        job = await db.get(Job, job_id)
        return (claim.heartbeat_at if claim else None), job.heartbeat_at


async def _backdate_both(device_id: int, job_id: int, *, seconds: float) -> None:
    from nso_adapter.store.models import DeviceClaim, Job

    old = datetime.now(UTC) - timedelta(seconds=seconds)
    async with session() as db:
        await db.execute(sa.update(DeviceClaim).where(DeviceClaim.device_id == device_id).values(heartbeat_at=old))
        await db.execute(sa.update(Job).where(Job.id == job_id).values(heartbeat_at=old))
        await db.commit()


async def _one_tick(job_id: int, reg: ClaimRegistration | None, monkeypatch) -> None:
    """Run exactly one heartbeat tick, without waiting the real interval."""
    monkeypatch.setattr(worker_mod, "_HEARTBEAT_INTERVAL", 0.01)
    task = asyncio.create_task(worker_mod._heartbeat(job_id, reg))
    await asyncio.sleep(0.15)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_registered_tick_refreshes_both_rows(adapter_client, monkeypatch):
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="hb-both", netbox_device_id=9980)
    job_id = await _seed_running_job(device_id, JobType.sync)
    reg = await acquire_claim(device_id, "job", job_id=job_id)
    await _backdate_both(device_id, job_id, seconds=600)
    before_claim, before_job = await _stamps(device_id, job_id)

    await _one_tick(job_id, reg, monkeypatch)

    after_claim, after_job = await _stamps(device_id, job_id)
    assert after_claim > before_claim, "the claim heartbeat did not advance"
    assert after_job > before_job


async def test_unregistered_tick_refreshes_the_job_only(adapter_client, monkeypatch):
    """The claimless lane — today's behavior, unchanged."""
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="hb-jobonly", netbox_device_id=9981)
    job_id = await _seed_running_job(device_id, JobType.provision)
    # A claim exists but this run has NOT registered it: the tick must leave it alone.
    await acquire_claim(device_id, "job", job_id=job_id)
    await _backdate_both(device_id, job_id, seconds=600)
    before_claim, before_job = await _stamps(device_id, job_id)

    await _one_tick(job_id, ClaimRegistration(), monkeypatch)

    after_claim, after_job = await _stamps(device_id, job_id)
    assert after_claim == before_claim, "an unregistered run refreshed someone else's claim"
    assert after_job > before_job


async def test_lane_switches_live_after_registration(adapter_client, monkeypatch):
    """The registration is read every tick, not captured at task creation."""
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="hb-live", netbox_device_id=9982)
    job_id = await _seed_running_job(device_id, JobType.provision)
    reg = ClaimRegistration()

    monkeypatch.setattr(worker_mod, "_HEARTBEAT_INTERVAL", 0.01)
    task = asyncio.create_task(worker_mod._heartbeat(job_id, reg))
    try:
        await asyncio.sleep(0.1)  # ticks while unregistered
        acquired = await acquire_claim(device_id, "job", job_id=job_id)
        reg.register(acquired.device_id, acquired.token)
        await _backdate_both(device_id, job_id, seconds=600)
        before_claim, _ = await _stamps(device_id, job_id)

        await asyncio.sleep(0.15)  # ticks after registration
        after_claim, _ = await _stamps(device_id, job_id)
        assert after_claim > before_claim, "the heartbeat never switched to the claimed lane"
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_revoked_claim_stops_the_heartbeat_without_resurrecting_it(adapter_client, monkeypatch):
    """M6.23's second half: a zero-row claim lock ends the heartbeat.

    Re-inserting the row would hand the device back to a holder recovery has already
    replaced.
    """
    from nso_adapter.store.models import DeviceClaim, JobType

    device_id = await seed_device(nso_device_name="hb-revoked", netbox_device_id=9983)
    job_id = await _seed_running_job(device_id, JobType.sync)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    monkeypatch.setattr(worker_mod, "_HEARTBEAT_INTERVAL", 0.01)
    task = asyncio.create_task(worker_mod._heartbeat(job_id, reg))
    await asyncio.sleep(0.05)

    async with session() as db:
        await db.execute(sa.delete(DeviceClaim).where(DeviceClaim.device_id == device_id))
        await db.commit()

    # It must END on its own, not keep looping.
    await asyncio.wait_for(task, timeout=5)
    assert task.done() and not task.cancelled()

    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None


# ── the reaper's entry points ────────────────────────────────────────────────


async def test_start_workers_reaps_stale_claims_before_starting(adapter_client):
    """Revocation must happen before any worker exists, or the requeued job is stranded
    behind the worker's live-claim skip."""
    from nso_adapter.store.models import DeviceClaim, JobStatus, JobType

    device_id = await seed_device(nso_device_name="hb-startup", netbox_device_id=9984)
    job_id = await _seed_running_job(device_id, JobType.sync)
    await acquire_claim(device_id, "job", job_id=job_id)
    await _backdate_both(device_id, job_id, seconds=claim_stale_cutoff() + 60)

    await worker_mod.start_workers(concurrency=1)
    try:
        async with session() as db:
            assert await db.get(DeviceClaim, device_id) is None
            from nso_adapter.store.models import Job

            assert (await db.get(Job, job_id)).status is JobStatus.queued
    finally:
        await worker_mod.stop_workers()


async def test_periodic_tick_reaps_stale_claims(adapter_client):
    from nso_adapter.core.scheduler import _scheduled_orphan_reap
    from nso_adapter.store.models import DeviceClaim, JobType

    device_id = await seed_device(nso_device_name="hb-periodic", netbox_device_id=9985)
    job_id = await _seed_running_job(device_id, JobType.sync)
    await acquire_claim(device_id, "job", job_id=job_id)
    await _backdate_both(device_id, job_id, seconds=claim_stale_cutoff() + 60)

    await _scheduled_orphan_reap()

    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None
