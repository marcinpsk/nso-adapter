# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Appendix S, chunk S2 (P0.8): every terminal writer allocates, site by site.

Sixteen physical terminal writes across six modules were the failure mode. One of them
forgetting the sequence produces a terminal job with ``settle_seq IS NULL`` — permanently
invisible to settlement, and silently, because nothing else about the row looks wrong.

``tests/test_no_direct_terminal_write.py`` is the mechanical half: it fails the build on a
seventeenth writer. This is the behavioral half: each of the sixteen sites in §2.2's
inventory that the schema permits is driven through its real production path. The detached
non-provision CHECK makes the claimless-corruption guard unreachable. Offboard's bulk write
is the remaining exempt site because it is device-less by construction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import sqlalchemy as sa

from nso_adapter.core import jobs as jobs_mod
from nso_adapter.core import worker as worker_mod
from nso_adapter.core.claim import ClaimRegistration, acquire_claim, release_claim
from nso_adapter.store.models import DeviceSettleCounter, Job, JobStatus, JobType
from tests.conftest import seed_device, session
from tests.core.test_jobs import _nso_client_for_connect
from tests.core.test_static_route_put import A, B, wire
from tests.core.test_static_route_removal import SrFake, run_removal_job, seed_removal_job, seed_tomb, sr_client

pytestmark = pytest.mark.anyio


# ── shared seeding ───────────────────────────────────────────────────────────


async def _running_job(device_id: int | None, job_type: JobType, *, context: dict | None = None) -> int:
    """A job as the worker head leaves it: started, at attempt 1."""
    async with session() as db:
        job = Job(
            job_type=job_type,
            device_id=device_id,
            status=JobStatus.running,
            coalescible=job_type not in (JobType.removal, JobType.provision),
            run_attempt=1,
            context=context,
        )
        db.add(job)
        await db.commit()
        return job.id


async def _queued_job(device_id: int | None, job_type: JobType) -> int:
    async with session() as db:
        job = Job(
            job_type=job_type,
            device_id=device_id,
            status=JobStatus.queued,
            coalescible=job_type not in (JobType.removal, JobType.provision),
        )
        db.add(job)
        await db.commit()
        return job.id


async def _job_row(job_id: int) -> Job:
    async with session() as db:
        return await db.get(Job, job_id)


async def _last_seq(device_id: int) -> int | None:
    async with session() as db:
        return await db.scalar(
            sa.select(DeviceSettleCounter.last_seq).where(DeviceSettleCounter.device_id == device_id)
        )


async def _claim_for(device_id: int, job_id: int) -> ClaimRegistration:
    reg = await acquire_claim(device_id, "job", job_id=job_id)
    assert reg is not None
    reg.run_attempt = 1
    return reg


# ── the sixteen drivers, each through its real production path ───────────────


async def _t1_mark_job_failed() -> tuple[int, int]:
    """``core/jobs.py`` ``_mark_job_failed``, reached by a runner body that raises."""
    device_id = await seed_device(nso_device_name="inv-t1", netbox_device_id=8301)
    job_id = await _running_job(device_id, JobType.sync)

    async def _boom(_device_id, _db):
        raise RuntimeError("the runner body blew up")

    await jobs_mod._run_with_db(job_id, device_id, _boom, reg=ClaimRegistration(run_attempt=1))
    return device_id, job_id


async def _t2_run_with_db_success() -> tuple[int, int]:
    """``core/jobs.py`` ``_run_with_db`` success — sync, sync_now, sync_from_nso, detect_drift."""
    device_id = await seed_device(nso_device_name="inv-t2", netbox_device_id=8302)
    job_id = await _running_job(device_id, JobType.sync)

    async def _ok(_device_id, _db):
        return {"synced": True}

    await jobs_mod._run_with_db(job_id, device_id, _ok, reg=ClaimRegistration(run_attempt=1))
    return device_id, job_id


async def _t3_run_connect_success() -> tuple[int, int]:
    """``core/jobs.py`` ``_run_connect`` success, through the real connect action."""
    device_id = await seed_device(nso_device_name="inv-t3", netbox_device_id=8303)
    job_id = await _running_job(device_id, JobType.connect)
    client = _nso_client_for_connect({"result": "connected"})
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        await jobs_mod._run_connect(job_id, device_id, ClaimRegistration(run_attempt=1))
    return device_id, job_id


async def _t4_run_provision_success() -> tuple[int, int]:
    """``core/jobs.py`` ``_run_provision`` success — the only writer that SETS a device_id."""
    device_id = await seed_device(nso_device_name="inv-t4", netbox_device_id=8304)
    job_id = await _running_job(None, JobType.provision, context={"nso_instance": "nso-dev", "device_name": "inv-t4"})
    reg = await _claim_for(device_id, job_id)

    async def _ok_provision(_db, **_params):
        return {"ok": True, "steps": [], "device_id": device_id}

    try:
        with patch("nso_adapter.core.onboarding.provision_nso_device", _ok_provision):
            await jobs_mod._run_provision(job_id, None, reg)
    finally:
        await release_claim(reg)
    return device_id, job_id


async def _t6_worker_mark_failed() -> tuple[int, int]:
    """``core/worker.py`` ``_mark_failed`` — the worker-machinery fault path."""
    device_id = await seed_device(nso_device_name="inv-t6", netbox_device_id=8306)
    job_id = await _running_job(device_id, JobType.sync)
    reg = await _claim_for(device_id, job_id)
    try:
        await worker_mod._mark_failed(job_id, "internal", "machinery fault", reg)
    finally:
        await release_claim(reg)
    return device_id, job_id


async def _t7_removal_residue_found() -> tuple[int, int]:
    """``core/removal.py`` the residue-found failure of ``_finalize_static_route_removal``."""
    device_id = await seed_device(nso_device_name="inv-t7", netbox_device_id=8307)
    fake = SrFake("inv-t7", service=None, device=[wire(A)])
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)
    job = await run_removal_job(device_id, job_id, sr_client(fake))
    assert job.result["residue_check"] == "found"
    return device_id, job_id


async def _t8_removal_proven() -> tuple[int, int]:
    """``core/removal.py`` the proven / no-carrier success of the same finalizer."""
    device_id = await seed_device(nso_device_name="inv-t8", netbox_device_id=8308)
    fake = SrFake("inv-t8", service=[wire(B)], device=[wire(B)])
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)
    job = await run_removal_job(device_id, job_id, sr_client(fake))
    assert job.result["residue_check"] == "clean"
    return device_id, job_id


async def _t9_removal_unproven_with_carrier() -> tuple[int, int]:
    """``core/removal.py`` the unproven-with-carrier failure of the same finalizer."""
    device_id = await seed_device(nso_device_name="inv-t9", netbox_device_id=8309)
    fake = SrFake("inv-t9", service=[wire(A), wire(B)], section_status="unsupported")
    job_id = await seed_removal_job(device_id, {})
    await seed_tomb(device_id, A, job_id=job_id, route_id=1)
    job = await run_removal_job(device_id, job_id, sr_client(fake))
    assert job.error["code"] == "static_route_removal_unproven"
    return device_id, job_id


async def _t10_removal_generic_success() -> tuple[int, int]:
    """``core/removal.py`` ``run_removal``'s generic-scope success."""
    from nso_adapter.core.removal import run_removal

    device_id = await seed_device(nso_device_name="inv-t10", netbox_device_id=8310)
    job_id = await _running_job(device_id, JobType.removal, context={"scope": "vlan"})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=AsyncMock()),
        patch("nso_adapter.core.removal._dispatch_scope", new=AsyncMock()),
    ):
        await run_removal(job_id=job_id, device_id=device_id, reg=ClaimRegistration(run_attempt=1))
    return device_id, job_id


async def _apply_job(device_id: int) -> int:
    return await _running_job(device_id, JobType.apply)


async def _t11_apply_nothing_eligible() -> tuple[int, int]:
    """``core/apply.py`` ``_finalize_job``'s nothing-eligible early return."""
    from nso_adapter.core.apply import run_apply

    device_id = await seed_device(nso_device_name="inv-t11", netbox_device_id=8311)
    job_id = await _apply_job(device_id)
    reg = await _claim_for(device_id, job_id)
    try:
        with (
            patch("nso_adapter.core.importer.get_nso_client", return_value=AsyncMock()),
            patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()),
        ):
            await run_apply(job_id=job_id, device_id=device_id, force=True, reg=reg)
    finally:
        await release_claim(reg)
    job = await _job_row(job_id)
    assert job.result["static_route_count_by_outcome"] == {"in_sync": 0, "apply_failed": 0}, (
        "T11 no longer drives the nothing-eligible early return"
    )
    return device_id, job_id


async def _seed_accepted_static_route(device_id: int) -> None:
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, prefix="10.7.0.0/24", next_hop="10.7.0.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()


async def _t12_apply_all_ok() -> tuple[int, int]:
    """``core/apply.py`` ``_finalize_job``'s all-ok success — the writer carrying the route results."""
    from nso_adapter.core.apply import run_apply

    device_id = await seed_device(nso_device_name="inv-t12", netbox_device_id=8312)
    job_id = await _apply_job(device_id)
    await _seed_accepted_static_route(device_id)
    reg = await _claim_for(device_id, job_id)
    try:
        with (
            patch("nso_adapter.core.importer.get_nso_client", return_value=AsyncMock()),
            patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()),
            patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock),
        ):
            await run_apply(job_id=job_id, device_id=device_id, force=True, reg=reg)
    finally:
        await release_claim(reg)
    job = await _job_row(job_id)
    assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
    return device_id, job_id


async def _t13_apply_any_failed() -> tuple[int, int]:
    """``core/apply.py`` ``_finalize_job``'s any-failed branch."""
    from nso_adapter.core.apply import run_apply
    from nso_adapter.nso.apply import NsoApplyError

    device_id = await seed_device(nso_device_name="inv-t13", netbox_device_id=8313)
    job_id = await _apply_job(device_id)
    await _seed_accepted_static_route(device_id)
    err = NsoApplyError(code="nso_error", message="route rejected", detail={})
    reg = await _claim_for(device_id, job_id)
    try:
        with (
            patch("nso_adapter.core.importer.get_nso_client", return_value=AsyncMock()),
            patch("nso_adapter.core.apply._post_apply_refresh_and_notify", new=AsyncMock()),
            patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock, side_effect=err),
        ):
            await run_apply(job_id=job_id, device_id=device_id, force=True, reg=reg)
    finally:
        await release_claim(reg)
    job = await _job_row(job_id)
    assert job.result["static_route_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
    return device_id, job_id


async def _t14_run_apply_fallback() -> tuple[int, int]:
    """``core/apply.py`` ``run_apply``'s generic except, after the rollback and re-fetch."""
    from nso_adapter.core.apply import run_apply

    device_id = await seed_device(nso_device_name="inv-t14", netbox_device_id=8314)
    job_id = await _apply_job(device_id)
    reg = await _claim_for(device_id, job_id)
    try:
        # The device the apply is pointed at does not exist, so _execute_apply raises out.
        with patch("nso_adapter.core.importer.get_nso_client", side_effect=KeyError("nso-dev")):
            await run_apply(job_id=job_id, device_id=99998, force=True, reg=reg)
    finally:
        await release_claim(reg)
    job = await _job_row(job_id)
    assert job.error["code"] == "internal"
    return device_id, job_id


async def _t15_recovery_terminalize_running() -> tuple[int, int]:
    """``core/claim.py`` ``terminalize_running``, through the real claimless recovery clock."""
    from datetime import timedelta

    device_id = await seed_device(nso_device_name="inv-t15", netbox_device_id=8315)
    job_id = await _running_job(device_id, JobType.apply)  # disposition_for(apply) is terminal
    async with session() as db:
        await db.execute(
            sa.update(Job)
            .where(Job.id == job_id)
            .values(heartbeat_at=datetime.now(UTC) - timedelta(seconds=worker_mod.PROVISION_STALE_AFTER + 600))
        )
        await db.commit()
    await worker_mod.requeue_orphaned_jobs()
    return device_id, job_id


async def _t16_offboard_bulk() -> tuple[None, int]:
    """``core/onboarding.py`` offboard's bulk write — EXEMPT: the device and its jobs part ways."""
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="inv-t16", netbox_device_id=8316)
    job_id = await _queued_job(device_id, JobType.sync)
    async with session() as db:
        await offboard_device(db, await db.get(Device, device_id))
    return None, job_id


@dataclass(frozen=True)
class Site:
    """One row of §2.2's inventory: where it writes, what it writes, and whether it sequences."""

    driver: Any
    status: JobStatus
    sequenced: bool


_INVENTORY = {
    "T1": Site(_t1_mark_job_failed, JobStatus.failed, True),
    "T2": Site(_t2_run_with_db_success, JobStatus.succeeded, True),
    "T3": Site(_t3_run_connect_success, JobStatus.succeeded, True),
    "T4": Site(_t4_run_provision_success, JobStatus.succeeded, True),
    "T6": Site(_t6_worker_mark_failed, JobStatus.failed, True),
    "T7": Site(_t7_removal_residue_found, JobStatus.failed, True),
    "T8": Site(_t8_removal_proven, JobStatus.succeeded, True),
    "T9": Site(_t9_removal_unproven_with_carrier, JobStatus.failed, True),
    "T10": Site(_t10_removal_generic_success, JobStatus.succeeded, True),
    "T11": Site(_t11_apply_nothing_eligible, JobStatus.succeeded, True),
    "T12": Site(_t12_apply_all_ok, JobStatus.succeeded, True),
    "T13": Site(_t13_apply_any_failed, JobStatus.failed, True),
    "T14": Site(_t14_run_apply_fallback, JobStatus.failed, True),
    "T15": Site(_t15_recovery_terminalize_running, JobStatus.failed, True),
    "T16": Site(_t16_offboard_bulk, JobStatus.failed, False),
}


@pytest.mark.parametrize("site_id", list(_INVENTORY))
async def test_every_terminal_writer_allocates(adapter_client, site_id):
    """S2.3 (P0.8) — all 14 device-bound sites allocate; the device-less site does not.

    Forbidden: any site producing a terminal, device-bound job with ``settle_seq IS NULL``.
    Such a job is permanently invisible to a settlement consumer, and nothing about the row
    says so.
    """
    site = _INVENTORY[site_id]
    device_id, job_id = await site.driver()

    job = await _job_row(job_id)
    assert job.status is site.status, f"{site_id}: wrong terminal status"
    if site.sequenced:
        assert job.device_id == device_id
        assert job.settle_seq == 1, f"{site_id}: a device-bound terminal job took no settlement sequence"
        assert await _last_seq(device_id) == 1, f"{site_id}: the job was sequenced but the counter did not move"
    else:
        assert job.device_id is None, f"{site_id}: an exempt site is exempt only because it has no device"
        assert job.settle_seq is None, f"{site_id}: a device-less job took a sequence"


# ── S2.4 / S2.4b / S2.4c / S2.5: the four allocation edge cases ──────────────


async def test_provision_success_sequences_against_the_device_it_created(adapter_client):
    """S2.4 — the sequence is allocated against the device the SAME statement attached.

    Forbidden: allocating against the pre-write NULL, which leaves the job unsequenced on
    the very device it just created — permanently invisible to that device's feed.
    """
    device_id, job_id = await _t4_run_provision_success()

    job = await _job_row(job_id)
    assert job.status is JobStatus.succeeded
    assert job.device_id == device_id, "the provision did not attach the device it created"
    assert job.settle_seq == 1
    assert await _last_seq(device_id) == 1


async def test_a_superseded_requeue_that_lands_failed_allocates_once(adapter_client):
    """S2.4b — the RETURNED status decides, not the requested one.

    ``terminalize_running(status=queued)`` is coerced to ``failed``/``superseded`` when a
    queued same-type successor already occupies the uniqueness slot (S6). Forbidden: reading
    the requested ``queued`` and skipping the allocation for a write that landed terminal —
    or allocating for a requeue that stayed ``queued``, which is not a terminal write at all.
    """
    from nso_adapter.core.claim import terminalize_running
    from nso_adapter.core.jobs import admit_coalescible_job
    from tests.core.test_settle_token import _queue, _start_run

    device_id = await seed_device(nso_device_name="inv-superseded", netbox_device_id=8320)
    job_id = await _queue(device_id, JobType.sync)
    _jid, _dev, _jt, reg = await _start_run(device_id, job_id)

    async with session() as db:
        created, winner = await admit_coalescible_job(db, device_id, JobType.sync)
        await db.commit()
        successor_id = (created or winner).id

    async with session() as db:
        landed = await terminalize_running(db, job_id, status=JobStatus.queued, expected_attempt=reg.run_attempt)
        await db.commit()

    assert landed is JobStatus.failed, "the coercion no longer happens; the pin no longer drives allocation"
    job = await _job_row(job_id)
    assert job.error["code"] == "superseded"
    assert job.settle_seq == 1, "the coerced terminal write took no sequence"
    assert await _last_seq(device_id) == 1, "exactly one sequence, allocated once"
    assert (await _job_row(successor_id)).settle_seq is None, "the queued successor is not terminal"


async def test_a_rejected_recovery_write_allocates_nothing(adapter_client, monkeypatch, rival_engine):
    """S2.4c — S1.2b's delayed recovery actor, now with the counter present.

    A recovery actor that selected its candidate, paused, and resumed into a world where the
    job has been requeued, restarted and given a queued successor has its *requested*
    ``queued`` coerced to a terminal ``failed``. Forbidden: that refused write allocating a
    sequence for a run that never reported.

    The legitimate half is S2.4b: the same coercion, against its own attempt, allocates
    exactly one.
    """
    from datetime import timedelta

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from nso_adapter.core.jobs import admit_coalescible_job
    from tests.core.test_settle_token import _queue, _recover, _start_run

    device_id = await seed_device(nso_device_name="inv-delayed", netbox_device_id=8321)
    job_id = await _queue(device_id, JobType.sync)

    # Six completed start/requeue cycles, so the run recovery observes is attempt 7.
    for _ in range(6):
        await _start_run(device_id, job_id)
        await _recover(device_id)
    await _start_run(device_id, job_id)

    # Uncovered and stale: the claimless recovery clock's candidate set.
    async with session() as db:
        await db.execute(sa.text("DELETE FROM device_claim WHERE device_id = :d"), {"d": device_id})
        await db.execute(
            sa.update(Job)
            .where(Job.id == job_id)
            .values(heartbeat_at=datetime.now(UTC) - timedelta(seconds=worker_mod.PROVISION_STALE_AFTER + 600))
        )
        await db.commit()

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    fired: list[bool] = []
    real_terminalize_running = worker_mod.terminalize_running

    async def _interleaved(db, jid, **kwargs):
        if not fired:
            fired.append(True)
            # Recovery B requeues it, a worker starts the next attempt, and admission
            # commits a queued same-type successor — so the resumed actor's `queued` is
            # coerced to a TERMINAL failed, which is what would allocate.
            async with rival() as other:
                await other.execute(
                    sa.update(Job)
                    .where(Job.id == jid)
                    .values(status=JobStatus.queued, started_at=None, heartbeat_at=None)
                )
                await other.commit()
            await _start_run(device_id, jid)
            async with rival() as other:
                await admit_coalescible_job(other, device_id, JobType.sync)
                await other.commit()
        return await real_terminalize_running(db, jid, **kwargs)

    monkeypatch.setattr(worker_mod, "terminalize_running", _interleaved)
    await worker_mod.requeue_orphaned_jobs()

    assert fired, "the barrier never ran — the interleave did not happen"
    assert (await _job_row(job_id)).status is JobStatus.running, "the delayed actor terminalized a live run"
    async with session() as db:
        sequenced = (await db.execute(sa.select(Job.id).where(Job.settle_seq.is_not(None)))).scalars().all()
    assert sequenced == [], "the rejected recovery write allocated a sequence"
    assert await _last_seq(device_id) == 0, "last_seq moved for a write that was refused"


async def test_device_null_provision_failure_is_exempt(adapter_client):
    """S2.5 — a provision that fails before acquiring a device remains unsequenced.

    It is terminal and device-less. A device-scoped cursor cannot reach it, and there is no
    counter to allocate from.
    """
    job_id = await _running_job(None, JobType.provision, context={"nso_instance": "nso-dev", "device_name": "inv-fail"})

    async def _boom_provision(_db, **_params):
        raise RuntimeError("nso unreachable")

    with patch("nso_adapter.core.onboarding.provision_nso_device", _boom_provision):
        await jobs_mod._run_provision(job_id, None, ClaimRegistration(run_attempt=1))

    failed_provision = await _job_row(job_id)
    assert failed_provision.status is JobStatus.failed
    assert failed_provision.device_id is None
    assert failed_provision.settle_seq is None
