# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Appendix S, chunk S1: the run-attempt token and the single terminal writer.

A status-only predicate cannot identify the execution that owns a terminal write.
Recovery requeues ``running -> queued`` and a successor re-enters ``running``, so
``queued|running`` suppresses the mandated rerun and ``running``-only clobbers the
successor. Every terminal write therefore names the execution it belongs to.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.core import jobs as jobs_mod
from nso_adapter.core import worker as worker_mod
from nso_adapter.core.claim import claim_stale_cutoff, release_claim, revoke_stale_claims
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio


async def _queue(device_id: int | None, job_type, *, context=None) -> int:
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        job = Job(job_type=job_type, device_id=device_id, status=JobStatus.queued, context=context or {})
        db.add(job)
        await db.commit()
        return job.id


async def _row(job_id: int):
    from nso_adapter.store.models import Job

    async with session() as db:
        return await db.get(Job, job_id)


async def _status(job_id: int):
    return (await _row(job_id)).status


async def _attempt(job_id: int) -> int:
    return (await _row(job_id)).run_attempt


async def _age_claim(device_id: int) -> None:
    """Push the claim's heartbeat past the stale cutoff so recovery may revoke it."""
    from nso_adapter.store.models import DeviceClaim

    async with session() as db:
        await db.execute(
            sa.update(DeviceClaim)
            .where(DeviceClaim.device_id == device_id)
            .values(heartbeat_at=datetime.now(UTC) - timedelta(seconds=claim_stale_cutoff() + 60))
        )
        await db.commit()


async def _ok_sync(_device_id: int, _db) -> dict:
    """Stands in for ``importer.sync_device``: a runner body that succeeds."""
    return {"synced": True}


async def _start_run(device_id: int, job_id: int):
    """Drive the real worker head: claim the device and start its queued head."""
    claimed = await worker_mod._claim_next_job()
    assert claimed is not None, "the worker did not start the queued head"
    assert claimed[0] == job_id
    return claimed


async def _recover(device_id: int) -> None:
    """Age the claim and let the real reaper revoke it and re-disposition the job."""
    await _age_claim(device_id)
    revoked = await revoke_stale_claims()
    assert [r.device_id for r in revoked] == [device_id]


async def _running_generation(device_id: int, job_id: int) -> int:
    """Give *job_id* the running generation an executing write carries. Returns its id."""
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode, GenerationStatus

    async with session() as db:
        generation = DeploymentGeneration(
            device_id=device_id,
            seq=1,
            mode=GenerationMode.networked,
            status=GenerationStatus.running,
            document={},
            digest="0" * 64,
            allowed_removal_keys={},
            source_push_seq={},
            stream_revisions={},
            job_id=job_id,
        )
        db.add(generation)
        await db.commit()
        return generation.id


# ── S1.1 / S1.2 (P0.7): an abandoned runner may not write terminal ───────────


async def test_abandoned_runner_cannot_terminalize_a_requeued_job(adapter_client, monkeypatch):
    """S1.1 — recovery requeued the job; the abandoned runner's write must not land.

    Forbidden: the write landing, which suppresses the rerun recovery mandated.
    """
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="s1-requeued", netbox_device_id=9901)
    job_id = await _queue(device_id, JobType.sync)

    _jid, _dev, _jt, reg = await _start_run(device_id, job_id)
    await _recover(device_id)
    assert await _status(job_id) is JobStatus.queued, "recovery did not requeue the job"

    monkeypatch.setattr("nso_adapter.core.importer.sync_device", _ok_sync)
    await jobs_mod._run_sync(job_id, device_id, reg)

    assert await _status(job_id) is JobStatus.queued, "the abandoned runner suppressed the mandated rerun"


async def test_abandoned_runner_cannot_terminalize_a_successors_run(adapter_client, monkeypatch):
    """S1.2 — a successor has already re-entered ``running``; only its own write may land."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="s1-successor", netbox_device_id=9902)
    job_id = await _queue(device_id, JobType.sync)

    _jid, _dev, _jt, abandoned = await _start_run(device_id, job_id)
    await _recover(device_id)
    _jid, _dev, _jt, successor = await _start_run(device_id, job_id)
    assert await _status(job_id) is JobStatus.running

    monkeypatch.setattr("nso_adapter.core.importer.sync_device", _ok_sync)
    await jobs_mod._run_sync(job_id, device_id, abandoned)
    assert await _status(job_id) is JobStatus.running, "the abandoned runner clobbered the successor's run"

    await jobs_mod._run_sync(job_id, device_id, successor)
    assert await _status(job_id) is JobStatus.succeeded, "the successor's own write was refused"


# ── S1.2b / S1.2c (r1-B2): a DELAYED RECOVERY ACTOR is bound by the token too ─


async def test_delayed_recovery_cannot_terminalize_a_successor_run(adapter_client, monkeypatch, rival_engine):
    """S1.2b — recovery selects a candidate, the world moves on, recovery resumes.

    ``requeue_orphaned_jobs`` reads its candidates without a row lock and terminalizes
    them statements later, and the claimless lane has no claim barrier. Between the two,
    another recovery requeues the job, a worker starts the next attempt and admission
    commits a queued same-type successor — so the resumed actor's *requested* ``queued``
    is coerced to ``failed``/``superseded`` (S6) and a status-only CAS would land it on
    an execution that never reported.

    S1.2 does not cover this: it drives a stale RUNNER, not a stale recovery actor.
    """
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="s1-delayed", netbox_device_id=9903)
    job_id = await _queue(device_id, JobType.sync)

    # Six completed start/requeue cycles, so the run recovery observes is attempt 7.
    for _ in range(6):
        await _start_run(device_id, job_id)
        await _recover(device_id)
    _jid, _dev, _jt, _reg = await _start_run(device_id, job_id)

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
    successor_ids: list[int] = []
    fired: list[bool] = []
    real_terminalize = worker_mod.terminalize_running

    async def _interleaved(db, jid, **kwargs):
        if not fired:
            fired.append(True)
            # Recovery B requeues it …
            async with rival() as other:
                await other.execute(
                    sa.update(Job)
                    .where(Job.id == jid)
                    .values(status=JobStatus.queued, started_at=None, heartbeat_at=None)
                )
                await other.commit()
            # … a worker starts the next attempt …
            await _start_run(device_id, jid)
            # … and admission commits a queued same-type successor.
            async with rival() as other:
                created, winner = await admit_queued_job(other, device_id, JobType.sync)
                await other.commit()
                successor_ids.append((created or winner).id)
        return await real_terminalize(db, jid, **kwargs)

    monkeypatch.setattr(worker_mod, "terminalize_running", _interleaved)
    await worker_mod.requeue_orphaned_jobs()

    assert fired, "the barrier never ran — the interleave did not happen"
    assert await _status(job_id) is JobStatus.running, "the delayed recovery terminalized a run that never reported"
    assert await _attempt(job_id) == 8
    assert await _status(successor_ids[0]) is JobStatus.queued


async def test_a_cancelled_runs_requeue_cannot_touch_a_successor(adapter_client):
    """S1.2c — the same delayed-actor shape through ``_requeue_own_claim``.

    The worker's own cancelled-run requeue knows the attempt it started, so it must not
    return a SUCCESSOR's run to the queue.
    """
    from nso_adapter.store.models import Job, JobStatus, JobType

    await seed_device(nso_device_name="s1-cancelled", netbox_device_id=9904)
    job_id = await _queue(None, JobType.provision, context={"nso_instance": "nso-dev", "device_name": "s1-cancelled"})

    claimed = await worker_mod._claim_next_job()
    assert claimed is not None and claimed[0] == job_id
    cancelled_reg = claimed[3]

    # The claimless recovery clock requeues it, and a successor run starts.
    async with session() as db:
        await db.execute(
            sa.update(Job)
            .where(Job.id == job_id)
            .values(heartbeat_at=datetime.now(UTC) - timedelta(seconds=worker_mod.PROVISION_STALE_AFTER + 600))
        )
        await db.commit()
    await worker_mod.requeue_orphaned_jobs()
    assert await _status(job_id) is JobStatus.queued

    second = await worker_mod._claim_next_job()
    assert second is not None and second[0] == job_id
    assert await _status(job_id) is JobStatus.running

    # The FIRST run's cancellation disposition arrives late.
    await worker_mod._dispose(job_id, JobType.provision, cancelled_reg)

    assert await _status(job_id) is JobStatus.running, "a cancelled run requeued its successor's execution"
    assert await _attempt(job_id) == 2


async def test_a_device_busy_provision_cannot_terminalize_a_successor_run(adapter_client, monkeypatch):
    """S1.2c sibling — the provision lane's ``device_busy`` refusal is bound by the token.

    ``ClaimUnavailableError`` is raised by the acquisition itself, so the run is still
    CLAIMLESS and has no claim row to lock. Its registration is the only ownership proof
    it has, and a write that omits it is a status-only compare-and-set: an abandoned
    attempt whose mapping is refused would mark the SUCCESSOR's run ``failed``.
    """
    from nso_adapter.core.claim import ClaimUnavailableError
    from nso_adapter.store.models import Job, JobStatus, JobType

    await seed_device(nso_device_name="s1-busy", netbox_device_id=9910)
    job_id = await _queue(None, JobType.provision, context={"nso_instance": "nso-dev", "device_name": "s1-busy"})

    claimed = await worker_mod._claim_next_job()
    assert claimed is not None and claimed[0] == job_id
    abandoned_reg = claimed[3]

    async with session() as db:
        await db.execute(
            sa.update(Job)
            .where(Job.id == job_id)
            .values(heartbeat_at=datetime.now(UTC) - timedelta(seconds=worker_mod.PROVISION_STALE_AFTER + 600))
        )
        await db.commit()
    await worker_mod.requeue_orphaned_jobs()
    second = await worker_mod._claim_next_job()
    assert second is not None and second[0] == job_id
    assert await _status(job_id) is JobStatus.running

    async def _device_busy(_db, **_params):
        raise ClaimUnavailableError("device 1 is claimed by another operation")

    monkeypatch.setattr("nso_adapter.core.onboarding.provision_nso_device", _device_busy)
    await jobs_mod._run_provision(job_id, None, abandoned_reg)

    assert await _status(job_id) is JobStatus.running, "an abandoned provision failed its successor's execution"
    assert await _attempt(job_id) == 2


# ── S1.3 (P0.7): a rejected write changes NO column ──────────────────────────


async def test_a_rejected_terminal_write_changes_no_column(adapter_client, monkeypatch):
    """S1.3 — a stale unguarded runner writing over an already-terminal job.

    Forbidden: any column of the row changing — status, result or error. ``updated_at``
    is asserted too, because it is the witness that no UPDATE touched the row at all.
    """
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="s1-immutable", netbox_device_id=9905)
    job_id = await _queue(device_id, JobType.sync)

    _jid, _dev, _jt, reg = await _start_run(device_id, job_id)

    # The job reaches its own terminal state, with a distinctive payload.
    async with session() as db:
        await db.execute(
            sa.update(Job)
            .where(Job.id == job_id)
            .values(
                status=JobStatus.failed,
                result={"landed": "first"},
                error={"code": "first_writer", "message": "already terminal", "detail": {}},
            )
        )
        await db.commit()

    before = await _row(job_id)
    snapshot = (before.status, before.result, before.error, before.device_id, before.updated_at)

    monkeypatch.setattr("nso_adapter.core.importer.sync_device", _ok_sync)
    await jobs_mod._run_sync(job_id, device_id, reg)

    after = await _row(job_id)
    assert (after.status, after.result, after.error, after.device_id, after.updated_at) == snapshot


# ── S1.4 / S1.4b (S6, M7): the REQUESTED status does not decide ──────────────


async def test_a_superseded_requeue_returns_failed_not_queued(adapter_client):
    """S1.4 — a requeue that coalesces with a queued successor RETURNS ``failed``.

    The caller may never read its requested status: only the returned one is the truth.
    """
    from nso_adapter.core.claim import terminalize_running
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="s1-superseded", netbox_device_id=9906)
    job_id = await _queue(device_id, JobType.sync)
    _jid, _dev, _jt, reg = await _start_run(device_id, job_id)

    async with session() as db:
        created, winner = await admit_queued_job(db, device_id, JobType.sync)
        await db.commit()
        successor_id = (created or winner).id

    async with session() as db:
        landed = await terminalize_running(db, job_id, status=JobStatus.queued, expected_attempt=reg.run_attempt)
        await db.commit()

    assert landed is JobStatus.failed, "the requested status decided instead of the returned one"
    assert await _status(job_id) is JobStatus.failed
    assert (await _row(job_id)).error["code"] == "superseded"
    assert await _status(successor_id) is JobStatus.queued


async def test_a_superseded_requeue_abandons_its_generation(adapter_client):
    """The elected successor can cross a generation the stale run no longer owns."""
    from nso_adapter.core.claim import terminalize_running
    from nso_adapter.core.generation import job_admissible
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import DeploymentGeneration, GenerationStatus, JobStatus, JobType

    device_id = await seed_device(nso_device_name="s1-superseded-generation", netbox_device_id=9911)
    # Apply is device-WRITING. A sync is admissible before the barrier is ever consulted.
    job_id = await _queue(device_id, JobType.apply)
    _jid, _dev, _jt, reg = await _start_run(device_id, job_id)
    generation_id = await _running_generation(device_id, job_id)

    async with session() as db:
        created, winner = await admit_queued_job(db, device_id, JobType.apply)
        await db.commit()
        successor_id = (created or winner).id

    async with session() as db:
        landed = await terminalize_running(db, job_id, status=JobStatus.queued, expected_attempt=reg.run_attempt)
        await db.commit()

    async with session() as db:
        generation = await db.get(DeploymentGeneration, generation_id)
        assert landed is JobStatus.failed
        assert generation.status is GenerationStatus.abandoned
        assert await job_admissible(db, successor_id, device_id)


async def test_a_successor_inserted_mid_decision_lands_superseded(adapter_client, monkeypatch, rival_engine):
    """S1.4b (M7) — admission commits a successor BETWEEN the lookup and the UPDATE.

    Without the savepoint the requeue violates the partial unique index, the
    ``IntegrityError`` aborts the caller's whole transaction and recovery loses the
    entire batch. With it, the job lands ``failed``/``superseded`` and the caller's
    transaction stays usable.
    """
    from nso_adapter.core.claim import terminalize_running
    from nso_adapter.core.jobs import admit_queued_job
    from nso_adapter.store.models import DeploymentGeneration, GenerationStatus, Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="s1-midrace", netbox_device_id=9907)
    job_id = await _queue(device_id, JobType.apply)
    _jid, _dev, _jt, reg = await _start_run(device_id, job_id)
    generation_id = await _running_generation(device_id, job_id)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    fired: list[bool] = []

    async with session() as db:
        # The seam is the successor lookup: it is the only ``scalar`` the requeue issues,
        # and the UPDATE follows it directly. Firing here puts the committed successor
        # exactly in the window the savepoint has to absorb.
        real_scalar = db.scalar

        async def _barrier(statement, *args, **kwargs):
            out = await real_scalar(statement, *args, **kwargs)
            if not fired:
                fired.append(True)
                async with rival() as other:
                    await admit_queued_job(other, device_id, JobType.apply)
                    await other.commit()
            return out

        monkeypatch.setattr(db, "scalar", _barrier)
        landed = await terminalize_running(db, job_id, status=JobStatus.queued, expected_attempt=reg.run_attempt)
        monkeypatch.undo()
        # The caller's transaction must still be usable: recovery batches several jobs.
        assert await db.scalar(sa.select(sa.func.count()).select_from(Job)) is not None
        await db.commit()

    assert fired, "the barrier never ran — no successor was inserted mid-decision"
    assert landed is JobStatus.failed, "the mid-decision successor was not absorbed into a superseded failure"
    assert await _status(job_id) is JobStatus.failed
    assert (await _row(job_id)).error["code"] == "superseded"
    async with session() as db:
        generation = await db.get(DeploymentGeneration, generation_id)
        assert generation.status is GenerationStatus.abandoned, "the absorbed requeue left its generation blocking"


# ── S1.5 / S1.7: the writers with no execution, and the device_id sentinel ───


async def test_queued_sourced_writers_need_no_token(adapter_client, monkeypatch):
    """S1.5 — offboarding and the claimless-corruption writer have nothing to name.

    Forbidden: requiring a token from them, which would leave offboard unable to
    terminalize a queued job at all. Both carry ``expect=queued`` instead, and offboard
    goes through the dedicated bulk helper — a per-job helper cannot express its
    unbounded row set.
    """
    from nso_adapter.core import claim as claim_mod
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device, Job, JobStatus, JobType

    # (a) the claimless-corruption writer: a non-provision job with no device.
    corrupt_id = await _queue(None, JobType.sync)
    assert await worker_mod._claim_next_claimless_job() is None
    corrupt = await _row(corrupt_id)
    assert corrupt.status is JobStatus.failed
    assert corrupt.error["code"] == "orphaned_claimless"
    assert corrupt.run_attempt == 0, "a queued-sourced write must not start an execution"

    # (b) offboard, through the bulk helper.
    device_id = await seed_device(nso_device_name="s1-queued-sourced", netbox_device_id=9908)
    queued_id = await _queue(device_id, JobType.sync)

    real_bulk = claim_mod.terminalize_queued_bulk
    calls: list[int] = []

    async def _spy(db, dev_id, **kwargs):
        calls.append(dev_id)
        return await real_bulk(db, dev_id, **kwargs)

    monkeypatch.setattr(claim_mod, "terminalize_queued_bulk", _spy)

    async with session() as db:
        await offboard_device(db, await db.get(Device, device_id))

    assert calls == [device_id], "offboard did not terminalize through the bulk helper"
    offboarded = await _row(queued_id)
    assert offboarded.status is JobStatus.failed
    assert offboarded.error["code"] == "device_offboarded"
    assert offboarded.device_id is None
    async with session() as db:
        assert await db.get(Job, queued_id) is not None


async def test_omitted_device_id_leaves_the_job_attached(adapter_client):
    """S1.7 (m10) — an omitted ``set_device_id`` means "unchanged", never "set NULL".

    ``None`` is a legal value that only the provision success path ever passes, so an
    omission read as None would silently detach every other terminalized job from its
    device and make it invisible to anything device-scoped.
    """
    from nso_adapter.core.claim import UNSET, terminalize
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="s1-sentinel", netbox_device_id=9909)
    job_id = await _queue(device_id, JobType.sync)
    _jid, _dev, _jt, reg = await _start_run(device_id, job_id)

    async with session() as db:
        write = await terminalize(
            db,
            job_id,
            status=JobStatus.succeeded,
            expect=JobStatus.running,
            run_attempt=reg.run_attempt,
            result={"ok": True},
        )
        await db.commit()

    assert write is not None
    assert write.device_id == device_id, "the write reported a device it did not read back"
    assert (await _row(job_id)).device_id == device_id, "the omission detached the job from its device"

    # The explicit sentinel is what an omission compiles to, and an explicit None still
    # detaches — the two are distinguishable, which is the whole point.
    await release_claim(reg)
    other_id = await _queue(device_id, JobType.detect_drift)
    _jid, _dev, _jt, other_reg = await _start_run(device_id, other_id)
    async with session() as db:
        await terminalize(
            db,
            other_id,
            status=JobStatus.succeeded,
            expect=JobStatus.running,
            run_attempt=other_reg.run_attempt,
            set_device_id=UNSET,
        )
        await db.commit()
    assert (await _row(other_id)).device_id == device_id
