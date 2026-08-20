# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Deployment generations: ordering, the success barrier, promotion and settlement.

#1558 (#1522 §G1/§G2/§H2). Every mutation here is a real HTTP intent push through the real
app, and every run goes through the real worker (``_claim_next_job`` + ``_run_one_job``)
against a recorded RESTCONF boundary. The properties under test are database properties — a
row lock converting allocation order into commit order, a compare-and-set that must not
fire, a worker that must refuse to start a job it can see and lock — so nothing is
hand-driven: a helper that called the choke points directly would be asserting the intended
semantics rather than the delivered ones.

The end-to-end vertical (receipt admission, the complete authorized document, execution of
the stored document, blocked-head retry) lives in ``test_generation_protocol.py``.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from tests.conftest import VALID_TOKEN, seed_device, session
from tests.core.test_generation_protocol import (
    _VLAN_ROOT,
    put_snmp,
    put_vlans,
    recorded_client,
    run_head,
    seed_settings,
)

pytestmark = pytest.mark.anyio

_AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

#: The VLAN set each device's intent currently holds, so a push can send the FULL set the
#: way the plugin does and a shrink can drop one entry from it.
_vlans: dict[int, list[int]] = {}

#: Each device's NSO name, so the recorded RESTCONF boundary answers for the device under
#: test rather than for one fixed name.
_names: dict[int, str] = {}


async def _device(name: str, netbox_device_id: int, *, auto_apply: bool = True) -> int:
    device_id = await seed_device(nso_device_name=name, netbox_device_id=netbox_device_id)
    await seed_settings(device_id, auto_apply=auto_apply)
    _vlans[device_id] = []
    _names[device_id] = name
    return device_id


async def _vlan(device_id: int, vid: int) -> None:
    """Add one VLAN to what the next push will send."""
    _vlans.setdefault(device_id, []).append(vid)


async def _set_auto_apply(device_id: int, value: bool) -> None:
    from nso_adapter.store.models import DeviceSettings

    async with session() as db:
        await db.execute(
            sa.update(DeviceSettings).where(DeviceSettings.device_id == device_id).values(auto_apply=value)
        )
        await db.commit()


async def _generations(device_id: int) -> list:
    from nso_adapter.store.models import DeploymentGeneration

    async with session() as db:
        return list(
            (
                await db.execute(
                    sa.select(DeploymentGeneration)
                    .where(DeploymentGeneration.device_id == device_id)
                    .order_by(DeploymentGeneration.seq)
                )
            )
            .scalars()
            .all()
        )


async def _stream(device_id: int, stream: str):
    from nso_adapter.store.models import DeviceProjectionStream

    async with session() as db:
        return await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == stream,
            )
        )


async def _push(client, device_id: int, section: str = "vlan", *, store_only: bool = False) -> None:
    """One real intent push over HTTP, carrying the device's full owned set for *section*."""
    query = "?store_only=true" if store_only else ""
    if section == "snmp":
        resp = await put_snmp(client, device_id, ["community"], query=query)
    else:
        resp = await put_vlans(client, device_id, _vlans.get(device_id, []), query=query)
    assert resp.status_code == 200, resp.text


#: The VLAN a shrink plants and then drops, so the shrink is a real one-entry un-own that
#: leaves the device's own VLANs alone.
_SHRUNK_VID = 999


async def _shrink(client, device_id: int, *, delete_origin: bool = False) -> int:
    """Drop one VLAN over HTTP. Unmarked → an un-own (detach); marked → a deletion (networked).

    Both pushes run with auto-apply OFF, so the device's chain gets exactly the removal
    generation this helper is about and not an apply generation beside it. Returns the
    removal job the shrink created.
    """
    from nso_adapter.store.models import Job, JobType

    kept = _vlans.get(device_id) or []
    await _set_auto_apply(device_id, False)
    try:
        planted = await put_vlans(client, device_id, [*kept, _SHRUNK_VID])
        assert planted.status_code == 200, planted.text
        resp = await put_vlans(client, device_id, kept, query="?delete_origin=true" if delete_origin else "")
        assert resp.status_code == 200, resp.text
    finally:
        await _set_auto_apply(device_id, True)
    async with session() as db:
        return await db.scalar(
            sa.select(Job.id)
            .where(Job.device_id == device_id, Job.job_type == JobType.removal)
            .order_by(Job.id.desc())
            .limit(1)
        )


async def _finish(device_id: int, status) -> int | None:
    """Run the device's head job to a terminal status through the REAL worker.

    The outcome is produced by the RESTCONF boundary refusing the commit, not by writing a
    status: a hand-written terminal status settles generations that no execution ever
    touched. ``failed`` therefore means "failed through the injected VLAN rejection", which
    is asserted: a head that fails some other way has not run the case the caller asked for.
    """
    from nso_adapter.store.models import JobStatus

    failing = status is JobStatus.failed
    client, rec = recorded_client(_names[device_id], fail_vlan=failing)
    job_id = await run_head(device_id, client)
    if failing:
        assert rec.bodies(_VLAN_ROOT), "the injected vlan rejection never fired"
    return job_id


async def _job_status(job_id: int):
    from nso_adapter.store.models import Job

    async with session() as db:
        return (await db.get(Job, job_id)).status


# ── §G1: one generation per authorized write, ordered, immutable ─────────────


async def test_a_normal_push_promotes_and_stores_its_document(adapter_client):
    from nso_adapter.core.generation import digest_document
    from nso_adapter.store.models import GenerationMode, GenerationStatus

    device_id = await _device("gen-basic", 9700)
    await _vlan(device_id, 10)
    await _push(adapter_client, device_id)

    (generation,) = await _generations(device_id)
    assert generation.seq == 1
    assert generation.mode is GenerationMode.networked
    assert generation.status is GenerationStatus.pending
    assert generation.job_id is not None, "the generation was not attached to the job that deploys it"
    # The document is the section's projection, and the digest covers it byte for byte.
    vlans = generation.document["vlan"]["vlan_intent"]
    assert [row["vlan_id"] for row in vlans] == [10]
    assert generation.digest == digest_document(generation.mode, generation.document, generation.allowed_removal_keys)

    section = await _stream(device_id, "vlan")
    assert (section.desired_revision, section.authorized_revision, section.applied_revision) == (1, 1, 0)


async def test_a_store_only_push_never_promotes_and_never_rides_along(adapter_client):
    """§G2 + tracker #103 — store-only family A, then a networked push for family B.

    The store-only write is real (``desired_revision`` moves), but it authorizes nothing:
    no generation carries it, ``authorized_revision`` stays put, and family B's generation
    names only family B. Driven by the real ``?store_only=true`` query flag, so the
    middleware and the contextvar are part of what is proven.
    """
    device_id = await _device("gen-storeonly", 9701)
    await _vlan(device_id, 12)

    await _push(adapter_client, device_id, section="snmp", store_only=True)

    assert await _generations(device_id) == []
    snmp = await _stream(device_id, "snmp")
    assert (snmp.desired_revision, snmp.authorized_revision) == (1, 0), "a store-only write promoted"

    await _push(adapter_client, device_id, section="vlan")
    (generation,) = await _generations(device_id)
    assert sorted(generation.stream_revisions) == ["vlan"], "the store-only family rode along"
    assert (await _stream(device_id, "snmp")).authorized_revision == 0


async def test_a_detach_never_coalesces_into_a_queued_networked_job(adapter_client):
    """§G1 — a queued networked apply, then an unmarked shrink (a detach).

    The two are different device operations; one job commits with one ``no-networking``
    setting. They must land as two ordered generations on two jobs, not one.
    """
    from nso_adapter.store.models import GenerationMode

    device_id = await _device("gen-nocoalesce", 9702)
    await _vlan(device_id, 20)
    await _push(adapter_client, device_id)
    removal_job = await _shrink(adapter_client, device_id)

    first, second = await _generations(device_id)
    assert (first.seq, first.mode) == (1, GenerationMode.networked)
    assert (second.seq, second.mode) == (2, GenerationMode.detach)
    assert second.job_id == removal_job
    assert first.job_id != second.job_id, "a detach was folded into the networked job"


@pytest.mark.parametrize("detach_first", [True, False])
async def test_detach_and_deletion_origin_keep_their_modes_in_either_order(adapter_client, detach_first):
    """§G1 — an un-own and a marked deletion, in both orders, keep their own modes.

    The mode is frozen with the push that authorized it. A later job may neither retract an
    un-owned family (#106) nor quietly detach an authorized deletion.
    """
    from nso_adapter.store.models import GenerationMode

    device_id = await _device(f"gen-order-{detach_first}", 9703 + detach_first)
    order = [False, True] if detach_first else [True, False]
    for delete_origin in order:
        await _shrink(adapter_client, device_id, delete_origin=delete_origin)

    modes = [(g.seq, g.mode) for g in await _generations(device_id)]
    expected = (
        [GenerationMode.detach, GenerationMode.networked]
        if detach_first
        else [GenerationMode.networked, GenerationMode.detach]
    )
    assert modes == [(1, expected[0]), (2, expected[1])]


async def test_the_push_sequence_travels_from_the_header_into_the_generation(adapter_client):
    """§G2 — ``X-Push-Seq`` is the write's provenance, end to end over real HTTP.

    Driven through the VLAN endpoint rather than the choke point, because the header has to
    survive the middleware and the request-scoped contextvar to reach the promotion at all.
    """
    from tests.conftest import VALID_TOKEN

    device_id = await _device("gen-pushseq", 9705)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent",
        json={"vlans": [{"vlan_id": 11, "name": "MGMT"}]},
        headers={"Authorization": f"Bearer {VALID_TOKEN}", "X-Push-Seq": "4711"},
    )
    assert resp.status_code == 200

    (generation,) = await _generations(device_id)
    assert generation.source_push_seq == {"vlan": 4711}
    assert (await _stream(device_id, "vlan")).source_push_seq == 4711


# ── §G2: settlement CASes the executed revision, never a newer raw row ───────


async def test_settlement_stamps_the_carried_revision_not_the_newer_store(adapter_client):
    """§G2 — the store moves on while the job RUNS; settlement may only claim what it sent.

    The job is already running when the next writes land, so those writes go to a successor
    generation the running job never carried. Stamping them applied would certify a device
    state this deployment never delivered.
    """
    from nso_adapter.store.models import GenerationStatus

    device_id = await _device("gen-cas", 9710)
    await _vlan(device_id, 30)
    await _push(adapter_client, device_id)

    async def successors():
        # Committed while the run is INSIDE the job, in the window the worker opens between
        # the `running` commit and the runner reading what it deploys.
        await _vlan(device_id, 31)
        await _push(adapter_client, device_id)
        await _push(adapter_client, device_id)

    client, _rec = recorded_client("gen-cas", on_sync_from=successors)
    assert await run_head(device_id, client) is not None
    assert (await _stream(device_id, "vlan")).desired_revision == 3

    generations = await _generations(device_id)
    assert len(generations) == 3
    assert generations[0].status is GenerationStatus.settled
    assert [g.status for g in generations[1:]] == [GenerationStatus.pending] * (len(generations) - 1)
    section = await _stream(device_id, "vlan")
    assert section.applied_revision == 1, "settlement stamped a revision the executed document never carried"
    assert section.desired_revision == 3


async def test_a_queued_job_coalesces_within_its_mode_and_settles_all_it_carried(adapter_client):
    """§G1 permits coalescing WITHIN a mode — and then settlement covers every carried revision.

    A still-queued networked job has sent nothing yet, so the writes that arrive before it
    starts are genuinely part of what it will deploy. The boundary is the job STARTING, not
    the generation being created: once running, ``admit_queued_job`` hands the next write a
    successor job instead (proven by the test above).
    """
    from nso_adapter.store.models import GenerationStatus, JobStatus

    device_id = await _device("gen-coalesce", 9712)
    await _vlan(device_id, 35)
    await _push(adapter_client, device_id)
    await _push(adapter_client, device_id)

    generations = await _generations(device_id)
    assert len({g.job_id for g in generations}) == 1, "two queued networked generations split their job"

    await _finish(device_id, JobStatus.succeeded)
    assert [g.status for g in await _generations(device_id)] == [GenerationStatus.settled] * 2
    assert (await _stream(device_id, "vlan")).applied_revision == 2


async def test_settlement_never_regresses_applied_revision(adapter_client):
    """A late settlement of an older generation must not undo a newer proven application."""
    from nso_adapter.store.models import DeviceProjectionStream, JobStatus

    device_id = await _device("gen-cas-regress", 9711)
    await _vlan(device_id, 40)
    await _push(adapter_client, device_id)
    (generation,) = await _generations(device_id)

    async with session() as db:
        await db.execute(
            sa.update(DeviceProjectionStream)
            .where(DeviceProjectionStream.device_id == device_id)
            .values(applied_revision=7)
        )
        await db.commit()

    await _finish(device_id, JobStatus.succeeded)
    assert (await _stream(device_id, "vlan")).applied_revision == 7


# ── §H2: the success barrier ─────────────────────────────────────────────────


async def _claim_and_release() -> tuple | None:
    """One worker poll, giving the device straight back so the next poll can be observed."""
    from nso_adapter.core import worker as worker_mod
    from nso_adapter.core.claim import release_claim

    claimed = await worker_mod._claim_next_job()
    if claimed is not None:
        await release_claim(claimed[3])
    return claimed


async def test_a_failed_predecessor_blocks_its_successor(adapter_client):
    """§H2 — the worker must refuse a job whose predecessor generation FAILED.

    Selecting the queued head is not enough: both jobs are queued, lockable and this
    device's own work. Only the generation chain says the second may not run.
    """
    from nso_adapter.store.models import GenerationStatus, JobStatus

    device_id = await _device("gen-failed-head", 9720)
    await _vlan(device_id, 50)
    removal_job = await _shrink(adapter_client, device_id)
    await _push(adapter_client, device_id)
    first, second = await _generations(device_id)
    assert second.job_id is not None and second.job_id != removal_job

    assert await _finish(device_id, JobStatus.failed) == removal_job
    assert (await _generations(device_id))[0].status is GenerationStatus.failed

    claimed = await _claim_and_release()
    assert claimed is None, f"a successor generation crossed a failed head: {claimed}"
    assert await _job_status(second.job_id) is JobStatus.queued


async def test_an_unknown_outcome_blocks_its_successor(adapter_client):
    """§H2 — recovery never watched the run, so the head is unknown, and unknown blocks."""
    from nso_adapter.core.claim import terminalize_running
    from nso_adapter.store.models import GenerationStatus, Job, JobStatus

    device_id = await _device("gen-unknown-head", 9721)
    await _vlan(device_id, 51)
    removal_job = await _shrink(adapter_client, device_id)
    await _push(adapter_client, device_id)
    _first, second = await _generations(device_id)

    async with session() as db:
        job = await db.get(Job, removal_job)
        job.status = JobStatus.running
        job.run_attempt = 1
        await db.commit()
        await terminalize_running(
            db,
            removal_job,
            status=JobStatus.failed,
            error={"code": "orphaned", "message": "Adapter restarted while the job was running", "detail": {}},
            expected_attempt=1,
        )
        await db.commit()

    head = (await _generations(device_id))[0]
    assert head.status is GenerationStatus.outcome_unknown

    claimed = await _claim_and_release()
    assert claimed is None, f"a successor crossed a head whose outcome is unknown: {claimed}"
    assert await _job_status(second.job_id) is JobStatus.queued


async def test_a_blocked_head_still_blocks_after_a_restart(adapter_client):
    """§H2 — restart while blocked: recovery must not open the barrier it found closed."""
    from nso_adapter.core.generation import recover_generations
    from nso_adapter.store.models import GenerationStatus, JobStatus

    device_id = await _device("gen-restart-blocked", 9722)
    await _vlan(device_id, 52)
    removal_job = await _shrink(adapter_client, device_id)
    await _push(adapter_client, device_id)
    _first, second = await _generations(device_id)
    assert await _finish(device_id, JobStatus.failed) == removal_job

    await recover_generations()

    head, successor = await _generations(device_id)
    assert head.status is GenerationStatus.failed, "recovery cleared a blocked head"
    assert successor.status is GenerationStatus.pending
    claimed = await _claim_and_release()
    assert claimed is None
    assert await _job_status(second.job_id) is JobStatus.queued


async def test_a_reconciled_head_releases_its_successor(adapter_client):
    """§H2's explicit exit — abandoning the blocked head is what lets the chain move on."""
    from nso_adapter.core.generation import reconcile_generation
    from nso_adapter.store.models import JobStatus

    device_id = await _device("gen-reconcile", 9723)
    await _vlan(device_id, 53)
    removal_job = await _shrink(adapter_client, device_id)
    await _push(adapter_client, device_id)
    _first, second = await _generations(device_id)
    assert await _finish(device_id, JobStatus.failed) == removal_job
    assert await _claim_and_release() is None

    async with session() as db:
        head = (await _generations(device_id))[0]
        assert await reconcile_generation(db, head.id)
        await db.commit()

    claimed = await _claim_and_release()
    assert claimed is not None and claimed[0] == second.job_id


async def test_reconcile_refuses_a_generation_that_is_not_a_blocked_head(adapter_client):
    from nso_adapter.core.generation import GenerationNotBlocked, reconcile_generation

    device_id = await _device("gen-reconcile-refuse", 9724)
    await _vlan(device_id, 54)
    await _push(adapter_client, device_id)
    (generation,) = await _generations(device_id)

    async with session() as db:
        with pytest.raises(GenerationNotBlocked, match="not a blocked head"):
            await reconcile_generation(db, generation.id)


async def test_a_blocked_head_is_never_rebuilt_from_a_store_that_moved_on(adapter_client):
    """§H2 — a retry sends THIS document: pushes landing behind the blocked head never edit it.

    The retry itself is covered end to end in ``test_generation_protocol.py``. What this pins
    is the property that makes it safe, and it is a property of the row.
    """
    from nso_adapter.store.models import GenerationStatus, JobStatus

    device_id = await _device("gen-retry", 9725)
    await _vlan(device_id, 60)
    removal_job = await _shrink(adapter_client, device_id)
    head = (await _generations(device_id))[0]
    before = (head.document, head.digest, head.mode)

    assert await _finish(device_id, JobStatus.failed) == removal_job
    await _vlan(device_id, 61)  # the store moves on under the failed head
    await _push(adapter_client, device_id)

    head = (await _generations(device_id))[0]
    assert head.status is GenerationStatus.failed
    assert (head.document, head.digest, head.mode) == before, "a later push rewrote the blocked head"


# ── restart with pending, unattached generations ─────────────────────────────


async def test_restart_gives_a_pending_uncovered_generation_a_job(adapter_client):
    """A generation whose admission could not attach it must not be stranded by a restart."""
    from nso_adapter.core.generation import recover_generations
    from nso_adapter.store.models import DeploymentGeneration, Job, JobStatus

    device_id = await _device("gen-restart-pending", 9730)
    await _vlan(device_id, 70)
    await _push(adapter_client, device_id)
    (generation,) = await _generations(device_id)

    # The process died between creating the generation and its job reaching a worker.
    async with session() as db:
        await db.execute(sa.delete(Job).where(Job.id == generation.job_id))
        await db.commit()
    async with session() as db:
        assert (
            await db.scalar(sa.select(DeploymentGeneration.job_id).where(DeploymentGeneration.id == generation.id))
            is None
        )

    await recover_generations()

    revived = (await _generations(device_id))[0]
    assert revived.job_id is not None, "a pending generation was left with no job to deploy it"
    assert await _job_status(revived.job_id) is JobStatus.queued


async def test_restart_marks_a_stranded_running_generation_unknown(adapter_client):
    """A generation left ``running`` by a dead process has an unknown outcome, not a failure."""
    from nso_adapter.core.generation import recover_generations
    from nso_adapter.store.models import DeploymentGeneration, GenerationStatus

    device_id = await _device("gen-restart-running", 9731)
    await _vlan(device_id, 71)
    await _push(adapter_client, device_id)
    (generation,) = await _generations(device_id)
    async with session() as db:
        await db.execute(
            sa.update(DeploymentGeneration)
            .where(DeploymentGeneration.id == generation.id)
            .values(status=GenerationStatus.running)
        )
        await db.commit()

    await recover_generations()
    assert (await _generations(device_id))[0].status is GenerationStatus.outcome_unknown


# ── concurrent creation: sequence order == commit order ──────────────────────


async def test_concurrent_creation_orders_by_commit_not_by_start(adapter_client, rival_engine):
    """§G1 — the later-STARTING transaction commits first and takes the LOWER sequence.

    Both transactions are open and holding uncommitted intent writes at the same time; the
    one that started FIRST has not reached the projection lock. Sequence order has to equal
    commit order, or the earlier-numbered document is executed last and silently reverts the
    write that actually committed first.
    """
    from nso_adapter.core.apply import enqueue_apply
    from nso_adapter.core.generation import note_write
    from nso_adapter.store.models import VlanIntent

    device_id = await _device("gen-concurrent", 9740)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as slow, rival() as fast:
        # The slow transaction starts FIRST and really writes, but has not yet reached the
        # projection lock — a SELECT-only "overlap" would prove nothing about ordering.
        slow.add(VlanIntent(device_id=device_id, vlan_id=80, name="slow"))
        await slow.flush()

        await note_write(fast, device_id, "snmp")
        await enqueue_apply(fast, device_id, force=True, stream="snmp")
        await fast.commit()

        await note_write(slow, device_id, "vlan")
        await enqueue_apply(slow, device_id, force=True, stream="vlan")
        await slow.commit()

    generations = await _generations(device_id)
    assert [(g.seq, sorted(g.stream_revisions)) for g in generations] == [(1, ["snmp"]), (2, ["vlan"])]


async def test_a_second_writer_cannot_allocate_until_the_first_commits(adapter_client, rival_engine):
    """§G1 — the reverse direction: the holder wins the sequence, the waiter genuinely waits.

    The waiter's whole promotion is launched as a concurrent task while the holder still has
    the projection lock. It must not complete until the holder commits, and it must then be
    ordered AFTER it — that is what makes ``seq`` the commit order rather than the arrival
    order.
    """
    from nso_adapter.core.apply import enqueue_apply
    from nso_adapter.core.generation import note_write
    from nso_adapter.store.models import VlanIntent

    device_id = await _device("gen-concurrent-barrier", 9742)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as holder, rival() as waiter:
        holder.add(VlanIntent(device_id=device_id, vlan_id=81, name="holder"))
        await note_write(holder, device_id, "vlan")
        await enqueue_apply(holder, device_id, force=True, stream="vlan")

        async def second_writer():
            await note_write(waiter, device_id, "snmp")
            await enqueue_apply(waiter, device_id, force=True, stream="snmp")
            await waiter.commit()

        blocked = asyncio.create_task(second_writer())
        await asyncio.sleep(0.2)
        assert not blocked.done(), "the second promotion allocated a sequence under a held lock"

        await holder.commit()
        await asyncio.wait_for(blocked, timeout=10)

    generations = await _generations(device_id)
    assert [(g.seq, sorted(g.stream_revisions)) for g in generations] == [(1, ["vlan"]), (2, ["snmp"])]


async def test_the_projection_lock_serializes_two_writers(adapter_client, rival_engine):
    """The lock is real: the second writer waits for the first to COMMIT before allocating."""
    from nso_adapter.core.generation import lock_projection

    device_id = await _device("gen-lock", 9741)
    # The counter row must already EXIST, or the two writers would serialize on the insert
    # conflict alone and the row lock this test is about would never be exercised.
    async with session() as db:
        await lock_projection(db, device_id)
        await db.commit()

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as holder, rival() as waiter:
        await lock_projection(holder, device_id)

        blocked = asyncio.create_task(lock_projection(waiter, device_id))
        await asyncio.sleep(0.2)
        assert not blocked.done(), "a second projection writer was not serialized"

        await holder.commit()
        await asyncio.wait_for(blocked, timeout=5)
        await waiter.rollback()


# ── mode boundary ────────────────────────────────────────────────────────────


async def test_attaching_across_the_mode_boundary_is_refused(adapter_client):
    """§G1 — coalescing a detach into a networked job is a hard refusal, not a reorder."""
    from nso_adapter.core.generation import GenerationModeConflict, attach_to_job, create_generation, note_write
    from nso_adapter.store.models import GenerationMode, Job

    device_id = await _device("gen-mode-conflict", 9750)
    await _vlan(device_id, 90)
    await _push(adapter_client, device_id)
    (networked,) = await _generations(device_id)

    async with session() as db:
        job = await db.get(Job, networked.job_id)
        await note_write(db, device_id, "vlan")
        detach = await create_generation(db, device_id, streams=("vlan",), mode=GenerationMode.detach)
        with pytest.raises(GenerationModeConflict):
            await attach_to_job(db, detach, job)
        await db.rollback()


async def test_a_generation_never_joins_a_job_it_is_not_contiguous_with(adapter_client):
    """Ordering, not convenience: joining a job that already skipped a generation reorders it."""
    from nso_adapter.store.models import GenerationMode

    device_id = await _device("gen-noncontiguous", 9751)
    await _vlan(device_id, 91)
    await _push(adapter_client, device_id)  # gen 1 -> apply job A
    await _shrink(adapter_client, device_id)  # gen 2 -> removal job R (detach)
    await _push(adapter_client, device_id)  # gen 3: A is still queued, but gen 2 sits in between

    first, second, third = await _generations(device_id)
    assert second.mode is GenerationMode.detach
    assert third.job_id is None, "generation 3 joined a job that would run it before generation 2"
    assert third.seq == 3 and first.job_id != second.job_id


async def test_a_settled_head_hands_its_successor_a_job(adapter_client):
    """The chain advances on success: the stranded successor is given its own job."""
    from nso_adapter.core.generation import advance_device_generations
    from nso_adapter.store.models import GenerationStatus, JobStatus

    device_id = await _device("gen-advance", 9752)
    await _vlan(device_id, 92)
    await _push(adapter_client, device_id)
    removal_job = await _shrink(adapter_client, device_id)
    await _push(adapter_client, device_id)
    first, _second, third = await _generations(device_id)
    assert third.job_id is None

    assert await _finish(device_id, JobStatus.succeeded) == first.job_id
    assert await _finish(device_id, JobStatus.succeeded) == removal_job
    await advance_device_generations(device_id)

    advanced = (await _generations(device_id))[2]
    assert advanced.status is GenerationStatus.pending
    assert advanced.job_id is not None
    assert await _job_status(advanced.job_id) is JobStatus.queued


async def test_standalone_advancement_takes_the_projection_lock(adapter_client, rival_engine):
    """Advancement must serialize with a writer before it reads or admits the head."""
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from nso_adapter.core.generation import advance_device_generations, lock_projection

    device_id = await _device("gen-advance-lock", 9753)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with rival() as holder:
        await lock_projection(holder, device_id)
        advancing = asyncio.create_task(advance_device_generations(device_id))
        await asyncio.sleep(0.1)
        assert not advancing.done(), "advancement read the chain without the projection lock"
        await holder.commit()

    assert await asyncio.wait_for(advancing, timeout=2) == 0


async def test_f8_abandoning_a_head_hands_the_released_successor_a_job_at_once(adapter_client):
    """#1558 rework 3, finding 3 — the abandon endpoint must advance the chain itself.

    Generation 3 was refused its job at admission (generation 2 sits in between), so nothing
    carries it. Advancement otherwise runs only after a worker finishes a job or the process
    restarts — and abandoning the blocker is exactly the case where no job will finish. The
    successor sat ``pending`` with no job until some unrelated write happened along.
    """
    from nso_adapter.store.models import GenerationStatus, JobStatus

    device_id = await _device("gen-abandon-advance", 9768)
    await _vlan(device_id, 69)
    await _push(adapter_client, device_id)  # G1 networked -> apply job A
    removal_job = await _shrink(adapter_client, device_id)  # G2 detach -> removal job R
    await _push(adapter_client, device_id)  # G3 networked, noncontiguous -> no job
    first, second, third = await _generations(device_id)
    assert third.job_id is None, "generation 3 was attached, so nothing needs releasing"

    assert await _finish(device_id, JobStatus.succeeded) == first.job_id
    assert await _finish(device_id, JobStatus.failed) == removal_job
    assert (await _generations(device_id))[1].status is GenerationStatus.failed
    assert await _queued_jobs(device_id) == [], "the released successor already had a job"

    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/abandon-generation", headers=_AUTH)
    assert resp.status_code == 202

    chain = await _generations(device_id)
    assert chain[1].status is GenerationStatus.abandoned
    advanced = chain[2]
    assert advanced.id == third.id and advanced.status is GenerationStatus.pending
    assert advanced.job_id is not None, "the released successor was left with no job to deploy it"
    assert await _job_status(advanced.job_id) is JobStatus.queued
    claimed = await _claim_and_release()
    assert claimed is not None and claimed[0] == advanced.job_id, (
        "the successor's job is not the one a worker would pick up next"
    )
    assert second.seq + 1 == advanced.seq, "an unrelated generation was advanced"


async def test_a_job_carrying_no_generation_is_never_blocked(adapter_client):
    """Reads must not queue behind a blocked write: a sync carries no document."""
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await _device("gen-read-unblocked", 9753)
    await _vlan(device_id, 93)
    removal_job = await _shrink(adapter_client, device_id)
    assert await _finish(device_id, JobStatus.failed) == removal_job

    async with session() as db:
        sync = Job(job_type=JobType.sync, device_id=device_id, status=JobStatus.queued)
        db.add(sync)
        await db.commit()
        sync_id = sync.id

    claimed = await _claim_and_release()
    assert claimed is not None and claimed[0] == sync_id


async def test_a_generationless_device_write_cannot_cross_a_blocked_head(adapter_client):
    """A device-writing job carrying no generation must still respect the barrier.

    Every producer now creates one, but a job can still reach a worker without a generation:
    an Apply on a device with nothing written, or a job whose generation was abandoned.
    Admitting one behind a blocked head would deploy over a device state nobody established.
    """
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await _device("gen-genless-write", 9754)
    await _vlan(device_id, 94)
    removal_job = await _shrink(adapter_client, device_id)
    assert await _finish(device_id, JobStatus.failed) == removal_job

    async with session() as db:
        manual = Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.queued)
        db.add(manual)
        await db.commit()
        manual_id = manual.id

    assert await _claim_and_release() is None
    assert await _job_status(manual_id) is JobStatus.queued


# ── #1558 rework 2 — the retry / reissue / operator-exit machinery ────────────


async def _queued_jobs(device_id: int) -> list:
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        return list(
            (
                await db.execute(
                    sa.select(Job)
                    .where(Job.device_id == device_id, Job.status == JobStatus.queued)
                    .order_by(Job.created_at, Job.id)
                )
            )
            .scalars()
            .all()
        )


async def _retry_head(device_id: int) -> int:
    """Re-admit this device's blocked head through the real endpoint's core call."""
    from nso_adapter.core.generation import executable_head, retry_generation

    async with session() as db:
        head = await executable_head(db, device_id)
        job = await retry_generation(db, head.id)
        await db.commit()
        return job.id


async def test_f1_a_retried_removal_runs_past_a_blocked_apply_successor(adapter_client):
    """§H2 — a retried head must not wait behind the successor its own failure blocked.

    A retry of a removal cannot take over the queued APPLY (different type), so it gets a
    later job. The worker used to stop at the FIFO head, find it inadmissible and stall the
    device for good.
    """
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await _device("gen-retry-past-apply", 9760)
    await _vlan(device_id, 60)
    removal_job = await _shrink(adapter_client, device_id, delete_origin=True)
    assert await _finish(device_id, JobStatus.failed) == removal_job

    # A successor apply queues behind the blocked removal.
    await _vlan(device_id, 61)
    await _push(adapter_client, device_id)
    queued = await _queued_jobs(device_id)
    assert [j.job_type for j in queued] == [JobType.apply]

    retried = await _retry_head(device_id)
    assert [j.id for j in await _queued_jobs(device_id)] == [queued[0].id, retried]

    assert await _finish(device_id, JobStatus.succeeded) == retried, (
        "the worker stalled on the blocked apply instead of running the retried removal"
    )


async def test_f1_b_retried_apply_runs_past_a_blocked_removal_successor(adapter_client):
    """The same stall in the other order: the retried head is an apply, the blocker a removal."""
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await _device("gen-retry-past-removal", 9761)
    await _vlan(device_id, 62)
    await _push(adapter_client, device_id)
    assert await _finish(device_id, JobStatus.failed) is not None

    # A marked deletion queues a removal behind the blocked apply. Auto-apply stays off so
    # this push contributes exactly one successor.
    await _shrink(adapter_client, device_id, delete_origin=True)
    await _set_auto_apply(device_id, False)

    queued = await _queued_jobs(device_id)
    assert [j.job_type for j in queued] == [JobType.removal]

    retried = await _retry_head(device_id)
    assert await _finish(device_id, JobStatus.succeeded) == retried, (
        "the worker stalled on the blocked removal instead of running the retried apply"
    )


async def test_f2_a_released_removal_successor_keeps_a_removal_job(adapter_client):
    """A retry takeover releases a networked REMOVAL successor; it must not become an apply.

    ``advance_device_generations`` routed only ``detach`` down the removal path, so a
    delete-origin removal was handed an apply job — which settles the generation without
    ever deleting anything.
    """
    from nso_adapter.store.models import GenerationMode, JobStatus, JobType

    device_id = await _device("gen-released-removal", 9762, auto_apply=False)
    await _vlan(device_id, 63)
    await _vlan(device_id, 64)
    await _push(adapter_client, device_id)

    # Two marked deletions: the first fails, the second queues its own removal job.
    _vlans[device_id] = [63]
    assert (await put_vlans(adapter_client, device_id, [63], query="?delete_origin=true")).status_code == 200
    first_removal = (await _queued_jobs(device_id))[0].id
    assert await _finish(device_id, JobStatus.failed) == first_removal

    _vlans[device_id] = []
    assert (await put_vlans(adapter_client, device_id, [], query="?delete_origin=true")).status_code == 200
    chain = await _generations(device_id)
    assert [g.mode for g in chain] == [GenerationMode.networked, GenerationMode.networked]

    # The retry takes the successor's queued removal job over, unattaching the successor.
    retried = await _retry_head(device_id)
    assert (await _generations(device_id))[1].job_id is None
    assert await _finish(device_id, JobStatus.succeeded) == retried

    successor = (await _generations(device_id))[1]
    assert successor.job_id is not None, "the released removal successor was never re-admitted"
    async with session() as db:
        from nso_adapter.store.models import Job

        job = await db.get(Job, successor.job_id)
    assert job.job_type is JobType.removal, "a delete-origin removal was re-admitted as an apply"
    assert job.context.get("scope") == "vlan"


async def test_f2_b_retry_does_not_take_over_a_generationless_removal_job(adapter_client):
    """A retry must preserve a queued legacy removal's only durable context."""
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await _blocked_device(adapter_client, "gen-retry-legacy-removal", 9768)
    (failed,) = await _generations(device_id)
    retry_context = failed.removal_context
    legacy_context = {"scope": "bgp"}

    async with session() as db:
        legacy = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.queued,
            context=legacy_context,
        )
        db.add(legacy)
        await db.commit()
        legacy_id = legacy.id

    retried_id = await _retry_head(device_id)

    async with session() as db:
        legacy = await db.get(Job, legacy_id)
        retried = await db.get(Job, retried_id)
    (generation,) = await _generations(device_id)
    assert retried_id != legacy_id
    assert legacy.context == legacy_context
    assert retried.context == retry_context
    assert generation.job_id == retried_id


async def _blocked_device(client, name: str, netbox_device_id: int) -> int:
    """A device whose head is a FAILED networked removal — the barrier's two exits apply."""
    from nso_adapter.store.models import JobStatus

    device_id = await _device(name, netbox_device_id)
    await _vlan(device_id, 65)
    removal_job = await _shrink(client, device_id, delete_origin=True)
    assert await _finish(device_id, JobStatus.failed) == removal_job
    return device_id


@asynccontextmanager
async def _first_request_holds_the_head():
    """Park the FIRST operator request inside its transaction, after it acted on the head.

    A bare ``asyncio.gather`` of two requests proves nothing: nothing forces them to overlap.
    This wraps the REAL ``retry_generation`` so request one holds its transaction — and
    therefore whatever it locked — open until the test releases it, which is the only state
    in which the second request's own revalidation is exercised.
    """
    from nso_adapter.core import generation as generation_mod

    release = asyncio.Event()
    parked = asyncio.Event()
    real = generation_mod.retry_generation

    async def gated(db, generation_id):
        job = await real(db, generation_id)
        parked.set()
        await release.wait()
        return job

    with patch("nso_adapter.core.generation.retry_generation", new=gated):
        yield release, parked


async def test_f3_a_two_concurrent_retries_admit_the_head_once(adapter_client):
    """Both operator requests target the same blocked head; only one may re-admit it."""
    device_id = await _blocked_device(adapter_client, "gen-retry-race", 9763)

    url = f"/api/v1/devices/{device_id}/actions/retry-generation"
    async with _first_request_holds_the_head() as (release, parked):
        first = asyncio.create_task(adapter_client.post(url, headers=_AUTH))
        await asyncio.wait_for(parked.wait(), timeout=3)
        second = asyncio.create_task(adapter_client.post(url, headers=_AUTH))
        await asyncio.sleep(0.3)
        release.set()
        one, two = await asyncio.wait_for(asyncio.gather(first, second), timeout=20)

    codes = sorted([one.status_code, two.status_code])
    assert codes == [202, 409], f"concurrent retries both re-admitted the head: {codes}"
    assert len(await _queued_jobs(device_id)) == 1, "the head was duplicated across two jobs"


async def test_f3_b_a_retry_and_an_abandon_cannot_both_win(adapter_client):
    """One request re-admits the head, the other gives up on it. Never both."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await _blocked_device(adapter_client, "gen-retry-abandon-race", 9764)

    base = f"/api/v1/devices/{device_id}/actions"
    async with _first_request_holds_the_head() as (release, parked):
        retrying = asyncio.create_task(adapter_client.post(f"{base}/retry-generation", headers=_AUTH))
        await asyncio.wait_for(parked.wait(), timeout=3)
        abandoning = asyncio.create_task(adapter_client.post(f"{base}/abandon-generation", headers=_AUTH))
        await asyncio.sleep(0.3)
        release.set()
        retry, abandon = await asyncio.wait_for(asyncio.gather(retrying, abandoning), timeout=20)

    codes = sorted([retry.status_code, abandon.status_code])
    assert codes == [202, 409], f"a retry and an abandon both acted on one head: {codes}"

    (head,) = await _generations(device_id)
    if head.status is GenerationStatus.abandoned:
        assert await _queued_jobs(device_id) == [], "an abandoned generation still has a job to run"
    else:
        assert head.status is GenerationStatus.pending
        assert len(await _queued_jobs(device_id)) == 1


async def test_f6_a_a_reissue_certifies_no_section_revision(adapter_client):
    """A promotion-free reissue carries no settleable revisions (#1522 §G2).

    It re-deploys already-authorized state for ONE scope; claiming every authorized
    section's revision lets its settlement certify a revision whose own deployment was
    abandoned.
    """
    from nso_adapter.core.tombstone_sweep import sweep_tombstones
    from nso_adapter.store.models import JobStatus, StaticRouteTombstone

    device_id = await _device("gen-reissue-revisions", 9765)
    await _vlan(device_id, 67)
    await _push(adapter_client, device_id)
    assert await _finish(device_id, JobStatus.failed) is not None

    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/abandon-generation", headers=_AUTH)
    assert resp.status_code == 202
    assert (await _stream(device_id, "vlan")).applied_revision == 0

    async with session() as db:
        db.add(
            StaticRouteTombstone(
                device_id=device_id,
                vrf="",
                prefix="10.9.7.0/24",
                next_hop="192.0.2.7",
                marking="detach",
                route_id=4007,
            )
        )
        await db.commit()
    assert await sweep_tombstones() == 1

    reissue = (await _generations(device_id))[-1]
    assert reissue.stream_revisions == {}, "a promotion-free reissue claimed authorized revisions"

    assert await _finish(device_id, JobStatus.succeeded) is not None
    assert (await _stream(device_id, "vlan")).applied_revision == 0, (
        "a static-route reissue certified an abandoned VLAN revision"
    )


async def test_f7_a_manual_apply_promotes_a_section_committed_alongside_it(adapter_client, rival_engine):
    """§H4 — the Apply selects its sections UNDER the projection lock, not before it.

    The rival holds the lock with an uncommitted snmp write. Selecting first and locking
    second reads the projection before that write is visible, so the Apply promotes vlan
    alone and the snmp intent is silently left undeployed.
    """
    from nso_adapter.core.generation import note_write

    device_id = await _device("gen-apply-under-lock", 9766, auto_apply=False)
    await _vlan(device_id, 68)
    await _push(adapter_client, device_id)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as holder:
        await note_write(holder, device_id, "snmp")

        applying = asyncio.create_task(adapter_client.post(f"/api/v1/devices/{device_id}/actions/apply", headers=_AUTH))
        await asyncio.sleep(0.2)
        await holder.commit()
        resp = await asyncio.wait_for(applying, timeout=10)

    assert resp.status_code == 202
    (generation,) = await _generations(device_id)
    assert sorted(generation.stream_revisions) == ["snmp", "vlan"], (
        "the Apply omitted a section committed while it was selecting"
    )


async def test_f3_c_a_lost_status_race_refuses_instead_of_acting_twice(adapter_client):
    """The compare-and-set is the second half of the guarantee, for a caller without the lock.

    One session holds a stale read of the blocked head — the exact state a second process
    reaches — while another abandons it. The retry must refuse rather than re-admit a
    generation the operator gave up on.
    """
    from nso_adapter.core.generation import GenerationNotBlocked, reconcile_generation, retry_generation
    from nso_adapter.store.models import DeploymentGeneration, GenerationStatus

    device_id = await _blocked_device(adapter_client, "gen-cas-race", 9767)
    (head,) = await _generations(device_id)

    async with session() as stale:
        cached = await stale.get(DeploymentGeneration, head.id)
        assert cached.status is GenerationStatus.failed

        async with session() as other:
            assert await reconcile_generation(other, head.id) is True
            await other.commit()

        with pytest.raises(GenerationNotBlocked, match="no longer a blocked head"):
            await retry_generation(stale, head.id)
        await stale.rollback()

    assert (await _generations(device_id))[0].status is GenerationStatus.abandoned
    assert await _queued_jobs(device_id) == []


# ── #1558 rework 3, finding 4 — an offboard under a PUT is a 404, not a 500 ───


async def test_f9_an_offboard_under_an_intent_put_answers_not_found(adapter_client, rival_engine):
    """The device is deleted while the PUT is inside ``note_write``'s projection lock.

    ``lock_projection`` refuses to promote for a device that no longer exists, and that
    refusal is a legitimate outcome of a legitimate request — the plugin's outbox retries an
    intent PUT while an operator offboards the device. Only the generation actions caught it,
    so every intent endpoint answered a 500 to a valid push.

    The rival stands in for ``core.onboarding.offboard_device``'s own deletes, PARKED
    mid-transaction: nothing else makes the two overlap deterministically, and the overlap is
    the whole point — a device already gone answers 404 from the endpoint's first lookup.
    """
    from nso_adapter.store.models import Device, ManagedScope

    device_id = await seed_device(nso_device_name="gen-offboard-put", netbox_device_id=9769)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as offboarding:
        await offboarding.execute(sa.delete(ManagedScope).where(ManagedScope.device_id == device_id))
        await offboarding.execute(sa.delete(Device).where(Device.id == device_id))
        await offboarding.flush()  # the rows are gone but the delete is UNCOMMITTED

        putting = asyncio.create_task(put_vlans(adapter_client, device_id, [10]))
        await asyncio.sleep(0.3)
        assert not putting.done(), "the PUT did not reach the projection lock the offboard holds"

        await offboarding.commit()
        resp = await asyncio.wait_for(putting, timeout=10)

    assert resp.status_code == 404, f"a concurrent offboard turned a valid intent PUT into {resp.status_code}"
    assert resp.json()["error"]["code"] == "not_found"
