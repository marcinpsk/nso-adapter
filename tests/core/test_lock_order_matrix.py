# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R1b chunk 3: the ordering matrix — M6.21, M8.4, and M6.9l(a)'s preservation pin.

Every case forces the disputed ordering explicitly. Two coroutines and an
``asyncio.gather`` can serialize by scheduling and prove nothing, so each test holds one
side's transaction open at a barrier, confirms the other side is genuinely blocked (or
genuinely skipped), and only then releases.
"""

from __future__ import annotations

import asyncio

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from tests.conftest import VALID_TOKEN, push_seq, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _seed_bfd_intent(device_id: int) -> int:
    from nso_adapter.store.models import BfdIntent

    async with session() as db:
        row = BfdIntent(device_id=device_id, interface_name="Ethernet1", min_tx=300)
        db.add(row)
        await db.commit()
        return row.id


async def _seed_queued_apply(device_id: int) -> int:
    from nso_adapter.store.models import Job, JobStatus, JobType

    async with session() as db:
        job = Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.queued, context={})
        db.add(job)
        await db.commit()
        return job.id


async def _offboard(device_id: int) -> None:
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device

    async with session() as db:
        await offboard_device(db, await db.get(Device, device_id))


async def _legacy_offboard(device_id: int, engine) -> None:
    """The pre-fix ordering: `jobs` first, intent rows only via the device cascade.

    The discriminating variant. Kept test-local — production has no such path any more —
    because a "no deadlock" assertion against a fixture that could not deadlock either way
    proves nothing.
    """
    from nso_adapter.store.models import BfdIntent, Device, Job

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        await db.execute(sa.update(Job).where(Job.device_id == device_id).values(device_id=None))
        await db.execute(sa.delete(BfdIntent).where(BfdIntent.device_id == device_id))
        await db.execute(sa.delete(Device).where(Device.id == device_id))
        await db.commit()


async def _endpoint_shaped_bfd_put(device_id: int, intent_id: int, job_id: int, engine, gate: asyncio.Event) -> None:
    """The canonical auto-apply endpoint shape: intent DML, then Q2's winner lock on `jobs`.

    `api/bfd.py` mutates its intent rows, flushes, and only then reaches for the queued
    apply. §3.9's order was re-derived to match that, precisely so none of the fourteen
    non-static-route endpoints has to be restructured.
    """
    from nso_adapter.store.models import BfdIntent, Job

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        await db.execute(sa.update(BfdIntent).where(BfdIntent.id == intent_id).values(min_tx=900))
        gate.set()
        await asyncio.sleep(0.6)  # hold the intent row while the other side runs
        await db.execute(sa.select(Job).where(Job.id == job_id).with_for_update())
        await db.commit()


async def test_teardown_deletes_intent_rows_before_it_touches_jobs(adapter_client):
    """M6.21's structural half — the statement sequence, not just the outcome.

    Necessary but not sufficient on its own (the restrictive-FK failure raises regardless
    of ordering, which is `test_teardown_deletes_every_intent_root_*`), and it catches the
    reverse: a newly added family that nobody seeded still trips this check.
    """
    from sqlalchemy import event

    from nso_adapter.store.db import get_engine

    device_id = await seed_device(nso_device_name="lo-structural", netbox_device_id=9700)
    await _seed_bfd_intent(device_id)
    await _seed_queued_apply(device_id)

    statements: list[str] = []

    def _record(_conn, _cursor, statement, *_rest):
        statements.append(" ".join(statement.split()).lower())

    engine = get_engine().sync_engine
    event.listen(engine, "before_cursor_execute", _record)
    try:
        await _offboard(device_id)
    finally:
        event.remove(engine, "before_cursor_execute", _record)

    first_jobs = next(i for i, s in enumerate(statements) if " jobs" in s or "from jobs" in s)
    intent_deletes = [i for i, s in enumerate(statements) if s.startswith("delete from") and "_intent" in s]
    assert intent_deletes, "no intent-root delete was issued at all"
    assert max(intent_deletes) < first_jobs, "teardown touched `jobs` while intent rows were still reachable"


async def test_an_intent_endpoint_and_teardown_do_not_deadlock(adapter_client, rival_engine):
    """M6.21 — codex's concrete cycle, run for real.

    The endpoint holds a `BfdIntent` row and reaches for the queued apply winner; teardown
    comes the other way. Under the re-derived order teardown never holds a `jobs` lock while
    an intent row is still reachable, so one side simply waits.
    """
    device_id = await seed_device(nso_device_name="lo-nodeadlock", netbox_device_id=9701)
    intent_id = await _seed_bfd_intent(device_id)
    job_id = await _seed_queued_apply(device_id)

    gate = asyncio.Event()
    endpoint = asyncio.create_task(_endpoint_shaped_bfd_put(device_id, intent_id, job_id, rival_engine, gate))
    await gate.wait()
    teardown = asyncio.create_task(_offboard(device_id))

    await asyncio.gather(endpoint, teardown)  # no DeadlockDetected escapes

    from nso_adapter.store.models import Device

    async with session() as db:
        assert await db.get(Device, device_id) is None


async def test_the_pre_fix_teardown_ordering_really_does_deadlock(adapter_client, rival_engine):
    """The discriminating variant for the test above."""
    device_id = await seed_device(nso_device_name="lo-deadlock", netbox_device_id=9702, attributes=[])
    intent_id = await _seed_bfd_intent(device_id)
    job_id = await _seed_queued_apply(device_id)

    gate = asyncio.Event()
    endpoint = asyncio.create_task(_endpoint_shaped_bfd_put(device_id, intent_id, job_id, rival_engine, gate))
    await gate.wait()
    from nso_adapter.store.db import get_engine

    legacy = asyncio.create_task(_legacy_offboard(device_id, get_engine()))

    results = await asyncio.gather(endpoint, legacy, return_exceptions=True)
    errors = [r for r in results if isinstance(r, BaseException)]
    assert errors, "the pre-fix ordering completed cleanly — the fixture cannot discriminate"
    assert any(isinstance(e, DBAPIError) and "deadlock" in str(e).lower() for e in errors), errors


# ── M8.4: the endpoint/worker handoff ────────────────────────────────────────


async def test_a_worker_cannot_start_the_winner_before_the_endpoint_commits(adapter_client, monkeypatch):
    """M8.4 — the forbidden outcome is J1 running with a pre-B snapshot and no successor.

    B mutates intent, hits the admission conflict on the already-queued J1, and is held
    before its outer commit while still holding J1's winner lock. With Q5's SKIP LOCKED the
    worker does not block — it skips the locked head and re-polls — so the assertion is on
    job state, not on a blocking call.

    Driven through the BFD endpoint deliberately: it is the canonical auto-apply shape and
    takes no device claim, so what is under test is the winner lock itself. On the
    static-route path the intent_put claim would additionally exclude the worker, masking
    the very mechanism this pins.
    """
    from nso_adapter.core import apply as apply_mod
    from nso_adapter.core.worker import _claim_next_job
    from nso_adapter.store.models import BfdIntent, DeviceSettings, Job, JobStatus

    device_id = await seed_device(nso_device_name="lo-m8-4", netbox_device_id=9703)
    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()
    j1 = await _seed_queued_apply(device_id)

    held = asyncio.Event()
    release = asyncio.Event()
    real_enqueue_apply = apply_mod.enqueue_apply

    async def _enqueue_then_hold(db, device, **kwargs):
        result = await real_enqueue_apply(db, device, **kwargs)
        held.set()
        await release.wait()
        return result

    monkeypatch.setattr(apply_mod, "enqueue_apply", _enqueue_then_hold)

    put = asyncio.create_task(
        adapter_client.put(
            f"/api/v1/devices/{device_id}/bfd-intent",
            json={"interfaces": [{"interface_name": "Ethernet1", "min_tx": 900}]},
            headers=AUTH | push_seq(),
        )
    )
    await asyncio.wait_for(held.wait(), timeout=10)

    # The worker must not start J1 from a snapshot that predates B's committed mutation.
    claimed = await _claim_next_job()
    assert claimed is None, "the worker started the winner while the endpoint still held it"
    async with session() as db:
        assert (await db.get(Job, j1)).status is JobStatus.queued

    release.set()
    resp = await asyncio.wait_for(put, timeout=20)
    assert resp.status_code == 200

    # After the commit: J1 is claimable, and anything starting now reads B's mutation.
    claimed = await _claim_next_job()
    assert claimed is not None
    assert claimed[0] == j1
    async with session() as db:
        assert (await db.get(Job, j1)).status is JobStatus.running
        rows = (await db.execute(sa.select(BfdIntent).where(BfdIntent.device_id == device_id))).scalars().all()
        assert [(r.interface_name, r.min_tx) for r in rows] == [("Ethernet1", 900)]


# ── M6.9l(a): the Path-A preservation pin ────────────────────────────────────


async def test_the_mapping_post_creates_the_device_and_nothing_else(adapter_client_with_nso):
    """M6.9l(a) — Path A today: no job, no inline refresh, no claim. A preservation pin.

    R1c adds a claim around the post-map refresh; this fixes what must NOT change on the
    branch that has no refresh at all.
    """
    from nso_adapter.store.models import Device, DeviceClaim, Job

    resp = await adapter_client_with_nso.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev", "nso_device_name": "path-a-node", "netbox_device_id": 9704},
        headers=AUTH,
    )
    assert resp.status_code == 201
    device_id = resp.json()["id"]

    async with session() as db:
        device = await db.get(Device, device_id)
        assert device.netbox_device_id == 9704
        assert device.last_sync_at is None, "an inline refresh ran on a path that has none today"
        assert (await db.execute(sa.select(sa.func.count()).select_from(Job))).scalar() == 0
        assert await db.get(DeviceClaim, device_id) is None


@pytest.mark.parametrize("when", ["before", "during"])
async def test_teardown_and_a_mapping_post_do_not_interleave(adapter_client_with_nso, when, monkeypatch):
    """M6.9s — last-writer-wins, with the fence doing serialization rather than arbitration."""
    from nso_adapter.config import get_config
    from nso_adapter.core.claim import acquire_claim, release_claim
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="path-a-race", netbox_device_id=9705)

    if when == "before":
        # (i) teardown completes first: the mapping is a genuinely fresh onboarding, and
        # that is not an error — the operator issued both operations.
        await _offboard(device_id)
        resp = await adapter_client_with_nso.post(
            "/api/v1/devices",
            json={"nso_instance": "nso-dev", "nso_device_name": "path-a-race", "netbox_device_id": 9705},
            headers=AUTH,
        )
        assert resp.status_code == 201
        assert resp.json()["id"] != device_id
        return

    # (ii) a rival holds the device: teardown waits its turn and never interleaves.
    from nso_adapter.core.claim import ClaimUnavailableError

    monkeypatch.setattr(get_config(), "intent_claim_wait_seconds", 0.3)
    holder = await acquire_claim(device_id, "job")
    try:
        with pytest.raises(ClaimUnavailableError):
            await _offboard(device_id)
    finally:
        await release_claim(holder)

    async with session() as db:
        assert await db.get(Device, device_id) is not None
        assert (await db.get(Device, device_id)).nso_device_name == "path-a-race"


async def test_a_device_row_and_its_claim_never_outlive_each_other(adapter_client):
    """The invariant every path above depends on: no claim without a device."""
    device_id = await seed_device(nso_device_name="lo-invariant", netbox_device_id=9706)
    await _seed_queued_apply(device_id)

    await _offboard(device_id)

    async with session() as db:
        orphans = await db.scalar(
            sa.text("SELECT count(*) FROM device_claim c LEFT JOIN devices d ON d.id = c.device_id WHERE d.id IS NULL")
        )
        assert orphans == 0
