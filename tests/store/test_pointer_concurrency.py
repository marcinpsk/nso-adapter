# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Pointer-advance concurrency (READSEM S4 D6).

S4 promotes :class:`RefreshOutcomePointer` to API truth, so its advance must be
DATABASE-atomic. The old protocol (SELECT → compare in Python → UPDATE at commit) has a
lost-update window between its SELECT and its COMMIT: a concurrent session can advance
the pointer inside that window and the late COMMIT overwrites it with an OLDER attempt.
The fix is ``INSERT … ON CONFLICT DO UPDATE … WHERE excluded.attempt_id > attempt_id`` —
the guard evaluates inside the database at write time.

Three tests on ONE lane, all against this test's private PostgreSQL clone: the sequential
contract (a late older terminalization is a no-op), the DETERMINISTIC lost-update
interleave, and atomic publication. The interleave is reproduced with a row lock: a holder
transaction updates the pointer uncommitted (taking the row lock), the victim runs
``record_result`` for an OLDER attempt — its SELECT sees the pre-lock value (MVCC), its
UPDATE then blocks on the lock — and when the holder commits the newer value, the victim's
stale write lands last.

NOTE (verified by probe): SQLAlchemy 2.0 REFRESHES clean identity-map instances on
re-SELECT, so a "stale cached object" cannot produce this bug — only the true
concurrent window can, which is why the red reproduction needs the lock choreography.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nso_adapter.nso.read_outcome import Freshness, Present
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, DeviceStaticRoute, RefreshOutcome, RefreshOutcomePointer


@pytest.fixture
async def pointer_engine(pg_url):
    """An engine on this test's private clone. The schema is already there (the template is
    built by ``alembic upgrade head``), so there is nothing to create."""
    engine = create_async_engine(pg_url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def lock_probe(pg_url):
    """AUTOCOMMIT connection for observing lock waits in ``pg_stat_activity``.

    Its own engine, so polling can never queue behind the very transactions it observes.
    """
    engine = create_async_engine(pg_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            yield conn
    finally:
        await engine.dispose()


async def _seed_device(factory, name: str, netbox_device_id: int) -> int:
    async with factory() as db:
        device = Device(nso_device_name=name, netbox_device_id=netbox_device_id, nso_instance="default")
        db.add(device)
        await db.commit()
        await db.refresh(device)
        return device.id


@pytest.mark.anyio
async def test_late_older_terminalization_is_a_noop(pointer_engine):
    """Sequential contract: after a newer attempt advanced the pointer, an older attempt
    terminalizing from a DIFFERENT session must not move it back."""
    factory = async_sessionmaker(pointer_engine, expire_on_commit=False)
    device_id = await _seed_device(factory, "ptr-seq1", 8821)
    async with factory() as db:
        a_old = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present({"r": []}, Freshness.fresh), refresh_source="poll"
        )
        a_new = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present({"r": []}, Freshness.fresh), refresh_source="poll"
        )
        await db.commit()
    async with factory() as db:
        await outcome_store.record_result(db, a_new, result="replaced", succeeded=True)
    async with factory() as db:
        await outcome_store.record_result(db, a_old, result="kept", succeeded=False)
    async with factory() as db:
        ptr = (
            await db.execute(
                select(RefreshOutcomePointer).where(
                    RefreshOutcomePointer.device_id == device_id,
                    RefreshOutcomePointer.family == "static_route",
                )
            )
        ).scalar_one()
        assert ptr.attempt_id == a_new


@pytest.mark.anyio
async def test_lost_update_window_cannot_regress_pointer(pointer_engine, lock_probe, pg_database):
    """Deterministic lost-update interleave (the R1-F7 scenario):

    1. seed pointer at attempt N (via the normal two-phase store);
    2. HOLDER txn: UPDATE pointer → N+2, uncommitted (row lock held);
    3. VICTIM task: ``record_result(N+1)`` — its SELECT sees N (MVCC, holder uncommitted),
       its write then blocks on the holder's row lock;
    4. holder commits (pointer = N+2) → victim unblocks and finishes LAST.

    With compare-in-Python the victim's stale decision overwrites N+2 with N+1 — the
    regression. The atomic upsert's WHERE guard re-evaluates against the committed N+2
    inside the database, so the victim's write must be a no-op.
    """
    factory = async_sessionmaker(pointer_engine, expire_on_commit=False)
    device_id = await _seed_device(factory, "pg-race", 8831)

    async with factory() as db:
        a_base = await outcome_store.record_read_outcome(
            db, device_id, "bgp", Present({"routers": []}, Freshness.fresh), refresh_source="poll"
        )
        await outcome_store.record_result(db, a_base, result="replaced", succeeded=True)
        a_victim = await outcome_store.record_read_outcome(
            db, device_id, "bgp", Present({"routers": []}, Freshness.fresh), refresh_source="poll"
        )
        a_holder = await outcome_store.record_read_outcome(
            db, device_id, "bgp", Present({"routers": []}, Freshness.fresh), refresh_source="poll"
        )
        await db.commit()

    async with pointer_engine.connect() as holder:
        await holder.execute(text("BEGIN"))
        await holder.execute(
            text("UPDATE refresh_outcome SET result='replaced', succeeded=true, completed_at=now() WHERE id=:aid"),
            {"aid": a_holder},
        )
        await holder.execute(
            text("UPDATE refresh_outcome_pointer SET attempt_id=:aid WHERE device_id=:did AND family='bgp'"),
            {"aid": a_holder, "did": device_id},
        )  # row lock now held, update uncommitted

        async def victim():
            async with factory() as db_v:
                await outcome_store.record_result(db_v, a_victim, result="kept", succeeded=False)

        victim_task = asyncio.create_task(victim())
        # SA-5: "task not done after a sleep" does not prove the victim passed its SELECT —
        # synchronize on the OBSERVED lock wait: poll pg_stat_activity until a backend in
        # this clone is waiting on a Lock (the victim's write blocked on the holder's row
        # lock). Only then is the interleave proven.
        for _ in range(100):
            waiting = (
                await lock_probe.exec_driver_sql(
                    "SELECT count(*) FROM pg_stat_activity "
                    f"WHERE datname = '{pg_database}' AND wait_event_type = 'Lock'"
                )
            ).scalar()
            if waiting:
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("victim never reached the blocked write")
        assert not victim_task.done()
        await holder.execute(text("COMMIT"))  # pointer = a_holder, victim unblocks
        await asyncio.wait_for(victim_task, timeout=10)

    async with factory() as db:
        ptr = (
            await db.execute(
                select(RefreshOutcomePointer).where(
                    RefreshOutcomePointer.device_id == device_id,
                    RefreshOutcomePointer.family == "bgp",
                )
            )
        ).scalar_one()
        assert ptr.attempt_id == a_holder, (
            "the lost-update window regressed the pointer — the advance must be "
            "database-atomic (ON CONFLICT ... WHERE), not compare-in-Python"
        )


@pytest.mark.anyio
async def test_payload_and_revision_publish_or_rollback_together(pointer_engine):
    """Simulate crash-before-commit and success with two real PostgreSQL sessions.

    The C1 atomic-publication invariant: mirror rows + pointer + payload_revision become
    visible together or not at all, which only the session holding the mirror writes can do.
    """
    factory = async_sessionmaker(pointer_engine, expire_on_commit=False)
    async with factory() as db:
        device = Device(nso_device_name="publication", netbox_device_id=8841, nso_instance="default")
        db.add(device)
        await db.flush()
        device_id = device.id
        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="198.18.10.0/24",
                next_hop="198.18.0.1",
                refresh_source="poll",
            )
        )
        base = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present({"route": []}), refresh_source="poll"
        )
        await outcome_store.record_result(
            db, base, result="replaced", succeeded=True, row_count=1, publish_payload=True
        )
        attempt = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present({"route": []}), refresh_source="poll"
        )
        await db.commit()

    async with factory() as publisher:
        row = await publisher.get(RefreshOutcome, attempt)
        await outcome_store.acquire_family_fence(publisher, device_id, "static_route")
        await publisher.execute(delete(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id))
        publisher.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="198.18.20.0/24",
                next_hop="198.18.0.2",
                refresh_source="poll",
            )
        )
        assert await outcome_store.stage_result(
            publisher, row, result="replaced", succeeded=True, row_count=1, publish_payload=True
        )

        async with factory() as observer:
            assert await observer.scalar(select(DeviceStaticRoute.prefix)) == "198.18.10.0/24"
            pointer = await observer.scalar(select(RefreshOutcomePointer))
            assert (pointer.attempt_id, pointer.payload_revision) == (base, base)

        await publisher.rollback()  # simulated process death before COMMIT

    async with factory() as observer:
        assert await observer.scalar(select(DeviceStaticRoute.prefix)) == "198.18.10.0/24"
        pointer = await observer.scalar(select(RefreshOutcomePointer))
        assert (pointer.attempt_id, pointer.payload_revision) == (base, base)

    async with factory() as publisher:
        row = await publisher.get(RefreshOutcome, attempt)
        await outcome_store.acquire_family_fence(publisher, device_id, "static_route")
        await publisher.execute(delete(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id))
        publisher.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="198.18.20.0/24",
                next_hop="198.18.0.2",
                refresh_source="poll",
            )
        )
        assert await outcome_store.stage_result(
            publisher, row, result="replaced", succeeded=True, row_count=1, publish_payload=True
        )
        await publisher.commit()

    async with factory() as observer:
        assert await observer.scalar(select(DeviceStaticRoute.prefix)) == "198.18.20.0/24"
        pointer = await observer.scalar(select(RefreshOutcomePointer))
        assert (pointer.attempt_id, pointer.payload_revision) == (attempt, attempt)
