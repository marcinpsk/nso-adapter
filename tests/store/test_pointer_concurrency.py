# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Pointer-advance concurrency (READSEM S4 D6).

S4 promotes :class:`RefreshOutcomePointer` to API truth, so its advance must be
DATABASE-atomic. The old protocol (SELECT → compare in Python → UPDATE at commit) has a
lost-update window between its SELECT and its COMMIT: a concurrent session can advance
the pointer inside that window and the late COMMIT overwrites it with an OLDER attempt.
The fix is a dialect-branched ``INSERT … ON CONFLICT DO UPDATE … WHERE excluded.attempt_id
> attempt_id`` — the guard evaluates inside the database at write time.

The PostgreSQL test (gated on ``ALEMBIC_PARITY_DB_URL``, like the schema-parity lane)
reproduces the window DETERMINISTICALLY with a row lock: a holder transaction updates the
pointer uncommitted (taking the row lock), the victim runs ``record_result`` for an OLDER
attempt — its SELECT sees the pre-lock value (MVCC), its UPDATE then blocks on the lock —
and when the holder commits the newer value, the victim's stale write lands last.
The sqlite test pins the sequential contract on the default lane (both dialects share the
upsert guard; sqlite's file lock cannot host the deterministic interleave).

NOTE (verified by probe): SQLAlchemy 2.0 REFRESHES clean identity-map instances on
re-SELECT, so a "stale cached object" cannot produce this bug — only the true
concurrent window can, which is why the red reproduction needs the lock choreography.
"""

from __future__ import annotations

import asyncio
import os
import uuid as uuid_mod

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nso_adapter.nso.read_outcome import Freshness, Present
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Base, Device, DeviceStaticRoute, RefreshOutcome, RefreshOutcomePointer
from tests.conftest import seed_device, session

_PARITY_URL = os.environ.get("ALEMBIC_PARITY_DB_URL")


@pytest.mark.anyio
async def test_late_older_terminalization_is_a_noop(adapter_client):
    """Sequential contract, default lane: after a newer attempt advanced the pointer, an
    older attempt terminalizing from a DIFFERENT session must not move it back."""
    device_id = await seed_device(nso_device_name="ptr-seq1", netbox_device_id=8821)
    async with session() as db:
        a_old = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present({"r": []}, Freshness.fresh), refresh_source="poll"
        )
        a_new = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present({"r": []}, Freshness.fresh), refresh_source="poll"
        )
        await db.commit()
    async with session() as db:
        await outcome_store.record_result(db, a_new, result="replaced", succeeded=True)
    async with session() as db:
        await outcome_store.record_result(db, a_old, result="kept", succeeded=False)
    async with session() as db:
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
@pytest.mark.skipif(not _PARITY_URL, reason="ALEMBIC_PARITY_DB_URL not set — PostgreSQL lane (CI only)")
async def test_pg_lost_update_window_cannot_regress_pointer():
    """REAL PostgreSQL, deterministic lost-update interleave (the R1-F7 scenario):

    1. seed pointer at attempt N (via the normal two-phase store);
    2. HOLDER txn: UPDATE pointer → N+2, uncommitted (row lock held);
    3. VICTIM task: ``record_result(N+1)`` — its SELECT sees N (MVCC, holder uncommitted),
       its write then blocks on the holder's row lock;
    4. holder commits (pointer = N+2) → victim unblocks and finishes LAST.

    With compare-in-Python the victim's stale decision overwrites N+2 with N+1 — the
    regression. The atomic upsert's WHERE guard re-evaluates against the committed N+2
    inside the database, so the victim's write must be a no-op.
    """
    scratch = f"ptr_race_{uuid_mod.uuid4().hex[:10]}"
    admin = create_async_engine(_PARITY_URL.replace("+psycopg2", "+asyncpg"), isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.exec_driver_sql(f'CREATE DATABASE "{scratch}"')
    from sqlalchemy.engine import make_url

    # NB: keep the URL an OBJECT — str() on a SQLAlchemy URL masks the password as
    # literal '***', which then fails auth on the scratch connect.
    scratch_url = make_url(_PARITY_URL.replace("+psycopg2", "+asyncpg")).set(database=scratch)
    engine = create_async_engine(scratch_url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)

        async with factory() as db:
            device = Device(nso_device_name="pg-race", netbox_device_id=8831, nso_instance="default")
            db.add(device)
            await db.commit()
            await db.refresh(device)
            device_id = device.id

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

        async with engine.connect() as holder:
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
            # SA-5: "task not done after a sleep" does not prove the victim passed its
            # SELECT — synchronize on the OBSERVED lock wait: poll pg_stat_activity until
            # a backend in this scratch DB is waiting on a Lock (the victim's write
            # blocked on the holder's row lock). Only then is the interleave proven.
            async with admin.connect() as probe:
                for _ in range(100):
                    waiting = (
                        await probe.exec_driver_sql(
                            "SELECT count(*) FROM pg_stat_activity "
                            f"WHERE datname = '{scratch}' AND wait_event_type = 'Lock'"
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
    finally:
        await engine.dispose()
        async with admin.connect() as conn:
            await conn.exec_driver_sql(f'DROP DATABASE "{scratch}" WITH (FORCE)')
        await admin.dispose()


@pytest.mark.anyio
@pytest.mark.skipif(not _PARITY_URL, reason="ALEMBIC_PARITY_DB_URL not set — PostgreSQL lane (CI only)")
async def test_pg_payload_and_revision_publish_or_rollback_together():
    """Simulate crash-before-commit and success with two real PostgreSQL sessions."""
    from sqlalchemy.engine import make_url

    scratch = f"publication_{uuid_mod.uuid4().hex[:10]}"
    admin = create_async_engine(_PARITY_URL.replace("+psycopg2", "+asyncpg"), isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.exec_driver_sql(f'CREATE DATABASE "{scratch}"')
    scratch_url = make_url(_PARITY_URL.replace("+psycopg2", "+asyncpg")).set(database=scratch)
    engine = create_async_engine(scratch_url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)
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
    finally:
        await engine.dispose()
        async with admin.connect() as conn:
            await conn.exec_driver_sql(f'DROP DATABASE "{scratch}" WITH (FORCE)')
        await admin.dispose()
