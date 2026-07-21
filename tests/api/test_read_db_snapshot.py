# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S4 D2: the ``get_read_db`` snapshot dependency (codex R2-1/R3-1).

A family GET assembles multi-SELECT payloads (BGP = router + six graph queries); without
a snapshot, a full-replace commit landing between two of those SELECTs produces a TORN
payload (old parents + new/empty children) that an old authoritative pointer would wave
through the plugin gate. ``get_read_db`` pins ONE read snapshot per request: PostgreSQL
via a REPEATABLE READ execution option applied before the first statement; SQLite via an
EXPLICIT ``BEGIN`` (sqlite3 legacy mode opens NO transaction for a bare SELECT — proven
by codex's two-connection probe, R3-1).

The control test documents the tear on a PLAIN session (why the dedicated dependency
exists); the guarantee tests prove both dialects serve wholly-old data mid-write.
WAL journaling is enabled on the sqlite scratch DB so the writer can commit while the
reader holds its snapshot (DELETE-mode journaling would block the writer instead).
"""

from __future__ import annotations

import os
import uuid as uuid_mod

import pytest
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nso_adapter.store.models import Base, Device

_PARITY_URL = os.environ.get("ALEMBIC_PARITY_DB_URL")


def _wal_sqlite_engine(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/snap.db")

    @event.listens_for(engine.sync_engine, "connect")
    def _wal(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    return engine


async def _seed_schema(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def _insert_device(engine, name: str) -> None:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as db:
        db.add(Device(nso_device_name=name, netbox_device_id=1, nso_instance="default"))
        await db.commit()


@pytest.mark.anyio
async def test_plain_session_tears_mid_read(tmp_path):
    """CONTROL (documents the hazard): a plain session's second SELECT sees a commit that
    landed after its first SELECT — sqlite legacy mode has no read transaction."""
    engine = _wal_sqlite_engine(tmp_path)
    try:
        await _seed_schema(engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as reader:
            first = (await reader.execute(select(Device))).scalars().all()
            assert first == []
            await _insert_device(engine, "torn")
            second = (await reader.execute(select(Device))).scalars().all()
            assert len(second) == 1, "plain session torn-read expected — the control premise"
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_get_read_db_snapshot_sqlite(tmp_path):
    """The dependency's session must serve wholly-old data across a mid-read commit."""
    from nso_adapter.api.deps import get_read_db

    engine = _wal_sqlite_engine(tmp_path)
    try:
        await _seed_schema(engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            gen = get_read_db(session)
            reader = await anext(gen)
            first = (await reader.execute(select(Device))).scalars().all()
            assert first == []
            await _insert_device(engine, "hidden")
            second = (await reader.execute(select(Device))).scalars().all()
            assert second == [], "get_read_db must pin a snapshot — mid-read commit visible"
            await gen.aclose()
        # outside the dependency the commit is visible (the snapshot was per-request)
        async with factory() as later:
            assert len((await later.execute(select(Device))).scalars().all()) == 1
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.skipif(not _PARITY_URL, reason="ALEMBIC_PARITY_DB_URL not set — PostgreSQL lane (CI only)")
async def test_get_read_db_snapshot_postgresql():
    """Same guarantee on PostgreSQL: REPEATABLE READ pins the snapshot at the first
    statement; a mid-read commit from another connection stays invisible."""
    from sqlalchemy.engine import make_url

    from nso_adapter.api.deps import get_read_db

    scratch = f"snap_{uuid_mod.uuid4().hex[:10]}"
    admin = create_async_engine(_PARITY_URL.replace("+psycopg2", "+asyncpg"), isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.exec_driver_sql(f'CREATE DATABASE "{scratch}"')
    engine = create_async_engine(make_url(_PARITY_URL.replace("+psycopg2", "+asyncpg")).set(database=scratch))
    try:
        await _seed_schema(engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            gen = get_read_db(session)
            reader = await anext(gen)
            assert (await reader.execute(select(Device))).scalars().all() == []
            await _insert_device(engine, "hidden-pg")
            assert (await reader.execute(select(Device))).scalars().all() == [], (
                "REPEATABLE READ snapshot must hide the mid-read commit"
            )
            await gen.aclose()
        async with factory() as later:
            assert len((await later.execute(select(Device))).scalars().all()) == 1
    finally:
        await engine.dispose()
        async with admin.connect() as conn:
            await conn.exec_driver_sql(f'DROP DATABASE "{scratch}" WITH (FORCE)')
        await admin.dispose()


@pytest.mark.anyio
async def test_get_read_db_is_read_only_scoped(tmp_path):
    """The dependency ends its transaction on exit (rollback) — it never leaves a
    lingering read transaction on the pooled connection."""
    from nso_adapter.api.deps import get_read_db

    engine = _wal_sqlite_engine(tmp_path)
    try:
        await _seed_schema(engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            gen = get_read_db(session)
            reader = await anext(gen)
            await reader.execute(select(Device))
            await gen.aclose()
            # the same session must be usable normally afterwards
            await session.execute(text("SELECT 1"))
    finally:
        await engine.dispose()
