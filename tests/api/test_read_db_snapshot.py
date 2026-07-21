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


# ── SA-3: END-TO-END coverage — real HTTP family GETs, not just the dependency ─────────


_FAMILY_GET_PATHS = {
    # canonical family key → the family GET path (device id formatted in)
    "static_route": "/api/v1/devices/{id}/static-routes",
    "redistribution": "/api/v1/devices/{id}/redistribution",
    "bgp": "/api/v1/devices/{id}/bgp-config",
    "isis": "/api/v1/devices/{id}/isis-interfaces",
    "ospf": "/api/v1/devices/{id}/ospf",
    "route_policy": "/api/v1/devices/{id}/route-policy",
    "bfd": "/api/v1/devices/{id}/bfd",
    "l2_service": "/api/v1/devices/{id}/l2-services",
    "vlan": "/api/v1/devices/{id}/vlan-database",
    "switchport": "/api/v1/devices/{id}/switchport",
    "svi": "/api/v1/devices/{id}/svi",
    "subinterface": "/api/v1/devices/{id}/subinterface",
    "interface_mtu": "/api/v1/devices/{id}/interface-mtu",
    "interface_ip": "/api/v1/devices/{id}/interface-ips",
    "snmp": "/api/v1/devices/{id}/snmp-config",
    "logging": "/api/v1/devices/{id}/logging-config",
    "lag_config": "/api/v1/devices/{id}/lag-config",
    "lag": "/api/v1/devices/{id}/lag-topology",
    "interface_attributes": "/api/v1/devices/{id}/interfaces-doc",
}


@pytest.mark.anyio
async def test_family_get_serves_snapshot_across_midrequest_commit(adapter_client, tmp_path):
    """END-TO-END (SA-3): a REAL static-routes GET whose mirror-rows SELECT races a commit.

    A before_cursor_execute hook fires a SYNC sqlite writer (WAL) the moment the handler's
    device_static_route SELECT begins — i.e. AFTER the snapshot opened and the pointer was
    read. The response must serve the pre-commit row set; without get_read_db the second
    row leaks in (sqlite legacy mode has no read transaction — the checked-in control)."""
    import sqlite3

    from sqlalchemy import event

    from nso_adapter.store.db import get_engine, get_session
    from tests.conftest import VALID_TOKEN, seed_device

    auth = {"Authorization": f"Bearer {VALID_TOKEN}"}
    device_id = await seed_device(nso_device_name="tear-e2e", netbox_device_id=8951)
    db_path = None
    engine = get_engine()
    db_path = engine.url.database
    # WAL so the mid-request writer can commit while the request's read txn is open.
    sync = sqlite3.connect(db_path, timeout=5)
    sync.execute("PRAGMA journal_mode=WAL")
    sync.commit()

    async for db in get_session():
        from datetime import datetime

        from nso_adapter.store.models import DeviceStaticRoute

        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="10.0.0.0/8",
                next_hop="192.0.2.1",
                last_refreshed_at=datetime(2026, 6, 1, 10, 0, 0),
                refresh_source="poll",
            )
        )
        await db.commit()
        break

    fired = []

    def _mid_request_write(conn, cursor, statement, parameters, context, executemany):
        if "device_static_route" in statement and statement.lstrip().upper().startswith("SELECT") and not fired:
            fired.append(statement)
            sync.execute(
                "INSERT INTO device_static_route "
                "(device_id, vrf, prefix, next_hop, last_refreshed_at, refresh_source) "
                "VALUES (?, '', '172.16.0.0/12', '192.0.2.9', '2026-06-01 10:00:00', 'poll')",
                (device_id,),
            )
            sync.commit()

    event.listen(engine.sync_engine, "before_cursor_execute", _mid_request_write)
    try:
        resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=auth)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", _mid_request_write)
        sync.close()

    assert resp.status_code == 200
    assert fired, "the hook must have observed the mirror-rows SELECT"
    prefixes = [r["prefix"] for r in resp.json()["routes"]]
    assert prefixes == ["10.0.0.0/8"], (
        f"mid-request commit leaked into the response ({prefixes}) — the family GET is not "
        "running on the get_read_db snapshot"
    )


@pytest.mark.anyio
async def test_every_family_get_reads_pointer_before_rows(adapter_client):
    """SA-3 sweep: for EVERY family GET, the refresh_outcome_pointer SELECT must precede
    any mirror-table SELECT (the benign-direction ordering, D2) — a family whose handler
    moves its pointer fetch after the rows regresses here, not in prod."""
    from sqlalchemy import event

    from nso_adapter.store.db import get_engine
    from tests.conftest import VALID_TOKEN, seed_device

    auth = {"Authorization": f"Bearer {VALID_TOKEN}"}
    device_id = await seed_device(nso_device_name="tear-order", netbox_device_id=8952)
    engine = get_engine()

    for family, path in _FAMILY_GET_PATHS.items():
        statements: list[str] = []

        def _record(conn, cursor, statement, parameters, context, executemany, _s=statements):
            _s.append(statement)

        event.listen(engine.sync_engine, "before_cursor_execute", _record)
        try:
            resp = await adapter_client.get(path.format(id=device_id), headers=auth)
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", _record)
        assert resp.status_code == 200, f"{family}: {resp.status_code}"

        pointer_idx = next((i for i, s in enumerate(statements) if "refresh_outcome_pointer" in s), None)
        assert pointer_idx is not None, f"{family}: no pointer SELECT — read_state not resolved from the store"
        mirror_selects = [
            i
            for i, s in enumerate(statements)
            if s.lstrip().upper().startswith("SELECT")
            and "refresh_outcome" not in s
            and "devices" not in s.split("FROM")[-1][:60]
        ]
        early_mirror = [i for i in mirror_selects if i < pointer_idx]
        assert not early_mirror, (
            f"{family}: mirror SELECT(s) at {early_mirror} precede the pointer SELECT at "
            f"{pointer_idx} — rows-before-pointer breaks the benign direction"
        )


@pytest.mark.anyio
async def test_every_family_get_depends_on_get_read_db(adapter_client):
    """SA-3 no-bypass: every family GET route must inject get_read_db (a silent revert to
    get_db keeps every other test green while dropping the snapshot guarantee)."""
    from nso_adapter.api.deps import get_read_db
    from nso_adapter.main import create_app

    app = create_app()
    by_path = {}
    for route in app.routes:
        if getattr(route, "methods", None) == {"GET"}:
            by_path[route.path] = route

    for family, path_tpl in _FAMILY_GET_PATHS.items():
        path = path_tpl.replace("{id}", "{device_id}")
        route = by_path.get(path)
        assert route is not None, f"{family}: route {path} not found"
        flat = set(_walk_dependant(route.dependant))
        assert get_read_db in flat, f"{family}: GET {path} does not inject get_read_db"


def _walk_dependant(dependant):
    out = []
    for d in dependant.dependencies:
        out.append(d.call)
        out.extend(_walk_dependant(d))
    return out
