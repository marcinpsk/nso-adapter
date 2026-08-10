# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S4 D2: the ``get_read_db`` snapshot dependency (codex R2-1/R3-1).

A family GET assembles multi-SELECT payloads (BGP = router + six graph queries); without
a snapshot, a full-replace commit landing between two of those SELECTs produces a TORN
payload (old parents + new/empty children) that an old authoritative pointer would wave
through the plugin gate. ``get_read_db`` pins ONE read snapshot per request, via a
REPEATABLE READ execution option applied before the first statement.

The control test documents the tear on a PLAIN session (why the dedicated dependency
exists): PostgreSQL's default READ COMMITTED takes a NEW snapshot per statement, so a
commit landing between two SELECTs of the same session is visible to the second. The
guarantee tests prove the dependency serves wholly-old data across that same commit.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nso_adapter.store.models import Device
from tests.conftest import session as store_session


@pytest.fixture
async def snapshot_engine(pg_url):
    """An engine on this test's private clone. The schema is already there (the template
    is built by ``alembic upgrade head``), so there is nothing to create."""
    engine = create_async_engine(pg_url)
    try:
        yield engine
    finally:
        await engine.dispose()


async def _insert_device(engine, name: str) -> None:
    """Commit a row from an INDEPENDENT session — i.e. a second connection, which is what
    makes the commit concurrent with (and invisible to) the reader's open snapshot."""
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as db:
        db.add(Device(nso_device_name=name, netbox_device_id=1, nso_instance="default"))
        await db.commit()


@pytest.mark.anyio
async def test_plain_session_tears_mid_read(snapshot_engine):
    """CONTROL (documents the hazard): a plain session's second SELECT sees a commit that
    landed after its first SELECT — READ COMMITTED re-snapshots per statement."""
    factory = async_sessionmaker(snapshot_engine, expire_on_commit=False)
    async with factory() as reader:
        first = (await reader.execute(select(Device))).scalars().all()
        assert first == []
        await _insert_device(snapshot_engine, "torn")
        second = (await reader.execute(select(Device))).scalars().all()
        assert len(second) == 1, "plain session torn-read expected — the control premise"


@pytest.mark.anyio
async def test_get_read_db_pins_a_snapshot(snapshot_engine):
    """The dependency's session must serve wholly-old data across a mid-read commit:
    REPEATABLE READ pins the snapshot at the first statement."""
    from nso_adapter.api.deps import get_read_db

    factory = async_sessionmaker(snapshot_engine, expire_on_commit=False)
    async with factory() as session:
        gen = get_read_db(session)
        reader = await anext(gen)
        assert (await reader.execute(select(Device))).scalars().all() == []
        await _insert_device(snapshot_engine, "hidden")
        assert (await reader.execute(select(Device))).scalars().all() == [], (
            "REPEATABLE READ snapshot must hide the mid-read commit"
        )
        await gen.aclose()
    # outside the dependency the commit is visible (the snapshot was per-request)
    async with factory() as later:
        assert len((await later.execute(select(Device))).scalars().all()) == 1


@pytest.mark.anyio
async def test_get_read_db_is_read_only_scoped(snapshot_engine):
    """The dependency ends its transaction on exit (rollback) — it never leaves a
    lingering read transaction on the pooled connection."""
    from nso_adapter.api.deps import get_read_db

    factory = async_sessionmaker(snapshot_engine, expire_on_commit=False)
    async with factory() as session:
        gen = get_read_db(session)
        reader = await anext(gen)
        await reader.execute(select(Device))
        await gen.aclose()
        # the same session must be usable normally afterwards
        await session.execute(text("SELECT 1"))


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
async def test_family_get_serves_snapshot_across_midrequest_commit(adapter_client):
    """END-TO-END (SA-3): a REAL static-routes GET whose mirror-rows SELECT races a commit.

    A before_cursor_execute hook fires a SYNC psycopg2 writer on a SECOND connection the
    moment the handler's device_static_route SELECT begins — i.e. AFTER the snapshot opened
    and the pointer was read. The response must serve the pre-commit row set; without
    get_read_db the READ COMMITTED session would let the second row leak in."""
    import sqlalchemy as sa
    from sqlalchemy import event
    from sqlalchemy.engine import make_url

    from nso_adapter.store.db import get_engine
    from tests.conftest import VALID_TOKEN, seed_device

    auth = {"Authorization": f"Bearer {VALID_TOKEN}"}
    device_id = await seed_device(nso_device_name="tear-e2e", netbox_device_id=8951)
    engine = get_engine()
    writer = sa.create_engine(
        make_url(engine.url).set(drivername="postgresql+psycopg2").render_as_string(hide_password=False),
        isolation_level="AUTOCOMMIT",
        poolclass=sa.pool.NullPool,
    )

    async with store_session() as db:
        from datetime import UTC, datetime

        from nso_adapter.store.models import DeviceStaticRoute

        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="10.0.0.0/8",
                next_hop="192.0.2.1",
                last_refreshed_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
                refresh_source="poll",
            )
        )
        await db.commit()

    fired = []

    def _mid_request_write(conn, cursor, statement, parameters, context, executemany):
        if "device_static_route" in statement and statement.lstrip().upper().startswith("SELECT") and not fired:
            fired.append(statement)
            with writer.connect() as wconn:
                wconn.exec_driver_sql(
                    "INSERT INTO device_static_route "
                    "(device_id, vrf, prefix, next_hop, last_refreshed_at, refresh_source) "
                    "VALUES (%s, '', '172.16.0.0/12', '192.0.2.9', '2026-06-01 10:00:00+00', 'poll')",
                    (device_id,),
                )

    event.listen(engine.sync_engine, "before_cursor_execute", _mid_request_write)
    try:
        resp = await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=auth)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", _mid_request_write)
        writer.dispose()

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
    from fastapi.routing import iter_route_contexts

    from nso_adapter.api.deps import get_read_db
    from nso_adapter.main import create_app

    app = create_app()
    by_path = {}
    # FastAPI >= 0.141 keeps each included router nested instead of flattening its
    # APIRoutes into app.routes; iter_route_contexts is the supported traversal.
    for route in iter_route_contexts(app.routes):
        if route.methods == {"GET"}:
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
