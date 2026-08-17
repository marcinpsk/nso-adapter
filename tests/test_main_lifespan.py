# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the lifespan helpers extracted from nso_adapter.main.

These cover the pieces the ``adapter_client`` fixture never exercises (it runs
lifespan with no NSO instances and SSE disabled): the per-instance NSO client
loop, the SSE event-dispatch fan-out + its config gating, stream startup, and
the teardown tail. Everything runs the real helpers; collaborators that are
already tested elsewhere (the surface refreshers, the persistent SSE subscriber)
are replaced with small async fakes, never MagicMocks. S5a D: the per-event
dispatch is a coalesced comprehensive refresh per changed device (the former
nine-handler fan-out is gone).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nso_adapter.main import (
    _build_netbox_client,
    _build_nso_clients,
    _close_netbox,
    _dispose_engine,
    _init_database,
    _make_sse_event_handler,
    _shutdown_sse,
    _start_sse_streams,
)
from tests.conftest import session


def _scheduler(**flags):
    base = {
        "enable_interface_ip_sync": False,
        "enable_snmp_sync": False,
        "enable_interface_mtu_sync": False,
        "enable_nso_streams": False,
    }
    base.update(flags)
    return SimpleNamespace(**base)


class _Provider:
    """Minimal secrets provider: echoes a deterministic value per ref."""

    def __init__(self):
        self.asked: list[str] = []

    def get(self, ref):
        self.asked.append(ref)
        return f"val-{ref}"


def _instance(name):
    return SimpleNamespace(
        name=name,
        base_url=f"http://{name}:8080/",
        host_header=None,
        ca_cert=None,
        username_ref=f"{name}_USER",
        password_ref=f"{name}_PASS",
    )


# --------------------------------------------------------------------------- #
# _dispatch_netconf_change — covered by the S5a D coalesced-dispatch tests below
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# S5a D — coalesced comprehensive dispatch (replaces the nine-handler fan-out)
# --------------------------------------------------------------------------- #


def _sse_event(*names):
    return {
        "ietf-restconf:notification": {
            "netconf-config-change": {
                "edit": [{"target": f"/ncs:devices/device[name='{n}']/config/x", "operation": "replace"} for n in names]
            }
        }
    }


async def _seed_sse_device(name="sse-rtr", instance="nso-dev", netbox_id=8801):
    from nso_adapter.store.models import Device as _Device

    async with session() as db:
        d = _Device(nso_instance=instance, nso_device_name=name, netbox_device_id=netbox_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id


async def _drain_coalescer(tasks: set[asyncio.Task]) -> None:
    while tasks:
        await asyncio.gather(*list(tasks), return_exceptions=True)


async def test_dispatch_runs_one_comprehensive_refresh_per_changed_device(adapter_client, monkeypatch):
    """S5a D (RED vs the nine-handler fan-out): a change event triggers ONE grain-b
    comprehensive projected refresh for the device — every family, one doc GET —
    instead of nine per-family section refreshes."""
    from nso_adapter import main as main_mod

    device_id = await _seed_sse_device("sse-rtr-1", netbox_id=8802)
    calls: list[tuple[int, str, bool]] = []

    async def rec_refresh(db, device, client, *, refresh_source, atomic=False):
        calls.append((device.id, refresh_source, atomic))
        return [], None

    monkeypatch.setattr("nso_adapter.core.importer.refresh_all_surfaces_for_device", rec_refresh)

    tasks: set[asyncio.Task] = set()
    coalescer = main_mod._DeviceRefreshCoalescer({"nso-dev": object()}, tasks, tasks.discard)
    cfg = SimpleNamespace(scheduler=_scheduler())
    async with session() as db:
        await main_mod._dispatch_netconf_change(cfg, _sse_event("sse-rtr-1"), db, {"nso-dev": object()}, coalescer)
    await _drain_coalescer(tasks)

    assert calls == [(device_id, "notification", False)]


async def test_dispatch_scopes_to_the_handler_instance_map(adapter_client, monkeypatch):
    """codex R1-F8: a same-name device on an UNMAPPED instance is untouched."""
    from nso_adapter import main as main_mod

    await _seed_sse_device("sse-dup", instance="nso-other", netbox_id=8803)
    calls: list[int] = []

    async def rec_refresh(db, device, client, *, refresh_source, atomic=False):
        calls.append(device.id)
        return [], None

    monkeypatch.setattr("nso_adapter.core.importer.refresh_all_surfaces_for_device", rec_refresh)

    tasks: set[asyncio.Task] = set()
    coalescer = main_mod._DeviceRefreshCoalescer({"nso-dev": object()}, tasks, tasks.discard)
    async with session() as db:
        await main_mod._dispatch_netconf_change(
            SimpleNamespace(scheduler=_scheduler()), _sse_event("sse-dup"), db, {"nso-dev": object()}, coalescer
        )
    await _drain_coalescer(tasks)

    assert calls == []


async def test_coalescer_merges_bursts_into_one_rerun(adapter_client, monkeypatch):
    """A trigger landing mid-refresh sets the dirty edge — exactly ONE rerun, even for
    several rapid triggers (the R6-4 bound)."""
    from nso_adapter import main as main_mod

    device_id = await _seed_sse_device("sse-burst", netbox_id=8804)
    starts: list[int] = []
    release = asyncio.Event()

    async def slow_refresh(db, device, client, *, refresh_source, atomic=False):
        starts.append(device.id)
        await release.wait()
        return [], None

    monkeypatch.setattr("nso_adapter.core.importer.refresh_all_surfaces_for_device", slow_refresh)

    tasks: set[asyncio.Task] = set()
    coalescer = main_mod._DeviceRefreshCoalescer({"nso-dev": object()}, tasks, tasks.discard)
    coalescer.trigger(device_id, "nso-dev", None)
    await asyncio.sleep(0.02)  # first refresh is in flight
    coalescer.trigger(device_id, "nso-dev", None)
    coalescer.trigger(device_id, "nso-dev", None)
    coalescer.trigger(device_id, "nso-dev", None)
    release.set()
    await _drain_coalescer(tasks)

    assert starts == [device_id, device_id]  # one in-flight + exactly one rerun


async def test_coalescer_consumes_dirty_edge_after_a_failing_refresh(adapter_client, monkeypatch):
    """codex R2-F5: event B arrives while refresh A is failing — the dirty edge must be
    consumed by an immediate rerun, not stranded until event C."""
    from nso_adapter import main as main_mod

    device_id = await _seed_sse_device("sse-fail", netbox_id=8805)
    attempts: list[int] = []
    gate = asyncio.Event()

    async def flaky_refresh(db, device, client, *, refresh_source, atomic=False):
        attempts.append(1)
        if len(attempts) == 1:
            await gate.wait()
            raise RuntimeError("refresh A boom")
        return [], None

    monkeypatch.setattr("nso_adapter.core.importer.refresh_all_surfaces_for_device", flaky_refresh)

    tasks: set[asyncio.Task] = set()
    coalescer = main_mod._DeviceRefreshCoalescer({"nso-dev": object()}, tasks, tasks.discard)
    coalescer.trigger(device_id, "nso-dev", None)
    await asyncio.sleep(0.02)
    coalescer.trigger(device_id, "nso-dev", None)  # B lands while A is mid-flight
    gate.set()  # A now fails
    await _drain_coalescer(tasks)

    assert len(attempts) == 2, "the dirty edge must rerun despite A's failure"


async def test_coalescer_notifies_once_per_completed_refresh(adapter_client, monkeypatch):
    """One notify per completed run (after the refresh, before the dirty check); a notify
    failure is swallowed."""
    from nso_adapter import main as main_mod
    from nso_adapter.core import importer as imp

    device_id = await _seed_sse_device("sse-notify", netbox_id=8806)

    async def ok_refresh(db, device, client, *, refresh_source, atomic=False):
        return [], None

    monkeypatch.setattr("nso_adapter.core.importer.refresh_all_surfaces_for_device", ok_refresh)

    notified: list[int] = []

    class _NB:
        async def notify_sync_complete(self, nb_id):
            notified.append(nb_id)
            raise RuntimeError("plugin down")  # must be swallowed

    monkeypatch.setattr(imp, "_netbox_client", _NB())

    tasks: set[asyncio.Task] = set()
    coalescer = main_mod._DeviceRefreshCoalescer({"nso-dev": object()}, tasks, tasks.discard)
    coalescer.trigger(device_id, "nso-dev", 8806)
    await _drain_coalescer(tasks)

    assert notified == [8806]


async def test_coalescer_child_is_shutdown_cancellable(adapter_client, monkeypatch):
    """codex R1-F6: the REAL refresh task is registered in dispatch_tasks — shutdown
    cancellation reaches it (not just the outer per-event task)."""
    from nso_adapter import main as main_mod

    device_id = await _seed_sse_device("sse-shutdown", netbox_id=8807)
    entered = asyncio.Event()

    async def hanging_refresh(db, device, client, *, refresh_source, atomic=False):
        entered.set()
        await asyncio.sleep(3600)

    monkeypatch.setattr("nso_adapter.core.importer.refresh_all_surfaces_for_device", hanging_refresh)

    tasks: set[asyncio.Task] = set()
    coalescer = main_mod._DeviceRefreshCoalescer({"nso-dev": object()}, tasks, tasks.discard)
    coalescer.trigger(device_id, "nso-dev", None)
    await asyncio.wait_for(entered.wait(), timeout=5.0)
    assert len(tasks) == 1
    for t in list(tasks):
        t.cancel()
    await asyncio.gather(*list(tasks), return_exceptions=True)
    # a fresh trigger after the cancel starts a new run (latch not wedged)
    assert coalescer._state[device_id]["running"] is False


# --------------------------------------------------------------------------- #
# _make_sse_event_handler — None-guard + schedules a dispatch task
# --------------------------------------------------------------------------- #


async def test_sse_handler_ignores_unparseable_frame(monkeypatch):
    dispatched: list = []

    async def rec_dispatch(cfg, parsed, db, clients, coalescer):
        dispatched.append(parsed)

    monkeypatch.setattr("nso_adapter.main._dispatch_netconf_change", rec_dispatch)
    handler = _make_sse_event_handler(SimpleNamespace(scheduler=_scheduler()), {"i": object()}, set())

    before = asyncio.all_tasks()
    assert handler("raw-frame", None) is None
    new = [t for t in asyncio.all_tasks() if t not in before and t is not asyncio.current_task()]

    assert new == []
    assert dispatched == []


async def test_sse_handler_dispatches_parsed_frame(monkeypatch):
    dispatched: list = []

    async def rec_dispatch(cfg, parsed, db, clients, coalescer):
        dispatched.append((parsed, db, coalescer))

    monkeypatch.setattr("nso_adapter.main._dispatch_netconf_change", rec_dispatch)

    async def fake_session():
        yield "DB-SESSION"

    monkeypatch.setattr("nso_adapter.main.get_session", fake_session)
    dispatch_tasks: set[asyncio.Task] = set()
    handler = _make_sse_event_handler(SimpleNamespace(scheduler=_scheduler()), {"i": object()}, dispatch_tasks)

    before = asyncio.all_tasks()
    handler("raw-frame", {"k": 1})
    spawned = [t for t in asyncio.all_tasks() if t not in before and t is not asyncio.current_task()]
    assert len(spawned) == 1
    assert len(dispatch_tasks) == 1  # retained (not GC'd), trackable for shutdown
    await asyncio.gather(*spawned)
    assert len(dispatched) == 1
    assert dispatched[0][0] == {"k": 1}
    assert dispatched[0][1] == "DB-SESSION"
    assert dispatched[0][2] is not None  # the lifespan-owned coalescer rides along
    assert dispatch_tasks == set()  # done-callback discards


async def test_sse_handler_logs_and_does_not_leak_failed_dispatch(monkeypatch):
    async def boom_dispatch(cfg, parsed, db, clients, coalescer):
        raise RuntimeError("dispatch boom")

    monkeypatch.setattr("nso_adapter.main._dispatch_netconf_change", boom_dispatch)

    async def fake_session():
        yield "DB-SESSION"

    monkeypatch.setattr("nso_adapter.main.get_session", fake_session)
    dispatch_tasks: set[asyncio.Task] = set()
    handler = _make_sse_event_handler(SimpleNamespace(scheduler=_scheduler()), {"i": object()}, dispatch_tasks)

    handler("raw-frame", {"k": 1})
    await asyncio.gather(*list(dispatch_tasks), return_exceptions=True)
    for _ in range(3):
        await asyncio.sleep(0)  # let the done-callback run
    assert dispatch_tasks == set()  # failed task discarded, not leaked


# --------------------------------------------------------------------------- #
# _build_nso_clients — per-instance construction + importer registration
# --------------------------------------------------------------------------- #


@pytest.fixture
def clean_nso_registry():
    from nso_adapter.core import importer

    snapshot = dict(importer._nso_clients)
    yield
    importer._nso_clients.clear()
    importer._nso_clients.update(snapshot)


def test_build_nso_clients_constructs_and_registers(clean_nso_registry):
    from nso_adapter.core.importer import get_nso_client

    cfg = SimpleNamespace(nso_instances=[_instance("nso-a"), _instance("nso-b")])
    provider = _Provider()

    clients = _build_nso_clients(cfg, provider)

    assert set(clients) == {"nso-a", "nso-b"}
    # each was registered with the importer under its name
    assert get_nso_client("nso-a") is clients["nso-a"]
    assert get_nso_client("nso-b") is clients["nso-b"]
    # credentials were resolved through the provider, both refs per instance
    assert provider.asked == ["nso-a_USER", "nso-a_PASS", "nso-b_USER", "nso-b_PASS"]


def test_build_nso_clients_empty_is_noop(clean_nso_registry):
    assert _build_nso_clients(SimpleNamespace(nso_instances=[]), _Provider()) == {}


# --------------------------------------------------------------------------- #
# _start_sse_streams — disabled short-circuit + enabled per-instance startup
# --------------------------------------------------------------------------- #


def test_start_sse_streams_disabled_returns_empty():
    cfg = SimpleNamespace(scheduler=_scheduler(enable_nso_streams=False), nso_instances=[_instance("x")])
    assert _start_sse_streams(cfg, _Provider(), {}, asyncio.Event(), set()) == []


async def test_start_sse_streams_enabled_spawns_one_task_per_instance(monkeypatch):
    seen: list[tuple[str, object]] = []

    async def fake_persistent_subscriber(subscriber, stream_url, handler, stop_event):
        seen.append((stream_url, subscriber))
        await stop_event.wait()

    monkeypatch.setattr("nso_adapter.main.persistent_subscriber", fake_persistent_subscriber)

    inst = _instance("nso-dev")
    cfg = SimpleNamespace(scheduler=_scheduler(enable_nso_streams=True), nso_instances=[inst])
    nso_clients = {"nso-dev": object()}
    stop = asyncio.Event()

    tasks = _start_sse_streams(cfg, _Provider(), nso_clients, stop, set())
    try:
        assert len(tasks) == 1
        await asyncio.sleep(0)  # let the subscriber coroutine start
        assert seen and seen[0][0] == "http://nso-dev:8080/restconf/streams/NETCONF/json"
    finally:
        stop.set()
        for t in tasks:
            await asyncio.wait_for(t, timeout=1.0)


# --------------------------------------------------------------------------- #
# teardown tail — _shutdown_sse / _close_netbox / _dispose_engine
# --------------------------------------------------------------------------- #


async def test_shutdown_sse_sets_stop_and_cancels_tasks():
    stop = asyncio.Event()

    async def runner():
        await asyncio.sleep(30)

    task = asyncio.ensure_future(runner())
    await asyncio.sleep(0)  # let it start running

    await _shutdown_sse(stop, [task], set())

    assert stop.is_set()
    assert task.done()
    assert task.cancelled()


async def test_shutdown_sse_cancels_in_flight_dispatch_tasks():
    """In-flight dispatch tasks are cancelled + drained at shutdown, not orphaned (s3-12)."""
    stop = asyncio.Event()

    async def runner():
        await asyncio.sleep(30)

    dispatch = asyncio.ensure_future(runner())
    await asyncio.sleep(0)
    await _shutdown_sse(stop, [], {dispatch})
    assert dispatch.cancelled()


async def test_shutdown_sse_no_tasks_just_signals():
    stop = asyncio.Event()
    await _shutdown_sse(stop, [], set())
    assert stop.is_set()


async def test_close_netbox_awaits_awaitable_aclose():
    class _AsyncClient:
        def __init__(self):
            self.closed = False

        async def _close(self):
            self.closed = True

        def aclose(self):
            return self._close()

    client = _AsyncClient()
    await _close_netbox(client)
    assert client.closed is True


async def test_close_netbox_tolerates_sync_aclose():
    sentinel = object()

    class _SyncClient:
        def __init__(self):
            self.calls = 0

        def aclose(self):
            # Mirrors a MagicMock-style client whose aclose() returns a plain,
            # non-awaitable value — the isawaitable guard must skip awaiting it.
            self.calls += 1
            return sentinel

    client = _SyncClient()
    await _close_netbox(client)  # non-awaitable return must not break teardown
    assert client.calls == 1


async def test_dispose_engine_noop_when_unset(monkeypatch):
    monkeypatch.setattr("nso_adapter.main.get_engine", lambda: None)
    await _dispose_engine()  # must not raise


async def test_dispose_engine_disposes_real_engine(monkeypatch, pg_url):
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(pg_url)
    monkeypatch.setattr("nso_adapter.main.get_engine", lambda: engine)

    await _dispose_engine()  # real engine, real dispose


@pytest.fixture
def clean_netbox_registry():
    from nso_adapter.core import importer

    snapshot = importer._netbox_client
    yield
    importer._netbox_client = snapshot


async def test_lifespan_binds_the_configured_database(clean_netbox_registry, pg_url, tmp_path, monkeypatch):
    """The lifespan run that keeps init_db/_dispose_engine real — the subject here.

    ``adapter_client`` patches both out (``store_engine`` owns the process globals, and
    a lifespan disposal there would orphan a sibling session's checked-out connection),
    so this is the only place proving the real lifespan reads ``database_url`` from the
    config file, binds THAT database, and disposes the engine on exit.

    The scheduler is the one thing kept out. ``start_scheduler`` fires an immediate sync
    kick and ``stop_scheduler`` deliberately does not wait for an in-flight tick, so that
    tick can still hold a connection when the per-test database is dropped — a straggler
    that fails the teardown leak check on a loaded runner. Nothing here waits for it either:
    a fire-and-forget job cannot be drained, so it must not be started.
    """
    from sqlalchemy.engine import make_url

    from nso_adapter import main as main_mod
    from nso_adapter.config import reset_config
    from nso_adapter.main import create_app
    from nso_adapter.store import db as store_db
    from tests.conftest import _write_config

    monkeypatch.setattr(main_mod, "start_scheduler", lambda: None)
    monkeypatch.setattr(main_mod, "stop_scheduler", lambda: None)

    _write_config(tmp_path, monkeypatch, database_url=pg_url)
    reset_config()

    app = create_app()
    try:
        async with app.router.lifespan_context(app):
            engine = store_db.get_engine()
            # Full normalized URL: host, port, driver and credentials all matter for
            # "did it read the config file" — a bare database-name match would not.
            assert make_url(engine.url).render_as_string(hide_password=False) == pg_url
            pool_before = engine.sync_engine.pool
            async with engine.connect() as conn:
                bound = (await conn.exec_driver_sql("SELECT current_database()")).scalar()
            assert bound == make_url(pg_url).database

        # dispose() empties the old pool and swaps in a fresh one — observable without
        # patching anything, and it fails if the lifespan stops disposing.
        assert engine.sync_engine.pool is not pool_before
        assert engine.sync_engine.pool.checkedin() == 0
    finally:
        store_db._engine = None
        store_db._session_factory = None


# --------------------------------------------------------------------------- #
# _build_netbox_client — ca_cert wiring (s3-28) + _init_database gate (s3-25)
# --------------------------------------------------------------------------- #


def _netbox_cfg(ca_cert):
    return SimpleNamespace(
        netbox=SimpleNamespace(base_url="https://netbox.example", ca_cert=ca_cert, api_token_ref="TOK")
    )


def test_build_netbox_client_pins_ca_cert(clean_netbox_registry):
    """s3-28: a configured NetboxConfig.ca_cert must be passed as the client's TLS verify
    (a private-CA endpoint can be pinned) instead of being a silent no-op."""
    app = SimpleNamespace(state=SimpleNamespace())
    client = _build_netbox_client(app, _netbox_cfg("/etc/ssl/netbox-ca.pem"), _Provider())
    assert client._verify == "/etc/ssl/netbox-ca.pem"


def test_build_netbox_client_defaults_verify_true(clean_netbox_registry):
    """No ca_cert configured → verify defaults to True (system trust store)."""
    app = SimpleNamespace(state=SimpleNamespace())
    client = _build_netbox_client(app, _netbox_cfg(None), _Provider())
    assert client._verify is True


@pytest.fixture
def unmigrated_pg_url(pg_admin):
    """A database with NO schema whatsoever — plain CREATE DATABASE, never TEMPLATE.

    The normal ``pg_url`` clone is already at head, so a reintroduced ``create_all`` there
    would be an idempotent no-op and a before/after table comparison would pass vacuously.
    Starting from zero tables is what makes "the lifespan created nothing" falsifiable.
    """
    import uuid as uuid_mod

    from tests.conftest import _drop_database, _url_for

    name = f"nsoadp_empty_{uuid_mod.uuid4().hex[:8]}"
    with pg_admin.connect() as conn:
        conn.exec_driver_sql(f'CREATE DATABASE "{name}"')
    try:
        yield _url_for(name, driver="postgresql+asyncpg")
    finally:
        _drop_database(pg_admin, name, expect_clean=True)


async def test_init_database_never_materializes_schema(monkeypatch, unmigrated_pg_url):
    """s3-25, PG-only: alembic is the ONE schema source. The lifespan binds the engine and
    mints the incarnation; it must never create tables, because a second materialiser in the
    startup path is exactly the DuplicateTable hazard.

    Runs against a genuinely EMPTY database, so any DDL the lifespan emits shows up as a
    table that nothing else could have made. A fake engine would only prove "no .begin()
    call"; a migrated clone would prove nothing at all."""
    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import create_async_engine

    from nso_adapter.store import db as store_db
    from nso_adapter.store import meta as store_meta

    def _tables(conn):
        return set(sa.inspect(conn).get_table_names())

    engine = create_async_engine(unmigrated_pg_url)
    try:
        async with engine.connect() as conn:
            before = await conn.run_sync(_tables)
        assert before == set(), f"the fixture must hand over an EMPTY database, got {sorted(before)}"

        ensure_calls = []

        async def _fake_ensure():
            # S4's mint needs a primed session this isolated test never sets up; patch it AND
            # assert it ran, since the incarnation mint is part of the init contract.
            ensure_calls.append(True)
            return ("00000000-0000-0000-0000-000000000001", None)

        monkeypatch.setattr(store_meta, "ensure_store_meta", _fake_ensure)
        try:
            await _init_database(SimpleNamespace(database_url=unmigrated_pg_url))
            assert ensure_calls, "the store-incarnation mint must run at init"
            async with engine.connect() as conn:
                after = await conn.run_sync(_tables)
        finally:
            bound = store_db.get_engine()
            if bound is not None:
                await bound.dispose()
            store_db._engine = None
            store_db._session_factory = None

        assert after == set(), f"the lifespan materialised schema: {sorted(after)}"
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------- #
# B3 — SSE emits notify_sync_complete per changed device (real overlay backstop)
# --------------------------------------------------------------------------- #
