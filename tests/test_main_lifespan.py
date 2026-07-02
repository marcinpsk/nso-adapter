# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the lifespan helpers extracted from nso_adapter.main.

These cover the pieces the ``adapter_client`` fixture never exercises (it runs
lifespan with no NSO instances and SSE disabled): the per-instance NSO client
loop, the SSE event-dispatch fan-out + its config gating, stream startup, and
the teardown tail. Everything runs the real helpers; collaborators that are
already tested elsewhere (the nine ``handle_*`` change handlers, the persistent
SSE subscriber) are replaced with small async fakes, never MagicMocks.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nso_adapter.main import (
    _build_netbox_client,
    _build_nso_clients,
    _close_netbox,
    _dispatch_netconf_change,
    _dispose_engine,
    _init_database,
    _make_sse_event_handler,
    _shutdown_sse,
    _start_sse_streams,
)

# The change handlers _dispatch_netconf_change invokes, in call order. The three
# annotated with a flag only fire when that scheduler sync flag is enabled.
_HANDLERS = [
    "handle_netconf_config_change",
    "handle_l2_service_change",
    ("handle_interface_ip_change", "enable_interface_ip_sync"),
    ("handle_snmp_config_change", "enable_snmp_sync"),
    "handle_vlan_database_change",
    "handle_switchport_change",
    "handle_svi_change",
    "handle_subinterface_change",
    ("handle_interface_mtu_change", "enable_interface_mtu_sync"),
]


def _handler_name(entry):
    return entry[0] if isinstance(entry, tuple) else entry


def _scheduler(**flags):
    base = {
        "enable_interface_ip_sync": False,
        "enable_snmp_sync": False,
        "enable_interface_mtu_sync": False,
        "enable_nso_streams": False,
    }
    base.update(flags)
    return SimpleNamespace(**base)


def _patch_recording_handlers(monkeypatch):
    """Replace the nine change handlers with async recorders; return the call log."""
    calls: list[str] = []

    def make(name):
        async def _rec(parsed, db, clients):
            calls.append(name)

        return _rec

    for entry in _HANDLERS:
        name = _handler_name(entry)
        monkeypatch.setattr(f"nso_adapter.main.{name}", make(name))
    return calls


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
# _dispatch_netconf_change — the config-gated fan-out
# --------------------------------------------------------------------------- #


async def test_dispatch_runs_all_handlers_when_flags_on(monkeypatch):
    calls = _patch_recording_handlers(monkeypatch)
    cfg = SimpleNamespace(
        scheduler=_scheduler(
            enable_interface_ip_sync=True,
            enable_snmp_sync=True,
            enable_interface_mtu_sync=True,
        )
    )

    await _dispatch_netconf_change(cfg, {"k": 1}, db="DB", clients={"i": object()})

    assert calls == [_handler_name(e) for e in _HANDLERS]


async def test_dispatch_skips_gated_handlers_when_flags_off(monkeypatch):
    calls = _patch_recording_handlers(monkeypatch)
    cfg = SimpleNamespace(scheduler=_scheduler())  # all three sync flags off

    await _dispatch_netconf_change(cfg, {"k": 1}, db="DB", clients={"i": object()})

    gated = {_handler_name(e) for e in _HANDLERS if isinstance(e, tuple)}
    assert gated == {
        "handle_interface_ip_change",
        "handle_snmp_config_change",
        "handle_interface_mtu_change",
    }
    assert set(calls).isdisjoint(gated)
    # the ungated handlers still ran, in order
    assert calls == [_handler_name(e) for e in _HANDLERS if not isinstance(e, tuple)]


# --------------------------------------------------------------------------- #
# _make_sse_event_handler — None-guard + schedules a dispatch task
# --------------------------------------------------------------------------- #


async def test_sse_handler_ignores_unparseable_frame(monkeypatch):
    calls = _patch_recording_handlers(monkeypatch)
    handler = _make_sse_event_handler(SimpleNamespace(scheduler=_scheduler()), {"i": object()}, set())

    before = asyncio.all_tasks()
    assert handler("raw-frame", None) is None
    new = [t for t in asyncio.all_tasks() if t not in before and t is not asyncio.current_task()]

    assert new == []
    assert calls == []


async def test_sse_handler_dispatches_parsed_frame(monkeypatch):
    calls = _patch_recording_handlers(monkeypatch)

    async def fake_session():
        yield "DB-SESSION"

    monkeypatch.setattr("nso_adapter.main.get_session", fake_session)
    cfg = SimpleNamespace(
        scheduler=_scheduler(
            enable_interface_ip_sync=True,
            enable_snmp_sync=True,
            enable_interface_mtu_sync=True,
        )
    )
    dispatch_tasks: set[asyncio.Task] = set()
    handler = _make_sse_event_handler(cfg, {"i": object()}, dispatch_tasks)

    before = asyncio.all_tasks()
    handler("raw-frame", {"k": 1})
    spawned = [t for t in asyncio.all_tasks() if t not in before and t is not asyncio.current_task()]
    assert len(spawned) == 1
    assert len(dispatch_tasks) == 1  # retained (not GC'd), trackable for shutdown

    await asyncio.gather(*spawned)
    assert calls == [_handler_name(e) for e in _HANDLERS]
    assert dispatch_tasks == set()  # done-callback discarded it


async def test_sse_handler_logs_and_does_not_leak_failed_dispatch(monkeypatch):
    """A dispatch that raises must be surfaced via the done-callback and removed from the
    tracking set — not silently swallowed by a fire-and-forget create_task (s3-12)."""
    from unittest.mock import AsyncMock

    async def fake_session():
        yield "DB-SESSION"

    monkeypatch.setattr("nso_adapter.main.get_session", fake_session)
    monkeypatch.setattr(
        "nso_adapter.main._dispatch_netconf_change", AsyncMock(side_effect=RuntimeError("dispatch boom"))
    )
    dispatch_tasks: set[asyncio.Task] = set()
    handler = _make_sse_event_handler(SimpleNamespace(scheduler=_scheduler()), {"i": object()}, dispatch_tasks)

    handler("raw-frame", {"k": 1})
    assert len(dispatch_tasks) == 1
    task = next(iter(dispatch_tasks))
    await asyncio.gather(task, return_exceptions=True)  # must not raise out of the handler/task

    assert dispatch_tasks == set()  # discarded despite the failure
    assert isinstance(task.exception(), RuntimeError)  # captured, not lost


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


async def test_dispose_engine_disposes_real_engine(monkeypatch):
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine("sqlite+aiosqlite://")
    monkeypatch.setattr("nso_adapter.main.get_engine", lambda: engine)

    await _dispose_engine()  # real engine, real dispose


# --------------------------------------------------------------------------- #
# _build_netbox_client — ca_cert wiring (s3-28) + _init_database gate (s3-25)
# --------------------------------------------------------------------------- #


@pytest.fixture
def clean_netbox_registry():
    from nso_adapter.core import importer

    snapshot = importer._netbox_client
    yield
    importer._netbox_client = snapshot


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


async def test_init_database_skips_create_all_on_postgres(monkeypatch):
    """s3-25: on PostgreSQL the entrypoint already ran `alembic upgrade head`, so the lifespan
    must NOT run create_all (two schema sources → DuplicateTable). The fake engine has no
    .begin(); reaching the create_all block would raise, so a clean return proves the gate."""
    import nso_adapter.main as main_mod

    fake_pg_engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    monkeypatch.setattr(main_mod, "init_db", lambda url: None)
    monkeypatch.setattr(main_mod, "get_engine", lambda: fake_pg_engine)

    await _init_database(SimpleNamespace(database_url="postgresql+asyncpg://u:p@db/adapter"))
    # no AttributeError on fake_pg_engine.begin → the create_all block was skipped
