# SPDX-License-Identifier: Apache-2.0
"""Shared pytest fixtures for all nso-adapter tests."""

from __future__ import annotations

import itertools
import os
import subprocess
import sys
import uuid
import warnings
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sqlalchemy as sa
from httpx import ASGITransport, AsyncClient
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session as SyncSession

from nso_adapter.bindings.netbox.client import NetboxClient
from nso_adapter.main import create_app
from nso_adapter.nso.client import NsoClient

# Hermetic tests: ignore any ambient DATABASE_URL. The dev container sets one to
# point at the dev Postgres; without this, get_config()'s env override would make
# tests run against real dev data instead of their isolated per-test database.
os.environ.pop("DATABASE_URL", None)

VALID_TOKEN = "test-bearer-token"

_REPO_ROOT = Path(__file__).resolve().parents[1]
ADMIN_URL = os.environ.get(
    "NSO_ADAPTER_TEST_DB_URL",
    "postgresql+psycopg2://postgres:postgres@127.0.0.1:55433/postgres",
)
STRICT_TEARDOWN = os.environ.get("NSO_ADAPTER_TEST_STRICT_TEARDOWN") == "1"

# Run-unique prefix: a crashed run's stragglers can never collide with this run's names.
_RUN = uuid.uuid4().hex[:8]
_clone_seq = itertools.count()


def _url_for(dbname: str, *, driver: str) -> str:
    # NB: str(URL) masks the password as literal '***' — always render_as_string.
    return make_url(ADMIN_URL).set(drivername=driver, database=dbname).render_as_string(hide_password=False)


def _drop_database(admin, name: str, *, expect_clean: bool) -> None:
    """Report stragglers BEFORE forcing. FORCE is last-resort cleanup, not the mechanism.

    A surviving connection means a fixture failed to close a session — a test bug we want
    visible. Silently FORCE-ing it away is how that bug stays invisible forever.
    """
    with admin.connect() as conn:
        # backend_type filter: PG's own autovacuum worker can be inside the clone at DROP
        # time and is not a leaked test session — only client backends count as stragglers.
        rows = conn.exec_driver_sql(
            "SELECT pid, state, left(query, 120) FROM pg_stat_activity "
            f"WHERE datname = '{name}' AND pid <> pg_backend_pid() "
            "AND backend_type = 'client backend'"
        ).fetchall()
        if rows and expect_clean:
            msg = f"{name}: {len(rows)} connection(s) survived teardown: {rows}"
            if STRICT_TEARDOWN:
                conn.exec_driver_sql(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
                raise AssertionError(msg)
            warnings.warn(msg, stacklevel=2)
        conn.exec_driver_sql(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture(scope="session")
def pg_admin():
    """AUTOCOMMIT admin engine for CREATE/DROP DATABASE. Sync + session-scoped on purpose:
    an asyncpg pool created on a session-scoped loop and used from per-test loops is UB."""
    engine = sa.create_engine(ADMIN_URL, isolation_level="AUTOCOMMIT", poolclass=sa.pool.NullPool)
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")  # FAIL LOUD: no silent skip lane any more
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def pg_template(pg_admin):
    """Build the schema ONCE — via alembic, the schema production runs — into a database
    used only as a clone source. Wrapped so a SETUP failure (broken migration chain, bad
    ALTER) still drops the half-built template instead of leaking it."""
    name = f"nsoadp_{_RUN}_tmpl"
    with pg_admin.connect() as conn:
        conn.exec_driver_sql(f'CREATE DATABASE "{name}"')
    try:
        try:
            subprocess.run(
                [sys.executable, "-m", "nso_adapter.db_migrate"],
                cwd=_REPO_ROOT,
                check=True,
                capture_output=True,
                # alembic/env.py rewrites +asyncpg -> +psycopg2 itself.
                env={**os.environ, "DATABASE_URL": _url_for(name, driver="postgresql+asyncpg")},
            )
        except subprocess.CalledProcessError as exc:  # surface alembic's own output
            raise RuntimeError(f"template build failed:\n{exc.stderr.decode()}") from exc
        yield name
    finally:
        _drop_database(pg_admin, name, expect_clean=False)  # build connections are ours


@pytest.fixture
def pg_database(pg_admin, pg_template):
    """A PRIVATE database per test, cloned from the template."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "m")  # xdist-ready, xdist-optional
    name = f"nsoadp_{_RUN}_{worker}_{next(_clone_seq):05d}"  # always << 63 bytes
    with pg_admin.connect() as conn:
        conn.exec_driver_sql(f'CREATE DATABASE "{name}" TEMPLATE "{pg_template}"')
    try:
        with pg_admin.connect() as conn:
            # Fail fast instead of wedging: the family fence is real on PG and can block.
            # CREATE DATABASE ... TEMPLATE does NOT copy pg_db_role_setting — set per clone.
            for stmt in (
                "lock_timeout = '5s'",
                "statement_timeout = '60s'",
                "idle_in_transaction_session_timeout = '60s'",
            ):
                conn.exec_driver_sql(f'ALTER DATABASE "{name}" SET {stmt}')
        yield name
    finally:
        # An ALTER failure above must not leak the clone — hence the try wrapping it.
        _drop_database(pg_admin, name, expect_clean=True)


@pytest.fixture
def pg_url(pg_database) -> str:
    return _url_for(pg_database, driver="postgresql+asyncpg")


@pytest.fixture
def pg_sync_session(pg_database):
    """Sync Session for the ORM-model tests. Schema comes from the template; FK
    enforcement is native — no PRAGMA."""
    engine = sa.create_engine(_url_for(pg_database, driver="postgresql+psycopg2"), poolclass=sa.pool.NullPool)
    try:
        with SyncSession(engine) as s:
            yield s
    finally:
        engine.dispose()


@asynccontextmanager
async def session():
    """One store session with deterministic close — replaces the historical
    ``get_session`` async-for-with-break loops, whose ``break`` left the
    generator (and its connection) suspended until GC."""
    from nso_adapter.store.db import get_session

    gen = get_session()
    db = await anext(gen)
    try:
        yield db
    finally:
        await gen.aclose()


# Enforceable zero-skip gate (pytest's -rs only REPORTS skips; it never fails the run).
def pytest_sessionfinish(session, exitstatus):
    if os.environ.get("NSO_ADAPTER_TEST_NO_SKIPS") != "1":
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter and reporter.stats.get("skipped"):
        session.exitstatus = 1


class count_queries:
    """Context manager counting SQL statements executed on the app's async engine.

    Guards against N+1 query patterns: assert the count does not scale with the
    number of rows (interfaces/devices). Attaches a ``before_cursor_execute``
    listener to the engine's sync facade for the duration of the block.
    """

    def __init__(self) -> None:
        self.count = 0

    def _on(self, *_args) -> None:
        self.count += 1

    def __enter__(self) -> count_queries:
        from sqlalchemy import event

        from nso_adapter.store.db import get_engine

        self._engine = get_engine().sync_engine
        event.listen(self._engine, "before_cursor_execute", self._on)
        return self

    def __exit__(self, *_exc) -> None:
        from sqlalchemy import event

        event.remove(self._engine, "before_cursor_execute", self._on)


# YAML fragment for the single-instance fixture. The leading newline+indent is what turns
# `nso_instances:` into a block sequence; the default " []" keeps it an inline empty list.
NSO_DEV_INSTANCE = """
  - name: nso-dev
    base_url: http://nso-dev:8080
    username_ref: NSO_USERNAME
    password_ref: NSO_PASSWORD"""


def _write_config(tmp_path, monkeypatch, *, database_url: str, nso_instances: str = " []") -> Path:
    """Write the app config file and its secret env refs; return the path."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        f"""
secrets:
  provider: local
nso_instances:{nso_instances}
netbox:
  base_url: http://netbox.local
  api_token_ref: "NETBOX_TOKEN"
api:
  adapter_token_ref: "ADAPTER_TOKEN"
database_url: {database_url}
"""
    )
    monkeypatch.setenv("CONFIG_FILE", str(cfg_file))
    monkeypatch.setenv("ADAPTER_TOKEN", VALID_TOKEN)
    monkeypatch.setenv("NETBOX_TOKEN", "nb-test-token")
    return cfg_file


@pytest.fixture
async def store_engine(pg_url):
    """The SOLE owner AND SOLE disposer of nso_adapter.store.db's globals for this test.

    Both adapter_client and db_session depend on this, so the eight tests taking BOTH
    (tests/core/test_importer.py) get exactly ONE engine. Without it each caller's
    init_db() would replace the globals: engine A orphaned with live connections
    (blocking the clone DROP), engine B disposed twice.
    """
    from nso_adapter.store import db as store_db

    store_db.init_db(pg_url)
    engine = store_db.get_engine()
    try:
        yield engine
    finally:
        await engine.dispose()  # runs AFTER every dependent fixture has closed
        store_db._engine = None  # no cross-test global bleed
        store_db._session_factory = None


@pytest.fixture
async def rival_engine(store_engine, pg_url):
    """A second, independent AsyncEngine on the SAME clone database.

    Stands in for another worker or process in the claim tests. Two AsyncSessions on two
    connections running ``INSERT … ON CONFLICT DO NOTHING`` in separate committed
    transactions is a genuine database-level race, so exclusivity is proven against real
    PostgreSQL rather than against an asyncio lock.

    Never touches ``nso_adapter.store.db``'s process globals — ``store_engine`` owns
    those. Depending on it means this engine is finalized FIRST, so its connections are
    gone before the clone is dropped (``_drop_database(..., expect_clean=True)`` fails the
    test otherwise, which is the point).
    """
    engine = create_async_engine(pg_url, poolclass=sa.pool.NullPool)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def adapter_client(store_engine, pg_url, tmp_path, monkeypatch):
    """FastAPI test client on a private PostgreSQL clone.

    Renamed from the per-file ``test_client`` that used to live in
    ``tests/api/test_api.py``.  Import this fixture by name in every test
    module that hits the FastAPI app.

    ``main.init_db``/``main._dispose_engine`` are patched out because ``store_engine``
    owns the process globals: SQLAlchemy's ``Pool.dispose`` leaves checked-out
    connections open, so a lifespan disposal while a sibling ``db_session`` still holds
    one would orphan that connection into a dead pool and block the clone DROP.
    ``test_lifespan_binds_the_configured_database`` covers the un-patched path.
    """
    _write_config(tmp_path, monkeypatch, database_url=pg_url)

    from nso_adapter.config import reset_config

    reset_config()

    app = create_app()

    # No make_provider/NetboxClient patches: the real LocalSecretsProvider resolves each
    # ref from the env vars set above, and the lifespan builds a real (no-I/O) NetboxClient.
    # set_netbox_client stays patched so the importer's global client stays None — tests that
    # exercise NetBox paths set their own. Only true side effects (scheduler/workers/SSE) are
    # stubbed so the in-process app doesn't spawn background tasks or open NSO streams.
    with (
        patch("nso_adapter.main.init_db"),
        patch("nso_adapter.main._dispose_engine", new=AsyncMock()),
        patch("nso_adapter.main.set_netbox_client"),
        patch("nso_adapter.main.start_scheduler"),
        patch("nso_adapter.main.stop_scheduler"),
        patch("nso_adapter.main.start_workers", new=AsyncMock()),
        patch("nso_adapter.main.stop_workers", new=AsyncMock()),
        patch("nso_adapter.main.persistent_subscriber", new=AsyncMock()),
    ):
        # ASGITransport does not call lifespan — run it manually so ensure_store_meta fires.
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                yield client


@pytest.fixture
async def adapter_client_with_nso(store_engine, pg_url, tmp_path, monkeypatch):
    """Like adapter_client but with one NSO instance ('nso-dev') declared in config.

    Required for tests that call onboard_device / rekey_device, which validate
    that the target NSO instance exists in the adapter config.
    """
    _write_config(tmp_path, monkeypatch, database_url=pg_url, nso_instances=NSO_DEV_INSTANCE)
    monkeypatch.setenv("NSO_USERNAME", "admin")
    monkeypatch.setenv("NSO_PASSWORD", "admin")

    from nso_adapter.config import reset_config

    reset_config()

    app = create_app()

    # See adapter_client: real LocalSecretsProvider + real (no-I/O) NetboxClient; only the
    # background side effects are stubbed. This fixture additionally has one NSO instance, so
    # the lifespan resolves NSO_USERNAME/NSO_PASSWORD and registers a real NsoClient for it.
    with (
        patch("nso_adapter.main.init_db"),
        patch("nso_adapter.main._dispose_engine", new=AsyncMock()),
        patch("nso_adapter.main.set_netbox_client"),
        patch("nso_adapter.main.start_scheduler"),
        patch("nso_adapter.main.stop_scheduler"),
        patch("nso_adapter.main.start_workers", new=AsyncMock()),
        patch("nso_adapter.main.stop_workers", new=AsyncMock()),
        patch("nso_adapter.main.persistent_subscriber", new=AsyncMock()),
    ):
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                yield client


@pytest.fixture
async def db_session(store_engine, pg_url, tmp_path, monkeypatch):
    """AsyncSession on the SHARED engine, with the process globals primed (N7).

    Primes config explicitly rather than inheriting whatever adapter_client ran last —
    that coupling is why tests/core/test_importer.py could not run solo. Never calls
    init_db() and never disposes: store_engine owns both.
    """
    from nso_adapter.config import reset_config
    from nso_adapter.store.meta import ensure_store_meta

    _write_config(tmp_path, monkeypatch, database_url=pg_url)
    reset_config()
    await ensure_store_meta()
    async with session() as db:
        yield db


@pytest.fixture
def fake_nso_client():
    """AsyncMock simulating NsoClient.

    ``list_devices`` returns a realistic multi-device NSO payload.
    ``get_device_ned_id`` returns a Cisco IOS NED ID.
    Patch into place with:
        monkeypatch.setattr("nso_adapter.core.importer.get_nso_client",
                            lambda *_: fake_nso_client)
    """
    m = MagicMock(spec=NsoClient)
    m.list_devices = AsyncMock(
        return_value=[
            {
                "name": "core-rtr-01",
                "address": "10.0.0.1",
                "authgroup": "default",
                "device-type": {"cli": {"ned-id": "cisco-ios-cli-6.95"}},
                "state": {"admin-state": "unlocked"},
            },
            {
                "name": "edge-rtr-02",
                "address": "10.0.0.2",
                "authgroup": "default",
                "device-type": {"netconf": {"ned-id": "juniper-junos-nc-4.1"}},
                "state": {"admin-state": "locked"},
            },
        ]
    )
    m.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")
    return m


@pytest.fixture
def fake_netbox_client():
    """AsyncMock simulating NetboxClient.

    ``get_interface`` returns a minimal interface dict.
    ``patch_interface`` and ``create_interface`` return the same dict.
    Patch into place with:
        monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client",
                            lambda: fake_netbox_client)
    """
    m = MagicMock(spec=NetboxClient)
    _iface = {"id": 1, "name": "GigabitEthernet0/0", "description": "", "enabled": True}
    m.get_interface = AsyncMock(return_value=_iface)
    m.patch_interface = AsyncMock(return_value=_iface)
    m.create_interface = AsyncMock(return_value=_iface)
    return m


# READSEM S4 golden determinism: the store incarnation is a random per-DB (uuid, born)
# pair riding every read_state block; golden tests pin it to these fixed values first.
GOLDEN_INCARNATION = "00000000-0000-0000-0000-000000000001"
GOLDEN_BORN_ISO = "2026-06-01T00:00:00Z"


async def pin_store_incarnation() -> None:
    """Overwrite the test DB's store_meta pair with the fixed golden values and reload
    the process cache (ensure_store_meta always re-reads)."""
    from datetime import UTC, datetime

    from sqlalchemy import update

    from nso_adapter.store.meta import ensure_store_meta
    from nso_adapter.store.models import StoreMeta

    async with session() as db:
        await db.execute(
            update(StoreMeta).values(incarnation=GOLDEN_INCARNATION, born=datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC))
        )
        await db.commit()
    await ensure_store_meta()


async def seed_device(
    *,
    nso_instance: str = "nso-dev",
    nso_device_name: str = "core-rtr-01",
    netbox_device_id: int = 42,
    attributes: list[str] | None = None,
):
    """Insert a Device + ManagedScope rows and return the Device id.

    This is a plain async helper, not a pytest fixture.  Import explicitly::

        from tests.conftest import seed_device

    Requires the app lifespan to have run first (i.e. ``adapter_client``
    fixture must be in scope so ``init_db()`` has been called).

    Usage inside a test::

        async def test_something(adapter_client):
            device_id = await seed_device(nso_device_name="my-router")
            resp = await adapter_client.get(f"/api/v1/devices/{device_id}", ...)
    """
    from nso_adapter.store.models import Device, ManagedScope

    async with session() as db:
        d = Device(
            nso_instance=nso_instance,
            nso_device_name=nso_device_name,
            netbox_device_id=netbox_device_id,
        )
        db.add(d)
        await db.flush()
        for attr in ["description"] if attributes is None else attributes:
            db.add(ManagedScope(device_id=d.id, attribute=attr))
        await db.commit()
        await db.refresh(d)
        return d.id


async def seed_l2_saps(device_id: int, services: list[dict]):
    """Insert DeviceL2Sap rows from a list of service dicts.

    Each service: ``{service_name, service_type, service_id?, saps: [{sap_id, port,
    outer_tag?, inner_tag?}]}``.
    """
    from datetime import UTC, datetime

    from nso_adapter.store.models import DeviceL2Sap

    now = datetime.now(UTC)
    async with session() as db:
        for svc in services:
            for sap in svc.get("saps", []):
                db.add(
                    DeviceL2Sap(
                        device_id=device_id,
                        service_name=svc["service_name"],
                        service_type=svc.get("service_type", ""),
                        service_id=svc.get("service_id"),
                        sap_id=sap["sap_id"],
                        port=sap.get("port", ""),
                        outer_tag=sap.get("outer_tag"),
                        inner_tag=sap.get("inner_tag"),
                        last_refreshed_at=now,
                        refresh_source="test",
                    )
                )
        await db.commit()
        return


async def seed_bgp_config(
    device_id: int,
    *,
    asn: str = "65100",
    router_id: str | None = None,
    scopes: list[dict] | None = None,
):
    """Insert a minimal BGP read-mirror graph for a device.

    ``scopes`` is a list of dicts with optional keys:
    ``vrf`` (str, default ""), ``afs`` (list[str]), ``peers`` (list[dict]).
    Each peer dict may have: ``peer_address``, ``enabled``, ``peer_group``,
    ``remote_as``, ``local_as``, ``ttl``, ``password``, ``peer_afs`` (list[str]).

    Returns the created DeviceBgpRouter id.

    Usage::

        async def test_bgp(adapter_client):
            device_id = await seed_device()
            router_id = await seed_bgp_config(device_id)
    """
    from nso_adapter.store.models import (
        DeviceBgpAddressFamily,
        DeviceBgpPeer,
        DeviceBgpPeerAddressFamily,
        DeviceBgpPeerGroup,
        DeviceBgpPeerGroupAddressFamily,
        DeviceBgpRouter,
        DeviceBgpScope,
    )

    if scopes is None:
        scopes = [{"vrf": "", "afs": ["ipv4-unicast"], "peers": []}]

    async with session() as db:
        router = DeviceBgpRouter(device_id=device_id, asn=asn, router_id=router_id, refresh_source="test")
        db.add(router)
        await db.flush()
        for scope_def in scopes:
            vrf = scope_def.get("vrf", "")
            scope = DeviceBgpScope(router_id=router.id, vrf=vrf)
            db.add(scope)
            await db.flush()
            for af_name in scope_def.get("afs", []):
                db.add(DeviceBgpAddressFamily(scope_id=scope.id, af=af_name))
            for peer_def in scope_def.get("peers", []):
                peer = DeviceBgpPeer(
                    scope_id=scope.id,
                    peer_address=peer_def.get("peer_address", "192.0.2.1"),
                    enabled=peer_def.get("enabled", True),
                    peer_group=peer_def.get("peer_group"),
                    remote_as=peer_def.get("remote_as"),
                    local_as=peer_def.get("local_as"),
                    ttl=peer_def.get("ttl"),
                    password=peer_def.get("password"),
                    source=peer_def.get("source"),
                    bfd_enabled=peer_def.get("bfd_enabled"),
                )
                db.add(peer)
                await db.flush()
                for paf_name in peer_def.get("peer_afs", []):
                    db.add(DeviceBgpPeerAddressFamily(peer_id=peer.id, af=paf_name, enabled=True))
                for paf_def in peer_def.get("peer_af_defs", []):
                    db.add(
                        DeviceBgpPeerAddressFamily(
                            peer_id=peer.id,
                            af=paf_def["af"],
                            enabled=paf_def.get("enabled", True),
                            routemap_in=paf_def.get("routemap_in"),
                            routemap_out=paf_def.get("routemap_out"),
                            prefixlist_in=paf_def.get("prefixlist_in"),
                            prefixlist_out=paf_def.get("prefixlist_out"),
                        )
                    )
            for pg_def in scope_def.get("peer_groups", []):
                pg = DeviceBgpPeerGroup(
                    scope_id=scope.id,
                    name=pg_def.get("name", "PG"),
                    remote_as=pg_def.get("remote_as"),
                    source=pg_def.get("source"),
                )
                db.add(pg)
                await db.flush()
                for pgaf_def in pg_def.get("af_defs", []):
                    db.add(
                        DeviceBgpPeerGroupAddressFamily(
                            peer_group_id=pg.id,
                            af=pgaf_def["af"],
                            routemap_in=pgaf_def.get("routemap_in"),
                            routemap_out=pgaf_def.get("routemap_out"),
                            prefixlist_in=pgaf_def.get("prefixlist_in"),
                            prefixlist_out=pgaf_def.get("prefixlist_out"),
                        )
                    )
        await db.commit()
        await db.refresh(router)
        return router.id


async def seed_lag_config(
    device_id: int,
    *,
    bundles: list[dict] | None = None,
    refresh_source: str = "test",
):
    """Insert LagBundleConfig + LagMemberConfig rows for a device.

    Each bundle dict: name, lag_id, min_links?, system_priority?, timer?,
    system_id?, admin_key?, members (list of {interface_name, mode?, port_priority?}).
    """
    from datetime import UTC, datetime

    from nso_adapter.store.models import LagBundleConfig, LagMemberConfig

    if bundles is None:
        bundles = []

    async with session() as db:
        now = datetime.now(UTC)
        for b in bundles:
            bundle = LagBundleConfig(
                device_id=device_id,
                name=b["name"],
                lag_id=b["lag_id"],
                min_links=b.get("min_links"),
                system_priority=b.get("system_priority"),
                system_id=b.get("system_id"),
                timer=b.get("timer"),
                admin_key=b.get("admin_key"),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
            db.add(bundle)
            await db.flush()
            for m in b.get("members", []):
                db.add(
                    LagMemberConfig(
                        lag_bundle_id=bundle.id,
                        interface_name=m["interface_name"],
                        mode=m.get("mode"),
                        port_priority=m.get("port_priority"),
                    )
                )
        await db.commit()


async def seed_vlan_database(device_id: int, vlans: list[dict]):
    """Insert DeviceVlan rows. Each dict: vlan_id, name?."""
    from datetime import UTC, datetime

    from nso_adapter.store.models import DeviceVlan

    now = datetime.now(UTC)
    async with session() as db:
        for v in vlans:
            db.add(
                DeviceVlan(
                    device_id=device_id,
                    vlan_id=v["vlan_id"],
                    name=v.get("name", ""),
                    last_refreshed_at=now,
                    refresh_source="test",
                )
            )
        await db.commit()
        return


async def seed_svi(device_id: int, interfaces: list[dict]):
    """Insert DeviceSvi rows. Each dict: interface_name, vlan_id, type?, vrf?."""
    from datetime import UTC, datetime

    from nso_adapter.store.models import DeviceSvi

    now = datetime.now(UTC)
    async with session() as db:
        for i in interfaces:
            db.add(
                DeviceSvi(
                    device_id=device_id,
                    interface_name=i["interface_name"],
                    vlan_id=i["vlan_id"],
                    svi_type=i.get("type", "svi"),
                    vrf=i.get("vrf") or None,
                    last_refreshed_at=now,
                    refresh_source="test",
                )
            )
        await db.commit()
        return


async def seed_subinterface(device_id: int, interfaces: list[dict]):
    """Insert DeviceSubinterface rows.

    Each dict: interface_name, parent_interface?, dot1q_vlan?, type?, vrf?.
    """
    from datetime import UTC, datetime

    from nso_adapter.store.models import DeviceSubinterface

    now = datetime.now(UTC)
    async with session() as db:
        for i in interfaces:
            db.add(
                DeviceSubinterface(
                    device_id=device_id,
                    interface_name=i["interface_name"],
                    parent_interface=i.get("parent_interface") or None,
                    dot1q_vlan=i.get("dot1q_vlan"),
                    sub_type=i.get("type", "subinterface"),
                    vrf=i.get("vrf") or None,
                    last_refreshed_at=now,
                    refresh_source="test",
                )
            )
        await db.commit()
        return


async def seed_switchport(device_id: int, interfaces: list[dict]):
    """Insert DeviceSwitchport rows (+ DeviceVlan rows for referenced vids + links).

    Each dict: interface_name, mode?, untagged_vlan?, tagged_vlans?.
    """
    from datetime import UTC, datetime

    from sqlalchemy import select

    from nso_adapter.store.models import (
        DeviceSwitchport,
        DeviceSwitchportTaggedVlan,
        DeviceVlan,
    )

    now = datetime.now(UTC)
    async with session() as db:
        existing = {
            r.vlan_id: r
            for r in (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device_id))).scalars().all()
        }
        vids: set[int] = set()
        for itf in interfaces:
            if itf.get("untagged_vlan") is not None:
                vids.add(itf["untagged_vlan"])
            vids.update(itf.get("tagged_vlans", []))
        for vid in vids:
            if vid not in existing:
                row = DeviceVlan(
                    device_id=device_id, vlan_id=vid, name="", last_refreshed_at=now, refresh_source="test"
                )
                db.add(row)
                await db.flush()
                existing[vid] = row
        for itf in interfaces:
            uv = existing.get(itf["untagged_vlan"]) if itf.get("untagged_vlan") is not None else None
            sp = DeviceSwitchport(
                device_id=device_id,
                interface_name=itf["interface_name"],
                mode=itf.get("mode"),
                untagged_vlan_id=uv.id if uv is not None else None,
                last_refreshed_at=now,
                refresh_source="test",
            )
            db.add(sp)
            await db.flush()
            for tv in itf.get("tagged_vlans", []):
                db.add(DeviceSwitchportTaggedVlan(switchport_id=sp.id, vlan_id=existing[tv].id))
        await db.commit()
        return
