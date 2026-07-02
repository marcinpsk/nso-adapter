# SPDX-License-Identifier: Apache-2.0
"""Shared pytest fixtures for all nso-adapter tests."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from nso_adapter.bindings.netbox.client import NetboxClient
from nso_adapter.main import create_app
from nso_adapter.nso.client import NsoClient

# Hermetic tests: ignore any ambient DATABASE_URL. The dev container sets one to
# point at the dev Postgres; without this, get_config()'s env override would make
# tests run against real dev data instead of their isolated per-test database.
os.environ.pop("DATABASE_URL", None)

VALID_TOKEN = "test-bearer-token"


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


@pytest.fixture
async def adapter_client(tmp_path, monkeypatch):
    """FastAPI test client backed by in-memory SQLite.

    Renamed from the per-file ``test_client`` that used to live in
    ``tests/api/test_api.py``.  Import this fixture by name in every test
    module that hits the FastAPI app.
    """
    cfg_text = f"""
secrets:
  provider: local
nso_instances: []
netbox:
  base_url: http://netbox.local
  api_token_ref: "NETBOX_TOKEN"
api:
  adapter_token_ref: "ADAPTER_TOKEN"
database_url: sqlite+aiosqlite:///{tmp_path}/test.db
"""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(cfg_text)
    monkeypatch.setenv("CONFIG_FILE", str(cfg_file))
    monkeypatch.setenv("ADAPTER_TOKEN", VALID_TOKEN)
    monkeypatch.setenv("NETBOX_TOKEN", "nb-test-token")

    from nso_adapter.config import reset_config

    reset_config()

    app = create_app()

    # No make_provider/NetboxClient patches: the real LocalSecretsProvider resolves each
    # ref from the env vars set above, and the lifespan builds a real (no-I/O) NetboxClient.
    # set_netbox_client stays patched so the importer's global client stays None — tests that
    # exercise NetBox paths set their own. Only true side effects (scheduler/workers/SSE) are
    # stubbed so the in-process app doesn't spawn background tasks or open NSO streams.
    with (
        patch("nso_adapter.main.set_netbox_client"),
        patch("nso_adapter.main.start_scheduler"),
        patch("nso_adapter.main.stop_scheduler"),
        patch("nso_adapter.main.start_workers", new=AsyncMock()),
        patch("nso_adapter.main.stop_workers", new=AsyncMock()),
        patch("nso_adapter.main.persistent_subscriber", new=AsyncMock()),
    ):
        # ASGITransport does not call lifespan — run it manually so init_db() fires.
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                yield client


@pytest.fixture
async def adapter_client_with_nso(tmp_path, monkeypatch):
    """Like adapter_client but with one NSO instance ('nso-dev') declared in config.

    Required for tests that call onboard_device / rekey_device, which validate
    that the target NSO instance exists in the adapter config.
    """
    cfg_text = f"""
secrets:
  provider: local
nso_instances:
  - name: nso-dev
    base_url: http://nso-dev:8080
    username_ref: NSO_USERNAME
    password_ref: NSO_PASSWORD
netbox:
  base_url: http://netbox.local
  api_token_ref: "NETBOX_TOKEN"
api:
  adapter_token_ref: "ADAPTER_TOKEN"
database_url: sqlite+aiosqlite:///{tmp_path}/test.db
"""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(cfg_text)
    monkeypatch.setenv("CONFIG_FILE", str(cfg_file))
    monkeypatch.setenv("ADAPTER_TOKEN", VALID_TOKEN)
    monkeypatch.setenv("NETBOX_TOKEN", "nb-test-token")
    monkeypatch.setenv("NSO_USERNAME", "admin")
    monkeypatch.setenv("NSO_PASSWORD", "admin")

    from nso_adapter.config import reset_config

    reset_config()

    app = create_app()

    # See adapter_client: real LocalSecretsProvider + real (no-I/O) NetboxClient; only the
    # background side effects are stubbed. This fixture additionally has one NSO instance, so
    # the lifespan resolves NSO_USERNAME/NSO_PASSWORD and registers a real NsoClient for it.
    with (
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
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, ManagedScope

    async for db in get_session():
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
    raise RuntimeError("seed_device: no DB session available")


async def seed_l2_saps(device_id: int, services: list[dict]):
    """Insert DeviceL2Sap rows from a list of service dicts.

    Each service: ``{service_name, service_type, service_id?, saps: [{sap_id, port,
    outer_tag?, inner_tag?}]}``.
    """
    from datetime import UTC, datetime

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceL2Sap

    now = datetime.now(UTC).replace(tzinfo=None)
    async for db in get_session():
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
    raise RuntimeError("seed_l2_saps: no DB session available")


async def seed_bgp_config(
    device_id: int,
    *,
    asn: str = "65100",
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
    from nso_adapter.store.db import get_session
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

    async for db in get_session():
        router = DeviceBgpRouter(device_id=device_id, asn=asn, refresh_source="test")
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
    raise RuntimeError("seed_bgp_config: no DB session available")


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

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import LagBundleConfig, LagMemberConfig

    if bundles is None:
        bundles = []

    async for db in get_session():
        now = datetime.now(UTC).replace(tzinfo=None)
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

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceVlan

    now = datetime.now(UTC).replace(tzinfo=None)
    async for db in get_session():
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

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSvi

    now = datetime.now(UTC).replace(tzinfo=None)
    async for db in get_session():
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

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSubinterface

    now = datetime.now(UTC).replace(tzinfo=None)
    async for db in get_session():
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

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import (
        DeviceSwitchport,
        DeviceSwitchportTaggedVlan,
        DeviceVlan,
    )

    now = datetime.now(UTC).replace(tzinfo=None)
    async for db in get_session():
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
