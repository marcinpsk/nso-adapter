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


def _mock_provider():
    p = MagicMock()
    p.get = MagicMock(return_value=VALID_TOKEN)
    return p


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

    with (
        patch("nso_adapter.main.make_provider", return_value=_mock_provider()),
        patch("nso_adapter.bindings.netbox.client.NetboxClient") as MockNb,
        patch("nso_adapter.main.set_netbox_client"),
        patch("nso_adapter.main.start_scheduler"),
        patch("nso_adapter.main.stop_scheduler"),
        patch("nso_adapter.main.start_workers", new=AsyncMock()),
        patch("nso_adapter.main.stop_workers", new=AsyncMock()),
        patch("nso_adapter.main.persistent_subscriber", new=AsyncMock()),
    ):
        MockNb.return_value = MagicMock()
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

    with (
        patch("nso_adapter.main.make_provider", return_value=_mock_provider()),
        patch("nso_adapter.bindings.netbox.client.NetboxClient") as MockNb,
        patch("nso_adapter.main.set_netbox_client"),
        patch("nso_adapter.main.start_scheduler"),
        patch("nso_adapter.main.stop_scheduler"),
        patch("nso_adapter.main.start_workers", new=AsyncMock()),
        patch("nso_adapter.main.stop_workers", new=AsyncMock()),
        patch("nso_adapter.main.persistent_subscriber", new=AsyncMock()),
    ):
        MockNb.return_value = MagicMock()
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
        await db.commit()
        await db.refresh(router)
        return router.id
    raise RuntimeError("seed_bgp_config: no DB session available")
