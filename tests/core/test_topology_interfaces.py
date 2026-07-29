# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/topology_interfaces.ensure_topology_interfaces.

Verifies the four-source union + the two skip rules (decisions 2/3/4 in
docs/nokia-lag-channel-modeling-plan.md): cfg.port + bound_ports + members-or-
referenced LAGs + name-only loopback/system/dotted units, EXCLUDING bp=None
colon-form ``lagXX:0`` unbound shells and pure-empty unreferenced LAGs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

from nso_adapter.bindings.netbox.client import NetboxClient
from tests.conftest import seed_device, session

TS = datetime(2026, 6, 2, 12, 0, 0, tzinfo=UTC)


def _nb_client():
    # NetboxClient is a real external HTTP boundary; bind the stand-in to its interface via
    # spec=. ensure_topology_interfaces only threads it to bulk_ensure_interfaces (stubbed
    # here), so it is never dereferenced — spec= keeps it from fabricating a renamed member.
    return MagicMock(spec=NetboxClient)


async def _seed_topology(device_id: int) -> None:
    from nso_adapter.store.models import (
        DbInterface,
        DeviceIsisInterface,
        InterfaceIpAddress,
        LagInterface,
        LagMember,
    )

    async with session() as db:
        # 1. cfg.port (attribute sync) — physical channelized port + port-level LAG.
        db.add(DbInterface(device_id=device_id, name="1/1/c22/1"))
        db.add(DbInterface(device_id=device_id, name="lag-99"))

        # 2. bound_ports — channel, SAP, and a bare LAG reference.
        db.add(
            DeviceIsisInterface(
                device_id=device_id,
                interface_name="LAG99:10",
                af="ipv4",
                bound_port="lag-99:10",
                last_refreshed_at=TS,
                refresh_source="test",
            )
        )
        db.add(
            DeviceIsisInterface(
                device_id=device_id,
                interface_name="to_peer",
                af="ipv4",
                bound_port="lag-67",
                last_refreshed_at=TS,
                refresh_source="test",
            )
        )
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name="XR-MGMT",
                address="10.0.0.1/30",
                vrf="",
                family="ipv4",
                bound_port="1/1/c22/1:4090",
                last_refreshed_at=TS,
                refresh_source="test",
            )
        )

        # 4. name-only loopback/system (bp=None) → create; colon-form unbound → skip.
        db.add(
            DeviceIsisInterface(
                device_id=device_id,
                interface_name="system",
                af="ipv4",
                bound_port=None,
                last_refreshed_at=TS,
                refresh_source="test",
            )
        )
        db.add(
            DeviceIsisInterface(
                device_id=device_id,
                interface_name="lag67:0",
                af="ipv4",
                bound_port=None,
                last_refreshed_at=TS,
                refresh_source="test",
            )
        )
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name="lo0",
                address="10.0.0.9/32",
                vrf="",
                family="ipv4",
                bound_port=None,
                last_refreshed_at=TS,
                refresh_source="test",
            )
        )

        # 3. LAGs: lag-2 has a member (create); lag-67 referenced via bound_port
        #    (create); lag-4 empty + unreferenced (skip).
        lag2 = LagInterface(device_id=device_id, name="lag-2", lag_id=2, last_refreshed_at=TS, refresh_source="test")
        db.add(lag2)
        await db.flush()
        db.add(LagMember(lag_interface_id=lag2.id, interface_name="1/1/c2/1", mode="active"))
        db.add(LagInterface(device_id=device_id, name="lag-67", lag_id=67, last_refreshed_at=TS, refresh_source="test"))
        db.add(LagInterface(device_id=device_id, name="lag-4", lag_id=4, last_refreshed_at=TS, refresh_source="test"))
        await db.commit()
        return


async def _run_ensure(device_id: int, nb_client) -> set[str]:
    """Call ensure_topology_interfaces with bulk_ensure stubbed; return the names set."""
    import nso_adapter.core.topology_interfaces as mod
    from nso_adapter.store.models import Device

    captured: dict = {}

    async def _fake_bulk(client, nb_device_id, names):
        captured["client"] = client
        captured["names"] = names
        captured["nb_device_id"] = nb_device_id
        return {n: i for i, n in enumerate(names)}

    orig = mod.bulk_ensure_interfaces
    mod.bulk_ensure_interfaces = _fake_bulk
    expected_nb_id = None
    try:
        async with session() as db:
            device = await db.get(Device, device_id)
            expected_nb_id = device.netbox_device_id
            await mod.ensure_topology_interfaces(db, device, nb_client)
    finally:
        mod.bulk_ensure_interfaces = orig
    # The resolved NetBox client + the device's netbox id must actually reach bulk_ensure.
    assert captured["client"] is nb_client
    assert captured["nb_device_id"] == expected_nb_id
    return set(captured.get("names", []))


async def test_unions_and_filters_sources(adapter_client):
    device_id = await seed_device(nso_device_name="topo-nokia", netbox_device_id=900)
    await _seed_topology(device_id)

    names = await _run_ensure(device_id, _nb_client())

    assert names == {
        "1/1/c22/1",  # cfg.port base
        "lag-99",  # cfg.port LAG base
        "lag-99:10",  # bound_port channel
        "1/1/c22/1:4090",  # bound_port SAP
        "lag-67",  # LAG referenced by bound_port (no members)
        "lag-2",  # LAG with a member
        "system",  # name-only bp=None (decision 4)
        "lo0",  # name-only bp=None (decision 4)
    }
    assert "lag67:0" not in names  # decision 3: colon-form unbound shell skipped
    assert "lag-4" not in names  # decision 2: empty + unreferenced LAG skipped


async def test_no_netbox_binding_returns_empty(adapter_client):
    device_id = await seed_device(nso_device_name="topo-nonb", netbox_device_id=None)
    await _seed_topology(device_id)

    nb = _nb_client()
    from nso_adapter.core.topology_interfaces import ensure_topology_interfaces
    from nso_adapter.store.models import Device

    async with session() as db:
        device = await db.get(Device, device_id)
        result = await ensure_topology_interfaces(db, device, nb)
    assert result == {}


async def test_no_client_returns_empty(adapter_client):
    device_id = await seed_device(nso_device_name="topo-nilclient", netbox_device_id=901)
    await _seed_topology(device_id)

    from nso_adapter.core.topology_interfaces import ensure_topology_interfaces
    from nso_adapter.store.models import Device

    async with session() as db:
        device = await db.get(Device, device_id)
        result = await ensure_topology_interfaces(db, device, None)
    assert result == {}


async def test_empty_topology_does_not_call_bulk_ensure(adapter_client):
    device_id = await seed_device(nso_device_name="topo-empty", netbox_device_id=902)
    # No DbInterface / isis / ip / lag rows seeded.

    import nso_adapter.core.topology_interfaces as mod
    from nso_adapter.store.models import Device

    spy = AsyncMock(return_value={})
    orig = mod.bulk_ensure_interfaces
    mod.bulk_ensure_interfaces = spy
    try:
        async with session() as db:
            device = await db.get(Device, device_id)
            result = await mod.ensure_topology_interfaces(db, device, _nb_client())
    finally:
        mod.bulk_ensure_interfaces = orig

    assert result == {}
    spy.assert_not_awaited()
