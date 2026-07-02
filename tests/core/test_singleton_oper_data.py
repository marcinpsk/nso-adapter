# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""s2-3 regression: NSO RESTCONF can render a single-entry YANG list as a bare
object instead of a one-element JSON array (it does *not* reliably follow RFC 7951
array encoding — the same reason ``NsoClient.list_devices`` carries an ``isinstance``
guard). A child-list parser that assumes an array then does ``for x in <dict>`` and
iterates the dict *keys* (strings), so ``x.get(...)``/``x[...]`` raises
AttributeError/TypeError (crash) or ``len(dict)`` mis-counts. Every oper-data parser
must route child lists through ``as_list`` so a singleton yields exactly one row.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.importer import _attrs_to_interface_list
from nso_adapter.core.interface_ip import refresh_interface_ips_for_device
from nso_adapter.core.l2_service import refresh_l2_services_for_device
from nso_adapter.core.lag_topology import refresh_lag_topology_for_device
from nso_adapter.core.subinterface import refresh_subinterface_for_device
from nso_adapter.core.svi import refresh_svi_for_device
from nso_adapter.core.vlan import refresh_switchport_for_device, refresh_vlan_database_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import (
    Device,
    DeviceL2Sap,
    DeviceSubinterface,
    DeviceSvi,
    DeviceSwitchport,
    DeviceVlan,
    InterfaceIpAddress,
    LagInterface,
    LagMember,
)
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


@pytest.mark.anyio
async def test_lag_topology_singleton_bare_object(adapter_client):
    """A single lag (and its single member) rendered as bare objects → one row each."""
    device_id = await seed_device(nso_device_name="single-lag", netbox_device_id=2600)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_lag_topology.return_value = {
            "device-name": "single-lag",
            "lag": {
                "name": "Port-channel1",
                "lag-id": 1,
                "member": {"interface-name": "GigabitEthernet0/1", "mode": "active"},
            },
        }

        await refresh_lag_topology_for_device(db, device, nso_client, refresh_source="poll")

        lags = (await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))).scalars().all()
        members = (await db.execute(select(LagMember))).scalars().all()
        assert len(lags) == 1
        assert lags[0].name == "Port-channel1"
        assert {(m.interface_name, m.mode) for m in members} == {("GigabitEthernet0/1", "active")}


@pytest.mark.anyio
async def test_interface_ip_singleton_bare_object(adapter_client):
    """A single interface with a single address, both bare objects → one row."""
    device_id = await seed_device(nso_device_name="single-ip", netbox_device_id=2601)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_interface_ips.return_value = {
            "interface": {
                "interface-name": "GigabitEthernet0/0",
                "address": {"address": "192.0.2.1/24", "vrf": "", "family": "ipv4", "secondary": False},
            }
        }

        await refresh_interface_ips_for_device(db, device, nso_client, refresh_source="poll")

        rows = (
            (await db.execute(select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].address == "192.0.2.1/24"
        assert rows[0].interface_name == "GigabitEthernet0/0"


@pytest.mark.anyio
async def test_l2_service_singleton_bare_object(adapter_client):
    """A single service with a single sap, both bare objects → one row."""
    device_id = await seed_device(nso_device_name="single-l2", netbox_device_id=2602)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_l2_services.return_value = {
            "service": {
                "service-name": "EPIPE-1",
                "service-type": "epipe",
                "service-id": 100,
                "sap": {"sap-id": "1/1/1:10", "port": "1/1/1", "outer-tag": 10},
            }
        }

        await refresh_l2_services_for_device(db, device, nso_client, refresh_source="poll")

        rows = (await db.execute(select(DeviceL2Sap).where(DeviceL2Sap.device_id == device.id))).scalars().all()
        assert len(rows) == 1
        assert rows[0].service_name == "EPIPE-1"
        assert rows[0].sap_id == "1/1/1:10"


@pytest.mark.anyio
async def test_svi_singleton_bare_object(adapter_client):
    """A single svi interface rendered as a bare object → one row."""
    device_id = await seed_device(nso_device_name="single-svi", netbox_device_id=2603)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_svi.return_value = {"interface": {"interface-name": "Vlan10", "vlan-id": 10, "type": "svi"}}

        await refresh_svi_for_device(db, device, nso_client, refresh_source="poll")

        rows = (await db.execute(select(DeviceSvi).where(DeviceSvi.device_id == device.id))).scalars().all()
        assert len(rows) == 1
        assert rows[0].interface_name == "Vlan10"
        assert rows[0].vlan_id == 10


@pytest.mark.anyio
async def test_subinterface_singleton_bare_object(adapter_client):
    """A single subinterface rendered as a bare object → one row."""
    device_id = await seed_device(nso_device_name="single-subif", netbox_device_id=2604)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_subinterface.return_value = {
            "interface": {
                "interface-name": "GigabitEthernet0/0.10",
                "parent-interface": "GigabitEthernet0/0",
                "dot1q-vlan": 10,
                "type": "subinterface",
            }
        }

        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="poll")

        rows = (
            (await db.execute(select(DeviceSubinterface).where(DeviceSubinterface.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].interface_name == "GigabitEthernet0/0.10"
        assert rows[0].dot1q_vlan == 10


@pytest.mark.anyio
async def test_vlan_database_singleton_bare_object(adapter_client):
    """A single VLAN rendered as a bare object → one row."""
    device_id = await seed_device(nso_device_name="single-vlan", netbox_device_id=2605)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_vlan_database.return_value = {"vlan": {"vlan-id": 10, "name": "MGMT"}}

        await refresh_vlan_database_for_device(db, device, nso_client, refresh_source="poll")

        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert len(rows) == 1
        assert rows[0].vlan_id == 10
        assert rows[0].name == "MGMT"


@pytest.mark.anyio
async def test_switchport_singleton_bare_object(adapter_client):
    """A single switchport interface rendered as a bare object → one row."""
    device_id = await seed_device(nso_device_name="single-swp", netbox_device_id=2606)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_switchport.return_value = {
            "interface": {"interface-name": "GigabitEthernet0/1", "mode": "access"}
        }

        await refresh_switchport_for_device(db, device, nso_client, refresh_source="poll")

        rows = (
            (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().all()
        )
        assert len(rows) == 1
        assert rows[0].interface_name == "GigabitEthernet0/1"
        assert rows[0].mode == "access"


def test_attrs_to_interface_list_singleton_bare_object():
    """importer: a single interface rendered as a bare object → one Interface."""
    entry = {
        "device-name": "sw01",
        "interface": {"interface-name": "GigabitEthernet0/1", "description": "uplink", "enabled": True},
    }
    result = _attrs_to_interface_list(entry)
    assert len(result) == 1
    assert result[0].name == "GigabitEthernet0/1"
    assert result[0].nso.description == "uplink"
