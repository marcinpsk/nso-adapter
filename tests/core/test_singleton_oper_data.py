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

from nso_adapter.core.bfd import refresh_bfd_interfaces_for_device
from nso_adapter.core.bgp import refresh_bgp_config_for_device
from nso_adapter.core.importer import _attrs_to_interface_list
from nso_adapter.core.interface_ip import refresh_interface_ips_for_device
from nso_adapter.core.interface_mtu import refresh_interface_mtu_for_device
from nso_adapter.core.isis import refresh_isis_interfaces_for_device
from nso_adapter.core.l2_service import refresh_l2_services_for_device
from nso_adapter.core.lag_config import refresh_lag_config_for_device
from nso_adapter.core.lag_topology import refresh_lag_topology_for_device
from nso_adapter.core.logging_config import refresh_logging_config_for_device
from nso_adapter.core.ospf import refresh_ospf_for_device
from nso_adapter.core.route_policy import refresh_route_policy_for_device
from nso_adapter.core.snmp import refresh_snmp_config_for_device
from nso_adapter.core.subinterface import refresh_subinterface_for_device
from nso_adapter.core.svi import refresh_svi_for_device
from nso_adapter.core.vlan import refresh_switchport_for_device, refresh_vlan_database_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import (
    Device,
    DeviceBfdInterface,
    DeviceBgpAddressFamily,
    DeviceBgpPeer,
    DeviceBgpPeerAddressFamily,
    DeviceBgpPeerGroup,
    DeviceBgpPeerGroupAddressFamily,
    DeviceBgpRouter,
    DeviceBgpScope,
    DeviceInterfaceMtu,
    DeviceIsisInterface,
    DeviceIsisProcess,
    DeviceL2Sap,
    DeviceLoggingHost,
    DeviceOspfInstance,
    DeviceOspfInterface,
    DeviceRoutePolicyCommunityList,
    DeviceRoutePolicyCommunityListEntry,
    DeviceRoutePolicyPrefixList,
    DeviceRoutePolicyPrefixListEntry,
    DeviceSubinterface,
    DeviceSvi,
    DeviceSwitchport,
    DeviceVlan,
    InterfaceIpAddress,
    LagBundleConfig,
    LagInterface,
    LagMember,
    LagMemberConfig,
    SnmpCommunity,
    SnmpHost,
    SnmpV3User,
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
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": {
                "interface-name": "GigabitEthernet0/0",
                "address": {"address": "192.0.2.1/24", "vrf": "", "family": "ipv4", "secondary": False},
            },
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
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "service": {
                "service-name": "EPIPE-1",
                "service-type": "epipe",
                "service-id": 100,
                "sap": {"sap-id": "1/1/1:10", "port": "1/1/1", "outer-tag": 10},
            },
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
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": {"interface-name": "Vlan10", "vlan-id": 10, "type": "svi"},
        }

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
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": {
                "interface-name": "GigabitEthernet0/0.10",
                "parent-interface": "GigabitEthernet0/0",
                "dot1q-vlan": 10,
                "type": "subinterface",
            },
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
async def test_bfd_singleton_bare_object(adapter_client):
    """A single bfd interface rendered as a bare object → one row (was a raw .get → crash)."""
    device_id = await seed_device(nso_device_name="single-bfd", netbox_device_id=2652)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": {"interface-name": "GigabitEthernet0/1", "min-tx": 300, "min-rx": 300, "multiplier": 3},
        }

        await refresh_bfd_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        rows = (
            (await db.execute(select(DeviceBfdInterface).where(DeviceBfdInterface.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].interface_name == "GigabitEthernet0/1"
        assert rows[0].multiplier == 3


@pytest.mark.anyio
async def test_interface_mtu_singleton_bare_object(adapter_client):
    """A single interface-mtu rendered as a bare object → one row (was a raw .get → crash)."""
    device_id = await seed_device(nso_device_name="single-mtu", netbox_device_id=2650)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": {"interface-name": "GigabitEthernet0/1", "mtu": 9000, "ip-mtu": 1500},
        }

        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="poll")

        rows = (
            (await db.execute(select(DeviceInterfaceMtu).where(DeviceInterfaceMtu.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].interface_name == "GigabitEthernet0/1"
        assert rows[0].mtu == 9000
        assert rows[0].ip_mtu == 1500


@pytest.mark.anyio
async def test_logging_host_singleton_bare_object(adapter_client):
    """A single logging host rendered as a bare object → one row (was a raw .get → crash)."""
    device_id = await seed_device(nso_device_name="single-log", netbox_device_id=2651)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "host": {"address": "10.0.0.53", "port": 514, "severity": "informational"},
        }

        await refresh_logging_config_for_device(db, device, nso_client, refresh_source="poll")

        rows = (
            (await db.execute(select(DeviceLoggingHost).where(DeviceLoggingHost.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].address == "10.0.0.53"
        assert rows[0].port == 514


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


@pytest.mark.anyio
async def test_bgp_singleton_bare_objects(adapter_client):
    """Every BGP child list (router / scope / address-family / peer / peer-group and
    the per-AF lists nested under peer and peer-group) rendered as a bare object → one
    row each. Without as_list the top-level ``for router_data in <dict>`` iterates the
    dict keys and crashes on ``router_data.get("asn")``."""
    device_id = await seed_device(nso_device_name="single-bgp", netbox_device_id=2610)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_bgp_config.return_value = {
            "router": {
                "asn": 65001,
                "scope": {
                    "vrf": "",
                    "address-family": {"afi": "ipv4-unicast"},
                    "peer": {
                        "peer-address": "10.0.0.1",
                        "remote-as": 65002,
                        "peer-address-family": {"afi": "ipv4-unicast", "routemap-in": "RM_IN"},
                    },
                    "peer-group": {
                        "name": "PG1",
                        "remote-as": 65003,
                        "peer-group-address-family": {"afi": "ipv6-unicast"},
                    },
                },
            },
        }

        ok = await refresh_bgp_config_for_device(db, device, nso_client, refresh_source="poll")
        assert ok is True

        async def _count(model):
            return (await db.execute(select(model))).scalars().all()

        routers = await _count(DeviceBgpRouter)
        assert len(routers) == 1
        assert routers[0].asn == "65001"
        assert len(await _count(DeviceBgpScope)) == 1
        afs = await _count(DeviceBgpAddressFamily)
        assert [a.af for a in afs] == ["ipv4-unicast"]
        peers = await _count(DeviceBgpPeer)
        assert [p.peer_address for p in peers] == ["10.0.0.1"]
        pafs = await _count(DeviceBgpPeerAddressFamily)
        assert [(p.af, p.routemap_in) for p in pafs] == [("ipv4-unicast", "RM_IN")]
        pgs = await _count(DeviceBgpPeerGroup)
        assert [g.name for g in pgs] == ["PG1"]
        pgafs = await _count(DeviceBgpPeerGroupAddressFamily)
        assert [g.af for g in pgafs] == ["ipv6-unicast"]


@pytest.mark.anyio
async def test_ospf_singleton_bare_objects(adapter_client):
    """OSPF instance + interface rendered as bare objects → one row each, and a single
    ``area`` under the instance is normalized to a one-element list (the ``areas`` JSON
    column is a list of areas — a bare dict would break any downstream iteration)."""
    device_id = await seed_device(nso_device_name="single-ospf", netbox_device_id=2611)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_ospf.return_value = {
            "instance": {
                "process-id": 1,
                "router-id": "1.1.1.1",
                "vrf": "",
                "area": {"area-id": "0.0.0.0"},
            },
            "interface": {"interface-name": "GigabitEthernet0/0", "process-id": 1, "area-id": "0.0.0.0"},
        }

        ok = await refresh_ospf_for_device(db, device, nso_client, refresh_source="poll")
        assert ok is True

        instances = (
            (await db.execute(select(DeviceOspfInstance).where(DeviceOspfInstance.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(instances) == 1
        assert instances[0].process_id == "1"  # String(64) column
        # the singleton area is stored as a one-element list, not a bare object
        assert instances[0].areas == [{"area-id": "0.0.0.0"}]

        interfaces = (
            (await db.execute(select(DeviceOspfInterface).where(DeviceOspfInterface.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(interfaces) == 1
        assert interfaces[0].interface_name == "GigabitEthernet0/0"


@pytest.mark.anyio
async def test_isis_singleton_bare_objects(adapter_client):
    """IS-IS process + interface rendered as bare objects → one row each.

    Without as_list the top-level ``for proc in entry.get("process")`` iterates the singleton
    dict's keys (strings) and crashes on ``proc.get("process-tag")``.
    """
    device_id = await seed_device(nso_device_name="single-isis", netbox_device_id=2612)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_isis_interfaces.return_value = {
            "process": {"process-tag": "1", "net": "49.0001.0000.0000.0001.00", "is-type": "level-2"},
            "interface": {"interface-name": "GigabitEthernet0/0", "af": "ipv4", "process-tag": "1"},
        }

        ok = await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")
        assert ok is True

        procs = (
            (await db.execute(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(procs) == 1
        assert procs[0].process_tag == "1"
        assert procs[0].is_type == "level-2-only"  # alias folded

        ifaces = (
            (await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(ifaces) == 1
        assert ifaces[0].interface_name == "GigabitEthernet0/0"


@pytest.mark.anyio
async def test_lag_config_singleton_bare_objects(adapter_client):
    """A single lag-config bundle (and its single member), both bare objects → one row each.

    Without as_list the top-level ``for bundle in entry.get("lag")`` and the nested
    ``for member in bundle.get("member")`` iterate the singleton dict's keys and crash.
    """
    device_id = await seed_device(nso_device_name="single-lagcfg", netbox_device_id=2613)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_lag_config.return_value = {
            "lag": {
                "name": "Port-channel1",
                "lag-id": 1,
                "member": {"interface-name": "GigabitEthernet0/1", "mode": "active"},
            }
        }

        ok = await refresh_lag_config_for_device(db, device, nso_client, refresh_source="poll")
        assert ok is True

        bundles = (
            (await db.execute(select(LagBundleConfig).where(LagBundleConfig.device_id == device.id))).scalars().all()
        )
        assert len(bundles) == 1
        assert bundles[0].name == "Port-channel1"
        members = (await db.execute(select(LagMemberConfig))).scalars().all()
        assert [(m.interface_name, m.mode) for m in members] == [("GigabitEthernet0/1", "active")]


@pytest.mark.anyio
async def test_snmp_singleton_bare_objects(adapter_client):
    """A single community / v3-user / host, each a bare object → one row each.

    Without as_list ``for comm in entry.get("community")`` iterates the singleton dict's keys
    and crashes on ``comm.get("name")``.
    """
    device_id = await seed_device(nso_device_name="single-snmp", netbox_device_id=2614)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_snmp_config.return_value = {
            "community": {"name": "abcdef012345", "access": "RO"},
            "v3-user": {"username": "obs", "has-auth-secret": True, "has-priv-secret": False},
            "host": {"address": "10.0.0.53", "version": "3", "user": "obs"},
        }

        ok = await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")
        assert ok is True

        comms = (await db.execute(select(SnmpCommunity).where(SnmpCommunity.device_id == device.id))).scalars().all()
        assert [c.community_hash for c in comms] == ["abcdef012345"]
        users = (await db.execute(select(SnmpV3User).where(SnmpV3User.device_id == device.id))).scalars().all()
        assert [u.username for u in users] == ["obs"]
        hosts = (await db.execute(select(SnmpHost).where(SnmpHost.device_id == device.id))).scalars().all()
        assert [h.address for h in hosts] == ["10.0.0.53"]


@pytest.mark.anyio
async def test_route_policy_singleton_bare_objects(adapter_client):
    """A single prefix-list / community-list (and their single entries), each a bare object.

    Without as_list ``for pl_data in data.get("prefix-list")`` iterates the singleton dict's
    keys and crashes on ``pl_data.get("name")``; likewise ``pl_data.get("entry")``.
    """
    device_id = await seed_device(nso_device_name="single-rp", netbox_device_id=2615)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_route_policy.return_value = {
            "prefix-list": {
                "name": "PL-1",
                "family": 4,
                "entry": {"sequence": 10, "action": "permit", "prefix": "10.0.0.0/8"},
            },
            "community-list": {
                "name": "CL-1",
                "entry": {"sequence": 10, "action": "permit", "community": "64500:1"},
            },
        }

        ok = await refresh_route_policy_for_device(db, device, nso_client, refresh_source="poll")
        assert ok is True

        pls = (
            (
                await db.execute(
                    select(DeviceRoutePolicyPrefixList).where(DeviceRoutePolicyPrefixList.device_id == device.id)
                )
            )
            .scalars()
            .all()
        )
        assert [p.name for p in pls] == ["PL-1"]
        pl_entries = (await db.execute(select(DeviceRoutePolicyPrefixListEntry))).scalars().all()
        assert [(e.sequence, e.prefix) for e in pl_entries] == [(10, "10.0.0.0/8")]
        cls = (
            (
                await db.execute(
                    select(DeviceRoutePolicyCommunityList).where(DeviceRoutePolicyCommunityList.device_id == device.id)
                )
            )
            .scalars()
            .all()
        )
        assert [c.name for c in cls] == ["CL-1"]
        cl_entries = (await db.execute(select(DeviceRoutePolicyCommunityListEntry))).scalars().all()
        assert [e.community for e in cl_entries] == ["64500:1"]
