# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/bgp._upsert_bgp_data — robustness to duplicate peers."""

from __future__ import annotations

from sqlalchemy import select

from tests.conftest import seed_device


async def _scope_with_peers(*peer_addrs: str) -> list[dict]:
    return [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"af": "ipv4-unicast"}],
                    "peer": [{"peer-address": a, "peer-group": f"G-{i}"} for i, a in enumerate(peer_addrs)],
                }
            ],
        }
    ]


async def test_duplicate_peer_in_scope_does_not_crash(adapter_client):
    """Same neighbor IP under two groups (same scope) → one row, no IntegrityError."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeer

    device_id = await seed_device(nso_device_name="bgp-dup", netbox_device_id=880)

    async for db in get_session():
        device = await db.get(Device, device_id)
        # 10.0.0.1 appears twice (two groups) — must not roll back the whole refresh.
        await _upsert_bgp_data(db, device, await _scope_with_peers("10.0.0.1", "10.0.0.2", "10.0.0.1"), "test")

        peers = (await db.execute(select(DeviceBgpPeer))).scalars().all()
        addrs = sorted(p.peer_address for p in peers)
        assert addrs == ["10.0.0.1", "10.0.0.2"]  # dup collapsed, both real peers kept
        break


async def test_duplicate_peer_merges_address_families(adapter_client):
    """A neighbor present in two groups with different AFs → one peer, both AFs merged."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeer, DeviceBgpPeerAddressFamily

    device_id = await seed_device(nso_device_name="bgp-merge", netbox_device_id=881)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"af": "ipv4-unicast"}],
                    "peer": [
                        {"peer-address": "10.0.0.1", "peer-group": "v4", "peer-address-family": [{"afi": "ipv4-unicast"}]},
                        {"peer-address": "10.0.0.1", "peer-group": "v6", "peer-address-family": [{"afi": "ipv6-unicast"}]},
                    ],
                }
            ],
        }
    ]

    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")

        peers = (await db.execute(select(DeviceBgpPeer))).scalars().all()
        assert len(peers) == 1
        afs = (await db.execute(select(DeviceBgpPeerAddressFamily))).scalars().all()
        assert sorted(a.af for a in afs) == ["ipv4-unicast", "ipv6-unicast"]
        break


async def test_inactive_peer_stored_disabled(adapter_client):
    """A deactivated (enabled=false) neighbor is stored with enabled=False."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeer

    device_id = await seed_device(nso_device_name="bgp-inactive", netbox_device_id=882)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"af": "ipv4-unicast"}],
                    "peer": [
                        {"peer-address": "10.0.0.1", "peer-group": "up", "enabled": True},
                        {"peer-address": "10.0.0.2", "peer-group": "down", "enabled": False},
                    ],
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        by_addr = {p.peer_address: p.enabled for p in (await db.execute(select(DeviceBgpPeer))).scalars().all()}
        assert by_addr == {"10.0.0.1": True, "10.0.0.2": False}
        break


async def test_peer_enabled_in_any_group_wins_on_merge(adapter_client):
    """Same neighbor in a deactivated and an active group → merged peer is enabled."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeer

    device_id = await seed_device(nso_device_name="bgp-mixed", netbox_device_id=883)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"af": "ipv4-unicast"}],
                    "peer": [
                        # deactivated occurrence first, active second — OR must win.
                        {"peer-address": "10.0.0.9", "peer-group": "down", "enabled": False},
                        {"peer-address": "10.0.0.9", "peer-group": "up", "enabled": True},
                    ],
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        peers = (await db.execute(select(DeviceBgpPeer))).scalars().all()
        assert len(peers) == 1
        assert peers[0].enabled is True
        break

async def test_peer_af_policy_in_maps_to_routemap(adapter_client):
    """Junos/Timos per-AF policy-in/out map to routemap_in/out; IOS routemap-in too."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeerAddressFamily

    device_id = await seed_device(nso_device_name="bgp-pol", netbox_device_id=882)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"af": "ipv4-unicast"}],
                    "peer": [
                        {
                            "peer-address": "10.0.0.1",
                            "peer-address-family": [
                                {"afi": "ipv4-unicast", "policy-in": "PIN", "policy-out": "POUT"}
                            ],
                        },
                        {
                            "peer-address": "10.0.0.2",
                            "peer-address-family": [
                                {"afi": "ipv4-unicast", "routemap-in": "RIN", "prefixlist-out": "PLO"}
                            ],
                        },
                    ],
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        afs = {(a.routemap_in, a.routemap_out, a.prefixlist_out) for a in (await db.execute(select(DeviceBgpPeerAddressFamily))).scalars().all()}
        assert ("PIN", "POUT", None) in afs  # policy-in/out -> routemap_in/out
        assert ("RIN", None, "PLO") in afs   # IOS routemap-in + prefixlist-out
        break

async def test_peer_source_imported(adapter_client):
    """Peer 'source' (update-source iface / local-address) is stored on the peer."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeer

    device_id = await seed_device(nso_device_name="bgp-src", netbox_device_id=883)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"af": "ipv4-unicast"}],
                    "peer": [{"peer-address": "10.0.0.1", "source": "84.116.255.1"}],
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        peers = (await db.execute(select(DeviceBgpPeer))).scalars().all()
        assert peers[0].source == "84.116.255.1"
        break


async def test_peer_group_object_imported(adapter_client):
    """Peer-group objects + their per-AF policies are mirrored (M15 B1, full-B)."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import (
        Device,
        DeviceBgpPeerGroup,
        DeviceBgpPeerGroupAddressFamily,
    )

    device_id = await seed_device(nso_device_name="bgp-pg", netbox_device_id=884)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"af": "ipv4-unicast"}],
                    "peer": [],
                    "peer-group": [
                        {
                            "name": "Arbor-IBGP",
                            "remote-as": "65100",
                            "source": "Loopback4",
                            "peer-group-address-family": [
                                {
                                    "afi": "ipv4-unicast",
                                    "routemap-in": "Arbor-IBGP-in",
                                    "routemap-out": "Arbor-IBGP-out",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        pgs = (await db.execute(select(DeviceBgpPeerGroup))).scalars().all()
        assert len(pgs) == 1
        assert pgs[0].name == "Arbor-IBGP"
        assert pgs[0].remote_as == "65100"
        assert pgs[0].source == "Loopback4"
        pgafs = (await db.execute(select(DeviceBgpPeerGroupAddressFamily))).scalars().all()
        assert len(pgafs) == 1
        assert pgafs[0].af == "ipv4-unicast"
        assert pgafs[0].routemap_in == "Arbor-IBGP-in"
        assert pgafs[0].routemap_out == "Arbor-IBGP-out"
        break
