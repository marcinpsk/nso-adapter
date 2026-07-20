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
                    "address-family": [{"afi": "ipv4-unicast"}],
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
                    "address-family": [{"afi": "ipv4-unicast"}],
                    "peer": [
                        {
                            "peer-address": "10.0.0.1",
                            "peer-group": "v4",
                            "peer-address-family": [{"afi": "ipv4-unicast"}],
                        },
                        {
                            "peer-address": "10.0.0.1",
                            "peer-group": "v6",
                            "peer-address-family": [{"afi": "ipv6-unicast"}],
                        },
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
                    "address-family": [{"afi": "ipv4-unicast"}],
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
                    "address-family": [{"afi": "ipv4-unicast"}],
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


async def test_disabled_duplicate_does_not_disable_active_peer(adapter_client):
    """Active occurrence first, deactivated duplicate second → merged peer stays enabled."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeer

    device_id = await seed_device(nso_device_name="bgp-mixed2", netbox_device_id=897)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"afi": "ipv4-unicast"}],
                    "peer": [
                        {"peer-address": "10.0.0.5", "peer-group": "up", "enabled": True},
                        {"peer-address": "10.0.0.5", "peer-group": "down", "enabled": False},
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
        assert peers[0].enabled is True  # the disabled duplicate did not flip it
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
                    "address-family": [{"afi": "ipv4-unicast"}],
                    "peer": [
                        {
                            "peer-address": "10.0.0.1",
                            "peer-address-family": [{"afi": "ipv4-unicast", "policy-in": "PIN", "policy-out": "POUT"}],
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
        afs = {
            (a.routemap_in, a.routemap_out, a.prefixlist_out)
            for a in (await db.execute(select(DeviceBgpPeerAddressFamily))).scalars().all()
        }
        assert ("PIN", "POUT", None) in afs  # policy-in/out -> routemap_in/out
        assert ("RIN", None, "PLO") in afs  # IOS routemap-in + prefixlist-out
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
                    "address-family": [{"afi": "ipv4-unicast"}],
                    "peer": [{"peer-address": "10.0.0.1", "source": "198.18.255.1"}],
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        peers = (await db.execute(select(DeviceBgpPeer))).scalars().all()
        assert peers[0].source == "198.18.255.1"
        break


async def test_peer_group_object_imported(adapter_client):
    """Peer-group objects + their per-AF policies are mirrored (full-B)."""
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
                    "address-family": [{"afi": "ipv4-unicast"}],
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


# ── malformed / duplicate oper-data is skipped, not fatal ─────────────────────


class _FakeNso:
    """Minimal NsoClient stand-in: returns a canned BGP entry or raises (NSO is the boundary)."""

    def __init__(self, entry=None, exc=None):
        self._entry = entry
        self._exc = exc
        self.calls: list[str] = []

    async def get_device_state_section(self, nso_device_name, wire_family):
        # bgp is envelope-flipped (READSEM S3 B3): the engine reads its section.
        assert wire_family == "bgp-config"
        self.calls.append(nso_device_name)
        if self._exc is not None:
            raise self._exc
        if self._entry is None:
            return None
        return {"status": "ok", **self._entry}


async def test_router_without_asn_is_skipped(adapter_client):
    """A router entry with a blank asn is skipped; valid routers still import."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpRouter

    device_id = await seed_device(nso_device_name="bgp-noasn", netbox_device_id=890)
    routers = [{"asn": "", "scope": []}, {"asn": "65100", "scope": []}]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        rows = (await db.execute(select(DeviceBgpRouter))).scalars().all()
        assert [r.asn for r in rows] == ["65100"]  # asn-less router skipped
        break


async def test_malformed_entries_are_skipped(adapter_client):
    """Empty af name, peer without address, and empty peer-group name are all skipped."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import (
        Device,
        DeviceBgpAddressFamily,
        DeviceBgpPeer,
        DeviceBgpPeerGroup,
    )

    device_id = await seed_device(nso_device_name="bgp-malformed", netbox_device_id=891)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"afi": ""}, {"afi": "ipv4-unicast"}],  # empty af skipped
                    "peer": [{"peer-address": ""}, {"peer-address": "10.0.0.1"}],  # nameless peer skipped
                    "peer-group": [{"name": ""}, {"name": "PG"}],  # empty pg name skipped
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        afs = (await db.execute(select(DeviceBgpAddressFamily))).scalars().all()
        peers = (await db.execute(select(DeviceBgpPeer))).scalars().all()
        pgs = (await db.execute(select(DeviceBgpPeerGroup))).scalars().all()
        assert [a.af for a in afs] == ["ipv4-unicast"]
        assert [p.peer_address for p in peers] == ["10.0.0.1"]
        assert [g.name for g in pgs] == ["PG"]
        break


async def test_duplicate_afis_and_peer_group_names_deduped(adapter_client):
    """Repeated peer-AF afis, a repeated peer-group name, and repeated pg-AF afis collapse."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import (
        Device,
        DeviceBgpPeerAddressFamily,
        DeviceBgpPeerGroup,
        DeviceBgpPeerGroupAddressFamily,
    )

    device_id = await seed_device(nso_device_name="bgp-dedup", netbox_device_id=892)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"afi": "ipv4-unicast"}],
                    "peer": [
                        {
                            "peer-address": "10.0.0.1",
                            "peer-address-family": [{"afi": "ipv4-unicast"}, {"afi": "ipv4-unicast"}],  # dup afi
                        }
                    ],
                    "peer-group": [
                        {"name": "PG", "peer-group-address-family": [{"afi": "ipv4-unicast"}, {"afi": "ipv4-unicast"}]},
                        {"name": "PG", "peer-group-address-family": [{"afi": "ipv6-unicast"}]},  # dup pg name → skipped
                    ],
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        pafs = (await db.execute(select(DeviceBgpPeerAddressFamily))).scalars().all()
        pgs = (await db.execute(select(DeviceBgpPeerGroup))).scalars().all()
        pgafs = (await db.execute(select(DeviceBgpPeerGroupAddressFamily))).scalars().all()
        assert [a.af for a in pafs] == ["ipv4-unicast"]  # dup afi collapsed
        assert len(pgs) == 1  # dup peer-group name collapsed
        assert [a.af for a in pgafs] == ["ipv4-unicast"]  # only the first PG's (deduped) afi
        break


# ── refresh_bgp_config_for_device + handle_bgp_config_change (the wrappers) ────


async def test_refresh_skips_device_without_nso_name(adapter_client):
    """A device with no nso_device_name short-circuits — NSO is never queried."""
    from nso_adapter.core.bgp import refresh_bgp_config_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    fake = _FakeNso(entry={"router": [{"asn": "65100", "scope": []}]})
    async for db in get_session():
        device = Device(nso_instance="nso-dev", nso_device_name="", netbox_device_id=999)
        await refresh_bgp_config_for_device(db, device, fake)
        break
    assert fake.calls == []  # never reached the NSO read


async def test_refresh_swallows_nso_error(adapter_client):
    """An NSO read error is logged, not raised, and leaves no partial rows."""
    from nso_adapter.core.bgp import refresh_bgp_config_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpRouter

    device_id = await seed_device(nso_device_name="bgp-err", netbox_device_id=893)
    fake = _FakeNso(exc=RuntimeError("NSO down"))
    async for db in get_session():
        device = await db.get(Device, device_id)
        await refresh_bgp_config_for_device(db, device, fake)  # must not raise
        rows = (await db.execute(select(DeviceBgpRouter).where(DeviceBgpRouter.device_id == device_id))).scalars().all()
        assert rows == []
        break
    assert fake.calls == ["bgp-err"]


async def test_refresh_happy_path_upserts_rows(adapter_client):
    """A successful read upserts rows and stamps the supplied refresh_source."""
    from nso_adapter.core.bgp import refresh_bgp_config_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeer, DeviceBgpRouter

    device_id = await seed_device(nso_device_name="bgp-ok", netbox_device_id=894)
    entry = {
        "router": [
            {
                "asn": "65100",
                "scope": [
                    {"vrf": "", "address-family": [{"afi": "ipv4-unicast"}], "peer": [{"peer-address": "10.0.0.1"}]}
                ],
            }
        ]
    }
    fake = _FakeNso(entry=entry)
    async for db in get_session():
        device = await db.get(Device, device_id)
        await refresh_bgp_config_for_device(db, device, fake, refresh_source="sse")
        routers = (
            (await db.execute(select(DeviceBgpRouter).where(DeviceBgpRouter.device_id == device_id))).scalars().all()
        )
        peers = (await db.execute(select(DeviceBgpPeer))).scalars().all()
        assert [r.asn for r in routers] == ["65100"]
        assert routers[0].refresh_source == "sse"
        assert [p.peer_address for p in peers] == ["10.0.0.1"]
        break


async def test_refresh_empty_entry_clears_rows(adapter_client):
    """A None/empty entry yields zero routers (the `if entry else []` branch)."""
    from nso_adapter.core.bgp import refresh_bgp_config_for_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpRouter

    device_id = await seed_device(nso_device_name="bgp-empty", netbox_device_id=896)
    fake = _FakeNso(entry=None)
    async for db in get_session():
        device = await db.get(Device, device_id)
        await refresh_bgp_config_for_device(db, device, fake)
        rows = (await db.execute(select(DeviceBgpRouter).where(DeviceBgpRouter.device_id == device_id))).scalars().all()
        assert rows == []
        break


async def test_handle_bgp_change_unknown_device_is_noop(adapter_client):
    """An SSE event for an unknown device returns without touching NSO."""
    from nso_adapter.core.bgp import handle_bgp_config_change
    from nso_adapter.store.db import get_session

    fake = _FakeNso(entry={"router": []})
    async for db in get_session():
        await handle_bgp_config_change(db, "no-such-device", fake)  # returns, no crash
        break
    assert fake.calls == []


async def test_handle_bgp_change_known_device_refreshes(adapter_client):
    """An SSE event for a known device drives a refresh tagged refresh_source='sse'."""
    from nso_adapter.core.bgp import handle_bgp_config_change
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceBgpRouter

    device_id = await seed_device(nso_device_name="bgp-sse", netbox_device_id=895)
    fake = _FakeNso(entry={"router": [{"asn": "65100", "scope": []}]})
    async for db in get_session():
        await handle_bgp_config_change(db, "bgp-sse", fake)
        routers = (
            (await db.execute(select(DeviceBgpRouter).where(DeviceBgpRouter.device_id == device_id))).scalars().all()
        )
        assert [r.asn for r in routers] == ["65100"]
        assert routers[0].refresh_source == "sse"
        break
    assert fake.calls == ["bgp-sse"]


async def test_same_neighbor_af_conflict_across_groups_is_observable(adapter_client):
    """s2-8: the same neighbor+afi listed under two groups with DIFFERENT policies can't
    be stored twice (uq_devicebgppeeraf_identity). First occurrence wins on the row, but
    the dropped second policy must be OBSERVABLE (logged), not silently swallowed."""
    from structlog.testing import capture_logs

    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeerAddressFamily

    device_id = await seed_device(nso_device_name="bgp-afconflict", netbox_device_id=898)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"afi": "ipv4-unicast"}],
                    "peer": [
                        {
                            "peer-address": "10.0.0.1",
                            "peer-group": "v4-A",
                            "peer-address-family": [{"afi": "ipv4-unicast", "routemap-in": "RM-A"}],
                        },
                        {
                            "peer-address": "10.0.0.1",
                            "peer-group": "v4-B",
                            "peer-address-family": [{"afi": "ipv4-unicast", "routemap-in": "RM-B"}],
                        },
                    ],
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        with capture_logs() as logs:
            await _upsert_bgp_data(db, device, routers, "test")
        afs = (await db.execute(select(DeviceBgpPeerAddressFamily))).scalars().all()
        # one row (unique constraint) — first occurrence wins
        assert len(afs) == 1
        assert afs[0].routemap_in == "RM-A"
        # the dropped RM-B policy is surfaced, not silently swallowed
        events = [e.get("event") for e in logs]
        assert "bgp.peer_af_conflict_across_groups" in events
        break


async def test_same_neighbor_identical_af_across_groups_is_quiet(adapter_client):
    """s2-8: an identical AF policy repeated across groups is a benign duplicate — no
    conflict warning (only a genuinely different second policy is observable)."""
    from structlog.testing import capture_logs

    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeerAddressFamily

    device_id = await seed_device(nso_device_name="bgp-afsame", netbox_device_id=899)
    routers = [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "peer": [
                        {
                            "peer-address": "10.0.0.1",
                            "peer-group": "v4-A",
                            "peer-address-family": [{"afi": "ipv4-unicast", "routemap-in": "RM"}],
                        },
                        {
                            "peer-address": "10.0.0.1",
                            "peer-group": "v4-B",
                            "peer-address-family": [{"afi": "ipv4-unicast", "routemap-in": "RM"}],
                        },
                    ],
                }
            ],
        }
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        with capture_logs() as logs:
            await _upsert_bgp_data(db, device, routers, "test")
        afs = (await db.execute(select(DeviceBgpPeerAddressFamily))).scalars().all()
        assert len(afs) == 1
        events = [e.get("event") for e in logs]
        assert "bgp.peer_af_conflict_across_groups" not in events
        break


async def test_router_id_imported(adapter_client):
    """Global BGP router-id (dash-keyed export leaf) is stored on the router row."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpRouter

    device_id = await seed_device(nso_device_name="bgp-rid", netbox_device_id=885)
    routers = [{"asn": "65100", "router-id": "10.255.0.1", "scope": []}]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        row = (await db.execute(select(DeviceBgpRouter))).scalars().one()
        assert row.router_id == "10.255.0.1"
        break


async def test_router_id_absent_stored_none(adapter_client):
    """A router with no router-id leaf stores None, not the empty string."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpRouter

    device_id = await seed_device(nso_device_name="bgp-norid", netbox_device_id=886)
    routers = [{"asn": "65100", "scope": []}]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_bgp_data(db, device, routers, "test")
        row = (await db.execute(select(DeviceBgpRouter))).scalars().one()
        assert row.router_id is None
        break
