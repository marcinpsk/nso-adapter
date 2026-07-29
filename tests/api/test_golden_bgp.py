# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/bgp-config.

Deep-equality proof of the EXACT nested JSON the BGP read-mirror emits. The
contract test pins key *sets* at each level; this pins key *values* end to end —
the drop guard for response-model typing across the router→scope→peer→af and
peer-group→af trees, where a missing optional (bfd_enabled, a per-AF policy ref)
would be silently swallowed by response_model_exclude_unset.

Covers: a maximal peer (every optional) + a minimal peer (optionals omitted, not
null), a peer-group with per-AF policies, a router with router_id set + a router
with router_id null (always present), and the empty device.

``last_refreshed_at`` is a formatted "<iso>Z" string, pinned literally.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import select

from tests.conftest import (
    GOLDEN_BORN_ISO,
    GOLDEN_INCARNATION,
    VALID_TOKEN,
    pin_store_incarnation,
    seed_bgp_config,
    seed_device,
    session,
)

_SYNTH_READ_STATE = {
    "outcome": "unavailable",
    "reason": "not_ready",
    "freshness": None,
    "result": None,
    "succeeded": None,
    "read_at": None,
    "attempt_id": None,
    "source_epoch": 1,
    "payload_revision": None,
    "incarnation": GOLDEN_INCARNATION,
    "incarnation_born": GOLDEN_BORN_ISO,
}

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0)


async def _set_router_refreshed(device_id: int) -> None:
    """seed_bgp_config leaves last_refreshed_at NULL; set it so the "<iso>Z" path is exercised."""
    from nso_adapter.store.models import DeviceBgpRouter

    async with session() as db:
        rows = (await db.execute(select(DeviceBgpRouter).where(DeviceBgpRouter.device_id == device_id))).scalars().all()
        for r in rows:
            r.last_refreshed_at = TS
        await db.commit()


@pytest.mark.anyio
async def test_bgp_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="bgp-golden", netbox_device_id=7915)
    await pin_store_incarnation()
    await seed_bgp_config(
        device_id,
        asn="65100",
        router_id="10.255.0.1",
        scopes=[
            {
                "vrf": "",
                "afs": ["ipv4-unicast", "ipv6-unicast"],
                "peers": [
                    {
                        "peer_address": "192.0.2.1",
                        "enabled": False,
                        "peer_group": "UPSTREAM",
                        "remote_as": "65001",
                        "local_as": "65100",
                        "ttl": 2,
                        "password": "s3cret",
                        "source": "Loopback0",
                        "bfd_enabled": True,
                        "peer_af_defs": [
                            {
                                "af": "ipv4-unicast",
                                "enabled": True,
                                "routemap_in": "RM-IN",
                                "routemap_out": "RM-OUT",
                                "prefixlist_in": "PL-IN",
                                "prefixlist_out": "PL-OUT",
                            }
                        ],
                    },
                    {"peer_address": "192.0.2.2", "enabled": True, "peer_afs": ["ipv4-unicast"]},
                ],
                "peer_groups": [
                    {
                        "name": "UPSTREAM",
                        "remote_as": "65001",
                        "source": "Loopback0",
                        "af_defs": [
                            {
                                "af": "ipv4-unicast",
                                "routemap_in": "RM-IN",
                                "routemap_out": "RM-OUT",
                                "prefixlist_in": "PL-IN",
                                "prefixlist_out": "PL-OUT",
                            }
                        ],
                    }
                ],
            }
        ],
    )
    await _set_router_refreshed(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "test",
        "read_state": _SYNTH_READ_STATE,
        "routers": [
            {
                "asn": "65100",
                "router_id": "10.255.0.1",
                "scopes": [
                    {
                        "vrf": "",
                        "address_families": ["ipv4-unicast", "ipv6-unicast"],
                        "peers": [
                            {
                                "peer_address": "192.0.2.1",
                                "enabled": False,
                                "address_families": [
                                    {
                                        "af": "ipv4-unicast",
                                        "enabled": True,
                                        "routemap_in": "RM-IN",
                                        "routemap_out": "RM-OUT",
                                        "prefixlist_in": "PL-IN",
                                        "prefixlist_out": "PL-OUT",
                                    }
                                ],
                                "peer_group": "UPSTREAM",
                                "remote_as": "65001",
                                "local_as": "65100",
                                "ttl": 2,
                                "password": "s3cret",
                                "source": "Loopback0",
                                "bfd_enabled": True,
                            },
                            {
                                "peer_address": "192.0.2.2",
                                "enabled": True,
                                "address_families": [{"af": "ipv4-unicast", "enabled": True}],
                            },
                        ],
                        "peer_groups": [
                            {
                                "name": "UPSTREAM",
                                "address_families": [
                                    {
                                        "af": "ipv4-unicast",
                                        "routemap_in": "RM-IN",
                                        "routemap_out": "RM-OUT",
                                        "prefixlist_in": "PL-IN",
                                        "prefixlist_out": "PL-OUT",
                                    }
                                ],
                                "remote_as": "65001",
                                "source": "Loopback0",
                            }
                        ],
                    }
                ],
            }
        ],
    }


@pytest.mark.anyio
async def test_bgp_golden_router_id_null(adapter_client):
    """router_id is ALWAYS present (null when unset); empty peer/peer-group lists stay []."""
    device_id = await seed_device(nso_device_name="bgp-golden-null", netbox_device_id=7916)
    await pin_store_incarnation()
    # Default scope: one af, no peers, no peer-groups; router_id unset. last_refreshed_at stays NULL.
    await seed_bgp_config(device_id, asn="65200")

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "test",
        "read_state": _SYNTH_READ_STATE,
        "routers": [
            {
                "asn": "65200",
                "router_id": None,
                "scopes": [
                    {
                        "vrf": "",
                        "address_families": ["ipv4-unicast"],
                        "peers": [],
                        "peer_groups": [],
                    }
                ],
            }
        ],
    }


@pytest.mark.anyio
async def test_bgp_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="bgp-golden-empty", netbox_device_id=7917)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "read_state": _SYNTH_READ_STATE,
        "routers": [],
    }
