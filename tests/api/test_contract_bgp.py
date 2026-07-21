# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/bgp-config.

Pins the EXACT JSON shape the adapter emits for the BGP read-mirror, which the
netbox-nso-plugin consumes in ``bgp_reconciler._reconcile_bgp_config`` (peers,
per-AF policies, and peer-group templates). Unlike the interfaces endpoint, the
BGP serializer OMITS optional keys when unset (rather than emitting ``null``), so
the contract is "required keys always present; any extra key is in the documented
optional set" at every nesting level.

Canonical contract: ``docs/api-contract.md`` § "GET /api/v1/devices/{id}/bgp-config".
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_bgp.py`` — the
``*_KEYS`` sets MUST stay identical across both files; if you change one, change the
doc and the other.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_bgp_config, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

# ── The contract. Keep in lockstep with the plugin mirror + docs/api-contract.md. ──
REQUIRED_TOP_KEYS = {"device_id", "last_refreshed_at", "refresh_source", "read_state", "routers"}
# router_id is always present (null when unset), like the top-level last_refreshed_at.
REQUIRED_ROUTER_KEYS = {"asn", "router_id", "scopes"}
REQUIRED_SCOPE_KEYS = {"vrf", "address_families", "peers", "peer_groups"}
REQUIRED_PEER_KEYS = {"peer_address", "enabled", "address_families"}
OPTIONAL_PEER_KEYS = {"peer_group", "remote_as", "local_as", "ttl", "password", "source", "bfd_enabled"}
REQUIRED_PEER_AF_KEYS = {"af", "enabled"}
OPTIONAL_PEER_AF_KEYS = {"routemap_in", "routemap_out", "prefixlist_in", "prefixlist_out"}
REQUIRED_PG_KEYS = {"name", "address_families"}
OPTIONAL_PG_KEYS = {"remote_as", "source"}
REQUIRED_PG_AF_KEYS = {"af"}
OPTIONAL_PG_AF_KEYS = {"routemap_in", "routemap_out", "prefixlist_in", "prefixlist_out"}


@pytest.mark.anyio
async def test_bgp_config_payload_matches_contract_exactly(adapter_client):
    """Every nesting level exposes its required keys; extras are only documented optionals."""
    device_id = await seed_device(nso_device_name="bgp-contract-dev", netbox_device_id=7910)
    await seed_bgp_config(
        device_id,
        asn="65100",
        router_id="10.255.0.1",
        scopes=[
            {
                "vrf": "",
                "afs": ["ipv4-unicast", "ipv6-unicast"],
                "peers": [
                    # MAXIMAL peer — every optional key set, so its key set == required ∪ optional.
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
                    # MINIMAL peer — no optionals set, so they must be OMITTED (not null).
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

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()

    assert set(body.keys()) == REQUIRED_TOP_KEYS
    assert isinstance(body["routers"], list) and len(body["routers"]) == 1
    bgp_router = body["routers"][0]
    assert set(bgp_router.keys()) == REQUIRED_ROUTER_KEYS
    assert bgp_router["router_id"] == "10.255.0.1"  # dotted-quad string, always present

    scope = bgp_router["scopes"][0]
    assert set(scope.keys()) == REQUIRED_SCOPE_KEYS
    assert scope["address_families"] == ["ipv4-unicast", "ipv6-unicast"]  # list[str]

    peers = {p["peer_address"]: p for p in scope["peers"]}
    maximal, minimal = peers["192.0.2.1"], peers["192.0.2.2"]

    # Maximal peer: every optional present → required ∪ optional.
    assert set(maximal.keys()) == REQUIRED_PEER_KEYS | OPTIONAL_PEER_KEYS
    # Minimal peer: optionals OMITTED, not emitted as null.
    assert set(minimal.keys()) == REQUIRED_PEER_KEYS

    paf = maximal["address_families"][0]
    assert set(paf.keys()) == REQUIRED_PEER_AF_KEYS | OPTIONAL_PEER_AF_KEYS

    pg = scope["peer_groups"][0]
    assert set(pg.keys()) == REQUIRED_PG_KEYS | OPTIONAL_PG_KEYS
    pgaf = pg["address_families"][0]
    assert set(pgaf.keys()) == REQUIRED_PG_AF_KEYS | OPTIONAL_PG_AF_KEYS

    # Types/format the consumer relies on.
    assert isinstance(maximal["enabled"], bool)
    assert isinstance(maximal["bfd_enabled"], bool)
    assert isinstance(maximal["remote_as"], str)  # ASN as string, not int


@pytest.mark.anyio
async def test_bgp_config_no_data_shape(adapter_client):
    """The empty shape keeps the same top-level keys (refresh_source='never')."""
    device_id = await seed_device(nso_device_name="bgp-contract-empty", netbox_device_id=7911)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == REQUIRED_TOP_KEYS
    assert body["routers"] == []
    assert body["refresh_source"] == "never"
