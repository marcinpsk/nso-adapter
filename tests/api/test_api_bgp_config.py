# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/bgp-config (M15 A3)."""
from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_bgp_config, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


# ── empty / auth ─────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_bgp_config_no_data_returns_never(adapter_client):
    """Device with no BGP rows → 200 with refresh_source='never', empty routers."""
    device_id = await seed_device(nso_device_name="bgp-empty-dev", netbox_device_id=900)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "never"
    assert body["last_refreshed_at"] is None
    assert body["routers"] == []


@pytest.mark.anyio
async def test_bgp_config_requires_auth(adapter_client):
    """Missing auth token → 401."""
    device_id = await seed_device(nso_device_name="bgp-noauth-dev", netbox_device_id=901)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_bgp_config_device_not_found(adapter_client):
    """Non-existent device id → 404."""
    resp = await adapter_client.get("/api/v1/devices/99999/bgp-config", headers=AUTH)
    assert resp.status_code == 404


# ── populated data ────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_bgp_config_basic_router_and_global_scope(adapter_client):
    """Single router, global scope with two AFs and one peer."""
    device_id = await seed_device(nso_device_name="bgp-basic-dev", netbox_device_id=902)
    await seed_bgp_config(
        device_id,
        asn="65100",
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
                        "peer_afs": ["ipv4-unicast"],
                    }
                ],
            }
        ],
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()

    assert body["device_id"] == device_id
    assert body["refresh_source"] == "test"
    assert len(body["routers"]) == 1
    bgp_router = body["routers"][0]
    assert bgp_router["asn"] == "65100"
    assert len(bgp_router["scopes"]) == 1

    scope = bgp_router["scopes"][0]
    assert scope["vrf"] == ""
    assert set(scope["address_families"]) == {"ipv4-unicast", "ipv6-unicast"}
    assert len(scope["peers"]) == 1

    peer = scope["peers"][0]
    assert peer["peer_address"] == "192.0.2.1"
    assert peer["enabled"] is False
    assert peer["peer_group"] == "UPSTREAM"
    assert peer["remote_as"] == "65001"
    assert "local_as" not in peer  # omitted when None
    assert "ttl" not in peer
    assert "password" not in peer
    assert len(peer["address_families"]) == 1
    assert peer["address_families"][0]["af"] == "ipv4-unicast"
    assert peer["address_families"][0]["enabled"] is True


@pytest.mark.anyio
async def test_bgp_config_multiple_vrf_scopes(adapter_client):
    """Router with global + two VRF scopes returns all three scopes."""
    device_id = await seed_device(nso_device_name="bgp-vrf-dev", netbox_device_id=903)
    await seed_bgp_config(
        device_id,
        asn="65100",
        scopes=[
            {"vrf": "", "afs": ["ipv4-unicast"], "peers": []},
            {"vrf": "ASPAN", "afs": ["ipv4-unicast"], "peers": [
                {"peer_address": "10.0.0.1", "remote_as": "65200", "peer_afs": ["ipv4-unicast"]}
            ]},
            {"vrf": "MTI", "afs": ["ipv4-unicast"], "peers": []},
        ],
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()

    scopes = body["routers"][0]["scopes"]
    assert len(scopes) == 3
    vrfs = {s["vrf"] for s in scopes}
    assert vrfs == {"", "ASPAN", "MTI"}

    aspan_scope = next(s for s in scopes if s["vrf"] == "ASPAN")
    assert len(aspan_scope["peers"]) == 1
    assert aspan_scope["peers"][0]["remote_as"] == "65200"


@pytest.mark.anyio
async def test_bgp_config_password_included_when_set(adapter_client):
    """Password is included in the response when present (plaintext by design)."""
    device_id = await seed_device(nso_device_name="bgp-pw-dev", netbox_device_id=904)
    await seed_bgp_config(
        device_id,
        asn="65100",
        scopes=[
            {
                "vrf": "",
                "afs": ["ipv4-unicast"],
                "peers": [{"peer_address": "192.0.2.2", "password": "bgpS3cr3t", "peer_afs": []}],
            }
        ],
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert resp.status_code == 200
    peer = resp.json()["routers"][0]["scopes"][0]["peers"][0]
    assert peer["password"] == "bgpS3cr3t"
