# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for PUT /api/v1/devices/{id}/bgp-intent (M16 B2)."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

MINIMAL_ROUTER = {
    "asn": "65100",
    "scopes": [
        {
            "vrf": "",
            "address_families": [{"af": "ipv4-unicast"}],
            "peers": [
                {
                    "peer_address": "192.0.2.1",
                    "remote_as": "65200",
                    "address_families": [{"af": "ipv4-unicast", "enabled": True}],
                }
            ],
        }
    ],
}


# ── auth / 404 ────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_put_bgp_intent_requires_auth(adapter_client):
    """Missing token → 401."""
    device_id = await seed_device(nso_device_name="bgp-intent-noauth", netbox_device_id=2000)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
    )
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_put_bgp_intent_device_not_found(adapter_client):
    """Non-existent device → 404."""
    resp = await adapter_client.put(
        "/api/v1/devices/99999/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
        headers=AUTH,
    )
    assert resp.status_code == 404


# ── happy path ────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_put_bgp_intent_happy_path(adapter_client):
    """PUT with a single router returns router_count=1."""
    device_id = await seed_device(nso_device_name="bgp-intent-happy", netbox_device_id=2001)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
        headers=AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["router_count"] == 1


@pytest.mark.anyio
async def test_put_bgp_intent_persists_rows(adapter_client):
    """PUT stores router/scope/AF/peer/peer-AF rows in the DB."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import (
        BgpAfIntent,
        BgpPeerAfIntent,
        BgpPeerIntent,
        BgpRouterIntent,
        BgpScopeIntent,
    )

    device_id = await seed_device(nso_device_name="bgp-intent-persist", netbox_device_id=2002)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    async for db in get_session():
        router = (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalar_one()
        assert router.asn == "65100"
        assert router.accepted_at is not None

        scope = (await db.execute(select(BgpScopeIntent).where(BgpScopeIntent.router_id == router.id))).scalar_one()
        assert scope.vrf == ""

        af = (await db.execute(select(BgpAfIntent).where(BgpAfIntent.scope_id == scope.id))).scalar_one()
        assert af.af == "ipv4-unicast"

        peer = (await db.execute(select(BgpPeerIntent).where(BgpPeerIntent.scope_id == scope.id))).scalar_one()
        assert peer.peer_address == "192.0.2.1"
        assert peer.remote_as == "65200"

        peer_af = (await db.execute(select(BgpPeerAfIntent).where(BgpPeerAfIntent.peer_id == peer.id))).scalar_one()
        assert peer_af.af == "ipv4-unicast"
        assert peer_af.enabled is True
        break


@pytest.mark.anyio
async def test_put_bgp_intent_full_replace(adapter_client):
    """Second PUT fully replaces the first (full-replace semantics)."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import BgpRouterIntent

    device_id = await seed_device(nso_device_name="bgp-intent-replace", netbox_device_id=2003)

    await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER, {**MINIMAL_ROUTER, "asn": "65200"}]},
        headers=AUTH,
    )

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [{**MINIMAL_ROUTER, "asn": "65300"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["router_count"] == 1

    async for db in get_session():
        routers = (
            (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalars().all()
        )
        assert len(routers) == 1
        assert routers[0].asn == "65300"
        break


@pytest.mark.anyio
async def test_put_bgp_intent_empty_routers_clears_intent(adapter_client):
    """PUT with empty routers list deletes all existing intent rows."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import BgpRouterIntent

    device_id = await seed_device(nso_device_name="bgp-intent-clear", netbox_device_id=2004)

    await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
        headers=AUTH,
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": []},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["router_count"] == 0

    async for db in get_session():
        count = len(
            (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalars().all()
        )
        assert count == 0
        break


@pytest.mark.anyio
async def test_put_bgp_intent_with_password(adapter_client):
    """Peer password is stored plaintext (by design)."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import BgpPeerIntent, BgpRouterIntent, BgpScopeIntent

    device_id = await seed_device(nso_device_name="bgp-intent-pw", netbox_device_id=2005)
    payload = {
        "routers": [
            {
                "asn": "65100",
                "scopes": [
                    {
                        "vrf": "",
                        "address_families": [],
                        "peers": [
                            {
                                "peer_address": "10.0.0.1",
                                "password": "s3cr3t",
                                "address_families": [],
                            }
                        ],
                    }
                ],
            }
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/bgp-intent", json=payload, headers=AUTH)
    assert resp.status_code == 200

    async for db in get_session():
        router = (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalar_one()
        scope = (await db.execute(select(BgpScopeIntent).where(BgpScopeIntent.router_id == router.id))).scalar_one()
        peer = (await db.execute(select(BgpPeerIntent).where(BgpPeerIntent.scope_id == scope.id))).scalar_one()
        assert peer.password == "s3cr3t"
        break


@pytest.mark.anyio
async def test_put_bgp_intent_auto_apply_enqueues_job(adapter_client):
    """PUT with auto_apply=True creates an apply job row."""
    from sqlalchemy import select

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSettings, Job, JobType

    device_id = await seed_device(nso_device_name="bgp-intent-autoapply", netbox_device_id=2006)

    async for db in get_session():
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()
        break

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    async for db in get_session():
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        ).scalar_one_or_none()
        assert job is not None
        break


@pytest.mark.anyio
async def test_put_bgp_intent_no_auto_apply_when_disabled(adapter_client):
    """PUT with auto_apply=False does NOT enqueue an apply job."""
    from sqlalchemy import select

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSettings, Job, JobType

    device_id = await seed_device(nso_device_name="bgp-intent-noapply", netbox_device_id=2007)

    async for db in get_session():
        db.add(DeviceSettings(device_id=device_id, auto_apply=False))
        await db.commit()
        break

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    async for db in get_session():
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        ).scalar_one_or_none()
        assert job is None
        break


# ── redistribution intent (AF-scoped) ─────────────────────────────────────────


def _router_with_redist(redistribution: list[dict], *, asn: str = "65100", vrf: str = "") -> dict:
    return {
        "asn": asn,
        "scopes": [
            {
                "vrf": vrf,
                "address_families": [{"af": "ipv4-unicast", "redistribution": redistribution}],
                "peers": [],
            }
        ],
    }


@pytest.mark.anyio
async def test_put_bgp_intent_creates_redistribution_rows(adapter_client):
    """AF-scoped redistribution entries become RedistributionIntent rows (dest_protocol=bgp)."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import RedistributionIntent

    device_id = await seed_device(nso_device_name="bgp-redist-create", netbox_device_id=2010)
    body = {
        "routers": [
            _router_with_redist(
                [
                    {"source_protocol": "ospf", "source_ref": "1", "route_map": "RM-OSPF", "metric": 100},
                    {"source_protocol": "connected", "source_ref": "", "route_map": None, "metric": None},
                ]
            )
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/bgp-intent", json=body, headers=AUTH)
    assert resp.status_code == 200

    async for db in get_session():
        rows = (
            (await db.execute(select(RedistributionIntent).where(RedistributionIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        break
    by_src = {r.source_protocol: r for r in rows}
    assert set(by_src) == {"ospf", "connected"}
    assert all(r.dest_protocol == "bgp" and r.dest_ref == "65100::ipv4-unicast" for r in rows)
    assert (by_src["ospf"].source_ref, by_src["ospf"].route_map, by_src["ospf"].metric) == ("1", "RM-OSPF", 100)
    assert (by_src["connected"].route_map, by_src["connected"].metric) == (None, None)


@pytest.mark.anyio
async def test_put_bgp_intent_redistribution_full_replace_and_update(adapter_client):
    """Re-PUT drops absent redistribution rows and updates the kept one in place."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import RedistributionIntent

    device_id = await seed_device(nso_device_name="bgp-redist-replace", netbox_device_id=2011)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={
            "routers": [
                _router_with_redist(
                    [
                        {"source_protocol": "ospf", "source_ref": "1", "route_map": "RM-A", "metric": 100},
                        {"source_protocol": "static", "source_ref": "", "route_map": None, "metric": None},
                    ]
                )
            ]
        },
        headers=AUTH,
    )

    # Re-PUT keeping ospf (changed route_map/metric), dropping static.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={
            "routers": [
                _router_with_redist(
                    [{"source_protocol": "ospf", "source_ref": "1", "route_map": "RM-B", "metric": 250}]
                )
            ]
        },
        headers=AUTH,
    )
    assert resp.status_code == 200

    async for db in get_session():
        rows = (
            (await db.execute(select(RedistributionIntent).where(RedistributionIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        break
    assert len(rows) == 1  # static dropped
    assert rows[0].source_protocol == "ospf"
    assert (rows[0].route_map, rows[0].metric) == ("RM-B", 250)  # updated in place


@pytest.mark.anyio
async def test_put_bgp_intent_redistribution_removal_enqueues_removal_job(adapter_client):
    """Dropping a redistribution row (with no router/peer change) still queues a removal job."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="bgp-redist-removal", netbox_device_id=2012)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={
            "routers": [
                _router_with_redist([{"source_protocol": "ospf", "source_ref": "1", "route_map": None, "metric": None}])
            ]
        },
        headers=AUTH,
    )

    # Same router/scope/AF, but the redistribution entry is gone → removal propagation.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [_router_with_redist([])]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    async for db in get_session():
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal))
        ).scalar_one_or_none()
        break
    assert job is not None  # redistribution removal alone triggers the bgp removal job
