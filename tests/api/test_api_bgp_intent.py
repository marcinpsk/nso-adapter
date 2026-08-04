# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for PUT /api/v1/devices/{id}/bgp-intent."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, seed_device, session

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

    async with session() as db:
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


@pytest.mark.anyio
async def test_put_bgp_intent_persists_router_id(adapter_client):
    """An accepted global router-id round-trips into the BgpRouterIntent row."""
    from nso_adapter.store.models import BgpRouterIntent

    device_id = await seed_device(nso_device_name="bgp-intent-rid", netbox_device_id=2008)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [{**MINIMAL_ROUTER, "router_id": "10.255.0.1"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    async with session() as db:
        router = (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalar_one()
        assert router.router_id == "10.255.0.1"


@pytest.mark.anyio
async def test_put_bgp_intent_router_id_defaults_none(adapter_client):
    """A router payload with no router_id persists None (field is optional)."""
    from nso_adapter.store.models import BgpRouterIntent

    device_id = await seed_device(nso_device_name="bgp-intent-norid", netbox_device_id=2009)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    async with session() as db:
        router = (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalar_one()
        assert router.router_id is None


@pytest.mark.anyio
async def test_put_bgp_intent_full_replace(adapter_client):
    """Second PUT fully replaces the first (full-replace semantics)."""
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

    async with session() as db:
        routers = (
            (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalars().all()
        )
        assert len(routers) == 1
        assert routers[0].asn == "65300"


@pytest.mark.anyio
async def test_put_bgp_intent_empty_routers_clears_intent(adapter_client):
    """PUT with empty routers list deletes all existing intent rows."""
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

    async with session() as db:
        count = len(
            (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalars().all()
        )
        assert count == 0


@pytest.mark.anyio
async def test_put_bgp_intent_with_password(adapter_client):
    """Peer password is stored plaintext (by design)."""
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

    async with session() as db:
        router = (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalar_one()
        scope = (await db.execute(select(BgpScopeIntent).where(BgpScopeIntent.router_id == router.id))).scalar_one()
        peer = (await db.execute(select(BgpPeerIntent).where(BgpPeerIntent.scope_id == scope.id))).scalar_one()
        assert peer.password == "s3cr3t"


@pytest.mark.anyio
async def test_put_bgp_intent_with_source(adapter_client):
    """Peer source (update-source iface / local-address IP) survives the PUT → intent rebuild.

    Was dropped because BgpPeerModel/BgpPeerIntent had no source field, so apply_bgp_config could
    never send it to the reconciler — the BGP session source was lost on every push.
    """
    from nso_adapter.store.models import BgpPeerIntent, BgpRouterIntent, BgpScopeIntent

    device_id = await seed_device(nso_device_name="bgp-intent-src", netbox_device_id=2009)
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
                                "peer_address": "10.0.0.2",
                                "source": "Loopback0",
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

    async with session() as db:
        router = (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalar_one()
        scope = (await db.execute(select(BgpScopeIntent).where(BgpScopeIntent.router_id == router.id))).scalar_one()
        peer = (await db.execute(select(BgpPeerIntent).where(BgpPeerIntent.scope_id == scope.id))).scalar_one()
        assert peer.source == "Loopback0"


@pytest.mark.anyio
async def test_put_bgp_intent_auto_apply_enqueues_job(adapter_client):
    """PUT with auto_apply=True creates an apply job row."""
    from sqlalchemy import select

    from nso_adapter.store.models import DeviceSettings, Job, JobType

    device_id = await seed_device(nso_device_name="bgp-intent-autoapply", netbox_device_id=2006)

    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    async with session() as db:
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        ).scalar_one_or_none()
        assert job is not None


@pytest.mark.anyio
async def test_put_bgp_intent_no_auto_apply_when_disabled(adapter_client):
    """PUT with auto_apply=False does NOT enqueue an apply job."""
    from sqlalchemy import select

    from nso_adapter.store.models import DeviceSettings, Job, JobType

    device_id = await seed_device(nso_device_name="bgp-intent-noapply", netbox_device_id=2007)

    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=False))
        await db.commit()

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [MINIMAL_ROUTER]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    async with session() as db:
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        ).scalar_one_or_none()
        assert job is None


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

    async with session() as db:
        rows = (
            (await db.execute(select(RedistributionIntent).where(RedistributionIntent.device_id == device_id)))
            .scalars()
            .all()
        )
    by_src = {r.source_protocol: r for r in rows}
    assert set(by_src) == {"ospf", "connected"}
    assert all(r.dest_protocol == "bgp" and r.dest_ref == "65100::ipv4-unicast" for r in rows)
    assert (by_src["ospf"].source_ref, by_src["ospf"].route_map, by_src["ospf"].metric) == ("1", "RM-OSPF", 100)
    assert (by_src["connected"].route_map, by_src["connected"].metric) == (None, None)


@pytest.mark.anyio
async def test_put_bgp_intent_redistribution_full_replace_and_update(adapter_client):
    """Re-PUT drops absent redistribution rows and updates the kept one in place."""
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

    async with session() as db:
        rows = (
            (await db.execute(select(RedistributionIntent).where(RedistributionIntent.device_id == device_id)))
            .scalars()
            .all()
        )
    assert len(rows) == 1  # static dropped
    assert rows[0].source_protocol == "ospf"
    assert (rows[0].route_map, rows[0].metric) == ("RM-B", 250)  # updated in place


@pytest.mark.anyio
async def test_put_bgp_intent_redistribution_removal_enqueues_removal_job(adapter_client):
    """Dropping a redistribution row (with no router/peer change) still queues a removal job."""
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

    async with session() as db:
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal))
        ).scalar_one_or_none()
    assert job is not None  # redistribution removal alone triggers the bgp removal job


# ── cleared owned scalars must retract (#83, the BGP leg) ─────────────────────
#
# The BGP PUT DELETEs the whole router→scope→peer→af tree and re-inserts it, so unlike every
# other scope it has no in-place pre-image to diff against. Only the identity SETS (router
# ASNs, peer addresses) were captured — enough to notice a peer that VANISHED, blind to the
# peer that merely lost its route-map. And a plain apply is a merge-PATCH, which never drops
# an omitted leaf: with no removal job queued, the device kept applying a policy that NetBox
# and the adapter both showed as gone.


def _peer_router(peer: dict) -> dict:
    return {"asn": "65100", "scopes": [{"vrf": "", "address_families": [{"af": "ipv4-unicast"}], "peers": [peer]}]}


async def _removal_job(device_id):
    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        return (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal))
        ).scalar_one_or_none()
    return None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("remote_as", "65200"),
        ("local_as", "64512"),
        ("ttl", 2),
        ("password", "s3cr3t"),
        ("peer_group", "IBGP"),
        ("source", "Loopback0"),
    ],
)
async def test_clearing_an_owned_peer_scalar_queues_a_retract(adapter_client, field, value):
    device_id = await seed_device(nso_device_name=f"bgp-clear-{field}", netbox_device_id=2100 + len(field))

    peer = {"peer_address": "192.0.2.1", "address_families": [{"af": "ipv4-unicast"}], field: value}
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [_peer_router(peer)]}, headers=AUTH
    )
    assert resp.status_code == 200
    assert await _removal_job(device_id) is None, "the initial push adds intent — nothing to retract"

    # Same peer, same identity — the operator just blanked the value.
    cleared_peer = {"peer_address": "192.0.2.1", "address_families": [{"af": "ipv4-unicast"}]}
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [_peer_router(cleared_peer)]}, headers=AUTH
    )
    assert resp.status_code == 200

    job = await _removal_job(device_id)
    assert job is not None, f"clearing {field} must queue a removal — a merge-PATCH cannot drop the leaf"
    # A cleared scalar is NOT an un-own: the peer is still owned and accepted, so the
    # PUT-replace must actually reach the device rather than detach with no-networking (#106).
    assert job.context.get("detach") is not True
    assert job.context.get("retract_deferred") is not True


@pytest.mark.anyio
async def test_removing_a_route_map_from_a_neighbour_queues_a_retract(adapter_client):
    """The finding's own example: the peer keeps its identity and its AF, and only the
    route-map reference is cleared."""
    device_id = await seed_device(nso_device_name="bgp-clear-rm", netbox_device_id=2130)

    with_rm = {
        "peer_address": "192.0.2.1",
        "remote_as": "65200",
        "address_families": [{"af": "ipv4-unicast", "routemap_in": "RM-IN", "routemap_out": "RM-OUT"}],
    }
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [_peer_router(with_rm)]}, headers=AUTH
    )
    assert await _removal_job(device_id) is None

    without_rm = {
        "peer_address": "192.0.2.1",
        "remote_as": "65200",
        "address_families": [{"af": "ipv4-unicast"}],
    }
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [_peer_router(without_rm)]}, headers=AUTH
    )
    assert resp.status_code == 200

    job = await _removal_job(device_id)
    assert job is not None, "clearing routemap_in/out must queue a removal"
    assert job.context.get("detach") is not True


@pytest.mark.anyio
async def test_dropping_an_address_family_from_a_surviving_peer_queues_a_retract(adapter_client):
    """The peer survives, so it is not a peer removal — but the AF is content the merge-PATCH
    cannot drop either. Nothing else in the diff would have caught it."""
    device_id = await seed_device(nso_device_name="bgp-clear-af", netbox_device_id=2131)

    both = {
        "peer_address": "192.0.2.1",
        "remote_as": "65200",
        "address_families": [{"af": "ipv4-unicast"}, {"af": "ipv6-unicast"}],
    }
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [_peer_router(both)]}, headers=AUTH
    )
    assert await _removal_job(device_id) is None

    one = {"peer_address": "192.0.2.1", "remote_as": "65200", "address_families": [{"af": "ipv4-unicast"}]}
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [_peer_router(one)]}, headers=AUTH
    )
    assert resp.status_code == 200
    assert await _removal_job(device_id) is not None, "a dropped address-family must queue a removal"


@pytest.mark.anyio
async def test_clearing_a_router_id_queues_a_retract(adapter_client):
    device_id = await seed_device(nso_device_name="bgp-clear-rid", netbox_device_id=2132)

    router = _peer_router({"peer_address": "192.0.2.1", "address_families": [{"af": "ipv4-unicast"}]})
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [{**router, "router_id": "10.0.0.1"}]},
        headers=AUTH,
    )
    assert await _removal_job(device_id) is None

    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [router]}, headers=AUTH)
    assert resp.status_code == 200
    assert await _removal_job(device_id) is not None, "clearing router_id must queue a removal"


@pytest.mark.anyio
async def test_an_unchanged_republish_queues_nothing(adapter_client):
    """The guard against the opposite failure: re-pushing the same intent must not manufacture
    a device-touching removal job out of nothing."""
    device_id = await seed_device(nso_device_name="bgp-clear-noop", netbox_device_id=2133)

    peer = {"peer_address": "192.0.2.1", "remote_as": "65200", "address_families": [{"af": "ipv4-unicast"}]}
    for _ in range(2):
        resp = await adapter_client.put(
            f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [_peer_router(peer)]}, headers=AUTH
        )
        assert resp.status_code == 200
    assert await _removal_job(device_id) is None


@pytest.mark.anyio
async def test_setting_a_scalar_that_was_unset_queues_nothing(adapter_client):
    """A GROW is not a clear — nothing to retract."""
    device_id = await seed_device(nso_device_name="bgp-clear-grow", netbox_device_id=2134)

    bare = {"peer_address": "192.0.2.1", "address_families": [{"af": "ipv4-unicast"}]}
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [_peer_router(bare)]}, headers=AUTH
    )
    grown = {**bare, "remote_as": "65200", "ttl": 2}
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [_peer_router(grown)]}, headers=AUTH
    )
    assert resp.status_code == 200
    assert await _removal_job(device_id) is None
