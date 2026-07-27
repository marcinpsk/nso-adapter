# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""BGP config refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_bgp_config_for_device() — called on-demand by scheduler / SSE coalescer
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import (
    Device,
    DeviceBgpAddressFamily,
    DeviceBgpPeer,
    DeviceBgpPeerAddressFamily,
    DeviceBgpPeerGroup,
    DeviceBgpPeerGroupAddressFamily,
    DeviceBgpRouter,
    DeviceBgpScope,
)

logger = structlog.get_logger(__name__)


def _paf_policy(paf_data: dict) -> dict:
    """Normalize a peer address-family's policy fields across NED dialects.

    IOS splits route-map vs prefix-list; Junos/Timos use a single per-AF policy
    (policy-in/out) which maps to a route-map.
    """
    return {
        "enabled": bool(paf_data.get("enabled", True)),
        "routemap_in": paf_data.get("routemap-in") or paf_data.get("policy-in") or None,
        "routemap_out": paf_data.get("routemap-out") or paf_data.get("policy-out") or None,
        "prefixlist_in": paf_data.get("prefixlist-in") or None,
        "prefixlist_out": paf_data.get("prefixlist-out") or None,
    }


def _add_peer_address_families(
    db: AsyncSession,
    peer: DeviceBgpPeer,
    paf_dicts: list[dict],
    seen_afs: dict[str, dict],
    *,
    device_id: int,
    vrf: str,
) -> None:
    """Add a peer's per-AF policy rows, deduped by afi across the neighbor's groups.

    A neighbor listed under >1 group can repeat an afi, but (peer_id, af) is unique
    so only the FIRST occurrence's policy is stored. If a later occurrence carries a
    genuinely DIFFERENT policy for that afi it cannot be represented — surface it
    (``bgp.peer_af_conflict_across_groups``) instead of dropping it silently.
    """
    for paf_data in paf_dicts:
        paf_name = paf_data.get("afi", "")
        if not paf_name:
            continue
        policy = _paf_policy(paf_data)
        prior = seen_afs.get(paf_name)
        if prior is not None:
            if prior != policy:
                logger.warning(
                    "bgp.peer_af_conflict_across_groups",
                    device_id=device_id,
                    vrf=vrf,
                    peer_address=peer.peer_address,
                    afi=paf_name,
                    kept=prior,
                    dropped=policy,
                )
            continue
        seen_afs[paf_name] = policy
        db.add(
            DeviceBgpPeerAddressFamily(
                peer_id=peer.id,
                af=paf_name,
                enabled=policy["enabled"],
                routemap_in=policy["routemap_in"],
                routemap_out=policy["routemap_out"],
                prefixlist_in=policy["prefixlist_in"],
                prefixlist_out=policy["prefixlist_out"],
            )
        )


async def _insert_bgp_peers(db: AsyncSession, scope_id: int, peer_dicts: list[dict], device_id: int, vrf: str) -> None:
    """Insert peers under a scope, collapsing the same neighbor across groups.

    A BGP neighbor is unique per (router, vrf), but a device may list the same
    neighbor IP under more than one group — a violation of
    uq_devicebgppeer_identity that would roll back the ENTIRE device refresh.
    Collapse to one peer per address and MERGE their address-families (scalar
    peer fields: first occurrence wins; ``enabled`` is the OR across occurrences).
    """
    peers_by_addr: dict[str, DeviceBgpPeer] = {}
    afis_by_addr: dict[str, dict[str, dict]] = {}
    for peer_data in peer_dicts:
        peer_addr = peer_data.get("peer-address", "")
        if not peer_addr:
            continue
        peer = peers_by_addr.get(peer_addr)
        if peer is None:
            peer = DeviceBgpPeer(
                scope_id=scope_id,
                peer_address=peer_addr,
                enabled=bool(peer_data.get("enabled", True)),
                peer_group=peer_data.get("peer-group") or None,
                remote_as=str(peer_data["remote-as"]) if peer_data.get("remote-as") is not None else None,
                local_as=str(peer_data["local-as"]) if peer_data.get("local-as") is not None else None,
                ttl=peer_data.get("ttl"),
                password=peer_data.get("password"),
                source=peer_data.get("source") or None,
                bfd_enabled=peer_data.get("bfd-enabled"),
            )
            db.add(peer)
            await db.flush()
            peers_by_addr[peer_addr] = peer
            afis_by_addr[peer_addr] = {}
        else:
            # Same peer in multiple groups (e.g. one active, one deactivated):
            # the peer is enabled if ANY occurrence is active.
            if bool(peer_data.get("enabled", True)):
                peer.enabled = True
            logger.debug("bgp.peer_merged_across_groups", device_id=device_id, vrf=vrf, peer_address=peer_addr)

        _add_peer_address_families(
            db,
            peer,
            as_list(peer_data.get("peer-address-family")),
            afis_by_addr[peer_addr],
            device_id=device_id,
            vrf=vrf,
        )


def _add_peer_group_address_families(db: AsyncSession, pg: DeviceBgpPeerGroup, pgaf_dicts: list[dict]) -> None:
    """Add a peer-group's per-AF policy rows, skipping blank/duplicate afis."""
    seen: set[str] = set()
    for pgaf_data in pgaf_dicts:
        pgaf_name = pgaf_data.get("afi", "")
        if not pgaf_name or pgaf_name in seen:
            continue
        seen.add(pgaf_name)
        db.add(
            DeviceBgpPeerGroupAddressFamily(
                peer_group_id=pg.id,
                af=pgaf_name,
                routemap_in=pgaf_data.get("routemap-in") or pgaf_data.get("policy-in") or None,
                routemap_out=pgaf_data.get("routemap-out") or pgaf_data.get("policy-out") or None,
                prefixlist_in=pgaf_data.get("prefixlist-in") or None,
                prefixlist_out=pgaf_data.get("prefixlist-out") or None,
            )
        )


async def _insert_bgp_peer_groups(db: AsyncSession, scope_id: int, pg_dicts: list[dict]) -> None:
    """Insert peer-group / template objects + their per-AF policies, skipping blank/duplicate names."""
    seen_pg: set[str] = set()
    for pg_data in pg_dicts:
        pg_name = pg_data.get("name", "")
        if not pg_name or pg_name in seen_pg:
            continue
        seen_pg.add(pg_name)
        pg = DeviceBgpPeerGroup(
            scope_id=scope_id,
            name=pg_name,
            remote_as=str(pg_data["remote-as"]) if pg_data.get("remote-as") is not None else None,
            source=pg_data.get("source") or None,
        )
        db.add(pg)
        await db.flush()
        _add_peer_group_address_families(db, pg, as_list(pg_data.get("peer-group-address-family")))


async def _insert_bgp_scope(db: AsyncSession, router_id: int, scope_data: dict, device_id: int) -> None:
    """Insert one scope (vrf) + its address-families, peers and peer-groups."""
    vrf = scope_data.get("vrf", "")
    scope = DeviceBgpScope(router_id=router_id, vrf=vrf)
    db.add(scope)
    await db.flush()

    for af_data in as_list(scope_data.get("address-family")):
        # network-state-export keys the scope address-family on "afi" (yang:1090), same as the
        # peer / peer-group address-family lists — not "af" (the DB column name). Reading "af"
        # here meant DeviceBgpAddressFamily rows were never created on real oper-data.
        af_name = af_data.get("afi", "")
        if af_name:
            db.add(DeviceBgpAddressFamily(scope_id=scope.id, af=af_name))

    await _insert_bgp_peers(db, scope.id, as_list(scope_data.get("peer")), device_id, vrf)
    await _insert_bgp_peer_groups(db, scope.id, as_list(scope_data.get("peer-group")))


async def _upsert_bgp_data(
    db: AsyncSession,
    device: Device,
    routers: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing BGP rows for *device*, then insert fresh ones."""
    await db.execute(delete(DeviceBgpRouter).where(DeviceBgpRouter.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)

    for router_data in routers:
        asn = str(router_data.get("asn", ""))
        if not asn:
            continue
        router = DeviceBgpRouter(
            device_id=device.id,
            asn=asn,
            router_id=router_data.get("router-id") or None,  # export leaf is dash-keyed
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(router)
        await db.flush()

        for scope_data in as_list(router_data.get("scope")):
            await _insert_bgp_scope(db, router.id, scope_data, device.id)


BGP_SPEC = FamilySpec(
    name="bgp",
    # as_list guards the singleton-rendered-as-bare-dict case; extract({}) → [] → clear.
    extract=lambda data: as_list(data.get("router")),
    materialize=_upsert_bgp_data,
    wire_name="bgp-config",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_bgp_config_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read BGP oper-data for *device* from NSO and upsert DB rows (via the shared refresh engine).

    Returns True on a successful read (or nothing to read); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, BGP_SPEC, refresh_source=refresh_source)
