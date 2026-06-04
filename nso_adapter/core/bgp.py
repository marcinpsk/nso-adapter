# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""BGP config refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_bgp_config_for_device() — called on-demand by scheduler
- handle_bgp_config_change()      — SSE hook (placeholder for future use)
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.client import NsoClient
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
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(router)
        await db.flush()

        for scope_data in router_data.get("scope", []):
            vrf = scope_data.get("vrf", "")
            scope = DeviceBgpScope(router_id=router.id, vrf=vrf)
            db.add(scope)
            await db.flush()

            for af_data in scope_data.get("address-family", []):
                af_name = af_data.get("af", "")
                if af_name:
                    db.add(DeviceBgpAddressFamily(scope_id=scope.id, af=af_name))

            # A BGP neighbor is unique per (router, vrf), but a device may list the
            # same neighbor IP under more than one group — a violation of
            # uq_devicebgppeer_identity that would roll back the ENTIRE device
            # refresh. Collapse to one peer per address and MERGE their
            # address-families (scalar peer fields: first occurrence wins).
            peers_by_addr: dict[str, DeviceBgpPeer] = {}
            afis_by_addr: dict[str, set[str]] = {}
            for peer_data in scope_data.get("peer", []):
                peer_addr = peer_data.get("peer-address", "")
                if not peer_addr:
                    continue
                peer = peers_by_addr.get(peer_addr)
                if peer is None:
                    peer = DeviceBgpPeer(
                        scope_id=scope.id,
                        peer_address=peer_addr,
                        enabled=bool(peer_data.get("enabled", True)),
                        peer_group=peer_data.get("peer-group") or None,
                        remote_as=str(peer_data["remote-as"]) if peer_data.get("remote-as") is not None else None,
                        local_as=str(peer_data["local-as"]) if peer_data.get("local-as") is not None else None,
                        ttl=peer_data.get("ttl"),
                        password=peer_data.get("password"),
                        source=peer_data.get("source") or None,
                    )
                    db.add(peer)
                    await db.flush()
                    peers_by_addr[peer_addr] = peer
                    afis_by_addr[peer_addr] = set()
                else:
                    # Same peer in multiple groups (e.g. one active, one deactivated):
                    # the peer is enabled if ANY occurrence is active.
                    if bool(peer_data.get("enabled", True)):
                        peer.enabled = True
                    logger.debug(
                        "bgp.peer_merged_across_groups", device_id=device.id, vrf=vrf, peer_address=peer_addr
                    )

                seen_afis = afis_by_addr[peer_addr]
                for paf_data in peer_data.get("peer-address-family", []):
                    paf_name = paf_data.get("afi", "")
                    if not paf_name or paf_name in seen_afis:
                        continue
                    seen_afis.add(paf_name)
                    db.add(
                        DeviceBgpPeerAddressFamily(
                            peer_id=peer.id,
                            af=paf_name,
                            enabled=bool(paf_data.get("enabled", True)),
                            # IOS splits route-map vs prefix-list; Junos/Timos use a single
                            # per-AF policy (policy-in/out) which maps to a route-map.
                            routemap_in=paf_data.get("routemap-in") or paf_data.get("policy-in") or None,
                            routemap_out=paf_data.get("routemap-out") or paf_data.get("policy-out") or None,
                            prefixlist_in=paf_data.get("prefixlist-in") or None,
                            prefixlist_out=paf_data.get("prefixlist-out") or None,
                        )
                    )

            # Peer-group / template objects with their own per-AF policies.
            seen_pg: set[str] = set()
            for pg_data in scope_data.get("peer-group", []):
                pg_name = pg_data.get("name", "")
                if not pg_name or pg_name in seen_pg:
                    continue
                seen_pg.add(pg_name)
                pg = DeviceBgpPeerGroup(
                    scope_id=scope.id,
                    name=pg_name,
                    remote_as=str(pg_data["remote-as"]) if pg_data.get("remote-as") is not None else None,
                    source=pg_data.get("source") or None,
                )
                db.add(pg)
                await db.flush()

                seen_pg_afis: set[str] = set()
                for pgaf_data in pg_data.get("peer-group-address-family", []):
                    pgaf_name = pgaf_data.get("afi", "")
                    if not pgaf_name or pgaf_name in seen_pg_afis:
                        continue
                    seen_pg_afis.add(pgaf_name)
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

    await db.commit()


async def refresh_bgp_config_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read BGP oper-data for *device* from NSO and upsert DB rows."""
    if not device.nso_device_name:
        logger.debug("bgp.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return

    try:
        entry = await nso_client.get_bgp_config(device.nso_device_name)
    except Exception as exc:
        logger.warning("bgp.refresh.nso_error", device_id=device.id, error=repr(exc))
        return

    routers = entry.get("router", []) if entry else []
    await _upsert_bgp_data(db, device, routers, refresh_source)

    peer_count = sum(len(scope_data.get("peer", [])) for r in routers for scope_data in r.get("scope", []))
    logger.info(
        "bgp.refresh.done",
        device_id=device.id,
        device_name=device.nso_device_name,
        router_count=len(routers),
        peer_count=peer_count,
        refresh_source=refresh_source,
    )


async def handle_bgp_config_change(
    db: AsyncSession,
    nso_device_name: str,
    nso_client: NsoClient,
) -> None:
    """Handle an SSE notification that BGP config changed for *nso_device_name*.

    Finds the Device row, then delegates to refresh_bgp_config_for_device.
    """
    result = await db.execute(select(Device).where(Device.nso_device_name == nso_device_name))
    device = result.scalar_one_or_none()
    if device is None:
        logger.debug("bgp.sse.unknown_device", nso_device_name=nso_device_name)
        return

    await refresh_bgp_config_for_device(db, device, nso_client, refresh_source="sse")
