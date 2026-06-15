# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Eager-load BGP router intent relationships for the apply + removal paths.

BgpRouterIntent's scopes/afs/peers/peer-afs are ``lazy="raise"`` (so a stray lazy
load can't crash mid-apply). Both the apply path and the removal PUT-replace need the
full tree, so this manually loads each level and attaches it with
``set_committed_value`` (writing raw lists into ``__dict__`` would later crash on
flush: "'list' object has no attribute '_sa_adapter'").
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value


async def attach_bgp_relationships(db: AsyncSession, routers: list) -> list:
    """Populate scopes→(address_families, peers→peer_address_families) on *routers*."""
    from nso_adapter.store.models import BgpAfIntent, BgpPeerAfIntent, BgpPeerIntent, BgpScopeIntent

    if not routers:
        return routers

    router_ids = [r.id for r in routers]
    all_scopes = (
        (await db.execute(select(BgpScopeIntent).where(BgpScopeIntent.router_id.in_(router_ids)))).scalars().all()
    )
    scopes_by_router: dict[int, list] = {}
    for s in all_scopes:
        scopes_by_router.setdefault(s.router_id, []).append(s)

    scope_ids = [s.id for s in all_scopes]
    afs_by_scope: dict[int, list] = {}
    peers_by_scope: dict[int, list] = {}
    all_peers: list = []
    if scope_ids:
        for af in (await db.execute(select(BgpAfIntent).where(BgpAfIntent.scope_id.in_(scope_ids)))).scalars().all():
            afs_by_scope.setdefault(af.scope_id, []).append(af)
        all_peers = (
            (await db.execute(select(BgpPeerIntent).where(BgpPeerIntent.scope_id.in_(scope_ids)))).scalars().all()
        )
        for p in all_peers:
            peers_by_scope.setdefault(p.scope_id, []).append(p)

    peer_ids = [p.id for p in all_peers]
    pafs_by_peer: dict[int, list] = {}
    if peer_ids:
        for paf in (
            (await db.execute(select(BgpPeerAfIntent).where(BgpPeerAfIntent.peer_id.in_(peer_ids)))).scalars().all()
        ):
            pafs_by_peer.setdefault(paf.peer_id, []).append(paf)

    for s in all_scopes:
        set_committed_value(s, "address_families", afs_by_scope.get(s.id, []))
        set_committed_value(s, "peers", peers_by_scope.get(s.id, []))
    for p in all_peers:
        set_committed_value(p, "peer_address_families", pafs_by_peer.get(p.id, []))
    for r in routers:
        set_committed_value(r, "scopes", scopes_by_router.get(r.id, []))
    return routers
