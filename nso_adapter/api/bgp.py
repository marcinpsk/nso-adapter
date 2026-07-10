# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""BGP endpoints: GET /bgp-config (read mirror) + PUT /bgp-intent (write path)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import NamedTuple

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import (
    BgpAfIntent,
    BgpPeerAfIntent,
    BgpPeerIntent,
    BgpRouterIntent,
    BgpScopeIntent,
    Device,
    DeviceBgpAddressFamily,
    DeviceBgpPeer,
    DeviceBgpPeerAddressFamily,
    DeviceBgpPeerGroup,
    DeviceBgpPeerGroupAddressFamily,
    DeviceBgpRouter,
    DeviceBgpScope,
    DeviceSettings,
    RedistributionIntent,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["bgp"])


class _BgpGraph(NamedTuple):
    """Per-device BGP read-mirror rows, pre-grouped by parent id for serialization."""

    scopes_by_router: dict[int, list[DeviceBgpScope]]
    afs_by_scope: dict[int, list[DeviceBgpAddressFamily]]
    peers_by_scope: dict[int, list[DeviceBgpPeer]]
    peer_afs_by_peer: dict[int, list[DeviceBgpPeerAddressFamily]]
    pgs_by_scope: dict[int, list[DeviceBgpPeerGroup]]
    pg_afs_by_pg: dict[int, list[DeviceBgpPeerGroupAddressFamily]]


async def _group_in(db: AsyncSession, model, fk_col, ids: list[int], key_attr: str, order_col) -> dict[int, list]:
    """Load ``model`` rows where ``fk_col`` is in *ids*, grouped by ``key_attr`` (empty ids → {})."""
    if not ids:
        return {}
    rows = (await db.execute(select(model).where(fk_col.in_(ids)).order_by(order_col))).scalars().all()
    grouped: dict[int, list] = {}
    for row in rows:
        grouped.setdefault(getattr(row, key_attr), []).append(row)
    return grouped


async def _load_bgp_graph(db: AsyncSession, bgp_routers: list[DeviceBgpRouter]) -> _BgpGraph:
    """Batch-load the scope/af/peer/peer-af/peer-group graph beneath a device's routers."""
    router_ids = [r.id for r in bgp_routers]
    scopes_by_router = await _group_in(
        db, DeviceBgpScope, DeviceBgpScope.router_id, router_ids, "router_id", DeviceBgpScope.vrf
    )

    scope_ids = [s.id for scopes in scopes_by_router.values() for s in scopes]
    afs_by_scope = await _group_in(
        db, DeviceBgpAddressFamily, DeviceBgpAddressFamily.scope_id, scope_ids, "scope_id", DeviceBgpAddressFamily.af
    )
    peers_by_scope = await _group_in(
        db, DeviceBgpPeer, DeviceBgpPeer.scope_id, scope_ids, "scope_id", DeviceBgpPeer.peer_address
    )
    pgs_by_scope = await _group_in(
        db, DeviceBgpPeerGroup, DeviceBgpPeerGroup.scope_id, scope_ids, "scope_id", DeviceBgpPeerGroup.name
    )

    peer_ids = [p.id for peers in peers_by_scope.values() for p in peers]
    peer_afs_by_peer = await _group_in(
        db,
        DeviceBgpPeerAddressFamily,
        DeviceBgpPeerAddressFamily.peer_id,
        peer_ids,
        "peer_id",
        DeviceBgpPeerAddressFamily.af,
    )

    pg_ids = [g.id for pgs in pgs_by_scope.values() for g in pgs]
    pg_afs_by_pg = await _group_in(
        db,
        DeviceBgpPeerGroupAddressFamily,
        DeviceBgpPeerGroupAddressFamily.peer_group_id,
        pg_ids,
        "peer_group_id",
        DeviceBgpPeerGroupAddressFamily.af,
    )

    return _BgpGraph(scopes_by_router, afs_by_scope, peers_by_scope, peer_afs_by_peer, pgs_by_scope, pg_afs_by_pg)


# Policy-ref fields included only when truthy (an empty string is omitted).
_AF_POLICY_FIELDS = ("routemap_in", "routemap_out", "prefixlist_in", "prefixlist_out")
# Peer attributes included only when not None (False/0/"" are kept).
_PEER_OPTIONAL_FIELDS = ("peer_group", "remote_as", "local_as", "ttl", "password", "source", "bfd_enabled")
_PG_OPTIONAL_FIELDS = ("remote_as", "source")


def _truthy_fields(obj, names: tuple[str, ...]) -> dict:
    return {n: getattr(obj, n) for n in names if getattr(obj, n)}


def _present_fields(obj, names: tuple[str, ...]) -> dict:
    return {n: getattr(obj, n) for n in names if getattr(obj, n) is not None}


def _serialize_peer(peer: DeviceBgpPeer, peer_afs: list[DeviceBgpPeerAddressFamily]) -> dict:
    return {
        "peer_address": peer.peer_address,
        "enabled": peer.enabled,
        "address_families": [
            {"af": paf.af, "enabled": paf.enabled, **_truthy_fields(paf, _AF_POLICY_FIELDS)} for paf in peer_afs
        ],
        **_present_fields(peer, _PEER_OPTIONAL_FIELDS),
    }


def _serialize_peer_group(pg: DeviceBgpPeerGroup, pg_afs: list[DeviceBgpPeerGroupAddressFamily]) -> dict:
    return {
        "name": pg.name,
        "address_families": [{"af": pgaf.af, **_truthy_fields(pgaf, _AF_POLICY_FIELDS)} for pgaf in pg_afs],
        **_present_fields(pg, _PG_OPTIONAL_FIELDS),
    }


def _serialize_scope(scope: DeviceBgpScope, graph: _BgpGraph) -> dict:
    return {
        "vrf": scope.vrf,
        "address_families": [af.af for af in graph.afs_by_scope.get(scope.id, [])],
        "peers": [
            _serialize_peer(peer, graph.peer_afs_by_peer.get(peer.id, []))
            for peer in graph.peers_by_scope.get(scope.id, [])
        ],
        "peer_groups": [
            _serialize_peer_group(pg, graph.pg_afs_by_pg.get(pg.id, [])) for pg in graph.pgs_by_scope.get(scope.id, [])
        ],
    }


@router.get("/{device_id}/bgp-config", dependencies=[Depends(verify_token)])
async def get_bgp_config(device_id: int, db: AsyncSession = Depends(get_db)):
    """Return the BGP config read-mirror for this device."""
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    bgp_routers = (
        (
            await db.execute(
                select(DeviceBgpRouter).where(DeviceBgpRouter.device_id == device_id).order_by(DeviceBgpRouter.asn)
            )
        )
        .scalars()
        .all()
    )

    if not bgp_routers:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "routers": [],
        }

    graph = await _load_bgp_graph(db, bgp_routers)
    latest_ts = max((r.last_refreshed_at for r in bgp_routers if r.last_refreshed_at), default=None)

    routers_out = [
        {
            "asn": bgp_router.asn,
            "scopes": [_serialize_scope(scope, graph) for scope in graph.scopes_by_router.get(bgp_router.id, [])],
        }
        for bgp_router in bgp_routers
    ]

    return {
        "device_id": device_id,
        "last_refreshed_at": latest_ts.isoformat() + "Z" if latest_ts else None,
        "refresh_source": bgp_routers[0].refresh_source,
        "routers": routers_out,
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/bgp-intent
# ---------------------------------------------------------------------------


class BgpPeerAfModel(BaseModel):
    af: str
    enabled: bool = True
    routemap_in: str | None = None
    routemap_out: str | None = None
    prefixlist_in: str | None = None
    prefixlist_out: str | None = None


class BgpPeerModel(BaseModel):
    peer_address: str
    enabled: bool = True
    peer_group: str | None = None
    remote_as: str | None = None
    local_as: str | None = None
    ttl: int | None = None
    password: str | None = None
    source: str | None = None
    address_families: list[BgpPeerAfModel] = []


class BgpRedistributionEntry(BaseModel):
    source_protocol: str
    source_ref: str = ""
    route_map: str | None = None
    metric: int | None = None


class BgpAfModel(BaseModel):
    af: str
    redistribution: list[BgpRedistributionEntry] = []


class BgpScopeModel(BaseModel):
    vrf: str = ""
    address_families: list[BgpAfModel] = []
    peers: list[BgpPeerModel] = []


class BgpRouterModel(BaseModel):
    asn: str
    scopes: list[BgpScopeModel] = []
    accepted_at: datetime | None = None


class BgpIntentUpdate(BaseModel):
    routers: list[BgpRouterModel]


async def _capture_bgp_identities(db: AsyncSession, device_id: int) -> tuple[set[str], set[str]]:
    """Snapshot existing router ASNs + peer addresses before the wipe, for removal detection."""
    asns = set(
        (await db.execute(select(BgpRouterIntent.asn).where(BgpRouterIntent.device_id == device_id))).scalars().all()
    )
    peers = set(
        (
            await db.execute(
                select(BgpPeerIntent.peer_address)
                .join(BgpScopeIntent, BgpPeerIntent.scope_id == BgpScopeIntent.id)
                .join(BgpRouterIntent, BgpScopeIntent.router_id == BgpRouterIntent.id)
                .where(BgpRouterIntent.device_id == device_id)
            )
        )
        .scalars()
        .all()
    )
    return asns, peers


async def _insert_peer(db: AsyncSession, scope_id: int, peer_data: BgpPeerModel) -> None:
    """Insert one peer intent row + its per-AF policy rows under a scope."""
    peer_row = BgpPeerIntent(
        scope_id=scope_id,
        peer_address=peer_data.peer_address,
        enabled=peer_data.enabled,
        peer_group=peer_data.peer_group,
        remote_as=peer_data.remote_as,
        local_as=peer_data.local_as,
        ttl=peer_data.ttl,
        password=peer_data.password,
        source=peer_data.source,
    )
    db.add(peer_row)
    await db.flush()
    for paf_data in peer_data.address_families:
        db.add(
            BgpPeerAfIntent(
                peer_id=peer_row.id,
                af=paf_data.af,
                enabled=paf_data.enabled,
                routemap_in=paf_data.routemap_in,
                routemap_out=paf_data.routemap_out,
                prefixlist_in=paf_data.prefixlist_in,
                prefixlist_out=paf_data.prefixlist_out,
            )
        )


async def _insert_scope(db: AsyncSession, router_id: int, scope_data: BgpScopeModel) -> None:
    """Insert one scope intent row + its address-family and peer children."""
    scope_row = BgpScopeIntent(router_id=router_id, vrf=scope_data.vrf)
    db.add(scope_row)
    await db.flush()
    for af_data in scope_data.address_families:
        db.add(BgpAfIntent(scope_id=scope_row.id, af=af_data.af))
    for peer_data in scope_data.peers:
        await _insert_peer(db, scope_row.id, peer_data)


async def _rebuild_router_intent(db: AsyncSession, device_id: int, routers: list[BgpRouterModel], now: datetime) -> int:
    """Full-replace the BGP router→scope→af/peer/peer-af intent tree. Returns the router count."""
    await db.execute(delete(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))
    await db.flush()
    count = 0
    for router_data in routers:
        accepted = router_data.accepted_at.replace(tzinfo=None) if router_data.accepted_at else now
        router_row = BgpRouterIntent(device_id=device_id, asn=router_data.asn, accepted_at=accepted)
        db.add(router_row)
        await db.flush()
        count += 1
        for scope_data in router_data.scopes:
            await _insert_scope(db, router_row.id, scope_data)
    await db.flush()
    return count


def _iter_redistribution(routers: list[BgpRouterModel]):
    """Yield ``(dest_ref, entry)`` for every AF-scoped redistribution entry in the payload."""
    for router_data in routers:
        for scope_data in router_data.scopes:
            for af_data in scope_data.address_families:
                dest_ref = f"{router_data.asn}:{scope_data.vrf}:{af_data.af}"
                for entry in af_data.redistribution:
                    yield dest_ref, entry


async def _sync_redistribution(
    db: AsyncSession, device_id: int, routers: list[BgpRouterModel], now: datetime
) -> list[tuple]:
    """Full-replace BGP (dest_protocol=bgp) redistribution intent rows. Returns the removed keys."""
    existing = (
        (
            await db.execute(
                select(RedistributionIntent).where(
                    RedistributionIntent.device_id == device_id,
                    RedistributionIntent.dest_protocol == "bgp",
                )
            )
        )
        .scalars()
        .all()
    )
    existing_map = {(r.dest_ref, r.source_protocol, r.source_ref): r for r in existing}
    incoming_keys = {(dest_ref, e.source_protocol, e.source_ref) for dest_ref, e in _iter_redistribution(routers)}

    removed = [k for k in existing_map if k not in incoming_keys]
    for key in removed:
        await db.delete(existing_map[key])

    for dest_ref, entry in _iter_redistribution(routers):
        key = (dest_ref, entry.source_protocol, entry.source_ref)
        row = existing_map.get(key)
        if row is None:
            row = RedistributionIntent(
                device_id=device_id,
                dest_protocol="bgp",
                dest_ref=dest_ref,
                source_protocol=entry.source_protocol,
                source_ref=entry.source_ref,
                accepted_at=now,
            )
            db.add(row)
        row.route_map = entry.route_map
        row.metric = entry.metric
    return removed


async def _maybe_enqueue_apply(db: AsyncSession, device_id: int, router_count: int) -> None:
    """Enqueue an apply job when the payload is non-empty and the device has auto_apply on."""
    if router_count <= 0:
        return
    settings = (
        await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    ).scalar_one_or_none()
    if settings and settings.auto_apply:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)


def _bgp_removed(
    existing_asns: set[str], existing_peers: set[str], routers: list[BgpRouterModel]
) -> tuple[list[str], list[str]]:
    """Return the (router ASNs, peer addresses) dropped out of the new payload.

    The peer diff is device-wide (across all routers/scopes) — the same grain the
    removal collateral guard compares at.
    """
    incoming_asns = {r.asn for r in routers}
    incoming_peers = {p.peer_address for r in routers for s in r.scopes for p in s.peers}
    return sorted(existing_asns - incoming_asns), sorted(existing_peers - incoming_peers)


@router.put("/{device_id}/bgp-intent", dependencies=[Depends(verify_token)])
async def put_bgp_intent(device_id: int, body: BgpIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's BGP intent mirror for this device atomically.

    Full-replace semantics per device: all existing intent rows for the device
    are deleted and replaced with the new payload.  If ``auto_apply`` is
    enabled and the new payload is non-empty, an apply job is enqueued. If a
    router/peer/redistribution was dropped, a removal job is queued so FASTMAP
    reverts it on-device (a merge-PATCH apply would not drop it).
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Snapshot identities before the wipe so removal can be detected afterwards.
    existing_asns, existing_peers = await _capture_bgp_identities(db, device_id)

    now = datetime.now(UTC).replace(tzinfo=None)
    router_count = await _rebuild_router_intent(db, device_id, body.routers, now)
    removed_redist = await _sync_redistribution(db, device_id, body.routers, now)

    await _maybe_enqueue_apply(db, device_id, router_count)

    removed_asns, removed_peers = _bgp_removed(existing_asns, existing_peers, body.routers)
    if removed_asns or removed_peers or removed_redist:
        from nso_adapter.core.removal import enqueue_removal

        # Thread the just-removed keys so the collateral guard can tell this intended
        # retraction from an orphaned service row (redistribute rows are nested,
        # non-guarded content — only the keyed router/peer lists matter here).
        await enqueue_removal(db, device_id, "bgp", removed={"router": removed_asns, "peer": removed_peers})

    await db.commit()

    return {"device_id": device_id, "router_count": router_count}
