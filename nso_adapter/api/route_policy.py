# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Route-policy endpoints: GET + PUT /api/v1/devices/{id}/route-policy(-intent)."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import (
    Device,
    DeviceRoutePolicyASPath,
    DeviceRoutePolicyASPathEntry,
    DeviceRoutePolicyCommunityList,
    DeviceRoutePolicyCommunityListEntry,
    DeviceRoutePolicyPrefixList,
    DeviceRoutePolicyPrefixListEntry,
    DeviceRoutePolicyRouteMap,
    DeviceRoutePolicyRouteMapEntry,
    RoutePolicyObjectIntent,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["route-policy"])


@router.get("/{device_id}/route-policy", dependencies=[Depends(verify_token)])
async def get_route_policy(device_id: int, db: AsyncSession = Depends(get_db)):
    """Return the route-policy config read-mirror for this device.

    Response shape matches the YANG contract in m17-route-policy-contract.md §3.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    prefix_lists = (
        (
            await db.execute(
                select(DeviceRoutePolicyPrefixList)
                .where(DeviceRoutePolicyPrefixList.device_id == device_id)
                .order_by(DeviceRoutePolicyPrefixList.name)
            )
        )
        .scalars()
        .all()
    )

    community_lists = (
        (
            await db.execute(
                select(DeviceRoutePolicyCommunityList)
                .where(DeviceRoutePolicyCommunityList.device_id == device_id)
                .order_by(DeviceRoutePolicyCommunityList.name)
            )
        )
        .scalars()
        .all()
    )

    as_paths = (
        (
            await db.execute(
                select(DeviceRoutePolicyASPath)
                .where(DeviceRoutePolicyASPath.device_id == device_id)
                .order_by(DeviceRoutePolicyASPath.name)
            )
        )
        .scalars()
        .all()
    )

    route_maps = (
        (
            await db.execute(
                select(DeviceRoutePolicyRouteMap)
                .where(DeviceRoutePolicyRouteMap.device_id == device_id)
                .order_by(DeviceRoutePolicyRouteMap.name)
            )
        )
        .scalars()
        .all()
    )

    # Bulk-load all entries in one query per family to avoid N+1.
    pl_ids = [p.id for p in prefix_lists]
    cl_ids = [c.id for c in community_lists]
    ap_ids = [a.id for a in as_paths]
    rm_ids = [r.id for r in route_maps]

    pl_entries_by_id: dict[int, list] = {p.id: [] for p in prefix_lists}
    cl_entries_by_id: dict[int, list] = {c.id: [] for c in community_lists}
    ap_entries_by_id: dict[int, list] = {a.id: [] for a in as_paths}
    rm_entries_by_id: dict[int, list] = {r.id: [] for r in route_maps}

    if pl_ids:
        for e in (
            (
                await db.execute(
                    select(DeviceRoutePolicyPrefixListEntry)
                    .where(DeviceRoutePolicyPrefixListEntry.prefix_list_id.in_(pl_ids))
                    .order_by(DeviceRoutePolicyPrefixListEntry.sequence)
                )
            )
            .scalars()
            .all()
        ):
            pl_entries_by_id[e.prefix_list_id].append(e)

    if cl_ids:
        for e in (
            (
                await db.execute(
                    select(DeviceRoutePolicyCommunityListEntry)
                    .where(DeviceRoutePolicyCommunityListEntry.community_list_id.in_(cl_ids))
                    .order_by(DeviceRoutePolicyCommunityListEntry.sequence)
                )
            )
            .scalars()
            .all()
        ):
            cl_entries_by_id[e.community_list_id].append(e)

    if ap_ids:
        for e in (
            (
                await db.execute(
                    select(DeviceRoutePolicyASPathEntry)
                    .where(DeviceRoutePolicyASPathEntry.as_path_id.in_(ap_ids))
                    .order_by(DeviceRoutePolicyASPathEntry.sequence)
                )
            )
            .scalars()
            .all()
        ):
            ap_entries_by_id[e.as_path_id].append(e)

    if rm_ids:
        for e in (
            (
                await db.execute(
                    select(DeviceRoutePolicyRouteMapEntry)
                    .where(DeviceRoutePolicyRouteMapEntry.route_map_id.in_(rm_ids))
                    .order_by(DeviceRoutePolicyRouteMapEntry.sequence)
                )
            )
            .scalars()
            .all()
        ):
            rm_entries_by_id[e.route_map_id].append(e)

    last_refreshed_at = None
    for obj in (*prefix_lists, *community_lists, *as_paths, *route_maps):
        ts = obj.last_refreshed_at
        if ts and (last_refreshed_at is None or ts > last_refreshed_at):
            last_refreshed_at = ts

    prefix_lists_out = []
    for pl in prefix_lists:
        pl_entries_out = [
            {
                "sequence": e.sequence,
                "action": e.action,
                "prefix": e.prefix,
                **({"ge": e.ge} if e.ge is not None else {}),
                **({"le": e.le} if e.le is not None else {}),
            }
            for e in pl_entries_by_id[pl.id]
        ]
        prefix_lists_out.append(
            {
                "name": pl.name,
                "family": pl.family,
                "entries": pl_entries_out,
            }
        )

    community_lists_out = []
    for cl in community_lists:
        cl_entries_out = [
            {"sequence": e.sequence, "action": e.action, "community": e.community} for e in cl_entries_by_id[cl.id]
        ]
        community_lists_out.append({"name": cl.name, "entries": cl_entries_out})

    as_paths_out = []
    for ap in as_paths:
        ap_entries_out = [
            {"sequence": e.sequence, "action": e.action, "pattern": e.pattern} for e in ap_entries_by_id[ap.id]
        ]
        as_paths_out.append({"name": ap.name, "entries": ap_entries_out})

    route_maps_out = []
    for rm in route_maps:
        rm_entries_out = []
        for e in rm_entries_by_id[rm.id]:
            rm_entry: dict = {
                "sequence": e.sequence,
                "action": e.action,
                "match_prefix_lists": e.match_prefix_lists or [],
                "match_community_lists": e.match_community_lists or [],
                "match_as_paths": e.match_as_paths or [],
                "match": e.match_json or "{}",
                "set": e.set_json or "{}",
            }
            rm_entries_out.append(rm_entry)
        route_maps_out.append({"name": rm.name, "entries": rm_entries_out})

    return {
        "device_id": device_id,
        "last_refreshed_at": last_refreshed_at.isoformat() + "Z" if last_refreshed_at else None,
        "prefix_lists": prefix_lists_out,
        "community_lists": community_lists_out,
        "as_paths": as_paths_out,
        "route_maps": route_maps_out,
    }


_VALID_FAMILIES = {"prefix_list", "community_list", "as_path", "route_map"}


@router.put("/{device_id}/route-policy-intent", dependencies=[Depends(verify_token)])
async def put_route_policy_intent(
    device_id: int,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """Store per-object route-policy intent for a device (full-replace per object).

    Body shape:
        {
          "objects": [
            {"family": "prefix_list", "name": "PL-RFC1918", "entries": [...], "accepted": true},
            ...
          ]
        }

    Each object with ``"accepted": true`` gets ``accepted_at`` stamped.
    Objects not present in this PUT are left untouched (partial-update semantics —
    callers send only the objects they want to accept).
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    objects = body.get("objects")
    if not isinstance(objects, list):
        raise api_error(422, "invalid_payload", "Body must contain an 'objects' list")

    now = datetime.now(UTC).replace(tzinfo=None)
    upserted = 0

    for obj in objects:
        family = obj.get("family")
        name = obj.get("name")
        entries = obj.get("entries")
        accepted = obj.get("accepted", False)

        if family not in _VALID_FAMILIES:
            raise api_error(422, "invalid_family", f"Unknown family: {family!r}")
        if not isinstance(name, str) or not name:
            raise api_error(422, "invalid_name", "Each object must have a non-empty 'name'")
        if not isinstance(entries, list):
            raise api_error(422, "invalid_entries", f"'entries' for {name!r} must be a list")

        existing_result = await db.execute(
            select(RoutePolicyObjectIntent).where(
                RoutePolicyObjectIntent.device_id == device_id,
                RoutePolicyObjectIntent.family == family,
                RoutePolicyObjectIntent.name == name,
            )
        )
        row = existing_result.scalar_one_or_none()

        if row is None:
            row = RoutePolicyObjectIntent(
                device_id=device_id,
                family=family,
                name=name,
                entries=entries,
                accepted_at=now if accepted else None,
            )
            db.add(row)
        else:
            row.entries = entries
            if accepted:
                row.accepted_at = now

        upserted += 1

    await db.commit()
    logger.info(
        "route_policy_intent.put",
        device_id=device_id,
        upserted=upserted,
    )

    # Return updated intent state for all objects on this device.
    result = await db.execute(
        select(RoutePolicyObjectIntent)
        .where(RoutePolicyObjectIntent.device_id == device_id)
        .order_by(RoutePolicyObjectIntent.family, RoutePolicyObjectIntent.name)
    )
    rows = result.scalars().all()
    return {
        "device_id": device_id,
        "objects": [
            {
                "id": r.id,
                "family": r.family,
                "name": r.name,
                "entries": r.entries,
                "accepted_at": r.accepted_at.isoformat() + "Z" if r.accepted_at else None,
                "last_apply_at": r.last_apply_at.isoformat() + "Z" if r.last_apply_at else None,
                "last_apply_error": r.last_apply_error,
            }
            for r in rows
        ],
    }
