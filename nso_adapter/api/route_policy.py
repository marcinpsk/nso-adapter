# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Route-policy endpoints: GET + PUT /api/v1/devices/{id}/route-policy(-intent)."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, api_error
from nso_adapter.core.removal import lost_content
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


async def _group_in(db: AsyncSession, model, fk_col, ids: list[int], key_attr: str, order_col) -> dict[int, list]:
    """Load ``model`` rows where ``fk_col`` is in *ids*, grouped by ``key_attr`` (empty ids → {})."""
    if not ids:
        return {}
    rows = (await db.execute(select(model).where(fk_col.in_(ids)).order_by(order_col))).scalars().all()
    grouped: dict[int, list] = {}
    for row in rows:
        grouped.setdefault(getattr(row, key_attr), []).append(row)
    return grouped


def _serialize_prefix_list(pl: DeviceRoutePolicyPrefixList, entries: list) -> dict:
    return {
        "name": pl.name,
        "family": pl.family,
        "entries": [
            {
                "sequence": e.sequence,
                "action": e.action,
                "prefix": e.prefix,
                **({"ge": e.ge} if e.ge is not None else {}),
                **({"le": e.le} if e.le is not None else {}),
            }
            for e in entries
        ],
    }


def _serialize_community_list(cl: DeviceRoutePolicyCommunityList, entries: list) -> dict:
    return {
        "name": cl.name,
        "invert_match": cl.invert_match,
        "entries": [{"sequence": e.sequence, "action": e.action, "community": e.community} for e in entries],
    }


def _serialize_as_path(ap: DeviceRoutePolicyASPath, entries: list) -> dict:
    return {
        "name": ap.name,
        "entries": [{"sequence": e.sequence, "action": e.action, "pattern": e.pattern} for e in entries],
    }


def _serialize_route_map(rm: DeviceRoutePolicyRouteMap, entries: list) -> dict:
    return {
        "name": rm.name,
        "entries": [
            {
                "sequence": e.sequence,
                "action": e.action,
                "match_prefix_lists": e.match_prefix_lists or [],
                "match_community_lists": e.match_community_lists or [],
                "match_as_paths": e.match_as_paths or [],
                "match": e.match_json or "{}",
                "set": e.set_json or "{}",
            }
            for e in entries
        ],
    }


async def _load_named(db: AsyncSession, model, device_id: int) -> list:
    """Load a route-policy family for a device, ordered by name."""
    return (await db.execute(select(model).where(model.device_id == device_id).order_by(model.name))).scalars().all()


# ── Read-mirror response models (GET /route-policy) ───────────────────────────
# Mostly fixed shapes; only a prefix-list entry omits ge/le when unset, so the
# endpoint uses exclude_unset. NB: NO top-level refresh_source. ``family`` is an
# int; a route-map entry's ``match``/``set`` are JSON *strings* (the reader emits
# match_json/set_json verbatim) while match_* are lists.


class RoutePolicyPrefixEntryOut(BaseModel):
    sequence: int
    action: str
    prefix: str
    ge: int | None = None
    le: int | None = None


class RoutePolicyPrefixListOut(BaseModel):
    name: str
    family: int
    entries: list[RoutePolicyPrefixEntryOut]


class RoutePolicyCommunityEntryOut(BaseModel):
    sequence: int
    action: str
    community: str


class RoutePolicyCommunityListOut(BaseModel):
    name: str
    invert_match: bool
    entries: list[RoutePolicyCommunityEntryOut]


class RoutePolicyASPathEntryOut(BaseModel):
    sequence: int
    action: str
    pattern: str


class RoutePolicyASPathOut(BaseModel):
    name: str
    entries: list[RoutePolicyASPathEntryOut]


class RoutePolicyRouteMapEntryOut(BaseModel):
    sequence: int
    action: str
    match_prefix_lists: list
    match_community_lists: list
    match_as_paths: list
    match: str  # JSON string (match_json or "{}")
    set: str  # JSON string (set_json or "{}")


class RoutePolicyRouteMapOut(BaseModel):
    name: str
    entries: list[RoutePolicyRouteMapEntryOut]


class RoutePolicyConfigOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # reader formats "<iso>Z"; None when never refreshed (no refresh_source here)
    prefix_lists: list[RoutePolicyPrefixListOut]
    community_lists: list[RoutePolicyCommunityListOut]
    as_paths: list[RoutePolicyASPathOut]
    route_maps: list[RoutePolicyRouteMapOut]


@router.get(
    "/{device_id}/route-policy",
    dependencies=[Depends(verify_token)],
    response_model=RoutePolicyConfigOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_route_policy(device_id: int, db: AsyncSession = Depends(get_db)):
    """Return the route-policy config read-mirror for this device.

    Response shape matches the YANG contract in m17-route-policy-contract.md §3.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    prefix_lists = await _load_named(db, DeviceRoutePolicyPrefixList, device_id)
    community_lists = await _load_named(db, DeviceRoutePolicyCommunityList, device_id)
    as_paths = await _load_named(db, DeviceRoutePolicyASPath, device_id)
    route_maps = await _load_named(db, DeviceRoutePolicyRouteMap, device_id)

    # Bulk-load all entries in one query per family to avoid N+1.
    pl_entries = await _group_in(
        db,
        DeviceRoutePolicyPrefixListEntry,
        DeviceRoutePolicyPrefixListEntry.prefix_list_id,
        [p.id for p in prefix_lists],
        "prefix_list_id",
        DeviceRoutePolicyPrefixListEntry.sequence,
    )
    cl_entries = await _group_in(
        db,
        DeviceRoutePolicyCommunityListEntry,
        DeviceRoutePolicyCommunityListEntry.community_list_id,
        [c.id for c in community_lists],
        "community_list_id",
        DeviceRoutePolicyCommunityListEntry.sequence,
    )
    ap_entries = await _group_in(
        db,
        DeviceRoutePolicyASPathEntry,
        DeviceRoutePolicyASPathEntry.as_path_id,
        [a.id for a in as_paths],
        "as_path_id",
        DeviceRoutePolicyASPathEntry.sequence,
    )
    rm_entries = await _group_in(
        db,
        DeviceRoutePolicyRouteMapEntry,
        DeviceRoutePolicyRouteMapEntry.route_map_id,
        [r.id for r in route_maps],
        "route_map_id",
        DeviceRoutePolicyRouteMapEntry.sequence,
    )

    last_refreshed_at = None
    for obj in (*prefix_lists, *community_lists, *as_paths, *route_maps):
        ts = obj.last_refreshed_at
        if ts and (last_refreshed_at is None or ts > last_refreshed_at):
            last_refreshed_at = ts

    return {
        "device_id": device_id,
        "last_refreshed_at": last_refreshed_at.isoformat() + "Z" if last_refreshed_at else None,
        "prefix_lists": [_serialize_prefix_list(pl, pl_entries.get(pl.id, [])) for pl in prefix_lists],
        "community_lists": [_serialize_community_list(cl, cl_entries.get(cl.id, [])) for cl in community_lists],
        "as_paths": [_serialize_as_path(ap, ap_entries.get(ap.id, [])) for ap in as_paths],
        "route_maps": [_serialize_route_map(rm, rm_entries.get(rm.id, [])) for rm in route_maps],
    }


_VALID_FAMILIES = {"prefix_list", "community_list", "as_path", "route_map"}


async def _upsert_route_policy_object(db: AsyncSession, device_id: int, obj: dict, before_entries: dict, now) -> bool:
    """Create-or-update one policy object; return True if its content SHRANK.

    A shrink (a route-map term deleted, a prefix-list line removed, a community member
    dropped, a match/set leaf blanked) is invisible to the (family, name) diff — the object
    itself survives — and a merge-PATCH apply cannot express it, so the deleted content stays
    in the service's CDB input and FASTMAP keeps creating it on the device. It has to trigger
    the PUT-replace retract (see nso_adapter.core.removal.lost_content).
    """
    family = obj.get("family")
    name = obj.get("name")
    entries = obj.get("entries")
    accepted = obj.get("accepted", False)
    invert_match = bool(obj.get("invert_match", False))

    if family not in _VALID_FAMILIES:
        raise api_error(422, "invalid_family", f"Unknown family: {family!r}")
    if not isinstance(name, str) or not name:
        raise api_error(422, "invalid_name", "Each object must have a non-empty 'name'")
    if not isinstance(entries, list):
        raise api_error(422, "invalid_entries", f"'entries' for {name!r} must be a list")

    row = (
        await db.execute(
            select(RoutePolicyObjectIntent).where(
                RoutePolicyObjectIntent.device_id == device_id,
                RoutePolicyObjectIntent.family == family,
                RoutePolicyObjectIntent.name == name,
            )
        )
    ).scalar_one_or_none()

    if row is None:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family=family,
                name=name,
                entries=entries,
                invert_match=invert_match,
                accepted_at=now if accepted else None,
            )
        )
        return False  # brand new — nothing it could have lost

    shrank = lost_content(before_entries.get((family, name)), entries)
    row.entries = entries
    row.invert_match = invert_match
    if accepted:
        row.accepted_at = now
    return shrank


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
    Full-replace semantics: the plugin always pushes the full owned set, so objects
    absent from the payload are removed from the mirror.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    objects = body.get("objects")
    if not isinstance(objects, list):
        raise api_error(422, "invalid_payload", "Body must contain an 'objects' list")

    now = datetime.now(UTC).replace(tzinfo=None)
    upserted = 0

    # Full-replace: drop objects for this device that are absent from the payload
    # (the plugin always pushes the full owned set).
    incoming_keys = {(o.get("family"), o.get("name")) for o in objects}
    existing_all = (
        (await db.execute(select(RoutePolicyObjectIntent).where(RoutePolicyObjectIntent.device_id == device_id)))
        .scalars()
        .all()
    )
    removed = [(r.family, r.name) for r in existing_all if (r.family, r.name) not in incoming_keys]
    # Pre-image of the surviving objects' CONTENT. The (family, name) diff above only sees an
    # object that vanished whole — it is blind to the object that merely lost a route-map TERM,
    # a prefix-list line, or a community member. Those live inside `entries`, which is
    # overwritten in place below, and a merge-PATCH apply cannot drop any of them: the deleted
    # term stays in the service's CDB input, so FASTMAP keeps creating it on the device
    # (tracker #83, the route-policy leg).
    before_entries = {(r.family, r.name): r.entries for r in existing_all}
    for r in existing_all:
        if (r.family, r.name) not in incoming_keys:
            await db.delete(r)
    await db.flush()

    cleared = False
    for obj in objects:
        upserted += 1
        if await _upsert_route_policy_object(db, device_id, obj, before_entries, now):
            cleared = True  # a term/line/member disappeared, or a leaf inside one was blanked

    await db.commit()

    # Which community-list members can this device's NED NOT hold? The apply path
    # silently skips them (a wildcard color on Nokia has no exact ext-community), which
    # would otherwise leave the plugin showing a phantom "pending apply" that never
    # clears — the intent carries a member the device can never mirror. Report them so
    # the plugin can mark them "unsupported on <ned>" and drop them from its drift/pending
    # comparison. Deterministic (codec-only, no device write), keyed by object name.
    from nso_adapter.core.community_dialect import community_dialect_for

    dialect = community_dialect_for(device.ned_id)
    unsupported_members: dict[str, list[str]] = {}
    for obj in objects:
        if obj.get("family") != "community_list":
            continue
        members = [e.get("community") for e in (obj.get("entries") or []) if isinstance(e, dict)]
        bad = dialect.unrepresentable_members(members)
        if bad:
            unsupported_members[obj["name"]] = bad

    logger.info(
        "route_policy_intent.put",
        device_id=device_id,
        upserted=upserted,
        removed=len(removed),
        unsupported=sum(len(v) for v in unsupported_members.values()),
    )

    if removed or cleared:
        from nso_adapter.core.removal import replace_on_removal
        from nso_adapter.nso.apply import apply_route_policy_config

        # retract=cleared: an object that only lost a term is still OWNED and accepted — nothing
        # is being un-owned, so the PUT-replace must actually reach the device rather than
        # detach with no-networking (#106's detach-by-default is for shrinking OWNERSHIP).
        await replace_on_removal(
            db,
            device,
            removed,
            RoutePolicyObjectIntent,
            apply_route_policy_config,
            retract=cleared,
        )

    # Return updated intent state for all objects on this device, plus the per-object
    # unsupported-member map so the plugin can surface codec-skipped members.
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
        "unsupported_members": unsupported_members,
    }
