# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Route-policy config refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_route_policy_for_device() — called on-demand by scheduler / SSE coalescer
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import EmptyPolicy
from nso_adapter.nso.shape import as_list
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
)

logger = structlog.get_logger(__name__)


def _content_hash(obj: object) -> str:
    """Stable SHA-256 of the canonical JSON representation of *obj*."""
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _required(entry: dict, *keys: str) -> bool:
    """Report whether every required (NOT-NULL) leaf is present and non-null.

    A malformed entry missing one of these would otherwise KeyError (direct subscript) or
    land None in a NOT-NULL column — either way aborting and wiping the 4-family full-replace.
    """
    return all(entry.get(k) is not None for k in keys)


def _dedup_by_name(items: list, family: str, device_name: str) -> list:
    """Drop objects repeating a name within one refresh (keep the first, log the rest).

    The store keys route-policy objects by ``(device_id, name)``; a reader that reports the
    same name twice — e.g. SR OS lets an ``as-path`` and an ``as-path-group`` (or two
    prefix-list reads) share a name — would otherwise abort the WHOLE full-replace refresh on
    a unique-constraint violation, FREEZING the device's read-mirror. This makes the refresh
    resilient to any such duplicate regardless of which reader produced it.
    """
    seen: set = set()
    out: list = []
    for item in items:
        name = item.get("name")
        if name in seen:
            logger.warning("route_policy.refresh.duplicate_name_skipped", device=device_name, family=family, name=name)
            continue
        seen.add(name)
        out.append(item)
    return out


async def _upsert_prefix_lists(db, device, items, now, refresh_source) -> None:
    for pl_data in _dedup_by_name(items, "prefix-list", device.nso_device_name):
        if not pl_data.get("name"):
            continue  # list without a name → nothing to key on
        pl = DeviceRoutePolicyPrefixList(
            device_id=device.id,
            name=pl_data["name"],
            family=pl_data.get("family", 4),
            content_hash=_content_hash(pl_data),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(pl)
        await db.flush()
        for e in as_list(pl_data.get("entry")):
            if not _required(e, "sequence", "action", "prefix"):
                continue
            db.add(
                DeviceRoutePolicyPrefixListEntry(
                    prefix_list_id=pl.id,
                    sequence=e["sequence"],
                    action=e["action"],
                    prefix=e["prefix"],
                    ge=e.get("ge"),
                    le=e.get("le"),
                )
            )


async def _upsert_community_lists(db, device, items, now, refresh_source, dialect) -> None:
    for cl_data in _dedup_by_name(items, "community-list", device.nso_device_name):
        if not cl_data.get("name"):
            continue
        cl = DeviceRoutePolicyCommunityList(
            device_id=device.id,
            name=cl_data["name"],
            invert_match=bool(cl_data.get("invert-match", False)),
            content_hash=_content_hash(cl_data),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(cl)
        await db.flush()
        for e in as_list(cl_data.get("entry")):
            # null/absent community would also crash dialect.to_canonical (None.strip()).
            if not _required(e, "sequence", "action", "community"):
                continue
            db.add(
                DeviceRoutePolicyCommunityListEntry(
                    community_list_id=cl.id,
                    sequence=e["sequence"],
                    action=e["action"],
                    community=dialect.to_canonical(e["community"]),
                )
            )


async def _upsert_as_paths(db, device, items, now, refresh_source) -> None:
    for ap_data in _dedup_by_name(items, "as-path", device.nso_device_name):
        if not ap_data.get("name"):
            continue
        ap = DeviceRoutePolicyASPath(
            device_id=device.id,
            name=ap_data["name"],
            content_hash=_content_hash(ap_data),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(ap)
        await db.flush()
        for e in as_list(ap_data.get("entry")):
            if not _required(e, "sequence", "action", "pattern"):
                continue
            db.add(
                DeviceRoutePolicyASPathEntry(
                    as_path_id=ap.id,
                    sequence=e["sequence"],
                    action=e["action"],
                    pattern=e["pattern"],
                )
            )


async def _upsert_route_maps(db, device, items, now, refresh_source) -> None:
    for rm_data in _dedup_by_name(items, "route-map", device.nso_device_name):
        if not rm_data.get("name"):
            continue
        rm = DeviceRoutePolicyRouteMap(
            device_id=device.id,
            name=rm_data["name"],
            content_hash=_content_hash(rm_data),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(rm)
        await db.flush()
        for e in as_list(rm_data.get("entry")):
            if not _required(e, "sequence", "action"):
                continue
            db.add(
                DeviceRoutePolicyRouteMapEntry(
                    route_map_id=rm.id,
                    sequence=e["sequence"],
                    action=e["action"],
                    match_prefix_lists=e.get("match-prefix-lists") or [],
                    match_community_lists=e.get("match-community-lists") or [],
                    match_as_paths=e.get("match-as-paths") or [],
                    match_json=e.get("match-json") or "{}",
                    set_json=e.get("set-json") or "{}",
                )
            )


async def _upsert_route_policy_data(
    db: AsyncSession,
    device: Device,
    data: dict,
    refresh_source: str,
) -> None:
    """Full-replace: delete existing route-policy rows for *device*, then insert."""
    # Per-NED community members are normalised to the canonical (Cisco/Junos) form
    # on the way in, so the plugin and drift-detection compare like-for-like.
    dialect = community_dialect_for(device.ned_id)
    await db.execute(delete(DeviceRoutePolicyPrefixList).where(DeviceRoutePolicyPrefixList.device_id == device.id))
    await db.execute(
        delete(DeviceRoutePolicyCommunityList).where(DeviceRoutePolicyCommunityList.device_id == device.id)
    )
    await db.execute(delete(DeviceRoutePolicyASPath).where(DeviceRoutePolicyASPath.device_id == device.id))
    await db.execute(delete(DeviceRoutePolicyRouteMap).where(DeviceRoutePolicyRouteMap.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)

    # as_list guards the RESTCONF singleton-rendered-as-bare-dict case for each top-level list;
    # an empty ``data`` dict (the AbsentAuthoritative clear) yields four empty lists → clear.
    await _upsert_prefix_lists(db, device, as_list(data.get("prefix-list")), now, refresh_source)
    await _upsert_community_lists(db, device, as_list(data.get("community-list")), now, refresh_source, dialect)
    await _upsert_as_paths(db, device, as_list(data.get("as-path")), now, refresh_source)
    await _upsert_route_maps(db, device, as_list(data.get("route-map")), now, refresh_source)

    await db.commit()


ROUTE_POLICY_SPEC = FamilySpec(
    name="route_policy",
    empty_policy=EmptyPolicy.pop,  # config family: a container-confirmed 404 is an authoritative clear
    getter=lambda client, name: client.get_route_policy(name),
    # The materializer takes the whole entry dict; extract({}) → the four-family clear. The engine
    # logs the clear (route_policy.refresh.cleared) — fixing the old silent clear-on-None path.
    extract=lambda data: data,
    materialize=_upsert_route_policy_data,
    wire_name="route-policy",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_route_policy_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read route-policy oper-data for *device* from NSO and upsert DB rows (via the shared refresh engine).

    Returns True on a successful read (or nothing to read); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, ROUTE_POLICY_SPEC, refresh_source=refresh_source)
