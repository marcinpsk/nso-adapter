# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Route-policy config refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_route_policy_for_device() — called on-demand by scheduler
- handle_route_policy_change()      — placeholder for future SSE hook
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.nso.client import NsoClient
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

    for pl_data in _dedup_by_name(data.get("prefix-list", []), "prefix-list", device.nso_device_name):
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
        for e in pl_data.get("entry", []):
            entry = DeviceRoutePolicyPrefixListEntry(
                prefix_list_id=pl.id,
                sequence=e["sequence"],
                action=e["action"],
                prefix=e["prefix"],
                ge=e.get("ge"),
                le=e.get("le"),
            )
            db.add(entry)

    for cl_data in _dedup_by_name(data.get("community-list", []), "community-list", device.nso_device_name):
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
        for e in cl_data.get("entry", []):
            entry = DeviceRoutePolicyCommunityListEntry(
                community_list_id=cl.id,
                sequence=e["sequence"],
                action=e["action"],
                community=dialect.to_canonical(e["community"]),
            )
            db.add(entry)

    for ap_data in _dedup_by_name(data.get("as-path", []), "as-path", device.nso_device_name):
        ap = DeviceRoutePolicyASPath(
            device_id=device.id,
            name=ap_data["name"],
            content_hash=_content_hash(ap_data),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(ap)
        await db.flush()
        for e in ap_data.get("entry", []):
            entry = DeviceRoutePolicyASPathEntry(
                as_path_id=ap.id,
                sequence=e["sequence"],
                action=e["action"],
                pattern=e["pattern"],
            )
            db.add(entry)

    for rm_data in _dedup_by_name(data.get("route-map", []), "route-map", device.nso_device_name):
        rm = DeviceRoutePolicyRouteMap(
            device_id=device.id,
            name=rm_data["name"],
            content_hash=_content_hash(rm_data),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(rm)
        await db.flush()
        for e in rm_data.get("entry", []):
            entry = DeviceRoutePolicyRouteMapEntry(
                route_map_id=rm.id,
                sequence=e["sequence"],
                action=e["action"],
                match_prefix_lists=e.get("match-prefix-lists") or [],
                match_community_lists=e.get("match-community-lists") or [],
                match_as_paths=e.get("match-as-paths") or [],
                match_json=e.get("match-json") or "{}",
                set_json=e.get("set-json") or "{}",
            )
            db.add(entry)

    await db.commit()


async def refresh_route_policy_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read route-policy oper-data for *device* from NSO and upsert DB rows."""
    if not device.nso_device_name:
        logger.debug("route_policy.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return

    try:
        nso_data = await nso_client.get_route_policy(device.nso_device_name)
    except Exception as exc:
        logger.warning(
            "route_policy.refresh.nso_error",
            device_id=device.id,
            device_name=device.nso_device_name,
            error=str(exc),
        )
        return

    if nso_data is None:
        # Device has no route-policy objects — clear any stale rows.
        await db.execute(delete(DeviceRoutePolicyPrefixList).where(DeviceRoutePolicyPrefixList.device_id == device.id))
        await db.execute(
            delete(DeviceRoutePolicyCommunityList).where(DeviceRoutePolicyCommunityList.device_id == device.id)
        )
        await db.execute(delete(DeviceRoutePolicyASPath).where(DeviceRoutePolicyASPath.device_id == device.id))
        await db.execute(delete(DeviceRoutePolicyRouteMap).where(DeviceRoutePolicyRouteMap.device_id == device.id))
        await db.commit()
        return

    await _upsert_route_policy_data(db, device, nso_data, refresh_source)

    pl_count = len(nso_data.get("prefix-list", []))
    cl_count = len(nso_data.get("community-list", []))
    ap_count = len(nso_data.get("as-path", []))
    rm_count = len(nso_data.get("route-map", []))
    logger.info(
        "route_policy.refresh.done",
        device_id=device.id,
        device_name=device.nso_device_name,
        prefix_lists=pl_count,
        community_lists=cl_count,
        as_paths=ap_count,
        route_maps=rm_count,
        source=refresh_source,
    )


async def handle_route_policy_change(device_name: str, db: AsyncSession, nso_client: NsoClient) -> None:
    """Refresh route-policy for a single device by name (placeholder SSE hook)."""
    result = await db.execute(select(Device).where(Device.nso_device_name == device_name))
    device = result.scalar_one_or_none()
    if device is None:
        logger.debug("route_policy.sse.device_not_found", device_name=device_name)
        return
    await refresh_route_policy_for_device(db, device, nso_client, refresh_source="sse")
