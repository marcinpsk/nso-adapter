# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Static route refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_static_routes_for_device() — called on-demand by scheduler
- handle_static_route_change()       — placeholder for future SSE hook
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import EmptyPolicy
from nso_adapter.store.models import Device, DeviceStaticRoute

logger = structlog.get_logger(__name__)


async def _upsert_static_routes(
    db: AsyncSession,
    device: Device,
    routes_data: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing rows for *device*, then insert fresh ones (the materializer)."""
    await db.execute(delete(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)
    for route in routes_data:
        prefix = route.get("prefix", "")
        next_hop = route.get("next-hop", "")
        if not prefix:
            continue
        db.add(
            DeviceStaticRoute(
                device_id=device.id,
                vrf=route.get("vrf", ""),
                prefix=prefix,
                next_hop=next_hop,
                interface_next_hop=route.get("interface-next-hop"),
                next_hop_vrf=route.get("next-hop-vrf"),
                metric=route.get("metric"),
                permanent=route.get("permanent"),
                tag=route.get("tag"),
                name=route.get("name"),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )
    await db.commit()


STATIC_ROUTE_SPEC = FamilySpec(
    name="static_route",
    empty_policy=EmptyPolicy.pop,  # config family: a container-confirmed 404 is an authoritative clear
    getter=lambda client, name: client.get_static_routes(name),
    extract=lambda data: data.get("route", []),
    materialize=_upsert_static_routes,
)


async def refresh_static_routes_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read oper-data for *device* from NSO and upsert DB rows (via the shared refresh engine).

    Returns True on a successful read (or nothing to read); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, STATIC_ROUTE_SPEC, refresh_source=refresh_source)


async def handle_static_route_change(
    db: AsyncSession,
    nso_device_name: str,
    nso_client: NsoClient,
) -> None:
    """Handle an SSE notification that static routes changed for *nso_device_name*.

    Finds the Device row, then delegates to refresh_static_routes_for_device.
    """
    from sqlalchemy import select

    from nso_adapter.store.models import Device

    result = await db.execute(select(Device).where(Device.nso_device_name == nso_device_name))
    device = result.scalar_one_or_none()
    if device is None:
        logger.debug("static_route.sse.unknown_device", nso_device_name=nso_device_name)
        return

    await refresh_static_routes_for_device(db, device, nso_client, refresh_source="sse")
