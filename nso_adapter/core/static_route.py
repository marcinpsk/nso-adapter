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

from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, DeviceStaticRoute

logger = structlog.get_logger(__name__)


async def _upsert_static_routes(
    db: AsyncSession,
    device: Device,
    routes_data: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing rows for *device*, then insert fresh ones."""
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
                metric=route.get("metric"),
                permanent=route.get("permanent"),
                tag=route.get("tag"),
                name=route.get("name"),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )
    await db.commit()


async def refresh_static_routes_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read oper-data for *device* from NSO and upsert DB rows."""
    if not device.nso_device_name:
        logger.debug("static_route.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return

    try:
        entry = await nso_client.get_static_routes(device.nso_device_name)
    except Exception as exc:
        logger.warning("static_route.refresh.nso_error", device_id=device.id, error=repr(exc))
        return

    routes_data = entry.get("route", []) if entry else []
    await _upsert_static_routes(db, device, routes_data, refresh_source)
    logger.info(
        "static_route.refresh.done",
        device_id=device.id,
        device_name=device.nso_device_name,
        route_count=len(routes_data),
        refresh_source=refresh_source,
    )


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
