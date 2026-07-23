# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Static route refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_static_routes_for_device() — called on-demand by scheduler / SSE coalescer
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
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
    extract=lambda data: data.get("route", []),
    materialize=_upsert_static_routes,
    wire_name="static-route",  # READSEM S3: fetch from the device-state envelope
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
