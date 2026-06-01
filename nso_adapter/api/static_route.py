# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/static-routes and PUT /api/v1/devices/{id}/static-route-intent."""
from __future__ import annotations

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceSettings, DeviceStaticRoute, StaticRouteIntent

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["static-routes"])


@router.get("/{device_id}/static-routes", dependencies=[Depends(verify_token)])
async def get_static_routes(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    result = await db.execute(
        select(DeviceStaticRoute)
        .where(DeviceStaticRoute.device_id == device_id)
        .order_by(DeviceStaticRoute.vrf, DeviceStaticRoute.prefix, DeviceStaticRoute.next_hop)
    )
    rows = result.scalars().all()

    if not rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "routes": [],
        }

    latest = max(rows, key=lambda r: r.last_refreshed_at or "")

    routes = []
    for row in rows:
        entry: dict = {
            "vrf": row.vrf,
            "prefix": row.prefix,
            "next_hop": row.next_hop,
        }
        if row.interface_next_hop is not None:
            entry["interface_next_hop"] = row.interface_next_hop
        if row.metric is not None:
            entry["metric"] = row.metric
        if row.permanent is not None:
            entry["permanent"] = row.permanent
        if row.tag is not None:
            entry["tag"] = row.tag
        if row.name is not None:
            entry["name"] = row.name
        routes.append(entry)

    last_ts = latest.last_refreshed_at
    return {
        "device_id": device_id,
        "last_refreshed_at": last_ts.isoformat() + "Z" if last_ts else None,
        "refresh_source": latest.refresh_source,
        "routes": routes,
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/static-route-intent
# ---------------------------------------------------------------------------


class StaticRouteEntry(BaseModel):
    vrf: str = ""
    prefix: str
    next_hop: str
    metric: int | None = None
    permanent: bool | None = None
    tag: int | None = None
    name: str | None = None
    accepted_at: datetime | None = None


class StaticRouteIntentUpdate(BaseModel):
    routes: list[StaticRouteEntry]


@router.put("/{device_id}/static-route-intent", dependencies=[Depends(verify_token)])
async def put_static_route_intent(
    device_id: int, body: StaticRouteIntentUpdate, db: AsyncSession = Depends(get_db)
):
    """Replace the adapter's static-route intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is
    enabled on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    existing_result = await db.execute(
        select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)
    )
    existing_rows: dict[tuple, StaticRouteIntent] = {
        (r.vrf, r.prefix, r.next_hop): r for r in existing_result.scalars().all()
    }

    # Determine which (vrf, prefix, next_hop) keys are in the new payload.
    new_keys: set[tuple] = {(item.vrf, item.prefix, item.next_hop) for item in body.routes}

    # Delete rows absent from the new payload.
    for key, row in existing_rows.items():
        if key not in new_keys:
            await db.delete(row)
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    for item in body.routes:
        key = (item.vrf, item.prefix, item.next_hop)
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        if key in existing_rows:
            row = existing_rows[key]
            row.accepted_at = accepted
            row.metric = item.metric
            row.permanent = item.permanent
            row.tag = item.tag
            row.name = item.name
        else:
            row = StaticRouteIntent(
                device_id=device_id,
                vrf=item.vrf,
                prefix=item.prefix,
                next_hop=item.next_hop,
                metric=item.metric,
                permanent=item.permanent,
                tag=item.tag,
                name=item.name,
                accepted_at=accepted,
            )
            db.add(row)
        count += 1

    await db.flush()

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply
        await enqueue_apply(db, device_id, force=True)

    await db.commit()
    return {"device_id": device_id, "count": count}
