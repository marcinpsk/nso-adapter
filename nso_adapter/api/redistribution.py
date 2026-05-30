# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/redistribution — read redistribution rows from DB."""
from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceRedistribution

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["redistribution"])


@router.get("/{device_id}/redistribution", dependencies=[Depends(verify_token)])
async def get_redistribution(device_id: int, db: AsyncSession = Depends(get_db)):
    """Return all redistribution statements cached from NSO for *device_id*.

    Returns 404 when the device is unknown, empty list when no rows are cached.
    Each entry carries: dest_protocol, dest_ref, source_protocol, source_ref,
    route_map (nullable), metric (nullable), metric_type (nullable).
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    result = await db.execute(
        select(DeviceRedistribution)
        .where(DeviceRedistribution.device_id == device_id)
        .order_by(
            DeviceRedistribution.dest_protocol,
            DeviceRedistribution.dest_ref,
            DeviceRedistribution.source_protocol,
        )
    )
    rows = result.scalars().all()

    if not rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "entries": [],
        }

    latest = max(rows, key=lambda r: r.last_refreshed_at or "")

    entries = []
    for row in rows:
        entry: dict = {
            "dest_protocol": row.dest_protocol,
            "dest_ref": row.dest_ref,
            "source_protocol": row.source_protocol,
            "source_ref": row.source_ref,
        }
        if row.route_map is not None:
            entry["route_map"] = row.route_map
        if row.metric is not None:
            entry["metric"] = row.metric
        if row.metric_type is not None:
            entry["metric_type"] = row.metric_type
        entries.append(entry)

    return {
        "device_id": device_id,
        "last_refreshed_at": latest.last_refreshed_at,
        "refresh_source": latest.refresh_source,
        "entries": entries,
    }
