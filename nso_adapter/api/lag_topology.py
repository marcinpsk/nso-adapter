# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/lag-topology endpoint."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, LagInterface

router = APIRouter(prefix="/api/v1/devices", tags=["lag-topology"])


@router.get("/{device_id}/lag-topology", dependencies=[Depends(verify_token)])
async def get_lag_topology(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    result = await db.execute(
        select(LagInterface)
        .where(LagInterface.device_id == device_id)
        .options(selectinload(LagInterface.members))
    )
    lags = result.scalars().all()

    if not lags:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "lags": [],
        }

    latest = max(lags, key=lambda lag: lag.last_refreshed_at)

    return {
        "device_id": device_id,
        "last_refreshed_at": latest.last_refreshed_at.isoformat() + "Z",
        "refresh_source": latest.refresh_source,
        "lags": [
            {
                "name": lag.name,
                "id": lag.lag_id,
                "members": [
                    {"interface": m.interface_name, "mode": m.mode}
                    for m in lag.members
                ],
            }
            for lag in lags
        ],
    }
