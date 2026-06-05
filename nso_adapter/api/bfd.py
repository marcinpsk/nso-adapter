# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/bfd — per-interface BFD read mirror."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceBfdInterface

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["bfd"])


@router.get("/{device_id}/bfd", dependencies=[Depends(verify_token)])
async def get_bfd(device_id: int, db: AsyncSession = Depends(get_db)):
    """Return the per-interface BFD read-mirror for this device."""
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    rows = (
        (
            await db.execute(
                select(DeviceBfdInterface)
                .where(DeviceBfdInterface.device_id == device_id)
                .order_by(DeviceBfdInterface.interface_name)
            )
        )
        .scalars()
        .all()
    )

    latest = max((r.last_refreshed_at for r in rows if r.last_refreshed_at), default=None)
    interfaces = []
    for r in rows:
        entry: dict = {
            "interface_name": r.interface_name,
            "micro_bfd": r.micro_bfd,
            "enabled": r.enabled,
        }
        if r.bound_port is not None:
            entry["bound_port"] = r.bound_port
        if r.min_tx is not None:
            entry["min_tx"] = r.min_tx
        if r.min_rx is not None:
            entry["min_rx"] = r.min_rx
        if r.multiplier is not None:
            entry["multiplier"] = r.multiplier
        interfaces.append(entry)

    return {
        "device_id": device_id,
        "last_refreshed_at": latest.isoformat() + "Z" if latest else None,
        "refresh_source": rows[0].refresh_source if rows else "never",
        "interfaces": interfaces,
    }
