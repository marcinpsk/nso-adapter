# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/interface-mtu — per-interface MTU, read (Phase 2b)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceInterfaceMtu

router = APIRouter(prefix="/api/v1/devices", tags=["interface-mtu"])


@router.get("/{device_id}/interface-mtu", dependencies=[Depends(verify_token)])
async def get_interface_mtu(device_id: int, db: AsyncSession = Depends(get_db)):
    """Return the device's per-interface MTU (mtu / ip-mtu / mpls-mtu + bound-port)."""
    if await db.get(Device, device_id) is None:
        raise api_error(404, "not_found", "Device not found")
    rows = (
        (
            await db.execute(
                select(DeviceInterfaceMtu)
                .where(DeviceInterfaceMtu.device_id == device_id)
                .order_by(DeviceInterfaceMtu.interface_name)
            )
        )
        .scalars()
        .all()
    )
    return {
        "device_id": device_id,
        "interfaces": [
            {
                "interface_name": r.interface_name,
                "mtu": r.mtu,
                "ip_mtu": r.ip_mtu,
                "mpls_mtu": r.mpls_mtu,
                "bound_port": r.bound_port or "",
            }
            for r in rows
        ],
    }
