# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/svi — L3 VLAN interfaces (SVIs / IRBs), read (M35)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceSvi

router = APIRouter(prefix="/api/v1/devices", tags=["svi"])


@router.get("/{device_id}/svi", dependencies=[Depends(verify_token)])
async def get_svi(device_id: int, db: AsyncSession = Depends(get_db)):
    """Return the device's SVIs/IRBs (no IPs — those ride interface-ip)."""
    if await db.get(Device, device_id) is None:
        raise api_error(404, "not_found", "Device not found")
    rows = (
        (await db.execute(select(DeviceSvi).where(DeviceSvi.device_id == device_id).order_by(DeviceSvi.vlan_id)))
        .scalars()
        .all()
    )
    return {
        "device_id": device_id,
        "interfaces": [
            {
                "interface_name": r.interface_name,
                "vlan_id": r.vlan_id,
                "type": r.svi_type,
                "vrf": r.vrf or "",
                "source": "svi",
            }
            for r in rows
        ],
    }
