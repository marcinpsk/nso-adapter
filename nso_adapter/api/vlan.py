# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M34: GET /api/v1/devices/{id}/vlan-database and /switchport."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceSwitchport, DeviceVlan

router = APIRouter(prefix="/api/v1/devices", tags=["vlan"])


@router.get("/{device_id}/vlan-database", dependencies=[Depends(verify_token)])
async def get_vlan_database(device_id: int, db: AsyncSession = Depends(get_db)):
    if await db.get(Device, device_id) is None:
        raise api_error(404, "not_found", "Device not found")
    rows = (
        await db.execute(
            select(DeviceVlan).where(DeviceVlan.device_id == device_id).order_by(DeviceVlan.vlan_id)
        )
    ).scalars().all()
    return {
        "device_id": device_id,
        "vlans": [
            {"vlan_id": r.vlan_id, "name": r.name or "", "source": "vlan-database"} for r in rows
        ],
    }


@router.get("/{device_id}/switchport", dependencies=[Depends(verify_token)])
async def get_switchport(device_id: int, db: AsyncSession = Depends(get_db)):
    if await db.get(Device, device_id) is None:
        raise api_error(404, "not_found", "Device not found")
    rows = (
        await db.execute(
            select(DeviceSwitchport)
            .where(DeviceSwitchport.device_id == device_id)
            .order_by(DeviceSwitchport.interface_name)
            .options(
                selectinload(DeviceSwitchport.untagged_vlan),
                selectinload(DeviceSwitchport.tagged_vlans),
            )
        )
    ).scalars().all()
    return {
        "device_id": device_id,
        "interfaces": [
            {
                "interface_name": r.interface_name,
                "mode": r.mode or "",
                "untagged_vlan": r.untagged_vlan.vlan_id if r.untagged_vlan else None,
                "tagged_vlans": sorted(v.vlan_id for v in r.tagged_vlans),
                "source": "switchport",
            }
            for r in rows
        ],
    }
