# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M34: GET /api/v1/devices/{id}/vlan-database and /switchport."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.core.importer import get_nso_client
from nso_adapter.core.switchport_intent import apply_switchport_config as apply_switchport_core
from nso_adapter.store.models import Device, DeviceSwitchport, DeviceVlan

router = APIRouter(prefix="/api/v1/devices", tags=["vlan"])


class SwitchportApply(BaseModel):
    interface_name: str
    mode: str | None = None
    untagged_vlan: int | None = None
    tagged_vlans: list[int] = []


class SwitchportApplyRequest(BaseModel):
    interfaces: list[SwitchportApply] = []


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


@router.post("/{device_id}/switchport/apply", dependencies=[Depends(verify_token)])
async def apply_switchport(
    device_id: int,
    payload: SwitchportApplyRequest,
    db: AsyncSession = Depends(get_db),
):
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")
    nso_client = get_nso_client(device.nso_instance)
    return await apply_switchport_core(device, payload, nso_client)
