# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M34: GET /api/v1/devices/{id}/vlan-database and /switchport."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datetime import UTC, datetime

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.core.importer import get_nso_client
from nso_adapter.core.switchport_intent import apply_switchport_config as apply_switchport_core
from nso_adapter.store.models import Device, DeviceSettings, DeviceSwitchport, DeviceVlan, VlanIntent

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


# ---------------------------------------------------------------------------
# PUT /{device_id}/vlan-intent  (M34 VLAN-database write path — deferred apply)
# ---------------------------------------------------------------------------


class VlanEntry(BaseModel):
    vlan_id: int
    name: str = ""
    accepted_at: datetime | None = None


class VlanIntentUpdate(BaseModel):
    vlans: list[VlanEntry]


@router.put("/{device_id}/vlan-intent", dependencies=[Depends(verify_token)])
async def put_vlan_intent(device_id: int, body: VlanIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's VLAN-database intent mirror for this device atomically.

    Full-replace: rows not in the body are deleted. ``accepted_at`` defaults to now.
    If ``auto_apply`` is enabled on the device, an apply job is enqueued. The single
    device Apply commits these via the vlan-reconciler.
    """
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")

    existing = await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))
    existing_rows: dict[int, VlanIntent] = {r.vlan_id: r for r in existing.scalars().all()}
    new_keys = {v.vlan_id for v in body.vlans}

    removed_vids = [vid for vid in existing_rows if vid not in new_keys]
    for vid in removed_vids:
        await db.delete(existing_rows[vid])
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    for item in body.vlans:
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        row = existing_rows.get(item.vlan_id)
        if row is None:
            row = VlanIntent(device_id=device_id, vlan_id=item.vlan_id)
            db.add(row)
        row.name = item.name or None
        row.accepted_at = accepted
        count += 1

    await db.flush()
    settings = (
        await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    ).scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()

    # Removal propagation: a dropped vid won't be removed by the next merge-PATCH
    # apply, so PUT-replace the vlan-reconciler instance with the full desired list
    # (remaining rows) → FASTMAP reverts the removed VLAN on the device.
    replaced = False
    if removed_vids:
        from nso_adapter.nso.apply import replace_vlan_config

        remaining = (
            await db.execute(
                select(VlanIntent).where(
                    VlanIntent.device_id == device_id, VlanIntent.accepted_at.is_not(None)
                )
            )
        ).scalars().all()
        try:
            nso_client = get_nso_client(device.nso_instance)
            await replace_vlan_config(nso_client, device.nso_device_name, remaining)
            replaced = True
        except Exception as exc:  # noqa: BLE001
            import structlog

            structlog.get_logger(__name__).error(
                "vlan_intent.replace_failed", device_id=device_id, error=repr(exc)
            )

    return {"device_id": device_id, "count": count, "removed": len(removed_vids), "replaced": replaced}
