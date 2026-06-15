# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/subinterface — dot1q L3 subinterfaces, read (M36).

PUT /api/v1/devices/{id}/subinterface-intent — write path (M36).
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceSettings, DeviceSubinterface, SubinterfaceIntent

router = APIRouter(prefix="/api/v1/devices", tags=["subinterface"])


@router.get("/{device_id}/subinterface", dependencies=[Depends(verify_token)])
async def get_subinterface(device_id: int, db: AsyncSession = Depends(get_db)):
    """Return the device's dot1q subinterfaces (no IPs — those ride interface-ip)."""
    if await db.get(Device, device_id) is None:
        raise api_error(404, "not_found", "Device not found")
    rows = (
        (
            await db.execute(
                select(DeviceSubinterface)
                .where(DeviceSubinterface.device_id == device_id)
                .order_by(DeviceSubinterface.interface_name)
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
                "parent_interface": r.parent_interface,
                "dot1q_vlan": r.dot1q_vlan,
                "type": r.sub_type,
                "vrf": r.vrf or "",
                "source": "subinterface",
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/subinterface-intent  (M36 write path)
# ---------------------------------------------------------------------------


class SubinterfaceEntry(BaseModel):
    interface_name: str
    parent_interface: str = ""
    dot1q_vlan: int | None = None
    type: str = "subinterface"
    vrf: str = ""
    accepted_at: datetime | None = None


class SubinterfaceIntentUpdate(BaseModel):
    interfaces: list[SubinterfaceEntry]


@router.put("/{device_id}/subinterface-intent", dependencies=[Depends(verify_token)])
async def put_subinterface_intent(device_id: int, body: SubinterfaceIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's subinterface intent mirror for this device atomically.

    Full-replace: rows not in the body are deleted. ``accepted_at`` defaults to now.
    If ``auto_apply`` is enabled on the device, an apply job is enqueued.
    """
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")

    existing = await db.execute(select(SubinterfaceIntent).where(SubinterfaceIntent.device_id == device_id))
    existing_rows: dict[str, SubinterfaceIntent] = {r.interface_name: r for r in existing.scalars().all()}
    new_keys = {i.interface_name for i in body.interfaces}

    removed = [name for name in existing_rows if name not in new_keys]
    for name in removed:
        await db.delete(existing_rows[name])
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    for item in body.interfaces:
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        row = existing_rows.get(item.interface_name)
        if row is None:
            row = SubinterfaceIntent(device_id=device_id, interface_name=item.interface_name)
            db.add(row)
        row.parent_interface = item.parent_interface or None
        row.dot1q_vlan = item.dot1q_vlan
        row.sub_type = item.type
        row.vrf = item.vrf or None
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

    replaced = False
    if removed:
        from nso_adapter.core.removal import replace_on_removal
        from nso_adapter.nso.apply import apply_subinterface_config

        replaced = await replace_on_removal(db, device, removed, SubinterfaceIntent, apply_subinterface_config)

    return {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
