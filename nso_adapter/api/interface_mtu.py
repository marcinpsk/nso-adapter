# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/interface-mtu — per-interface MTU, read (Phase 2b).

PUT /api/v1/devices/{id}/interface-mtu-intent — write path (Phase 2b).
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceInterfaceMtu, DeviceSettings, InterfaceMtuIntent

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


# ---------------------------------------------------------------------------
# PUT /{device_id}/interface-mtu-intent  (Phase 2b write path)
# ---------------------------------------------------------------------------


class InterfaceMtuEntry(BaseModel):
    interface_name: str
    mtu: int | None = None
    ip_mtu: int | None = None
    mpls_mtu: int | None = None
    accepted_at: datetime | None = None


class InterfaceMtuIntentUpdate(BaseModel):
    interfaces: list[InterfaceMtuEntry]


@router.put("/{device_id}/interface-mtu-intent", dependencies=[Depends(verify_token)])
async def put_interface_mtu_intent(device_id: int, body: InterfaceMtuIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's per-interface MTU intent mirror for this device atomically.

    Full-replace: rows not in the body are deleted (and the device MTU reverted via
    a PUT-replace of the mtu-reconciler service). ``accepted_at`` defaults to now.
    If ``auto_apply`` is enabled on the device, an apply job is enqueued.
    """
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")

    existing = await db.execute(select(InterfaceMtuIntent).where(InterfaceMtuIntent.device_id == device_id))
    existing_rows: dict[str, InterfaceMtuIntent] = {r.interface_name: r for r in existing.scalars().all()}
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
            row = InterfaceMtuIntent(device_id=device_id, interface_name=item.interface_name)
            db.add(row)
        row.mtu = item.mtu
        row.ip_mtu = item.ip_mtu
        row.mpls_mtu = item.mpls_mtu
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
        from nso_adapter.nso.apply import apply_mtu_config

        replaced = await replace_on_removal(db, device, removed, InterfaceMtuIntent, apply_mtu_config)

    return {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
