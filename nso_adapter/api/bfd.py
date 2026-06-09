# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/bfd — per-interface BFD read mirror."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import BfdIntent, Device, DeviceBfdInterface, DeviceSettings

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


# ---------------------------------------------------------------------------
# PUT /{device_id}/bfd-intent  (BFD write path — deferred apply)
# ---------------------------------------------------------------------------


class BfdEntry(BaseModel):
    interface_name: str
    min_tx: int | None = None
    min_rx: int | None = None
    multiplier: int | None = None
    micro_bfd: bool = False
    accepted_at: datetime | None = None


class BfdIntentUpdate(BaseModel):
    interfaces: list[BfdEntry]


@router.put("/{device_id}/bfd-intent", dependencies=[Depends(verify_token)])
async def put_bfd_intent(device_id: int, body: BfdIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's per-interface BFD intent mirror for this device atomically.

    Full-replace: rows not in the body are deleted. ``accepted_at`` defaults to now.
    If ``auto_apply`` is enabled, an apply job is enqueued; the single device Apply
    commits these via the bfd-reconciler.
    """
    if await db.get(Device, device_id) is None:
        raise api_error(404, "not_found", "Device not found")

    existing = await db.execute(select(BfdIntent).where(BfdIntent.device_id == device_id))
    existing_rows: dict[str, BfdIntent] = {r.interface_name: r for r in existing.scalars().all()}
    new_keys = {i.interface_name for i in body.interfaces}

    for name, row in existing_rows.items():
        if name not in new_keys:
            await db.delete(row)
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    for item in body.interfaces:
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        row = existing_rows.get(item.interface_name)
        if row is None:
            row = BfdIntent(device_id=device_id, interface_name=item.interface_name)
            db.add(row)
        row.min_tx = item.min_tx
        row.min_rx = item.min_rx
        row.multiplier = item.multiplier
        row.micro_bfd = item.micro_bfd
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
    return {"device_id": device_id, "count": count}
