# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Remote syslog servers: GET /logging-config (read) + PUT /logging-intent (write)."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceLoggingHost, DeviceSettings, LoggingHostIntent

router = APIRouter(prefix="/api/v1/devices", tags=["logging-config"])


@router.get("/{device_id}/logging-config", dependencies=[Depends(verify_token)])
async def get_logging_config(device_id: int, db: AsyncSession = Depends(get_db)):
    """Return the device's remote syslog servers."""
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    rows = (
        (await db.execute(select(DeviceLoggingHost).where(DeviceLoggingHost.device_id == device_id).order_by(DeviceLoggingHost.address)))
        .scalars()
        .all()
    )
    if not rows:
        return {"device_id": device_id, "last_refreshed_at": None, "refresh_source": "never", "hosts": []}

    latest = max(rows, key=lambda r: r.last_refreshed_at or "")
    hosts = []
    for r in rows:
        entry: dict = {"address": r.address}
        for attr in ("port", "severity", "facility", "transport", "vrf", "source"):
            val = getattr(r, attr)
            if val is not None:
                entry[attr] = val
        hosts.append(entry)
    ts = latest.last_refreshed_at
    return {
        "device_id": device_id,
        "last_refreshed_at": ts.isoformat() + "Z" if ts else None,
        "refresh_source": latest.refresh_source,
        "hosts": hosts,
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/logging-intent
# ---------------------------------------------------------------------------


class LoggingHostEntry(BaseModel):
    address: str
    port: int | None = None
    severity: str = ""
    facility: str = ""
    transport: str = ""
    vrf: str = ""
    source: str = ""
    accepted_at: datetime | None = None


class LoggingIntentUpdate(BaseModel):
    hosts: list[LoggingHostEntry]


@router.put("/{device_id}/logging-intent", dependencies=[Depends(verify_token)])
async def put_logging_intent(device_id: int, body: LoggingIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's remote-syslog intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied. If ``auto_apply`` is enabled
    on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    existing_result = await db.execute(select(LoggingHostIntent).where(LoggingHostIntent.device_id == device_id))
    existing_rows: dict[str, LoggingHostIntent] = {r.address: r for r in existing_result.scalars().all()}
    new_keys = {item.address for item in body.hosts}

    for addr, row in existing_rows.items():
        if addr not in new_keys:
            await db.delete(row)
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    for item in body.hosts:
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        row = existing_rows.get(item.address)
        if row is None:
            row = LoggingHostIntent(device_id=device_id, address=item.address)
            db.add(row)
        row.port = item.port
        row.severity = item.severity
        row.facility = item.facility
        row.transport = item.transport
        row.vrf = item.vrf
        row.source = item.source
        row.accepted_at = accepted
        count += 1

    await db.flush()

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()
    return {"device_id": device_id, "count": count}
