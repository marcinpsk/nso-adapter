# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/logging-config — remote syslog servers (read)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceLoggingHost

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
