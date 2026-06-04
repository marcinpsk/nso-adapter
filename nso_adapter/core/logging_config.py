# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Logging/syslog refresh — reads NSO oper-data and upserts the DB (full-replace)."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, DeviceLoggingHost

logger = structlog.get_logger(__name__)


async def _upsert_logging_config(db: AsyncSession, device: Device, entry: dict | None, refresh_source: str) -> None:
    """Full-replace the device's logging hosts from the oper-data *entry*."""
    await db.execute(delete(DeviceLoggingHost).where(DeviceLoggingHost.device_id == device.id))
    now = datetime.now(UTC).replace(tzinfo=None)
    hosts = (entry or {}).get("host", []) if entry else []
    for h in hosts:
        addr = h.get("address")
        if not addr:
            continue
        db.add(
            DeviceLoggingHost(
                device_id=device.id,
                address=addr,
                port=h.get("port"),
                severity=h.get("severity"),
                facility=h.get("facility"),
                transport=h.get("transport"),
                vrf=h.get("vrf"),
                source=h.get("source"),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )
    await db.commit()


async def refresh_logging_config_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read logging oper-data for *device* from NSO and upsert DB rows."""
    if not device.nso_device_name:
        return
    try:
        entry = await nso_client.get_logging_config(device.nso_device_name)
    except Exception as exc:
        logger.warning("logging.refresh.nso_error", device_id=device.id, error=repr(exc))
        return
    await _upsert_logging_config(db, device, entry, refresh_source)
    logger.info(
        "logging.refresh.done",
        device_id=device.id,
        nso_device_name=device.nso_device_name,
        host_count=len((entry or {}).get("host", []) if entry else []),
        source=refresh_source,
    )


async def handle_logging_config_change(db: AsyncSession, nso_device_name: str, nso_client: NsoClient) -> None:
    """SSE hook: refresh logging for the device named *nso_device_name*."""
    result = await db.execute(select(Device).where(Device.nso_device_name == nso_device_name))
    device = result.scalar_one_or_none()
    if device is not None:
        await refresh_logging_config_for_device(db, device, nso_client, refresh_source="sse")
