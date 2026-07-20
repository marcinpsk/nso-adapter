# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Logging/syslog refresh — reads NSO oper-data and upserts the DB (full-replace)."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import EmptyPolicy
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import Device, DeviceLoggingHost

logger = structlog.get_logger(__name__)


async def _upsert_logging_config(db: AsyncSession, device: Device, hosts: list[dict], refresh_source: str) -> None:
    """Full-replace the device's logging hosts (the materializer)."""
    await db.execute(delete(DeviceLoggingHost).where(DeviceLoggingHost.device_id == device.id))
    now = datetime.now(UTC).replace(tzinfo=None)
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


LOGGING_CONFIG_SPEC = FamilySpec(
    name="logging",
    empty_policy=EmptyPolicy.pop,
    getter=lambda client, name: client.get_logging_config(name),
    # as_list guards the singleton-rendered-as-bare-dict case (was a raw .get → crash).
    extract=lambda data: as_list(data.get("host")),
    materialize=_upsert_logging_config,
    wire_name="logging-config",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_logging_config_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read logging oper-data for *device* from NSO and upsert DB rows (via the shared engine).

    Returns True on a successful read (or nothing to read); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, LOGGING_CONFIG_SPEC, refresh_source=refresh_source)


async def handle_logging_config_change(db: AsyncSession, nso_device_name: str, nso_client: NsoClient) -> None:
    """SSE hook: refresh logging for the device named *nso_device_name*."""
    result = await db.execute(select(Device).where(Device.nso_device_name == nso_device_name))
    device = result.scalar_one_or_none()
    if device is not None:
        await refresh_logging_config_for_device(db, device, nso_client, refresh_source="sse")
