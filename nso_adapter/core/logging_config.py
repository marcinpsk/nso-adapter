# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Logging/syslog refresh — reads NSO oper-data and upserts the DB (full-replace)."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, log_skipped_entries, run_family_refresh
from nso_adapter.domain.service_observation import project_logging
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, DeviceLoggingHost, DeviceLoggingLevels

logger = structlog.get_logger(__name__)


async def _upsert_logging_config(db: AsyncSession, device: Device, entry: dict, refresh_source: str) -> None:
    """Full-replace the device's logging hosts + local-levels singleton (the materializer).

    ``entry`` is the whole logging-config payload; ``extract({})`` feeds ``{}`` so the
    authoritative clear runs the same path: no hosts, no local-levels → both wiped.
    """
    document = project_logging(entry)
    log_skipped_entries("logging", device.id, document.unprojectable)
    await db.execute(delete(DeviceLoggingHost).where(DeviceLoggingHost.device_id == device.id))
    now = datetime.now(UTC)
    for h in document.hosts or []:
        db.add(
            DeviceLoggingHost(
                device_id=device.id,
                address=h.address,
                port=h.port,
                severity=h.severity,
                facility=h.facility,
                transport=h.transport,
                vrf=h.vrf,
                source=h.source,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    levels = document.local_levels
    row = (
        await db.execute(select(DeviceLoggingLevels).where(DeviceLoggingLevels.device_id == device.id))
    ).scalar_one_or_none()
    if levels is not None and levels.present:
        if row is None:
            row = DeviceLoggingLevels(device_id=device.id)
            db.add(row)
        row.console_severity = levels.console_severity
        row.monitor_severity = levels.monitor_severity
        row.module_severity = levels.module_severity
        row.last_refreshed_at = now
        row.refresh_source = refresh_source
    elif row is not None:
        await db.delete(row)


LOGGING_CONFIG_SPEC = FamilySpec(
    name="logging",
    # Whole-entry payload: the materializer destructures host + local-levels itself
    # (extract({}) == {} is the authoritative-clear "nothing" payload).
    extract=lambda data: data,
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
