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
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import Device, DeviceLoggingHost, DeviceLoggingLevels

logger = structlog.get_logger(__name__)


async def _upsert_logging_config(db: AsyncSession, device: Device, entry: dict, refresh_source: str) -> None:
    """Full-replace the device's logging hosts + local-levels singleton (the materializer).

    ``entry`` is the whole logging-config payload; ``extract({})`` feeds ``{}`` so the
    authoritative clear runs the same path: no hosts, no local-levels → both wiped.
    """
    # as_list guards the singleton-rendered-as-bare-dict case (was a raw .get → crash).
    hosts = as_list(entry.get("host"))
    await db.execute(delete(DeviceLoggingHost).where(DeviceLoggingHost.device_id == device.id))
    now = datetime.now(UTC)
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

    # local-levels (NX-P4a): a pure device mirror — present iff the export reports ≥1
    # severity (observational presence, not ownership); absent → delete the row.
    levels = entry.get("local-levels") or {}
    row = (
        await db.execute(select(DeviceLoggingLevels).where(DeviceLoggingLevels.device_id == device.id))
    ).scalar_one_or_none()
    if levels:
        if row is None:
            row = DeviceLoggingLevels(device_id=device.id)
            db.add(row)
        row.console_severity = levels.get("console-severity")
        row.monitor_severity = levels.get("monitor-severity")
        row.module_severity = levels.get("module-severity")
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
