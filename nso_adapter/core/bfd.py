# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Per-interface BFD refresh — reads NSO bfd-config oper-data and upserts the DB."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, DeviceBfdInterface

logger = structlog.get_logger(__name__)


async def _upsert_bfd_data(
    db: AsyncSession,
    device: Device,
    interfaces: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing BFD-interface rows for *device*, then insert fresh."""
    await db.execute(delete(DeviceBfdInterface).where(DeviceBfdInterface.device_id == device.id))
    now = datetime.now(UTC).replace(tzinfo=None)
    seen: set[str] = set()
    for iface in interfaces:
        name = iface.get("interface-name", "")
        if not name or name in seen:
            continue
        seen.add(name)
        db.add(
            DeviceBfdInterface(
                device_id=device.id,
                interface_name=name,
                bound_port=iface.get("bound-port") or None,
                min_tx=iface.get("min-tx"),
                min_rx=iface.get("min-rx"),
                multiplier=iface.get("multiplier"),
                micro_bfd=bool(iface.get("micro-bfd", False)),
                enabled=bool(iface.get("enabled", True)),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )
    await db.commit()


async def refresh_bfd_interfaces_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read BFD oper-data for *device* from NSO and upsert DB rows."""
    if not device.nso_device_name:
        logger.debug("bfd.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return
    try:
        entry = await nso_client.get_bfd_config(device.nso_device_name)
    except Exception as exc:
        logger.warning("bfd.refresh.nso_error", device_id=device.id, error=repr(exc))
        return
    interfaces = entry.get("interface", []) if entry else []
    await _upsert_bfd_data(db, device, interfaces, refresh_source)
    logger.info(
        "bfd.refresh.done",
        device_id=device.id,
        device_name=device.nso_device_name,
        interface_count=len(interfaces),
        refresh_source=refresh_source,
    )
