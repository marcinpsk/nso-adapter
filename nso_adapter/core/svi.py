# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""SVI/IRB read mirror.

- refresh_svi_for_device() — read the device's svi oper-data from NSO and
  full-replace the device_svi rows.
- handle_svi_change()       — SSE config-change handler.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import Device, DeviceSvi

logger = structlog.get_logger(__name__)


async def _upsert_svi(db: AsyncSession, device: Device, interfaces: list[dict], refresh_source: str) -> None:
    """Full-replace the device's SVI/IRB rows (the materializer)."""
    now = datetime.now(UTC).replace(tzinfo=None)
    await db.execute(delete(DeviceSvi).where(DeviceSvi.device_id == device.id))
    for item in interfaces:
        name = item.get("interface-name")
        if not name:
            continue
        db.add(
            DeviceSvi(
                device_id=device.id,
                interface_name=name,
                vlan_id=int(item.get("vlan-id") or 0),
                svi_type=item.get("type") or "svi",
                vrf=item.get("vrf") or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )
    await db.commit()


SVI_SPEC = FamilySpec(
    name="svi",
    extract=lambda data: as_list(data.get("interface")),
    materialize=_upsert_svi,
    wire_name="svi",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_svi_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read svi oper-data for *device* and full-replace its device_svi rows (via the shared engine).

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, SVI_SPEC, refresh_source=refresh_source)
