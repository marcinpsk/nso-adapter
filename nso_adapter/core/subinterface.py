# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""dot1q subinterface read mirror.

- refresh_subinterface_for_device() — read the device's subinterface oper-data
  from NSO and full-replace the device_subinterface rows.
- handle_subinterface_change()      — SSE config-change handler.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import EmptyPolicy
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import Device, DeviceSubinterface

logger = structlog.get_logger(__name__)


async def _upsert_subinterface(db: AsyncSession, device: Device, interfaces: list[dict], refresh_source: str) -> None:
    """Full-replace the device's dot1q subinterface rows (the materializer)."""
    now = datetime.now(UTC).replace(tzinfo=None)
    await db.execute(delete(DeviceSubinterface).where(DeviceSubinterface.device_id == device.id))
    for item in interfaces:
        name = item.get("interface-name")
        if not name:
            continue
        dot1q = item.get("dot1q-vlan")
        db.add(
            DeviceSubinterface(
                device_id=device.id,
                interface_name=name,
                parent_interface=item.get("parent-interface") or None,
                dot1q_vlan=int(dot1q) if dot1q is not None else None,
                sub_type=item.get("type") or "subinterface",
                vrf=item.get("vrf") or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )
    await db.commit()


SUBINTERFACE_SPEC = FamilySpec(
    name="subinterface",
    empty_policy=EmptyPolicy.pop,
    getter=lambda client, name: client.get_subinterface(name),
    extract=lambda data: as_list(data.get("interface")),
    materialize=_upsert_subinterface,
    wire_name="subinterface",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_subinterface_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read subinterface oper-data for *device* and full-replace its rows (via the shared engine).

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, SUBINTERFACE_SPEC, refresh_source=refresh_source)
