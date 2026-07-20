# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Interface MTU read mirror (Phase 2b).

- refresh_interface_mtu_for_device() — read the device's interface-mtu oper-data
  from NSO and full-replace the device_interface_mtu rows.
- handle_interface_mtu_change()      — SSE config-change handler.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.lag_topology import parse_changed_nso_devices
from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import EmptyPolicy
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import Device, DeviceInterfaceMtu

logger = structlog.get_logger(__name__)


def _int_or_none(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def _upsert_interface_mtu(db: AsyncSession, device: Device, interfaces: list[dict], refresh_source: str) -> None:
    """Full-replace the device's interface-MTU rows (the materializer)."""
    now = datetime.now(UTC).replace(tzinfo=None)
    await db.execute(delete(DeviceInterfaceMtu).where(DeviceInterfaceMtu.device_id == device.id))
    for item in interfaces:
        name = item.get("interface-name")
        if not name:
            continue
        db.add(
            DeviceInterfaceMtu(
                device_id=device.id,
                interface_name=name,
                mtu=_int_or_none(item.get("mtu")),
                ip_mtu=_int_or_none(item.get("ip-mtu")),
                mpls_mtu=_int_or_none(item.get("mpls-mtu")),
                bound_port=item.get("bound-port") or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )
    await db.commit()


INTERFACE_MTU_SPEC = FamilySpec(
    name="interface_mtu",
    empty_policy=EmptyPolicy.pop,
    getter=lambda client, name: client.get_interface_mtu(name),
    # as_list guards the singleton-rendered-as-bare-dict case (was a raw .get → crash).
    extract=lambda data: as_list(data.get("interface")),
    materialize=_upsert_interface_mtu,
    wire_name="interface-mtu",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_interface_mtu_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read interface-mtu oper-data for *device* and full-replace its rows (via the shared engine).

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, INTERFACE_MTU_SPEC, refresh_source=refresh_source)


async def handle_interface_mtu_change(
    event_data: dict,
    db: AsyncSession,
    nso_clients: dict[str, NsoClient],
) -> None:
    """Process a NETCONF config-change event and refresh interface-mtu rows."""
    changed = parse_changed_nso_devices(event_data)
    if not changed:
        return
    result = await db.execute(select(Device).where(Device.nso_device_name.in_(changed)))
    for device in result.scalars().all():
        nso_client = nso_clients.get(device.nso_instance)
        if nso_client is None:
            continue
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="notification")
