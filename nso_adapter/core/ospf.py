# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""OSPF refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_ospf_for_device() — called on-demand by scheduler
- handle_ospf_change()      — placeholder for future SSE hook
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, DeviceOspfInstance, DeviceOspfInterface

logger = structlog.get_logger(__name__)


async def _upsert_ospf_data(
    db: AsyncSession,
    device: Device,
    instances: list[dict],
    interfaces: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing OSPF rows for *device*, then insert fresh ones."""
    await db.execute(delete(DeviceOspfInstance).where(DeviceOspfInstance.device_id == device.id))
    await db.execute(delete(DeviceOspfInterface).where(DeviceOspfInterface.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)

    for inst in instances:
        db.add(
            DeviceOspfInstance(
                device_id=device.id,
                process_id=inst["process-id"],
                router_id=inst.get("router-id"),
                vrf=inst.get("vrf", ""),
                areas=inst.get("area", []),
                enabled=inst.get("enabled"),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    for iface in interfaces:
        iface_name = iface.get("interface-name", "")
        if not iface_name:
            continue
        db.add(
            DeviceOspfInterface(
                device_id=device.id,
                interface_name=iface_name,
                process_id=iface.get("process-id"),
                area_id=iface.get("area-id"),
                passive=bool(iface.get("passive", False)),
                priority=iface.get("priority"),
                cost=iface.get("cost"),
                network_type=iface.get("network-type"),
                auth_type=iface.get("auth-type"),
                auth_present=bool(iface.get("auth-present", False)),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    await db.commit()


async def refresh_ospf_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read OSPF oper-data for *device* from NSO and upsert DB rows."""
    if not device.nso_device_name:
        logger.debug("ospf.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return

    try:
        entry = await nso_client.get_ospf(device.nso_device_name)
    except Exception as exc:
        logger.warning("ospf.refresh.nso_error", device_id=device.id, error=repr(exc))
        return

    instances = entry.get("instance", []) if entry else []
    interfaces = entry.get("interface", []) if entry else []
    await _upsert_ospf_data(db, device, instances, interfaces, refresh_source)
    logger.info(
        "ospf.refresh.done",
        device_id=device.id,
        device_name=device.nso_device_name,
        instance_count=len(instances),
        interface_count=len(interfaces),
        refresh_source=refresh_source,
    )


async def handle_ospf_change(
    db: AsyncSession,
    nso_device_name: str,
    nso_client: NsoClient,
) -> None:
    """Handle an SSE notification that OSPF config changed for *nso_device_name*.

    Finds the Device row, then delegates to refresh_ospf_for_device.
    """
    result = await db.execute(select(Device).where(Device.nso_device_name == nso_device_name))
    device = result.scalar_one_or_none()
    if device is None:
        logger.debug("ospf.sse.unknown_device", nso_device_name=nso_device_name)
        return

    await refresh_ospf_for_device(db, device, nso_client, refresh_source="sse")
