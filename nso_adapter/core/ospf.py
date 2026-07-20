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

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import EmptyPolicy
from nso_adapter.nso.shape import as_list
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
        process_id = inst.get("process-id")
        if not process_id:
            continue  # malformed instance with no process-id → nothing to key on; skip
        db.add(
            DeviceOspfInstance(
                device_id=device.id,
                process_id=process_id,
                router_id=inst.get("router-id"),
                vrf=inst.get("vrf", ""),
                areas=as_list(inst.get("area")),
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


async def _materialize_ospf(db: AsyncSession, device: Device, entry: dict, refresh_source: str) -> None:
    """Engine materializer: destructure the OSPF entry into its two lists and full-replace.

    ``as_list`` normalizes the RESTCONF singleton-as-bare-object case for the ``instance`` and
    ``interface`` YANG lists. An empty entry ``{}`` (the AbsentAuthoritative clear) yields two
    empty lists → clear.
    """
    await _upsert_ospf_data(
        db,
        device,
        as_list(entry.get("instance")),
        as_list(entry.get("interface")),
        refresh_source,
    )


OSPF_SPEC = FamilySpec(
    name="ospf",
    empty_policy=EmptyPolicy.pop,  # config family: a container-confirmed 404 is an authoritative clear
    getter=lambda client, name: client.get_ospf(name),
    extract=lambda data: data,
    materialize=_materialize_ospf,
)


async def refresh_ospf_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read OSPF oper-data for *device* from NSO and upsert DB rows (via the shared refresh engine).

    Returns True on a successful read (or nothing to read); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, OSPF_SPEC, refresh_source=refresh_source)


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
