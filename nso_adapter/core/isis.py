# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""IS-IS interface refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_isis_interfaces_for_device() — called on-demand by scheduler
- handle_isis_interface_change()       — placeholder for future SSE hook
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, DeviceIsisInterface, DeviceIsisProcess

logger = structlog.get_logger(__name__)


async def _upsert_isis_data(
    db: AsyncSession,
    device: Device,
    processes: list[dict],
    interfaces: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing IS-IS rows for *device*, then insert fresh ones."""
    await db.execute(delete(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id))
    await db.execute(delete(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)

    for proc in processes:
        db.add(
            DeviceIsisProcess(
                device_id=device.id,
                process_tag=proc.get("process-tag", ""),
                net=proc.get("net"),
                is_type=proc.get("is-type"),
                metric_style=proc.get("metric-style"),
                overload_bit=proc.get("overload-bit"),
                area_auth_type=proc.get("area-auth-type"),
                area_auth_present=proc.get("area-auth-present"),
                area_auth_key=proc.get("area-auth-key"),
                domain_auth_type=proc.get("domain-auth-type"),
                domain_auth_present=proc.get("domain-auth-present"),
                domain_auth_key=proc.get("domain-auth-key"),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    for iface in interfaces:
        iface_name = iface.get("interface-name", "")
        af = iface.get("af", "")
        if not iface_name or not af:
            continue
        db.add(
            DeviceIsisInterface(
                device_id=device.id,
                interface_name=iface_name,
                af=af,
                process_tag=iface.get("process-tag", ""),
                circuit_type=iface.get("circuit-type"),
                network_type=iface.get("network-type"),
                metric=iface.get("metric"),
                passive=bool(iface.get("passive", False)),
                bound_port=iface.get("bound-port") or None,
                hello_auth_type=iface.get("hello-auth-type") or None,
                hello_auth_present=iface.get("hello-auth-present"),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    await db.commit()


async def refresh_isis_interfaces_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read IS-IS oper-data for *device* from NSO and upsert DB rows."""
    if not device.nso_device_name:
        logger.debug("isis.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return

    try:
        entry = await nso_client.get_isis_interfaces(device.nso_device_name)
    except Exception as exc:
        logger.warning("isis.refresh.nso_error", device_id=device.id, error=repr(exc))
        return

    processes = entry.get("process", []) if entry else []
    interfaces = entry.get("interface", []) if entry else []
    await _upsert_isis_data(db, device, processes, interfaces, refresh_source)
    logger.info(
        "isis.refresh.done",
        device_id=device.id,
        device_name=device.nso_device_name,
        process_count=len(processes),
        interface_count=len(interfaces),
        refresh_source=refresh_source,
    )


async def handle_isis_interface_change(
    db: AsyncSession,
    nso_device_name: str,
    nso_client: NsoClient,
) -> None:
    """Handle an SSE notification that IS-IS config changed for *nso_device_name*.

    Finds the Device row, then delegates to refresh_isis_interfaces_for_device.
    """
    result = await db.execute(select(Device).where(Device.nso_device_name == nso_device_name))
    device = result.scalar_one_or_none()
    if device is None:
        logger.debug("isis.sse.unknown_device", nso_device_name=nso_device_name)
        return

    await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="sse")
