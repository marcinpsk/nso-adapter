# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Interface IP address refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_interface_ips_for_device() — called on-demand by SSE handler or scheduler
- handle_interface_ip_change() — called by the SSE on_event handler
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.lag_topology import parse_changed_nso_devices
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, InterfaceIpAddress

logger = structlog.get_logger(__name__)


async def _upsert_ip_addresses(
    db: AsyncSession,
    device: Device,
    interfaces_data: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing rows for *device*, then insert fresh ones."""
    await db.execute(delete(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)
    for iface in interfaces_data:
        iface_name = iface.get("interface-name", "")
        bound_port = iface.get("bound-port") or None  # None for non-Nokia or unbound
        for addr_entry in iface.get("address", []):
            address = addr_entry.get("address", "")
            # `or default` (not `.get(k, default)`): the export can carry an explicit null,
            # and None would violate these NOT-NULL columns (vrf is also in the dedup unique).
            vrf = addr_entry.get("vrf") or ""
            family = addr_entry.get("family") or "ipv4"
            secondary = addr_entry.get("secondary", False)
            if not address:
                continue
            db.add(
                InterfaceIpAddress(
                    device_id=device.id,
                    interface_name=iface_name,
                    address=address,
                    vrf=vrf,
                    family=family,
                    secondary=bool(secondary),
                    bound_port=bound_port,
                    last_refreshed_at=now,
                    refresh_source=refresh_source,
                )
            )
    await db.commit()


async def refresh_interface_ips_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read oper-data for *device* from NSO and upsert DB rows."""
    if not device.nso_device_name:
        logger.debug("interface_ip.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return

    try:
        entry = await nso_client.get_interface_ips(device.nso_device_name)
    except Exception as exc:
        logger.warning("interface_ip.refresh.nso_error", device_id=device.id, error=repr(exc))
        return

    interfaces_data = entry.get("interface", []) if entry else []
    await _upsert_ip_addresses(db, device, interfaces_data, refresh_source)
    total_addrs = sum(len(iface.get("address", [])) for iface in interfaces_data)
    logger.info(
        "interface_ip.refresh.done",
        device_id=device.id,
        nso_device_name=device.nso_device_name,
        interface_count=len(interfaces_data),
        address_count=total_addrs,
        source=refresh_source,
    )


async def handle_interface_ip_change(
    event_data: dict,
    db: AsyncSession,
    nso_clients: dict[str, NsoClient],
) -> None:
    """Process a NETCONF config-change event and refresh interface IP addresses."""
    changed = parse_changed_nso_devices(event_data)
    if not changed:
        return

    result = await db.execute(select(Device).where(Device.nso_device_name.in_(changed)))
    devices = result.scalars().all()

    for device in devices:
        nso_client = nso_clients.get(device.nso_instance)
        if nso_client is None:
            logger.debug(
                "interface_ip.event.no_client",
                device_id=device.id,
                instance=device.nso_instance,
            )
            continue
        await refresh_interface_ips_for_device(db, device, nso_client, refresh_source="notification")
