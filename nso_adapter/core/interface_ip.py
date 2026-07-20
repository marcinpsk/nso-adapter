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
from nso_adapter.nso.shape import as_list
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
        for addr_entry in as_list(iface.get("address")):
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
) -> bool:
    """Read oper-data for *device* from NSO and upsert DB rows.

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    if not device.nso_device_name:
        logger.debug("interface_ip.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return True

    try:
        entry = await nso_client.get_interface_ips(device.nso_device_name)
    except Exception as exc:
        logger.warning("interface_ip.refresh.nso_error", device_id=device.id, error=repr(exc))
        return False

    if entry is None:
        # B2 (F5): the client returns None on a 404, which is NOT an authoritative "this device has
        # no IPs" — it means the export isn't serving this device (unsupported NED / not-ready /
        # absent). A genuinely IP-less but *synced* device returns a PRESENT entry with an empty
        # `interface` list (200 — see network-state-export ips.py `_refresh_device`), so a real
        # total-clear still wipes below. Keep the last-known rows rather than committing an empty
        # mirror over a transient/absent read (the onboarding empty-wipe class).
        #
        # Contract note — do NOT copy this keep-on-None to the other refreshers. interface_ip
        # (and interface-attrs) are PRESENT-EMPTY "inventory" families: a synced device always
        # returns 200, so None means only unsupported-NED and keeping is safe. Every *config*
        # family (snmp, lag, svi, subinterface, vlan, static-route, isis, ospf, bgp, bfd, mtu,
        # logging, route-policy, ...) is POP-ON-EMPTY by deliberate export design: a synced-but-
        # empty device 404s so the adapter CLEARS. There, None is authoritative and keep-on-None
        # would strand legitimately-removed rows — clear-on-None is correct (see core/snmp.py).
        logger.info(
            "interface_ip.refresh.empty_suspect",
            device_id=device.id,
            nso_device_name=device.nso_device_name,
            reason="nso_returned_none",
        )
        return True

    interfaces_data = as_list(entry.get("interface"))
    await _upsert_ip_addresses(db, device, interfaces_data, refresh_source)
    total_addrs = sum(len(as_list(iface.get("address"))) for iface in interfaces_data)
    logger.info(
        "interface_ip.refresh.done",
        device_id=device.id,
        nso_device_name=device.nso_device_name,
        interface_count=len(interfaces_data),
        address_count=total_addrs,
        source=refresh_source,
    )
    return True


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
