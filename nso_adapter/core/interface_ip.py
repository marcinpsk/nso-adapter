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
from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import EmptyPolicy
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


# interface_ip is a keep-on-None PRESENT-EMPTY "inventory" family: a synced device always
# returns a 200 (a present entry, empty `interface: []` when it genuinely has no addresses —
# see network-state-export ips.py `_refresh_device`), so a client `None` (404) means only
# unsupported-NED / unknown / not-ready and must NOT wipe the mirror. That semantics is now
# DECLARED as EmptyPolicy.present (the engine keeps rows + reports success on that 404, and
# still full-replaces to empty on a real present-empty read) — no longer per-file prose.
INTERFACE_IP_SPEC = FamilySpec(
    name="interface_ip",
    empty_policy=EmptyPolicy.present,
    getter=lambda client, name: client.get_interface_ips(name),
    extract=lambda data: as_list(data.get("interface")),
    materialize=_upsert_ip_addresses,
    wire_name="interface-ip",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_interface_ips_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read oper-data for *device* from NSO and upsert DB rows (via the shared refresh engine).

    Returns True on a successful read (including a non-authoritative 404 / intentional skip);
    False when the NSO read failed and the last-known rows were left untouched (degraded).
    """
    return await run_family_refresh(db, device, nso_client, INTERFACE_IP_SPEC, refresh_source=refresh_source)


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
