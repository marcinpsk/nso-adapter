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
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.domain.observation import project_interface_ips
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

    now = datetime.now(UTC)
    projection = project_interface_ips({"interface": interfaces_data})
    for invalid in projection.unprojectable:
        logger.warning("interface_ip.entry_skipped", device_id=device.id, index=invalid.index, reason=invalid.reason)
    for iface in projection.interfaces or []:
        for address in iface.addresses or []:
            db.add(
                InterfaceIpAddress(
                    device_id=device.id,
                    interface_name=iface.interface,
                    address=address.address,
                    vrf=address.vrf,
                    family=address.family,
                    secondary=address.secondary,
                    bound_port=iface.bound_port,
                    last_refreshed_at=now,
                    refresh_source=refresh_source,
                )
            )


# interface_ip is a PRESENT-EMPTY "inventory" family: the `status` leaf and the coverage of
# `interface` and every `address` decide (ok + covered full-replaces, even to empty;
# unsupported/error/not covered keep the mirror).
INTERFACE_IP_SPEC = FamilySpec(
    name="interface_ip",
    extract=lambda data: data["interface"],
    materialize=_upsert_ip_addresses,
    wire_name="interface-ip",  # READSEM S3: fetch from the device-state envelope
    requires=("interface", "interface/address"),
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
