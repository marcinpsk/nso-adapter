# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/interface-ips and PUT /api/v1/devices/{id}/ip-intent endpoints."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import DbInterface, Device, DeviceSettings, InterfaceIpAddress, InterfaceIpIntent

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["interface-ips"])


def _extract_prefix_length(address: str) -> int | None:
    """Extract the numeric prefix length from an 'ip/prefix-length' string."""
    if "/" in address:
        try:
            return int(address.split("/", 1)[1])
        except (ValueError, IndexError):
            pass
    return None


@router.get("/{device_id}/interface-ips", dependencies=[Depends(verify_token)])
async def get_interface_ips(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    result = await db.execute(select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device_id))
    rows = result.scalars().all()

    if not rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "interfaces": [],
        }

    latest = max(rows, key=lambda r: r.last_refreshed_at)

    # Group addresses by interface name; track bound_port per interface.
    by_interface: dict[str, list] = defaultdict(list)
    bound_port_by_iface: dict[str, str | None] = {}
    for row in rows:
        by_interface[row.interface_name].append(
            {
                "address": row.address,
                "prefix_length": _extract_prefix_length(row.address),
                "family": row.family,
                "secondary": row.secondary,
                "vrf": row.vrf,
            }
        )
        if row.interface_name not in bound_port_by_iface:
            bound_port_by_iface[row.interface_name] = row.bound_port

    return {
        "device_id": device_id,
        "last_refreshed_at": latest.last_refreshed_at.isoformat() + "Z",
        "refresh_source": latest.refresh_source,
        "interfaces": [
            {
                "interface": iface_name,
                "bound_port": bound_port_by_iface.get(iface_name),
                "addresses": addrs,
            }
            for iface_name, addrs in sorted(by_interface.items())
        ],
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/ip-intent
# ---------------------------------------------------------------------------


class IpAddressEntry(BaseModel):
    interface: str
    address: str  # "ip/prefix-length" — e.g. "10.0.0.1/24" or "2001:db8::1/64"
    family: str  # "ipv4" | "ipv6"
    secondary: bool = False
    vrf: str = ""  # "" = global/default routing table
    accepted_at: datetime | None = None
    # Greenfield Nokia routed-interface binding: for an operator-created routed
    # sub-interface the adapter never imported, the plugin supplies the SR OS binding so
    # the apply can create `router Base interface <name> port <parent-binding>:<encap-tag>`.
    routed: bool = False  # this is a Nokia routed logical interface (create DbInterface if absent)
    parent_binding: str | None = None  # the bound port/LAG, e.g. "lag-99"
    encap_tag: str | None = None  # dot1q tag, e.g. "99"


class IpIntentUpdate(BaseModel):
    addresses: list[IpAddressEntry]


@router.put("/{device_id}/ip-intent", dependencies=[Depends(verify_token)])
async def put_ip_intent(device_id: int, body: IpIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's IP intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is
    enabled on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    ifaces_result = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
    ifaces = {iface.name: iface for iface in ifaces_result.scalars().all()}

    # Greenfield Nokia routed sub-interface: the operator created it in NetBox, so the
    # adapter never imported a DbInterface for it. Materialise a minimal routed row from the
    # binding the plugin supplied, so the intent FK resolves and the apply emits the SR OS
    # `router Base interface <name> port <parent-binding>:<encap-tag>`. Existing rows missing
    # the binding (legacy import) are backfilled, never clobbered.
    for item in body.addresses:
        if not (item.routed and item.parent_binding):
            continue
        iface = ifaces.get(item.interface)
        if iface is None:
            iface = DbInterface(
                device_id=device_id,
                name=item.interface,
                kind="logical",
                parent_binding=item.parent_binding,
                encap_tag=item.encap_tag,
            )
            db.add(iface)
            ifaces[item.interface] = iface
            logger.info(
                "ip_intent.put.greenfield_interface",
                device_id=device_id,
                interface=item.interface,
                parent_binding=item.parent_binding,
                encap_tag=item.encap_tag,
            )
        else:
            # Backfill binding on an existing (imported) row when missing; never clobber.
            if not iface.parent_binding:
                iface.parent_binding = item.parent_binding
            if not iface.encap_tag and item.encap_tag:
                iface.encap_tag = item.encap_tag
    await db.flush()  # assign ids to freshly-created interfaces before intent rows reference them

    existing_result = await db.execute(
        select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id.in_([i.id for i in ifaces.values()]))
    )
    existing_rows: dict[tuple, InterfaceIpIntent] = {
        (r.interface_id, r.address, r.vrf): r for r in existing_result.scalars().all()
    }

    # Determine which (interface_id, address, vrf) keys are in the new payload.
    new_keys: set[tuple] = set()
    for item in body.addresses:
        iface = ifaces.get(item.interface)
        if iface:
            new_keys.add((iface.id, item.address, item.vrf))

    # Delete rows absent from the new payload.
    for key, row in existing_rows.items():
        if key not in new_keys:
            await db.delete(row)
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    for item in body.addresses:
        iface = ifaces.get(item.interface)
        if iface is None:
            logger.warning("ip_intent.put.unknown_interface", device_id=device_id, interface=item.interface)
            continue
        key = (iface.id, item.address, item.vrf)
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        if key in existing_rows:
            row = existing_rows[key]
            row.accepted_at = accepted
        else:
            row = InterfaceIpIntent(
                interface_id=iface.id,
                address=item.address,
                vrf=item.vrf,
                family=item.family,
                secondary=item.secondary,
                accepted_at=accepted,
            )
            db.add(row)
        count += 1

    await db.flush()

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()
    logger.info("ip_intent.put.ok", device_id=device_id, address_count=count)
    return {"device_id": device_id, "address_count": count, "updated_at": now.isoformat() + "Z"}
