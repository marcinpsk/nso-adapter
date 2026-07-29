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

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, api_error
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import iso_z
from nso_adapter.store import outcome_store
from nso_adapter.store.models import DbInterface, Device, DeviceSettings, InterfaceIpAddress, InterfaceIpIntent

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["interface-ips"])


# ── Read-mirror response models (GET /interface-ips) ──────────────────────────
# Fixed/EMIT-NULL shape: bound_port and each address's prefix_length are always
# present (null when absent), so this endpoint does NOT use exclude_unset.


class InterfaceIpAddressOut(BaseModel):
    address: str
    prefix_length: int | None
    family: str
    secondary: bool
    vrf: str


class InterfaceIpEntryOut(BaseModel):
    interface: str
    bound_port: str | None
    addresses: list[InterfaceIpAddressOut]


class InterfaceIpsOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # reader formats "<iso>Z"; None when never refreshed
    refresh_source: str  # legacy freshness (S5 retires it); read_state is the S4 truth
    read_state: FamilyReadState
    interfaces: list[InterfaceIpEntryOut]


def _extract_prefix_length(address: str) -> int | None:
    """Extract the numeric prefix length from an 'ip/prefix-length' string."""
    if "/" in address:
        try:
            return int(address.split("/", 1)[1])
        except (ValueError, IndexError):
            pass
    return None


@router.get(
    "/{device_id}/interface-ips",
    dependencies=[Depends(verify_token)],
    response_model=InterfaceIpsOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_interface_ips(device_id: int, db: AsyncSession = Depends(get_read_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "interface_ip"), source_epoch=device.source_epoch
    )

    result = await db.execute(select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device_id))
    rows = result.scalars().all()

    if not rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "read_state": read_state,
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
        "last_refreshed_at": iso_z(latest.last_refreshed_at),
        "refresh_source": latest.refresh_source,
        "read_state": read_state,
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


async def _delete_rows_absent_from_payload(db, existing_rows, new_keys, iface_name_by_id):
    """Delete intent rows absent from the new payload; return what was removed.

    Returns the affected interface names (the removal job's per-instance scope) and
    the removed (interface, address, vrf) triples, sorted — the value-grain residue
    input run_removal checks after the replace commit (#104 phase-3).
    """
    removed_interfaces: set[str] = set()
    removed_addresses: list[list[str]] = []
    for key, row in existing_rows.items():
        if key not in new_keys:
            name = iface_name_by_id.get(row.interface_id, "")
            removed_interfaces.add(name)
            if name:
                removed_addresses.append([name, row.address, row.vrf or ""])
            await db.delete(row)
    removed_interfaces.discard("")
    removed_addresses.sort()
    return removed_interfaces, removed_addresses


class IpIntentResult(BaseModel):
    device_id: int
    address_count: int
    removed_interfaces: int
    replaced: bool
    updated_at: str  # "<iso>Z" stamped at write time


@router.put(
    "/{device_id}/ip-intent",
    dependencies=[Depends(verify_token)],
    response_model=IpIntentResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
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

    # Intent must ALWAYS land. The operator may reference an interface the adapter never
    # imported from the device (a greenfield sub-interface created in NetBox). Materialise a
    # minimal interface row for ANY unknown interface so the intent FK resolves and the IP is
    # stored + visible — never silently dropped (the old behaviour lost the intent with no
    # trace: it looked accepted but vanished, a disaster). The *apply* decides whether the
    # interface can be realised on the device and reports that explicitly; ingest never judges
    # capability. Nokia routed bindings, when supplied, are recorded so the apply can emit the
    # SR OS `router … interface <name> port <parent-binding>:<encap-tag>`.
    for item in body.addresses:
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
            # Backfill Nokia binding on an existing (imported) row when missing; never clobber.
            if item.parent_binding and not iface.parent_binding:
                iface.parent_binding = item.parent_binding
            if item.encap_tag and not iface.encap_tag:
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

    # Delete rows absent from the new payload, tracking which interfaces lost intent so the
    # removal can be propagated to the device (a merge-PATCH apply never drops the address),
    # and the removed (interface, address, vrf) VALUES so run_removal can do the value-grain
    # residue check after the per-instance replace/delete (#104 phase-3).
    iface_name_by_id = {i.id: i.name for i in ifaces.values()}
    removed_interfaces, removed_addresses = await _delete_rows_absent_from_payload(
        db, existing_rows, new_keys, iface_name_by_id
    )
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

    # Removal propagation: a merge-PATCH apply can't drop an address the payload removed, so
    # enqueue an interface_config removal (PUT-replace/DELETE per affected interface) — mirrors
    # every other service's replace_on_removal, and always runs (removal is not auto_apply-gated).
    replaced = False
    if removed_interfaces:
        from nso_adapter.core.removal import enqueue_removal

        await enqueue_removal(
            db,
            device_id,
            "interface_config",
            interfaces=sorted(removed_interfaces),
            removed={"address": removed_addresses},
        )
        replaced = True

    await db.commit()
    logger.info(
        "ip_intent.put.ok", device_id=device_id, address_count=count, removed_interfaces=len(removed_interfaces)
    )
    return {
        "device_id": device_id,
        "address_count": count,
        "removed_interfaces": len(removed_interfaces),
        "replaced": replaced,
        "updated_at": iso_z(now),
    }
