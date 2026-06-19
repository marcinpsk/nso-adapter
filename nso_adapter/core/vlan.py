# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""VLAN database + L2 switchport refresh — reads NSO oper-data and upserts the DB.

Mirrors core/snmp.py (refresh_*_for_device) + core/lag_topology.py (SSE handlers).
The switchport refresh resolves untagged/tagged VLAN links to the device's
DeviceVlan rows by vlan-id (so the VLAN database must be refreshed first).
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.core.lag_topology import parse_changed_nso_devices
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import (
    Device,
    DeviceSwitchport,
    DeviceSwitchportTaggedVlan,
    DeviceVlan,
)

logger = structlog.get_logger(__name__)


def _now():
    return datetime.now(UTC).replace(tzinfo=None)


def _parse_vlan_string(raw) -> list[int]:
    """Expand the NSO 'tagged-vlans' string ('805,1518-1519,3629') into a sorted int list.

    Also tolerates a list (legacy/test) — returns it as ints.
    """
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        return sorted(int(v) for v in raw)
    vlans: set[int] = set()
    for chunk in str(raw).split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            if "-" in chunk:
                start, end = (int(x) for x in chunk.split("-", 1))
            else:
                start = end = int(chunk)
        except ValueError:
            continue
        # Bound to the legal 802.1Q range so a malformed upstream string
        # (e.g. "1-999999999") can't blow up memory via range expansion.
        if not (1 <= start <= end <= 4094):
            continue
        vlans.update(range(start, end + 1))
    return sorted(vlans)


async def refresh_vlan_database_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read the VLAN database for *device* and upsert+prune DeviceVlan rows."""
    if not device.nso_device_name:
        return
    try:
        entry = await nso_client.get_vlan_database(device.nso_device_name)
    except Exception as exc:
        logger.warning("vlan.refresh.nso_error", device_id=device.id, error=repr(exc))
        return

    vlans = (entry or {}).get("vlan", []) or (entry or {}).get("vlans", [])
    existing = {
        r.vlan_id: r
        for r in (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
    }
    seen: set[int] = set()
    now = _now()
    for item in vlans:
        try:
            vid = int(item.get("vlan-id", item.get("vlan_id")))
        except (TypeError, ValueError):
            continue
        seen.add(vid)
        row = existing.get(vid) or DeviceVlan(device_id=device.id, vlan_id=vid)
        row.name = item.get("name") or ""
        row.last_refreshed_at = now
        row.refresh_source = refresh_source
        db.add(row)
    for vid, row in existing.items():
        if vid not in seen:
            await db.delete(row)
    await db.commit()
    logger.info("vlan.refresh.done", device_id=device.id, vlans=len(seen), source=refresh_source)


async def refresh_switchport_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read switchport state for *device* and upsert+prune DeviceSwitchport rows.

    Resolves untagged/tagged VLAN ids to the device's DeviceVlan rows (by vlan-id);
    unknown vlan-ids are simply left unlinked (untagged) / skipped (tagged).
    """
    if not device.nso_device_name:
        return
    try:
        entry = await nso_client.get_switchport(device.nso_device_name)
    except Exception as exc:
        logger.warning("switchport.refresh.nso_error", device_id=device.id, error=repr(exc))
        return

    interfaces = (entry or {}).get("interface", []) or (entry or {}).get("interfaces", [])
    vlan_by_vid = {
        r.vlan_id: r
        for r in (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
    }
    existing = {
        r.interface_name: r
        for r in (
            await db.execute(
                select(DeviceSwitchport)
                .where(DeviceSwitchport.device_id == device.id)
                .options(selectinload(DeviceSwitchport.tagged_vlans))
            )
        )
        .scalars()
        .all()
    }
    seen: set[str] = set()
    now = _now()
    for item in interfaces:
        name = item.get("interface-name") or item.get("interface_name")
        if not name:
            continue
        seen.add(name)
        row = existing.get(name) or DeviceSwitchport(device_id=device.id, interface_name=name)
        row.mode = item.get("mode") or ""
        untagged = item.get("untagged-vlan", item.get("untagged_vlan"))
        uv = vlan_by_vid.get(int(untagged)) if untagged is not None else None
        row.untagged_vlan_id = uv.id if uv is not None else None
        row.last_refreshed_at = now
        row.refresh_source = refresh_source
        db.add(row)
        await db.flush()
        # rebuild tagged-vlan join rows
        await db.execute(delete(DeviceSwitchportTaggedVlan).where(DeviceSwitchportTaggedVlan.switchport_id == row.id))
        for tv in _parse_vlan_string(item.get("tagged-vlans") or item.get("tagged_vlans")):
            vlan = vlan_by_vid.get(tv)
            if vlan is not None:
                db.add(DeviceSwitchportTaggedVlan(switchport_id=row.id, vlan_id=vlan.id))
    for name, row in existing.items():
        if name not in seen:
            await db.delete(row)
    await db.commit()
    logger.info("switchport.refresh.done", device_id=device.id, interfaces=len(seen), source=refresh_source)


async def _handle_change(event_data, db, nso_clients, refresh_fn) -> None:
    changed = parse_changed_nso_devices(event_data)
    if not changed:
        return
    devices = (await db.execute(select(Device).where(Device.nso_device_name.in_(changed)))).scalars().all()
    for device in devices:
        nso_client = nso_clients.get(device.nso_instance)
        if nso_client is None:
            continue
        await refresh_fn(db, device, nso_client, refresh_source="notification")


async def handle_vlan_database_change(event_data, db, nso_clients) -> None:
    """SSE: refresh the VLAN database for devices in a netconf-config-change event."""
    await _handle_change(event_data, db, nso_clients, refresh_vlan_database_for_device)


async def handle_switchport_change(event_data, db, nso_clients) -> None:
    """SSE: refresh switchport state for devices in a netconf-config-change event."""
    await _handle_change(event_data, db, nso_clients, refresh_switchport_for_device)
