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

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.domain.read_projection import project_vlans
from nso_adapter.domain.switching_observation import parse_vlan_string, project_switchports
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import (
    Device,
    DeviceSwitchport,
    DeviceSwitchportTaggedVlan,
    DeviceVlan,
)

logger = structlog.get_logger(__name__)


def _now():
    return datetime.now(UTC)


async def _upsert_vlans(
    db: AsyncSession,
    device: Device,
    data: dict,
    refresh_source: str,
) -> None:
    """Diff-by-key materializer: upsert each read VLAN by vlan-id, prune the unseen.

    NOT a full-replace — existing rows are updated in place so their FKs (switchport
    untagged/tagged links) survive. An empty *vlans* list (the AbsentAuthoritative clear)
    prunes every row, which is the correct authoritative clear.
    """
    existing = {
        r.vlan_id: r
        for r in (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
    }
    seen: set[int] = set()
    now = _now()
    projection = project_vlans(data)
    if projection.unprojectable:
        if "conflicting collection aliases" in projection.unprojectable[0].reason:
            raise ValueError("vlan-database has conflicting collection aliases")
        vlans = as_list(data["vlan"] if "vlan" in data else data.get("vlans"))
        raw = vlans[projection.unprojectable[0].index]
        raw_vlan_id = raw.get("vlan-id", raw.get("vlan_id")) if isinstance(raw, dict) else None
        raise ValueError(
            f"a vlan-database item for device {device.id} carries a vlan-id of type "
            f"{type(raw_vlan_id).__name__}, not a valid VLAN id"
        )
    for entry in projection.vlans or []:
        vid = entry.vlan_id
        seen.add(vid)
        row = existing.get(vid) or DeviceVlan(device_id=device.id, vlan_id=vid)
        row.name = entry.name or ""
        row.last_refreshed_at = now
        row.refresh_source = refresh_source
        db.add(row)
    for vid, row in existing.items():
        if vid not in seen:
            await db.delete(row)


VLAN_DATABASE_SPEC = FamilySpec(
    name="vlan",
    extract=lambda data: data,
    materialize=_upsert_vlans,
    wire_name="vlan-database",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_vlan_database_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read the VLAN database for *device* and upsert+prune DeviceVlan rows (via the shared refresh engine).

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, VLAN_DATABASE_SPEC, refresh_source=refresh_source)


async def _upsert_switchports(
    db: AsyncSession,
    device: Device,
    data: dict,
    refresh_source: str,
) -> None:
    """Diff-by-key materializer: upsert each switchport by interface-name, prune the unseen.

    Resolves untagged/tagged VLAN ids to the device's DeviceVlan rows (by vlan-id) — so the
    VLAN database must already be refreshed (caller ordering); unknown vlan-ids are left
    unlinked (untagged) / skipped (tagged). An empty *interfaces* list (the AbsentAuthoritative
    clear) prunes every switchport.
    """
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
    projection = project_switchports(data)
    if projection.unprojectable:
        if "conflicting collection aliases" in projection.unprojectable[0].reason:
            raise ValueError("switchport has conflicting collection aliases")
        interfaces = as_list(data["interface"] if "interface" in data else data.get("interfaces"))
        raw = interfaces[projection.unprojectable[0].index]
        if isinstance(raw, dict):
            tagged = raw["tagged-vlans"] if "tagged-vlans" in raw else raw.get("tagged_vlans")
            parse_vlan_string(tagged)
            untagged = raw.get("untagged-vlan", raw.get("untagged_vlan"))
            if untagged is not None:
                raise ValueError(
                    f"a switchport item for device {device.id} carries an invalid untagged-vlan "
                    f"(type {type(untagged).__name__})"
                )
        raise ValueError("switchport item has an invalid interface-name, mode, or duplicate identity")
    for item in projection.interfaces or []:
        name = item.interface_name
        seen.add(name)
        row = existing.get(name) or DeviceSwitchport(device_id=device.id, interface_name=name)
        row.mode = item.mode or ""
        uv = vlan_by_vid.get(item.untagged_vlan) if item.untagged_vlan is not None else None
        row.untagged_vlan_id = uv.id if uv is not None else None
        row.last_refreshed_at = now
        row.refresh_source = refresh_source
        db.add(row)
        await db.flush()
        await db.execute(delete(DeviceSwitchportTaggedVlan).where(DeviceSwitchportTaggedVlan.switchport_id == row.id))
        for tv in item.tagged_vlans or []:
            vlan = vlan_by_vid.get(tv)
            if vlan is not None:
                db.add(DeviceSwitchportTaggedVlan(switchport_id=row.id, vlan_id=vlan.id))
    for name, row in existing.items():
        if name not in seen:
            await db.delete(row)


SWITCHPORT_SPEC = FamilySpec(
    name="switchport",
    extract=lambda data: data,
    materialize=_upsert_switchports,
    wire_name="switchport",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_switchport_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read switchport state for *device* and upsert+prune DeviceSwitchport rows (via the shared refresh engine).

    Resolves untagged/tagged VLAN ids to the device's DeviceVlan rows (by vlan-id);
    unknown vlan-ids are simply left unlinked (untagged) / skipped (tagged).

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, SWITCHPORT_SPEC, refresh_source=refresh_source)
