# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/ospf and PUT /api/v1/devices/{id}/ospf-intent."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import (
    Device,
    DeviceOspfInstance,
    DeviceOspfInterface,
    OspfInstanceIntent,
    OspfInterfaceIntent,
    RedistributionIntent,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["ospf"])


@router.get("/{device_id}/ospf", dependencies=[Depends(verify_token)])
async def get_ospf(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    inst_result = await db.execute(
        select(DeviceOspfInstance)
        .where(DeviceOspfInstance.device_id == device_id)
        .order_by(DeviceOspfInstance.process_id)
    )
    inst_rows = inst_result.scalars().all()

    iface_result = await db.execute(
        select(DeviceOspfInterface)
        .where(DeviceOspfInterface.device_id == device_id)
        .order_by(DeviceOspfInterface.interface_name)
    )
    iface_rows = iface_result.scalars().all()

    all_rows = list(inst_rows) + list(iface_rows)
    if not all_rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "instances": [],
            "interfaces": [],
        }

    latest = max(all_rows, key=lambda r: r.last_refreshed_at or "")

    instances = []
    for row in inst_rows:
        entry: dict = {"process_id": row.process_id, "vrf": row.vrf or "", "areas": row.areas or []}
        if row.router_id is not None:
            entry["router_id"] = row.router_id
        instances.append(entry)

    interfaces = []
    for row in iface_rows:
        entry = {
            "interface_name": row.interface_name,
            "passive": row.passive,
            "auth_present": row.auth_present,
        }
        if row.process_id is not None:
            entry["process_id"] = row.process_id
        if row.area_id is not None:
            entry["area_id"] = row.area_id
        if row.priority is not None:
            entry["priority"] = row.priority
        if row.cost is not None:
            entry["cost"] = row.cost
        if row.network_type is not None:
            entry["network_type"] = row.network_type
        if row.auth_type is not None:
            entry["auth_type"] = row.auth_type
        interfaces.append(entry)

    return {
        "device_id": device_id,
        "last_refreshed_at": latest.last_refreshed_at,
        "refresh_source": latest.refresh_source,
        "instances": instances,
        "interfaces": interfaces,
    }


# ── Intent models ────────────────────────────────────────────────────────────


class RedistributionEntry(BaseModel):
    source_protocol: str
    source_ref: str = ""
    route_map: str | None = None
    metric: int | None = None
    metric_type: str | None = None


class OspfInstanceEntry(BaseModel):
    process_id: int
    router_id: str | None = None
    vrf: str = ""
    areas: list[dict] = []
    redistribution: list[RedistributionEntry] = []


class OspfInterfaceEntry(BaseModel):
    interface_name: str
    process_id: int | None = None
    area_id: str | None = None
    passive: bool = False
    priority: int | None = None
    cost: int | None = None
    network_type: str | None = None
    auth_type: str | None = None
    auth_key: str | None = None


class OspfIntentUpdate(BaseModel):
    instances: list[OspfInstanceEntry] = []
    interfaces: list[OspfInterfaceEntry] = []


@router.put("/{device_id}/ospf-intent", dependencies=[Depends(verify_token)])
async def put_ospf_intent(device_id: int, payload: OspfIntentUpdate, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    now = datetime.now(UTC).replace(tzinfo=None)

    # Full-replace instance intents
    existing_inst = await db.execute(select(OspfInstanceIntent).where(OspfInstanceIntent.device_id == device_id))
    existing_inst_map = {row.process_id: row for row in existing_inst.scalars().all()}
    incoming_inst_pids = {e.process_id for e in payload.instances}

    removed_any = [("inst", pid) for pid in existing_inst_map if pid not in incoming_inst_pids]
    for pid in list(existing_inst_map):
        if pid not in incoming_inst_pids:
            await db.delete(existing_inst_map[pid])

    for entry in payload.instances:
        row = existing_inst_map.get(entry.process_id)
        if row is None:
            row = OspfInstanceIntent(device_id=device_id, process_id=entry.process_id, accepted_at=now)
            db.add(row)
        row.router_id = entry.router_id
        row.vrf = entry.vrf
        row.areas = entry.areas

    # Full-replace interface intents
    existing_iface = await db.execute(select(OspfInterfaceIntent).where(OspfInterfaceIntent.device_id == device_id))
    existing_iface_map = {row.interface_name: row for row in existing_iface.scalars().all()}
    incoming_iface_names = {e.interface_name for e in payload.interfaces}

    removed_any += [("iface", n) for n in existing_iface_map if n not in incoming_iface_names]
    for name in list(existing_iface_map):
        if name not in incoming_iface_names:
            await db.delete(existing_iface_map[name])

    for entry in payload.interfaces:
        row = existing_iface_map.get(entry.interface_name)
        if row is None:
            row = OspfInterfaceIntent(device_id=device_id, interface_name=entry.interface_name, accepted_at=now)
            db.add(row)
        row.process_id = entry.process_id
        row.area_id = entry.area_id
        row.passive = entry.passive
        row.priority = entry.priority
        row.cost = entry.cost
        row.network_type = entry.network_type
        row.auth_type = entry.auth_type
        row.auth_key = entry.auth_key

    # Full-replace redistribution intent rows for this device (dest_protocol=ospf)
    existing_redist = await db.execute(
        select(RedistributionIntent).where(
            RedistributionIntent.device_id == device_id,
            RedistributionIntent.dest_protocol == "ospf",
        )
    )
    existing_redist_map = {(r.dest_ref, r.source_protocol, r.source_ref): r for r in existing_redist.scalars().all()}
    incoming_redist_keys: set[tuple] = set()
    for inst_entry in payload.instances:
        dest_ref = str(inst_entry.process_id)
        for re in inst_entry.redistribution:
            incoming_redist_keys.add((dest_ref, re.source_protocol, re.source_ref))

    removed_any += [("redist", k) for k in existing_redist_map if k not in incoming_redist_keys]
    for key in list(existing_redist_map):
        if key not in incoming_redist_keys:
            await db.delete(existing_redist_map[key])

    for inst_entry in payload.instances:
        dest_ref = str(inst_entry.process_id)
        for re in inst_entry.redistribution:
            key = (dest_ref, re.source_protocol, re.source_ref)
            row = existing_redist_map.get(key)
            if row is None:
                row = RedistributionIntent(
                    device_id=device_id,
                    dest_protocol="ospf",
                    dest_ref=dest_ref,
                    source_protocol=re.source_protocol,
                    source_ref=re.source_ref,
                    accepted_at=now,
                )
                db.add(row)
            row.route_map = re.route_map
            row.metric = re.metric
            row.metric_type = re.metric_type

    await db.commit()

    # Removal propagation: PUT-replace the ospf-reconciler instance with the full
    # remaining desired state so removed processes/interfaces/redist are reverted.
    if removed_any:
        from nso_adapter.core.importer import get_nso_client
        from nso_adapter.nso.apply import apply_ospf_config

        insts = (
            await db.execute(select(OspfInstanceIntent).where(OspfInstanceIntent.device_id == device_id))
        ).scalars().all()
        ifaces = (
            await db.execute(select(OspfInterfaceIntent).where(OspfInterfaceIntent.device_id == device_id))
        ).scalars().all()
        redist = (
            await db.execute(
                select(RedistributionIntent).where(
                    RedistributionIntent.device_id == device_id,
                    RedistributionIntent.dest_protocol == "ospf",
                )
            )
        ).scalars().all()
        try:
            nso_client = get_nso_client(device.nso_instance)
            await apply_ospf_config(nso_client, device.nso_device_name, insts, ifaces, redist, replace=True)
        except Exception as exc:  # noqa: BLE001
            structlog.get_logger(__name__).error("ospf_intent.replace_failed", device_id=device_id, error=repr(exc))

    return {
        "device_id": device_id,
        "instance_count": len(payload.instances),
        "interface_count": len(payload.interfaces),
    }
