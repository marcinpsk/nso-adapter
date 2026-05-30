# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/isis-interfaces and PUT /api/v1/devices/{id}/isis-interface-intent."""
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
    DeviceIsisInterface,
    DeviceIsisProcess,
    IsisInterfaceIntent,
    IsisProcessIntent,
    RedistributionIntent,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["isis"])


@router.get("/{device_id}/isis-interfaces", dependencies=[Depends(verify_token)])
async def get_isis_interfaces(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    proc_result = await db.execute(
        select(DeviceIsisProcess)
        .where(DeviceIsisProcess.device_id == device_id)
        .order_by(DeviceIsisProcess.process_tag)
    )
    proc_rows = proc_result.scalars().all()

    iface_result = await db.execute(
        select(DeviceIsisInterface)
        .where(DeviceIsisInterface.device_id == device_id)
        .order_by(DeviceIsisInterface.interface_name, DeviceIsisInterface.af)
    )
    iface_rows = iface_result.scalars().all()

    all_rows = list(proc_rows) + list(iface_rows)
    if not all_rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "processes": [],
            "interfaces": [],
        }

    latest = max(all_rows, key=lambda r: r.last_refreshed_at or "")

    processes = []
    for row in proc_rows:
        entry: dict = {"process_tag": row.process_tag}
        if row.net is not None:
            entry["net"] = row.net
        if row.is_type is not None:
            entry["is_type"] = row.is_type
        if row.metric_style is not None:
            entry["metric_style"] = row.metric_style
        if row.overload_bit is not None:
            entry["overload_bit"] = row.overload_bit
        if row.area_auth_type is not None:
            entry["area_auth_type"] = row.area_auth_type
        if row.area_auth_present is not None:
            entry["area_auth_present"] = row.area_auth_present
        if row.domain_auth_type is not None:
            entry["domain_auth_type"] = row.domain_auth_type
        if row.domain_auth_present is not None:
            entry["domain_auth_present"] = row.domain_auth_present
        processes.append(entry)

    interfaces = []
    for row in iface_rows:
        entry = {
            "interface_name": row.interface_name,
            "af": row.af,
            "process_tag": row.process_tag,
        }
        if row.circuit_type is not None:
            entry["circuit_type"] = row.circuit_type
        if row.network_type is not None:
            entry["network_type"] = row.network_type
        if row.metric is not None:
            entry["metric"] = row.metric
        entry["passive"] = row.passive
        interfaces.append(entry)

    last_ts = latest.last_refreshed_at
    return {
        "device_id": device_id,
        "last_refreshed_at": last_ts.isoformat() + "Z" if last_ts else None,
        "refresh_source": latest.refresh_source,
        "processes": processes,
        "interfaces": interfaces,
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/isis-interface-intent
# ---------------------------------------------------------------------------


class RedistributionEntry(BaseModel):
    source_protocol: str
    source_ref: str = ""
    route_map: str | None = None
    metric: int | None = None
    metric_type: str | None = None


class IsisInterfaceEntry(BaseModel):
    interface_name: str
    af: str
    process_tag: str = ""
    circuit_type: str | None = None
    network_type: str | None = None
    metric: int | None = None
    passive: bool = False
    accepted_at: datetime | None = None


class IsisProcessEntry(BaseModel):
    process_tag: str = ""
    net: str | None = None
    is_type: str | None = None
    metric_style: str | None = None
    overload_bit: bool | None = None
    area_auth_type: str | None = None
    area_auth_key: str | None = None
    domain_auth_type: str | None = None
    domain_auth_key: str | None = None
    accepted_at: datetime | None = None
    redistribution: list[RedistributionEntry] = []


class IsisInterfaceIntentUpdate(BaseModel):
    interfaces: list[IsisInterfaceEntry]
    processes: list[IsisProcessEntry] = []


@router.put("/{device_id}/isis-interface-intent", dependencies=[Depends(verify_token)])
async def put_isis_interface_intent(
    device_id: int, body: IsisInterfaceIntentUpdate, db: AsyncSession = Depends(get_db)
):
    """Replace the adapter's IS-IS interface and process intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is
    enabled on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    now = datetime.now(UTC).replace(tzinfo=None)

    # ── Interface intent (full-replace) ──────────────────────────────────────
    existing_result = await db.execute(
        select(IsisInterfaceIntent).where(IsisInterfaceIntent.device_id == device_id)
    )
    existing_rows: dict[tuple, IsisInterfaceIntent] = {
        (r.interface_name, r.af): r for r in existing_result.scalars().all()
    }

    new_iface_keys: set[tuple] = {(item.interface_name, item.af) for item in body.interfaces}

    for key, row in existing_rows.items():
        if key not in new_iface_keys:
            await db.delete(row)
    await db.flush()

    iface_count = 0
    for item in body.interfaces:
        key = (item.interface_name, item.af)
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        if key in existing_rows:
            row = existing_rows[key]
            row.process_tag = item.process_tag
            row.circuit_type = item.circuit_type
            row.network_type = item.network_type
            row.metric = item.metric
            row.passive = item.passive
            row.accepted_at = accepted
        else:
            row = IsisInterfaceIntent(
                device_id=device_id,
                interface_name=item.interface_name,
                af=item.af,
                process_tag=item.process_tag,
                circuit_type=item.circuit_type,
                network_type=item.network_type,
                metric=item.metric,
                passive=item.passive,
                accepted_at=accepted,
            )
            db.add(row)
        iface_count += 1

    await db.flush()

    # ── Process intent (full-replace) ────────────────────────────────────────
    existing_proc_result = await db.execute(
        select(IsisProcessIntent).where(IsisProcessIntent.device_id == device_id)
    )
    existing_proc_rows: dict[str, IsisProcessIntent] = {
        r.process_tag: r for r in existing_proc_result.scalars().all()
    }

    new_proc_tags: set[str] = {item.process_tag for item in body.processes}

    for tag, row in existing_proc_rows.items():
        if tag not in new_proc_tags:
            await db.delete(row)
    await db.flush()

    proc_count = 0
    for item in body.processes:
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        if item.process_tag in existing_proc_rows:
            row = existing_proc_rows[item.process_tag]
            row.net = item.net
            row.is_type = item.is_type
            row.metric_style = item.metric_style
            row.overload_bit = item.overload_bit
            row.area_auth_type = item.area_auth_type
            row.area_auth_key = item.area_auth_key
            row.domain_auth_type = item.domain_auth_type
            row.domain_auth_key = item.domain_auth_key
            row.accepted_at = accepted
        else:
            row = IsisProcessIntent(
                device_id=device_id,
                process_tag=item.process_tag,
                net=item.net,
                is_type=item.is_type,
                metric_style=item.metric_style,
                overload_bit=item.overload_bit,
                area_auth_type=item.area_auth_type,
                area_auth_key=item.area_auth_key,
                domain_auth_type=item.domain_auth_type,
                domain_auth_key=item.domain_auth_key,
                accepted_at=accepted,
            )
            db.add(row)
        proc_count += 1

    await db.flush()

    # Full-replace redistribution intent rows for this device (dest_protocol=isis)
    existing_redist = await db.execute(
        select(RedistributionIntent).where(
            RedistributionIntent.device_id == device_id,
            RedistributionIntent.dest_protocol == "isis",
        )
    )
    existing_redist_map = {
        (r.dest_ref, r.source_protocol, r.source_ref): r
        for r in existing_redist.scalars().all()
    }
    incoming_redist_keys: set[tuple] = set()
    for proc_entry in body.processes:
        dest_ref = proc_entry.process_tag
        for re in proc_entry.redistribution:
            incoming_redist_keys.add((dest_ref, re.source_protocol, re.source_ref))

    for key in list(existing_redist_map):
        if key not in incoming_redist_keys:
            await db.delete(existing_redist_map[key])

    for proc_entry in body.processes:
        dest_ref = proc_entry.process_tag
        for re in proc_entry.redistribution:
            key = (dest_ref, re.source_protocol, re.source_ref)
            row = existing_redist_map.get(key)
            if row is None:
                row = RedistributionIntent(
                    device_id=device_id,
                    dest_protocol="isis",
                    dest_ref=dest_ref,
                    source_protocol=re.source_protocol,
                    source_ref=re.source_ref,
                    accepted_at=now,
                )
                db.add(row)
            row.route_map = re.route_map
            row.metric = re.metric
            row.metric_type = re.metric_type

    from nso_adapter.store.models import DeviceSettings

    settings_result = await db.execute(
        select(DeviceSettings).where(DeviceSettings.device_id == device_id)
    )
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and (iface_count > 0 or proc_count > 0):
        from nso_adapter.core.apply import enqueue_apply
        await enqueue_apply(db, device_id, force=True)

    await db.commit()
    return {"device_id": device_id, "interface_count": iface_count, "process_count": proc_count}
