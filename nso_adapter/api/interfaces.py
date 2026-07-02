# SPDX-License-Identifier: Apache-2.0
"""Interfaces and sync_state endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import DbInterface, Device, InterfaceAttrState, InterfaceIntent

router = APIRouter(prefix="/api/v1/devices", tags=["interfaces"])


def _attr_out(attr_state: InterfaceAttrState, intent_row: InterfaceIntent | None) -> dict:
    return {
        "nso_value": attr_state.nso_value,
        "netbox_value": attr_state.netbox_value,
        "intent_value": intent_row.intent_value if intent_row else None,
        "status": attr_state.sync_state.value,
        "last_apply_at": (
            intent_row.last_apply_at.isoformat() + "Z" if intent_row and intent_row.last_apply_at else None
        ),
        "last_apply_error": intent_row.last_apply_error if intent_row else None,
    }


@router.get("/{device_id}/interfaces", dependencies=[Depends(verify_token)])
async def list_interfaces(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    ifaces_result = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
    ifaces = ifaces_result.scalars().all()
    iface_ids = [iface.id for iface in ifaces]

    # Batch the per-interface children into two queries (not 2 per interface).
    attrs_by_iface: dict[int, list[InterfaceAttrState]] = {}
    intents_by_iface: dict[int, dict[str, InterfaceIntent]] = {}
    if iface_ids:
        attr_rows = await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id.in_(iface_ids)))
        for a in attr_rows.scalars().all():
            attrs_by_iface.setdefault(a.interface_id, []).append(a)
        intent_rows = await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id.in_(iface_ids)))
        for r in intent_rows.scalars().all():
            intents_by_iface.setdefault(r.interface_id, {})[r.attribute] = r

    out = []
    for iface in ifaces:
        intent_by_attr = intents_by_iface.get(iface.id, {})
        attrs = {a.attribute: _attr_out(a, intent_by_attr.get(a.attribute)) for a in attrs_by_iface.get(iface.id, [])}
        out.append(
            {
                "name": iface.name,
                "netbox_interface_id": iface.netbox_interface_id,
                "attrs": attrs,
                # M27R: logical-interface modeling (NULL for physical ports / Cisco / Junos).
                "parent_binding": iface.parent_binding,
                "kind": iface.kind,
                "encap_tag": iface.encap_tag,
                "vrf": iface.vrf,
                "service": iface.service,
            }
        )
    return out


@router.get("/{device_id}/state", dependencies=[Depends(verify_token)])
async def get_state(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    ifaces_result = await db.execute(select(DbInterface.id).where(DbInterface.device_id == device_id))
    iface_ids = ifaces_result.scalars().all()

    by_status: dict[str, int] = {
        "unknown": 0,
        "imported": 0,
        "changed": 0,
        "error": 0,
        "accepted": 0,
        "deploying": 0,
        "in_sync": 0,
        "apply_failed": 0,
        "drifted": 0,
    }
    managed = 0
    last_checked_at = None

    if iface_ids:
        # One query for all attr states, aggregated in Python (not one query per interface).
        attr_rows = await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id.in_(iface_ids)))
        managed_ifaces: set[int] = set()
        for attr in attr_rows.scalars().all():
            managed_ifaces.add(attr.interface_id)
            key = attr.sync_state.value
            by_status[key] = by_status.get(key, 0) + 1
            if attr.last_checked_at and (last_checked_at is None or attr.last_checked_at > last_checked_at):
                last_checked_at = attr.last_checked_at
        managed = len(managed_ifaces)

    return {
        "device_id": device_id,
        "managed_interfaces": managed,
        "by_status": by_status,
        "last_checked_at": last_checked_at.isoformat() + "Z" if last_checked_at else None,
    }
