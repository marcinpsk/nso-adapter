# SPDX-License-Identifier: Apache-2.0
"""Interfaces and sync_state endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, api_error
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import iso_z
from nso_adapter.store import outcome_store
from nso_adapter.store.models import DbInterface, Device, InterfaceAttrState, InterfaceIntent

router = APIRouter(prefix="/api/v1/devices", tags=["interfaces"])


# ── Response models ───────────────────────────────────────────────────────────
# Both endpoints are EMIT-NULL fixed shapes (every key always present, nullable
# values serialised as null), so neither uses response_model_exclude_unset.
# ``attrs`` is a dynamic map keyed by attribute name.


class InterfaceAttrOut(BaseModel):
    nso_value: str | None
    netbox_value: str | None
    intent_value: str | None
    status: str
    last_apply_at: str | None  # "<iso>Z" when applied, else null
    last_apply_error: dict | None


class InterfaceOut(BaseModel):
    name: str
    netbox_interface_id: int | None
    attrs: dict[str, InterfaceAttrOut]
    parent_binding: str | None
    kind: str | None
    encap_tag: str | None
    vrf: str | None
    service: str | None


class InterfaceStateOut(BaseModel):
    device_id: int
    managed_interfaces: int
    by_status: dict[str, int]  # SyncState value -> count
    last_checked_at: str | None  # "<iso>Z" or null


def _attr_out(attr_state: InterfaceAttrState, intent_row: InterfaceIntent | None) -> dict:
    return {
        "nso_value": attr_state.nso_value,
        "netbox_value": attr_state.netbox_value,
        "intent_value": intent_row.intent_value if intent_row else None,
        "status": attr_state.sync_state.value,
        "last_apply_at": iso_z(intent_row.last_apply_at) if intent_row else None,
        "last_apply_error": intent_row.last_apply_error if intent_row else None,
    }


@router.get(
    "/{device_id}/interfaces",
    dependencies=[Depends(verify_token)],
    response_model=list[InterfaceOut],
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def list_interfaces(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    return await _assemble_interfaces(db, device_id)


async def _assemble_interfaces(db: AsyncSession, device_id: int) -> list[dict]:
    """Build the one interfaces assembly.

    Served bare by the legacy list endpoint (S5 retires it; its shape cannot gain a
    top-level key, R1-F1) and wrapped by /interfaces-doc.
    """
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


class InterfacesDocOut(BaseModel):
    """Object-shaped interfaces document (READSEM S4 D1).

    The S4 plugin consumes THIS; the bare-list /interfaces stays byte-identical for
    pre-S4 consumers until S5.
    """

    device_id: int
    read_state: FamilyReadState
    interfaces: list[InterfaceOut]


@router.get(
    "/{device_id}/interfaces-doc",
    dependencies=[Depends(verify_token)],
    response_model=InterfacesDocOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_interfaces_doc(device_id: int, db: AsyncSession = Depends(get_read_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "interface_attributes"), source_epoch=device.source_epoch
    )
    return {
        "device_id": device_id,
        "read_state": read_state,
        "interfaces": await _assemble_interfaces(db, device_id),
    }


@router.get(
    "/{device_id}/state",
    dependencies=[Depends(verify_token)],
    response_model=InterfaceStateOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
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
        "last_checked_at": iso_z(last_checked_at),
    }
