# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Intent API — manage per-device intent mirror (Phase 2, M5).

PUT /api/v1/devices/{id}/intent  — full snapshot replace (idempotent)
GET /api/v1/devices/{id}/intent  — read current mirror
"""

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
    ComplianceStatus,
    DbInterface,
    Device,
    DeviceSettings,
    InterfaceAttrState,
    InterfaceIntent,
    ManagedScope,
)

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["intent"])


class IntentAttribute(BaseModel):
    interface: str
    attribute: str
    intent_value: str | bool | None
    accepted_at: datetime | None = None


class IntentUpdate(BaseModel):
    attributes: list[IntentAttribute]


def _intent_row_out(row: InterfaceIntent, if_name: str) -> dict:
    return {
        "interface": if_name,
        "attribute": row.attribute,
        "intent_value": row.intent_value,
        "accepted_at": row.accepted_at.isoformat() + "Z" if row.accepted_at else None,
        "last_apply_at": row.last_apply_at.isoformat() + "Z" if row.last_apply_at else None,
        "last_apply_error": row.last_apply_error,
    }


@router.put("/{device_id}/intent", dependencies=[Depends(verify_token)])
async def put_intent(device_id: int, body: IntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's intent mirror for this device atomically.

    The plugin sends the full snapshot (not a delta); a missing entry
    unambiguously means "no intent".  Existing rows not in the request body
    are deleted.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Validate all requested attributes against the device's managed scope
    scope_result = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
    managed_attrs = {row.attribute for row in scope_result.scalars().all()}
    for item in body.attributes:
        if item.attribute not in managed_attrs:
            raise api_error(
                422,
                "validation_error",
                f"Attribute {item.attribute!r} is not in the managed scope for device {device_id}",
            )

    # Build a lookup: interface name → DbInterface.id
    ifaces_result = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
    ifaces = {iface.name: iface for iface in ifaces_result.scalars().all()}

    # Delete all existing intent rows for this device (full replace)
    existing_intent = await db.execute(
        select(InterfaceIntent).where(InterfaceIntent.interface_id.in_([i.id for i in ifaces.values()]))
    )
    for row in existing_intent.scalars().all():
        await db.delete(row)
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    for item in body.attributes:
        iface = ifaces.get(item.interface)
        if iface is None:
            logger.warning("intent.put.unknown_interface", device_id=device_id, interface=item.interface)
            continue
        value = str(item.intent_value) if item.intent_value is not None else None
        row = InterfaceIntent(
            interface_id=iface.id,
            attribute=item.attribute,
            intent_value=value,
            accepted_at=item.accepted_at.replace(tzinfo=None) if item.accepted_at else now,
        )
        db.add(row)
        count += 1

        # Stamp the attr_state as accepted so the apply job finds it eligible.
        # Transition imported/changed → accepted; leave in_sync/drifted/apply_failed
        # alone so force-apply on already-deployed intent still works.
        attr_result = await db.execute(
            select(InterfaceAttrState).where(
                InterfaceAttrState.interface_id == iface.id,
                InterfaceAttrState.attribute == item.attribute,
            )
        )
        attr_state = attr_result.scalar_one_or_none()
        if attr_state and attr_state.compliance_status in {
            ComplianceStatus.imported,
            ComplianceStatus.changed,
            ComplianceStatus.unknown,
        }:
            attr_state.compliance_status = ComplianceStatus.accepted

    await db.flush()

    # If auto_apply is enabled, enqueue an apply job
    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()
    logger.info("intent.put.ok", device_id=device_id, attribute_count=count)
    return {
        "device_id": device_id,
        "attribute_count": count,
        "updated_at": now.isoformat() + "Z",
    }


@router.get("/{device_id}/intent", dependencies=[Depends(verify_token)])
async def get_intent(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    ifaces_result = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
    ifaces = {iface.id: iface for iface in ifaces_result.scalars().all()}

    rows = []
    for iface in ifaces.values():
        intent_result = await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == iface.id))
        for row in intent_result.scalars().all():
            rows.append(_intent_row_out(row, iface.name))

    updated_at = datetime.now(UTC).replace(tzinfo=None)
    return {
        "device_id": device_id,
        "attributes": rows,
        "updated_at": updated_at.isoformat() + "Z",
    }
