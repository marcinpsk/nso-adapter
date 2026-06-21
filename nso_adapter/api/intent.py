# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Intent API — manage per-device intent mirror (Phase 2).

PUT /api/v1/devices/{id}/intent  — full snapshot replace (idempotent)
GET /api/v1/devices/{id}/intent  — read current mirror
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import (
    DbInterface,
    Device,
    DeviceSettings,
    InterfaceAttrState,
    InterfaceIntent,
    ManagedScope,
    SyncState,
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
            # Intent must ALWAYS land. The operator may reference an interface the adapter never
            # imported (a greenfield interface created in NetBox). Materialise a minimal interface
            # row so the attribute intent is stored + visible, never silently dropped (the old
            # behaviour lost it with only a warning — it looked accepted but vanished). The apply
            # decides whether the interface can be realised and reports that explicitly; ingest
            # never judges capability. Mirrors put_ip_intent's greenfield handling.
            iface = DbInterface(device_id=device_id, name=item.interface, kind="logical")
            db.add(iface)
            await db.flush()  # assign id before the intent + attr_state FK it
            ifaces[item.interface] = iface
            logger.info("intent.put.greenfield_interface", device_id=device_id, interface=item.interface)
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
        # Transition imported/changed/unknown → accepted; leave in_sync/drifted/apply_failed
        # alone so force-apply on already-deployed intent still works. A greenfield interface
        # (or an attribute the device was never imported with) has no state yet → create it
        # accepted, so the freshly-landed intent is also apply-eligible (not stored-but-inert).
        attr_result = await db.execute(
            select(InterfaceAttrState).where(
                InterfaceAttrState.interface_id == iface.id,
                InterfaceAttrState.attribute == item.attribute,
            )
        )
        attr_state = attr_result.scalar_one_or_none()
        if attr_state is None:
            db.add(InterfaceAttrState(interface_id=iface.id, attribute=item.attribute, sync_state=SyncState.accepted))
        elif attr_state.sync_state in {SyncState.imported, SyncState.changed, SyncState.unknown}:
            attr_state.sync_state = SyncState.accepted

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


async def _device_intent_counts(db: AsyncSession, device_id: int) -> dict[str, dict]:
    """Count rows of every ``*_intent`` table for a device, with apply-state breakdown.

    Generic (driven by the SQLAlchemy metadata, so DB-agnostic and auto-covering every
    current and future intent scope). Tables keyed by ``device_id`` are counted directly;
    those keyed by ``interface_id`` are joined through ``interfaces``. Child intent tables
    (no device/interface key, e.g. ``bgp_af_intent``) are skipped — they are covered
    transitively by their parent scope. Only scopes with a non-zero count are returned. Table
    names come from the model catalogue, not user input, so the f-strings carry no injection risk.
    """
    from nso_adapter.store.models import Base

    out: dict[str, dict] = {}
    for tname, table in sorted(Base.metadata.tables.items()):
        if not tname.endswith("_intent"):
            continue
        cols = set(table.c.keys())
        if "device_id" in cols:
            frm, where = f"{tname} x", "x.device_id = :d"
        elif "interface_id" in cols:
            frm, where = f"{tname} x join interfaces i on x.interface_id = i.id", "i.device_id = :d"
        else:
            continue
        sel = "count(*) as total"
        if "last_apply_at" in cols:
            sel += ", count(x.last_apply_at) as applied"
        if "last_apply_error" in cols:
            sel += ", count(x.last_apply_error) as failed"
        row = (await db.execute(text(f"select {sel} from {frm} where {where}"), {"d": device_id})).mappings().first()
        if row and row["total"]:
            out[tname] = {
                "count": row["total"],
                "applied": row.get("applied", 0) or 0,
                "failed": row.get("failed", 0) or 0,
            }
    return out


@router.get("/{device_id}/intent-summary", dependencies=[Depends(verify_token)])
async def get_intent_summary(device_id: int, db: AsyncSession = Depends(get_db)):
    """Per-scope summary of the adapter's intent mirror for a device.

    Surfaces what the adapter currently holds as owned intent (per ``*_intent`` table), so the
    plugin can detect adapter↔NetBox drift (intent the adapter holds that NetBox no longer
    owns — the split-brain) and offer a re-sync. Returns only non-empty scopes.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    scopes = await _device_intent_counts(db, device_id)
    return {"device_id": device_id, "scopes": scopes}
