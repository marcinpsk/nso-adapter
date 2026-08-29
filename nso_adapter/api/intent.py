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
from pydantic import BaseModel, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_409_PUSH_SEQ, RESP_422_VALIDATION, api_error
from nso_adapter.api.intent_push import begin_delivery, get_intent_delivery
from nso_adapter.api.timestamps import UtcInstant, iso_z
from nso_adapter.core.request_flags import PendingClearProvenance
from nso_adapter.store.models import (
    DbInterface,
    Device,
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
    accepted_at: UtcInstant | None = None


class IntentUpdate(BaseModel):
    attributes: list[IntentAttribute]


class IntentRowOut(BaseModel):
    """One mirror row — EMIT-NULL (all six keys always present)."""

    interface: str
    attribute: str
    intent_value: str | None
    accepted_at: str | None
    last_apply_at: str | None
    last_apply_error: dict | None


class IntentReadOut(BaseModel):
    """GET /intent — attributes plus a read-time (frozen-in-test) updated_at mint."""

    device_id: int
    attributes: list[IntentRowOut]
    updated_at: str


class IntentPutResultOut(BaseModel):
    """PUT /intent — count of landed rows plus a write-time updated_at mint."""

    device_id: int
    attribute_count: int
    updated_at: str


class IntentScopeCount(BaseModel):
    """Per-scope apply-state breakdown in the intent-summary map."""

    count: int
    applied: int
    failed: int


class PendingClearSummary(BaseModel):
    """A stream with a clear that has no admitted networked carrier."""

    provenance: PendingClearProvenance
    since: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


class IntentSummaryOut(BaseModel):
    """GET /intent-summary with owned-intent counts and unresolved stream clears."""

    device_id: int
    scopes: dict[str, IntentScopeCount]
    pending_clear: dict[str, PendingClearSummary]


def _intent_row_out(row: InterfaceIntent, if_name: str) -> dict:
    return {
        "interface": if_name,
        "attribute": row.attribute,
        "intent_value": row.intent_value,
        "accepted_at": iso_z(row.accepted_at),
        "last_apply_at": iso_z(row.last_apply_at),
        "last_apply_error": row.last_apply_error,
    }


@router.put(
    "/{device_id}/intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentPutResultOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ, **RESP_422_VALIDATION},
)
async def put_intent(
    device_id: int,
    body: IntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's intent mirror for this device atomically.

    The plugin sends the full snapshot (not a delta); a missing entry
    unambiguously means "no intent".  Existing rows not in the request body
    are deleted.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Every accepted write records its projection revision, store-only and
    # auto-apply-off included, and takes the device's projection lock before anything is
    # read (#1522 §G2). Only a promotion authorizes a deployment.
    from nso_adapter.core.receipt import record_response

    if (replay := await begin_delivery(db, device_id, delivery)) is not None:
        return replay

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

    now = datetime.now(UTC)
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
            accepted_at=item.accepted_at if item.accepted_at else now,
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
    from nso_adapter.core.generation import auto_apply_requested

    if await auto_apply_requested(db, device_id, count):
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True, stream=delivery.stream)

    result = {
        "device_id": device_id,
        "attribute_count": count,
        "updated_at": iso_z(now),
    }
    await record_response(db, device_id, delivery, result)
    await db.commit()
    logger.info("intent.put.ok", device_id=device_id, attribute_count=count)
    return result


@router.get(
    "/{device_id}/intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentReadOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_intent(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    ifaces_result = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
    ifaces = {iface.id: iface for iface in ifaces_result.scalars().all()}

    rows = []
    if ifaces:
        # One query for all intent rows (not one per interface).
        intent_result = await db.execute(
            select(InterfaceIntent).where(InterfaceIntent.interface_id.in_(list(ifaces.keys())))
        )
        for row in intent_result.scalars().all():
            iface = ifaces.get(row.interface_id)
            if iface is not None:
                rows.append(_intent_row_out(row, iface.name))

    updated_at = datetime.now(UTC)
    return {
        "device_id": device_id,
        "attributes": rows,
        "updated_at": iso_z(updated_at),
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


async def _device_pending_clears(db: AsyncSession, device_id: int) -> dict[str, dict]:
    """Return one path-free truthfulness summary per pending stream."""
    from nso_adapter.store.models import StreamPendingClear

    rows = (
        (
            await db.execute(
                select(StreamPendingClear)
                .where(StreamPendingClear.device_id == device_id)
                .order_by(StreamPendingClear.stream)
            )
        )
        .scalars()
        .all()
    )
    out: dict[str, dict] = {}
    for row in rows:
        if row.stream in out:
            raise RuntimeError(f"device {device_id} has multiple pending-clear provenances for {row.stream!r}")
        out[row.stream] = {"provenance": row.provenance, "since": iso_z(row.recorded_at)}
    return out


@router.get(
    "/{device_id}/intent-summary",
    dependencies=[Depends(verify_token)],
    response_model=IntentSummaryOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
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
    return {
        "device_id": device_id,
        "scopes": scopes,
        "pending_clear": await _device_pending_clears(db, device_id),
    }
