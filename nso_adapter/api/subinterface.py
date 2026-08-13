# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/subinterface — dot1q L3 subinterfaces, read.

PUT /api/v1/devices/{id}/subinterface-intent — write path.
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import (
    RESP_401,
    RESP_404_DEVICE,
    RESP_409_PUSH_SEQ,
    RESP_422_VALIDATION,
    IntentApplyResult,
    api_error,
)
from nso_adapter.api.intent_push import begin_delivery, get_intent_delivery
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import UtcInstant
from nso_adapter.core.removal import is_cleared
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, DeviceSettings, DeviceSubinterface, SubinterfaceIntent

router = APIRouter(prefix="/api/v1/devices", tags=["subinterface"])


class SubinterfaceIfaceOut(BaseModel):
    interface_name: str
    parent_interface: str | None  # null when unset
    dot1q_vlan: int | None  # null when unset
    type: str
    vrf: str  # coerced to "" when unset
    source: str


class SubinterfaceOut(BaseModel):
    device_id: int
    read_state: FamilyReadState  # the S4 truth (this family never had legacy freshness fields)
    interfaces: list[SubinterfaceIfaceOut]


@router.get(
    "/{device_id}/subinterface",
    dependencies=[Depends(verify_token)],
    response_model=SubinterfaceOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_subinterface(device_id: int, db: AsyncSession = Depends(get_read_db)):
    """Return the device's dot1q subinterfaces (no IPs — those ride interface-ip)."""
    if (device := await db.get(Device, device_id)) is None:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "subinterface"), source_epoch=device.source_epoch
    )
    rows = (
        (
            await db.execute(
                select(DeviceSubinterface)
                .where(DeviceSubinterface.device_id == device_id)
                .order_by(DeviceSubinterface.interface_name)
            )
        )
        .scalars()
        .all()
    )
    return {
        "device_id": device_id,
        "read_state": read_state,
        "interfaces": [
            {
                "interface_name": r.interface_name,
                "parent_interface": r.parent_interface,
                "dot1q_vlan": r.dot1q_vlan,
                "type": r.sub_type,
                "vrf": r.vrf or "",
                "source": "subinterface",
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/subinterface-intent (write path)
# ---------------------------------------------------------------------------


class SubinterfaceEntry(BaseModel):
    interface_name: str
    parent_interface: str = ""
    dot1q_vlan: int | None = None
    type: str = "subinterface"
    vrf: str = ""
    accepted_at: UtcInstant | None = None


# Scalars the writer emits only when set — `if row.vrf:` (nso/apply.py). parent_interface/dot1q_vlan are ALWAYS emitted, so a merge-PATCH does carry those — only vrf is undroppable.
# A merge-PATCH apply can never drop one that goes back to unset, so clearing any of
# them must enqueue a PUT-replace retract. See core.removal.is_cleared.
_STATE_FIELDS = ("vrf",)


class SubinterfaceIntentUpdate(BaseModel):
    interfaces: list[SubinterfaceEntry]


@router.put(
    "/{device_id}/subinterface-intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentApplyResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ, **RESP_422_VALIDATION},
)
async def put_subinterface_intent(
    device_id: int,
    body: SubinterfaceIntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's subinterface intent mirror for this device atomically.

    Full-replace: rows not in the body are deleted. ``accepted_at`` defaults to now.
    If ``auto_apply`` is enabled on the device, an apply job is enqueued.
    """
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")

    # Every accepted write records its projection revision, store-only and
    # auto-apply-off included, and takes the device's projection lock before anything is
    # read (#1522 §G2). Only a promotion authorizes a deployment.
    from nso_adapter.core.receipt import record_response

    if (replay := await begin_delivery(db, device_id, delivery)) is not None:
        return replay

    existing = await db.execute(select(SubinterfaceIntent).where(SubinterfaceIntent.device_id == device_id))
    existing_rows: dict[str, SubinterfaceIntent] = {r.interface_name: r for r in existing.scalars().all()}
    new_keys = {i.interface_name for i in body.interfaces}

    removed = [name for name in existing_rows if name not in new_keys]
    for name in removed:
        await db.delete(existing_rows[name])
    await db.flush()

    now = datetime.now(UTC)
    count = 0
    cleared = False
    for item in body.interfaces:
        accepted = item.accepted_at if item.accepted_at else now
        row = existing_rows.get(item.interface_name)
        before = {f: getattr(row, f) for f in _STATE_FIELDS} if row is not None else None
        if row is None:
            row = SubinterfaceIntent(device_id=device_id, interface_name=item.interface_name)
            db.add(row)
        row.parent_interface = item.parent_interface or None
        row.dot1q_vlan = item.dot1q_vlan
        row.sub_type = item.type
        row.vrf = item.vrf or None
        row.accepted_at = accepted
        if before is not None and any(is_cleared(before[f], getattr(row, f)) for f in _STATE_FIELDS):
            cleared = True
        count += 1

    await db.flush()
    settings = (
        await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    ).scalar_one_or_none()
    apply_requested = bool(settings and settings.auto_apply and count > 0)
    from nso_adapter.core.generation import request_settlement_cohort

    settlement_cohort = await request_settlement_cohort(db, int(bool(removed or cleared)) + int(apply_requested))
    replaced = False
    if removed or cleared:
        from nso_adapter.core.removal import replace_on_removal
        from nso_adapter.nso.apply import apply_subinterface_config

        replaced = await replace_on_removal(
            db,
            device,
            removed,
            SubinterfaceIntent,
            apply_subinterface_config,
            retract=cleared,
            settlement_cohort=settlement_cohort,
        )

    if apply_requested:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True, stream=delivery.stream, settlement_cohort=settlement_cohort)

    result = {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
    await record_response(db, device_id, delivery, result)
    await db.commit()
    return result
