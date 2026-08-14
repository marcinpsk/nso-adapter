# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""L2 service API: GET /devices/{id}/l2-services (read) + PUT /devices/{id}/l2-sap-intent (write)."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
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
from nso_adapter.store.models import Device, DeviceL2Sap, DeviceSettings, L2SapIntent

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["l2-service"])


# ── Read-mirror response models (GET /l2-services) ────────────────────────────
# EMIT-NULL fixed shape (service_id/outer_tag/inner_tag null when unset), and the
# response has NO top-level timestamp — so this endpoint does NOT use exclude_unset.


class L2SapOut(BaseModel):
    sap_id: str
    port: str
    outer_tag: int | None
    inner_tag: int | None


class L2ServiceOut(BaseModel):
    service_name: str
    service_type: str
    service_id: int | None
    saps: list[L2SapOut]


class L2ServicesOut(BaseModel):
    device_id: int
    read_state: FamilyReadState  # the S4 truth (this family never had legacy freshness fields)
    services: list[L2ServiceOut]


@router.get(
    "/{device_id}/l2-services",
    dependencies=[Depends(verify_token)],
    response_model=L2ServicesOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_l2_services(device_id: int, db: AsyncSession = Depends(get_read_db)):
    if (device := await db.get(Device, device_id)) is None:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "l2_service"), source_epoch=device.source_epoch
    )

    rows = (
        (
            await db.execute(
                select(DeviceL2Sap)
                .where(DeviceL2Sap.device_id == device_id)
                .order_by(DeviceL2Sap.service_name, DeviceL2Sap.sap_id)
            )
        )
        .scalars()
        .all()
    )

    services: dict[str, dict] = {}
    for r in rows:
        svc = services.setdefault(
            r.service_name,
            {
                "service_name": r.service_name,
                "service_type": r.service_type,
                "service_id": r.service_id,
                "saps": [],
            },
        )
        svc["saps"].append({"sap_id": r.sap_id, "port": r.port, "outer_tag": r.outer_tag, "inner_tag": r.inner_tag})

    return {"device_id": device_id, "read_state": read_state, "services": list(services.values())}


# ---------------------------------------------------------------------------
# PUT /{device_id}/l2-sap-intent (write path)
# ---------------------------------------------------------------------------


class L2SapEntry(BaseModel):
    service_name: str
    service_type: str  # epipe | vpls
    sap_id: str
    port: str = ""
    outer_tag: int | None = None
    inner_tag: int | None = None
    accepted_at: UtcInstant | None = None


# Scalars the writer emits only when set — `if row.port:` / `if row.outer_tag is not None:` (nso/apply.py)
# A merge-PATCH apply can never drop one that goes back to unset, so clearing any of
# them must enqueue a PUT-replace retract. See core.removal.is_cleared.
_STATE_FIELDS = ("port", "outer_tag", "inner_tag")


class L2SapIntentUpdate(BaseModel):
    saps: list[L2SapEntry]


@router.put(
    "/{device_id}/l2-sap-intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentApplyResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ, **RESP_422_VALIDATION},
)
async def put_l2_sap_intent(
    device_id: int,
    body: L2SapIntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's L2 SAP intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied. If ``auto_apply`` is enabled
    on the device, an apply job is enqueued after the upsert.
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

    existing_result = await db.execute(select(L2SapIntent).where(L2SapIntent.device_id == device_id))
    existing_rows: dict[tuple, L2SapIntent] = {(r.service_name, r.sap_id): r for r in existing_result.scalars().all()}

    new_keys: set[tuple] = {(item.service_name, item.sap_id) for item in body.saps}

    removed = [key for key in existing_rows if key not in new_keys]
    for key in removed:
        await db.delete(existing_rows[key])
    await db.flush()

    now = datetime.now(UTC)
    count = 0
    cleared = False
    for item in body.saps:
        key = (item.service_name, item.sap_id)
        accepted = item.accepted_at if item.accepted_at else now
        if key in existing_rows:
            row = existing_rows[key]
            before = {f: getattr(row, f) for f in _STATE_FIELDS}
            row.service_type = item.service_type
            row.port = item.port
            row.outer_tag = item.outer_tag
            row.inner_tag = item.inner_tag
            row.accepted_at = accepted
            if any(is_cleared(before[f], getattr(row, f)) for f in _STATE_FIELDS):
                cleared = True
        else:
            row = L2SapIntent(
                device_id=device_id,
                service_name=item.service_name,
                service_type=item.service_type,
                sap_id=item.sap_id,
                port=item.port,
                outer_tag=item.outer_tag,
                inner_tag=item.inner_tag,
                accepted_at=accepted,
            )
            db.add(row)
        count += 1

    await db.flush()

    replaced = False
    if removed or cleared:
        from nso_adapter.core.removal import replace_on_removal
        from nso_adapter.nso.apply import apply_l2_saps

        replaced = await replace_on_removal(db, device, removed, L2SapIntent, apply_l2_saps, retract=cleared)

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True, stream=delivery.stream)

    result = {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
    await record_response(db, device_id, delivery, result)
    await db.commit()
    return result
