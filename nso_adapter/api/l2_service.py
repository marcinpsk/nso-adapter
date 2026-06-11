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

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceL2Sap, DeviceSettings, L2SapIntent

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["l2-service"])


@router.get("/{device_id}/l2-services", dependencies=[Depends(verify_token)])
async def get_l2_services(device_id: int, db: AsyncSession = Depends(get_db)):
    if await db.get(Device, device_id) is None:
        raise api_error(404, "not_found", "Device not found")

    rows = (
        await db.execute(
            select(DeviceL2Sap)
            .where(DeviceL2Sap.device_id == device_id)
            .order_by(DeviceL2Sap.service_name, DeviceL2Sap.sap_id)
        )
    ).scalars().all()

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
        svc["saps"].append(
            {"sap_id": r.sap_id, "port": r.port, "outer_tag": r.outer_tag, "inner_tag": r.inner_tag}
        )

    return {"device_id": device_id, "services": list(services.values())}


# ---------------------------------------------------------------------------
# PUT /{device_id}/l2-sap-intent  (M37 P2b write path)
# ---------------------------------------------------------------------------


class L2SapEntry(BaseModel):
    service_name: str
    service_type: str  # epipe | vpls
    sap_id: str
    port: str = ""
    outer_tag: int | None = None
    inner_tag: int | None = None
    accepted_at: datetime | None = None


class L2SapIntentUpdate(BaseModel):
    saps: list[L2SapEntry]


@router.put("/{device_id}/l2-sap-intent", dependencies=[Depends(verify_token)])
async def put_l2_sap_intent(device_id: int, body: L2SapIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's L2 SAP intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied. If ``auto_apply`` is enabled
    on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")

    existing_result = await db.execute(select(L2SapIntent).where(L2SapIntent.device_id == device_id))
    existing_rows: dict[tuple, L2SapIntent] = {
        (r.service_name, r.sap_id): r for r in existing_result.scalars().all()
    }

    new_keys: set[tuple] = {(item.service_name, item.sap_id) for item in body.saps}

    removed = [key for key in existing_rows if key not in new_keys]
    for key in removed:
        await db.delete(existing_rows[key])
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    for item in body.saps:
        key = (item.service_name, item.sap_id)
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        if key in existing_rows:
            row = existing_rows[key]
            row.service_type = item.service_type
            row.port = item.port
            row.outer_tag = item.outer_tag
            row.inner_tag = item.inner_tag
            row.accepted_at = accepted
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

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()

    replaced = False
    if removed:
        from nso_adapter.core.removal import replace_on_removal
        from nso_adapter.nso.apply import apply_l2_saps

        replaced = await replace_on_removal(db, device, removed, L2SapIntent, apply_l2_saps)

    return {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
