# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/l2-services endpoint (Nokia epipe/vpls + SAPs, read-only)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceL2Sap

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
