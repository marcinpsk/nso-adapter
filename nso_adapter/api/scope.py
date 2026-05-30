# SPDX-License-Identifier: Apache-2.0
"""Scope API — manage per-device attribute scope."""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.store.models import Device, DeviceSettings, ManagedScope

router = APIRouter(prefix="/api/v1/devices", tags=["scope"])


def _scope_out(device_id: int, attrs: list[ManagedScope], settings: DeviceSettings | None) -> dict:
    updated_at = max((s.updated_at for s in attrs), default=datetime.utcnow())
    return {
        "device_id": device_id,
        "attributes": [s.attribute for s in attrs],
        "auto_apply": settings.auto_apply if settings else False,
        "updated_at": updated_at.isoformat() + "Z",
    }


@router.get("/{device_id}/scope", dependencies=[Depends(verify_token)])
async def get_scope(device_id: int, db: AsyncSession = Depends(get_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    scope_result = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
    attrs = list(scope_result.scalars().all())
    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    return _scope_out(device_id, attrs, settings)


class ScopeUpdate(BaseModel):
    attributes: list[str]
    auto_apply: bool = False


@router.put("/{device_id}/scope", dependencies=[Depends(verify_token)])
async def update_scope(device_id: int, body: ScopeUpdate, db: AsyncSession = Depends(get_db)):
    from nso_adapter.core.onboarding import set_scope

    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    attrs = await set_scope(db, device, body.attributes)

    # Upsert DeviceSettings
    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings is None:
        settings = DeviceSettings(device_id=device_id, auto_apply=body.auto_apply)
        db.add(settings)
    else:
        settings.auto_apply = body.auto_apply
    await db.commit()

    return _scope_out(device_id, attrs, settings)
