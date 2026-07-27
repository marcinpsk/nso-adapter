# SPDX-License-Identifier: Apache-2.0
"""Scope API — manage per-device attribute scope."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, api_error
from nso_adapter.store.models import Device, DeviceSettings, ManagedScope

router = APIRouter(prefix="/api/v1/devices", tags=["scope"])


class ScopeOut(BaseModel):
    """EMIT-NULL fixed-key scope shape (updated_at = max attr time, or a read-time mint if empty)."""

    device_id: int
    attributes: list[str]
    auto_apply: bool
    sync_before_apply: bool
    updated_at: str


def _scope_out(device_id: int, attrs: list[ManagedScope], settings: DeviceSettings | None) -> dict:
    updated_at = max((s.updated_at for s in attrs), default=datetime.now(UTC).replace(tzinfo=None))
    return {
        "device_id": device_id,
        "attributes": [s.attribute for s in attrs],
        "auto_apply": settings.auto_apply if settings else False,
        "sync_before_apply": settings.sync_before_apply if settings else True,
        "updated_at": updated_at.isoformat() + "Z",
    }


@router.get(
    "/{device_id}/scope",
    dependencies=[Depends(verify_token)],
    response_model=ScopeOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
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
    sync_before_apply: bool = True
    # Fast-path mgmt-IP failover inputs (NetBox primary_ip / oob_ip, host only). Optional so
    # an older plugin that omits them doesn't clear stored IPs; an explicit null DOES clear.
    primary_ip: str | None = None
    oob_ip: str | None = None


@router.put(
    "/{device_id}/scope",
    dependencies=[Depends(verify_token)],
    response_model=ScopeOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
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
        settings = DeviceSettings(
            device_id=device_id,
            auto_apply=body.auto_apply,
            sync_before_apply=body.sync_before_apply,
        )
        db.add(settings)
    else:
        settings.auto_apply = body.auto_apply
        settings.sync_before_apply = body.sync_before_apply

    # Only touch failover IPs when the caller actually sent them (an explicit null clears).
    if body.model_fields_set & {"primary_ip", "oob_ip"}:
        from nso_adapter.core.failover import upsert_failover_ips

        await upsert_failover_ips(db, device, body.primary_ip, body.oob_ip)
    await db.commit()

    return _scope_out(device_id, attrs, settings)
