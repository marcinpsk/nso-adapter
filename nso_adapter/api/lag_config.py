# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/lag-config endpoint."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, api_error
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.core.importer import get_nso_client
from nso_adapter.core.lag_intent import apply_lag_config as apply_lag_config_core
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, LagBundleConfig

router = APIRouter(prefix="/api/v1/devices", tags=["lag-config"])


class LagMemberApply(BaseModel):
    interface_name: str
    mode: str | None = None
    port_priority: int | None = None


class LagBundleApply(BaseModel):
    name: str
    lag_id: int
    min_links: int | None = None
    system_priority: int | None = None
    system_id: str | None = None
    timer: str | None = None
    admin_key: int | None = None
    members: list[LagMemberApply] = []


class LagConfigApplyRequest(BaseModel):
    bundles: list[LagBundleApply] = []


# ── Read-mirror response models (GET /lag-config) ─────────────────────────────
# OMIT shape: bundle/member optionals are omitted when unset -> exclude_unset.


class LagMemberOut(BaseModel):
    interface_name: str
    mode: str | None = None
    port_priority: int | None = None


class LagBundleOut(BaseModel):
    name: str
    lag_id: int
    min_links: int | None = None
    system_priority: int | None = None
    system_id: str | None = None
    timer: str | None = None
    admin_key: int | None = None
    # NX-P2: True for a vPC-protected bundle (omitted/False = ordinary, onboardable). The plugin
    # gates Accept on this so a vPC bundle never enters a writable intent (the writer refuses it).
    vpc_sensitive: bool = False
    members: list[LagMemberOut]


class LagConfigOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # reader formats "<iso>Z"; None when never refreshed
    refresh_source: str  # legacy freshness (S5 retires it); read_state is the S4 truth
    read_state: FamilyReadState
    bundles: list[LagBundleOut]


@router.get(
    "/{device_id}/lag-config",
    dependencies=[Depends(verify_token)],
    response_model=LagConfigOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_lag_config(device_id: int, db: AsyncSession = Depends(get_read_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(await outcome_store.get_current_outcome(db, device_id, "lag_config"))

    result = await db.execute(
        select(LagBundleConfig)
        .where(LagBundleConfig.device_id == device_id)
        .options(selectinload(LagBundleConfig.members))
    )
    bundles = result.scalars().all()

    if not bundles:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "read_state": read_state,
            "bundles": [],
        }

    latest = max(bundles, key=lambda b: b.last_refreshed_at)

    return {
        "device_id": device_id,
        "last_refreshed_at": latest.last_refreshed_at.isoformat() + "Z",
        "refresh_source": latest.refresh_source,
        "read_state": read_state,
        "bundles": [
            {
                "name": b.name,
                "lag_id": b.lag_id,
                **({"min_links": b.min_links} if b.min_links is not None else {}),
                **({"system_priority": b.system_priority} if b.system_priority is not None else {}),
                **({"system_id": b.system_id} if b.system_id is not None else {}),
                **({"timer": b.timer} if b.timer is not None else {}),
                **({"admin_key": b.admin_key} if b.admin_key is not None else {}),
                # OMIT shape: emit only when True (ordinary bundles read False via the model default)
                **({"vpc_sensitive": True} if b.vpc_sensitive else {}),
                "members": [
                    {
                        "interface_name": m.interface_name,
                        **({"mode": m.mode} if m.mode is not None else {}),
                        **({"port_priority": m.port_priority} if m.port_priority is not None else {}),
                    }
                    for m in b.members
                ],
            }
            for b in bundles
        ],
    }


# Union result envelope, documented via responses={200: {...}} + response_model=None so the
# handler dict passes through untouched (zero wire risk). See core.lag_intent for the branches.


class LagConfigApplyDeployed(BaseModel):
    status: Literal["deployed"]
    device: str
    bundle_count: int


class LagConfigApplyError(BaseModel):
    status: Literal["error"]
    error: str
    message: str
    detail: dict | None = None  # present only on an NSO commit failure (absent on no_nso_device_name)


LagConfigApplyResult = LagConfigApplyDeployed | LagConfigApplyError


@router.post(
    "/{device_id}/lag-config/apply",
    dependencies=[Depends(verify_token)],
    response_model=None,
    responses={
        200: {"model": LagConfigApplyResult, "description": "Apply result envelope (deployed | error)"},
        **RESP_401,
        **RESP_404_DEVICE,
        **RESP_422_VALIDATION,
    },
)
async def apply_lag_config(
    device_id: int,
    payload: LagConfigApplyRequest,
    db: AsyncSession = Depends(get_db),
):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    nso_client = get_nso_client(device.nso_instance)
    return await apply_lag_config_core(device, payload, nso_client)
