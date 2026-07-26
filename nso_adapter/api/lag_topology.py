# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/lag-topology endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.api.deps import get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, api_error
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, LagInterface

router = APIRouter(prefix="/api/v1/devices", tags=["lag-topology"])


# ── Read-mirror response models (GET /lag-topology) ───────────────────────────
# Fixed shape; member ``mode`` is a non-null str. "<iso>Z" timestamp.


class LagTopologyMemberOut(BaseModel):
    interface: str
    mode: str


class LagTopologyLagOut(BaseModel):
    name: str
    id: int
    members: list[LagTopologyMemberOut]


class LagTopologyOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # reader formats "<iso>Z"; None when never refreshed
    refresh_source: str  # legacy freshness (S5 retires it); read_state is the S4 truth
    read_state: FamilyReadState
    lags: list[LagTopologyLagOut]


@router.get(
    "/{device_id}/lag-topology",
    dependencies=[Depends(verify_token)],
    response_model=LagTopologyOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_lag_topology(device_id: int, db: AsyncSession = Depends(get_read_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "lag"), source_epoch=device.source_epoch
    )

    result = await db.execute(
        select(LagInterface).where(LagInterface.device_id == device_id).options(selectinload(LagInterface.members))
    )
    lags = result.scalars().all()

    if not lags:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "read_state": read_state,
            "lags": [],
        }

    latest = max(lags, key=lambda lag: lag.last_refreshed_at)

    return {
        "device_id": device_id,
        "last_refreshed_at": latest.last_refreshed_at.isoformat() + "Z",
        "refresh_source": latest.refresh_source,
        "read_state": read_state,
        "lags": [
            {
                "name": lag.name,
                "id": lag.lag_id,
                "members": [{"interface": m.interface_name, "mode": m.mode} for m in lag.members],
            }
            for lag in lags
        ],
    }
