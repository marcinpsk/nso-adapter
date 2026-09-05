# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/lag-config endpoint."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, StoredIntentResult, api_error
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import iso_z, latest_refreshed
from nso_adapter.core.generation import DeviceProjectionGone
from nso_adapter.core.switching_intent import (
    LagBundleSnapshot,
    LagMemberSnapshot,
    SwitchingRequestRefused,
    replace_lag_snapshot,
)
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, LagBundleConfig

router = APIRouter(prefix="/api/v1/devices", tags=["lag-config"])


Uint16 = Annotated[int, Field(strict=True, ge=0, le=65535)]
Uint32 = Annotated[int, Field(strict=True, ge=0, le=4294967295)]
RootName = Annotated[str, Field(min_length=1, max_length=128)]


class _StrictRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LagMemberApply(_StrictRequestModel):
    interface_name: str = Field(min_length=1, max_length=128)
    mode: str | None = Field(default=None, max_length=16)
    port_priority: Uint16 | None = None


class LagBundleApply(_StrictRequestModel):
    name: str = Field(min_length=1, max_length=128)
    lag_id: Uint32
    min_links: Uint16 | None = None
    system_priority: Uint16 | None = None
    system_id: str | None = Field(default=None, max_length=17)
    timer: str | None = Field(default=None, max_length=8)
    admin_key: Uint16 | None = None
    members: list[LagMemberApply] = Field(default_factory=list)

    @field_validator("members")
    @classmethod
    def _member_names_are_unique(cls, members: list[LagMemberApply]) -> list[LagMemberApply]:
        names = [member.interface_name for member in members]
        if len(names) != len(set(names)):
            raise ValueError("member interface_name values must be unique within a bundle")
        return members


class LagConfigApplyRequest(_StrictRequestModel):
    bundles: list[LagBundleApply]
    #: The bundle roots this preparation authorizes RETRACTING from the device. Required,
    #: an explicit empty list included: an omitted root with no marking is an un-own, and
    #: the two cannot be told apart from the snapshot alone.
    deleted_roots: list[RootName]

    @field_validator("bundles")
    @classmethod
    def _bundle_names_are_unique(cls, bundles: list[LagBundleApply]) -> list[LagBundleApply]:
        names = [bundle.name for bundle in bundles]
        if len(names) != len(set(names)):
            raise ValueError("bundle name values must be unique")
        return bundles


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
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "lag_config"), source_epoch=device.source_epoch
    )

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

    latest = latest_refreshed(bundles)

    return {
        "device_id": device_id,
        "last_refreshed_at": iso_z(latest.last_refreshed_at),
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


@router.post(
    "/{device_id}/lag-config/apply",
    dependencies=[Depends(verify_token)],
    response_model=StoredIntentResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def apply_lag_config(
    device_id: int,
    payload: LagConfigApplyRequest,
    db: AsyncSession = Depends(get_db),
):
    bundles = tuple(
        LagBundleSnapshot(
            name=bundle.name,
            lag_id=bundle.lag_id,
            min_links=bundle.min_links,
            system_priority=bundle.system_priority,
            system_id=bundle.system_id,
            timer=bundle.timer,
            admin_key=bundle.admin_key,
            members=tuple(
                LagMemberSnapshot(
                    interface_name=member.interface_name,
                    mode=member.mode,
                    port_priority=member.port_priority,
                )
                for member in bundle.members
            ),
        )
        for bundle in payload.bundles
    )
    try:
        prepared = await replace_lag_snapshot(db, device_id, bundles, deleted_roots=payload.deleted_roots)
    except DeviceProjectionGone:
        raise api_error(404, "not_found", "Device not found")
    except SwitchingRequestRefused as exc:
        await db.rollback()
        raise api_error(422, "validation_error", str(exc)) from None
    await db.commit()
    return {
        "status": prepared.status,
        "device_id": device_id,
        "stream": prepared.stream,
        "count": prepared.count,
        "removed": prepared.removed,
        "desired_revision": prepared.desired_revision,
        "selection_revision": prepared.selection_revision,
    }
