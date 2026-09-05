# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/vlan-database and /switchport."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import (
    RESP_401,
    RESP_404_DEVICE,
    RESP_409_PUSH_SEQ,
    RESP_422_VALIDATION,
    IntentApplyResult,
    StoredIntentResult,
    api_error,
)
from nso_adapter.api.intent_push import begin_delivery, get_intent_delivery
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import UtcInstant
from nso_adapter.core.generation import DeviceProjectionGone
from nso_adapter.core.removal import is_cleared
from nso_adapter.core.switching_intent import (
    SwitchingRequestRefused,
    SwitchportSnapshot,
    replace_switchport_snapshot,
)
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, DeviceSwitchport, DeviceVlan, VlanIntent

router = APIRouter(prefix="/api/v1/devices", tags=["vlan"])


Uint16 = Annotated[int, Field(strict=True, ge=0, le=65535)]
RootName = Annotated[str, Field(min_length=1, max_length=128)]


class _StrictSwitchportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SwitchportApply(_StrictSwitchportRequest):
    interface_name: str = Field(min_length=1, max_length=128)
    mode: str | None = Field(default=None, max_length=16)
    untagged_vlan: Uint16 | None = None
    tagged_vlans: list[Uint16] = Field(default_factory=list)

    @field_validator("tagged_vlans")
    @classmethod
    def _tagged_vlans_are_unique(cls, tagged_vlans: list[int]) -> list[int]:
        if len(tagged_vlans) != len(set(tagged_vlans)):
            raise ValueError("tagged_vlans values must be unique")
        return tagged_vlans


class SwitchportApplyRequest(_StrictSwitchportRequest):
    interfaces: list[SwitchportApply]
    #: The switchport roots this preparation authorizes RETRACTING from the device.
    #: Required, an explicit empty list included — see ``LagConfigApplyRequest``.
    deleted_roots: list[RootName]

    @field_validator("interfaces")
    @classmethod
    def _interface_names_are_unique(cls, interfaces: list[SwitchportApply]) -> list[SwitchportApply]:
        names = [interface.interface_name for interface in interfaces]
        if len(names) != len(set(names)):
            raise ValueError("interface_name values must be unique")
        return interfaces


# ── Read-mirror response models ───────────────────────────────────────────────
# Fixed shapes with no top-level timestamp; every key always present (untagged_vlan
# null / tagged_vlans [] when empty, name/mode coerced to ""), so no exclude_unset.


class VlanOut(BaseModel):
    vlan_id: int
    name: str
    source: str


class VlanDatabaseOut(BaseModel):
    device_id: int
    read_state: FamilyReadState  # the S4 truth (this family never had legacy freshness fields)
    vlans: list[VlanOut]


class SwitchportIfaceOut(BaseModel):
    interface_name: str
    mode: str
    untagged_vlan: int | None
    tagged_vlans: list[int]
    source: str


class SwitchportOut(BaseModel):
    device_id: int
    read_state: FamilyReadState  # the S4 truth (this family never had legacy freshness fields)
    interfaces: list[SwitchportIfaceOut]


@router.get(
    "/{device_id}/vlan-database",
    dependencies=[Depends(verify_token)],
    response_model=VlanDatabaseOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_vlan_database(device_id: int, db: AsyncSession = Depends(get_read_db)):
    if (device := await db.get(Device, device_id)) is None:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "vlan"), source_epoch=device.source_epoch
    )

    rows = (
        (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device_id).order_by(DeviceVlan.vlan_id)))
        .scalars()
        .all()
    )
    return {
        "device_id": device_id,
        "read_state": read_state,
        "vlans": [{"vlan_id": r.vlan_id, "name": r.name or "", "source": "vlan-database"} for r in rows],
    }


@router.get(
    "/{device_id}/switchport",
    dependencies=[Depends(verify_token)],
    response_model=SwitchportOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_switchport(device_id: int, db: AsyncSession = Depends(get_read_db)):
    if (device := await db.get(Device, device_id)) is None:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "switchport"), source_epoch=device.source_epoch
    )

    rows = (
        (
            await db.execute(
                select(DeviceSwitchport)
                .where(DeviceSwitchport.device_id == device_id)
                .order_by(DeviceSwitchport.interface_name)
                .options(
                    selectinload(DeviceSwitchport.untagged_vlan),
                    selectinload(DeviceSwitchport.tagged_vlans),
                )
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
                "mode": r.mode or "",
                "untagged_vlan": r.untagged_vlan.vlan_id if r.untagged_vlan else None,
                "tagged_vlans": sorted(v.vlan_id for v in r.tagged_vlans),
                "source": "switchport",
            }
            for r in rows
        ],
    }


@router.post(
    "/{device_id}/switchport/apply",
    dependencies=[Depends(verify_token)],
    response_model=StoredIntentResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def apply_switchport(
    device_id: int,
    payload: SwitchportApplyRequest,
    db: AsyncSession = Depends(get_db),
):
    interfaces = tuple(
        SwitchportSnapshot(
            interface_name=interface.interface_name,
            mode=interface.mode,
            untagged_vlan=interface.untagged_vlan,
            tagged_vlans=tuple(interface.tagged_vlans),
        )
        for interface in payload.interfaces
    )
    try:
        prepared = await replace_switchport_snapshot(db, device_id, interfaces, deleted_roots=payload.deleted_roots)
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


# ---------------------------------------------------------------------------
# PUT /{device_id}/vlan-intent (VLAN-database write path — deferred apply)
# ---------------------------------------------------------------------------


class VlanEntry(BaseModel):
    vlan_id: int
    name: str = ""
    accepted_at: UtcInstant | None = None


# Scalars the writer emits only when set — `if row.name:` (nso/apply.py)
# A merge-PATCH apply can never drop one that goes back to unset, so clearing any of
# them must enqueue a PUT-replace retract. See core.removal.is_cleared.
_STATE_FIELDS = ("name",)


class VlanIntentUpdate(BaseModel):
    vlans: list[VlanEntry]


@router.put(
    "/{device_id}/vlan-intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentApplyResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ, **RESP_422_VALIDATION},
)
async def put_vlan_intent(
    device_id: int,
    body: VlanIntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's VLAN-database intent mirror for this device atomically.

    Full-replace: rows not in the body are deleted. ``accepted_at`` defaults to now.
    If ``auto_apply`` is enabled on the device, an apply job is enqueued. The single
    device Apply commits these via the vlan-reconciler.

    ONE transaction covers the whole delivery (#1522 §G1/§G2): the receipt that admits this
    ``X-Push-Seq``, the row changes, the projection revision, the deployment generations the
    change authorizes, their jobs, and the stored response. A re-delivery of the same
    sequence replays that response and applies nothing; the same sequence with a different
    body, or an older sequence, is refused.
    """
    from nso_adapter.core.receipt import record_response

    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")

    # Takes the device's projection lock BEFORE anything is read, so the rows this request
    # mutates, the document its generation snapshots and the receipt that admits it are all
    # one serialized unit against every other writer of this device.
    if (replay := await begin_delivery(db, device_id, delivery)) is not None:
        return replay

    existing = await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))
    existing_rows: dict[int, VlanIntent] = {r.vlan_id: r for r in existing.scalars().all()}
    new_keys = {v.vlan_id for v in body.vlans}

    removed_vids = [vid for vid in existing_rows if vid not in new_keys]
    for vid in removed_vids:
        await db.delete(existing_rows[vid])
    await db.flush()

    now = datetime.now(UTC)
    count = 0
    cleared = False
    for item in body.vlans:
        accepted = item.accepted_at if item.accepted_at else now
        row = existing_rows.get(item.vlan_id)
        before = {f: getattr(row, f) for f in _STATE_FIELDS} if row is not None else None
        if row is None:
            row = VlanIntent(device_id=device_id, vlan_id=item.vlan_id)
            db.add(row)
        row.name = item.name or None
        row.accepted_at = accepted
        if before is not None and any(is_cleared(before[f], getattr(row, f)) for f in _STATE_FIELDS):
            cleared = True
        count += 1

    await db.flush()
    from nso_adapter.core.generation import prepare_request_settlement

    apply_requested, settlement_cohort = await prepare_request_settlement(
        db,
        device_id,
        mutation_count=count,
        removal_generation_count=int(bool(removed_vids or cleared)),
    )

    # Removal propagation: a dropped vid won't be removed by the next merge-PATCH apply, so
    # PUT-replace the vlan-reconciler instance with the remaining list. Enqueued BEFORE the
    # apply so it carries the lower job id and the worker runs it first, and inside THIS
    # transaction so the shrink cannot outlive the generation that authorizes it.
    replaced = False
    if removed_vids or cleared:
        from nso_adapter.core.removal import replace_on_removal

        replaced = await replace_on_removal(
            db,
            device,
            removed_vids,
            VlanIntent,
            stream=delivery.stream,
            retract=cleared,
            settlement_cohort=settlement_cohort,
        )

    if apply_requested:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True, stream=delivery.stream, settlement_cohort=settlement_cohort)

    result = {"device_id": device_id, "count": count, "removed": len(removed_vids), "replaced": replaced}
    await record_response(db, device_id, delivery, result)
    await db.commit()
    return result
