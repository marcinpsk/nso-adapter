# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/bfd — per-interface BFD read mirror."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, IntentApplyResult, api_error
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.core.removal import is_cleared
from nso_adapter.store import outcome_store
from nso_adapter.store.models import BfdIntent, Device, DeviceBfdInterface, DeviceSettings

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["bfd"])


# ── Read-mirror response models (GET /bfd) ────────────────────────────────────
# OMIT shape: bound_port/min_tx/min_rx/multiplier omitted when unset ->
# exclude_unset; micro_bfd/enabled are always-present bools.


class BfdInterfaceOut(BaseModel):
    interface_name: str
    micro_bfd: bool
    enabled: bool
    bound_port: str | None = None
    min_tx: int | None = None
    min_rx: int | None = None
    multiplier: int | None = None


class BfdConfigOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # reader formats "<iso>Z"; None when never refreshed
    refresh_source: str  # legacy freshness (S5 retires it); read_state is the S4 truth
    read_state: FamilyReadState
    interfaces: list[BfdInterfaceOut]


@router.get(
    "/{device_id}/bfd",
    dependencies=[Depends(verify_token)],
    response_model=BfdConfigOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_bfd(device_id: int, db: AsyncSession = Depends(get_read_db)):
    """Return the per-interface BFD read-mirror for this device."""
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(await outcome_store.get_current_outcome(db, device_id, "bfd"))

    rows = (
        (
            await db.execute(
                select(DeviceBfdInterface)
                .where(DeviceBfdInterface.device_id == device_id)
                .order_by(DeviceBfdInterface.interface_name)
            )
        )
        .scalars()
        .all()
    )

    latest = max((r.last_refreshed_at for r in rows if r.last_refreshed_at), default=None)
    interfaces = []
    for r in rows:
        entry: dict = {
            "interface_name": r.interface_name,
            "micro_bfd": r.micro_bfd,
            "enabled": r.enabled,
        }
        if r.bound_port is not None:
            entry["bound_port"] = r.bound_port
        if r.min_tx is not None:
            entry["min_tx"] = r.min_tx
        if r.min_rx is not None:
            entry["min_rx"] = r.min_rx
        if r.multiplier is not None:
            entry["multiplier"] = r.multiplier
        interfaces.append(entry)

    return {
        "device_id": device_id,
        "last_refreshed_at": latest.isoformat() + "Z" if latest else None,
        "refresh_source": rows[0].refresh_source if rows else "never",
        "read_state": read_state,
        "interfaces": interfaces,
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/bfd-intent  (BFD write path — deferred apply)
# ---------------------------------------------------------------------------


class BfdEntry(BaseModel):
    interface_name: str
    min_tx: int | None = None
    min_rx: int | None = None
    multiplier: int | None = None
    micro_bfd: bool = False
    accepted_at: datetime | None = None


# Scalars the writer emits only when set — `if row.min_tx is not None:` (nso/apply.py)
# A merge-PATCH apply can never drop one that goes back to unset, so clearing any of
# them must enqueue a PUT-replace retract. See core.removal.is_cleared.
_STATE_FIELDS = ("min_tx", "min_rx", "multiplier")


class BfdIntentUpdate(BaseModel):
    interfaces: list[BfdEntry]


@router.put(
    "/{device_id}/bfd-intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentApplyResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def put_bfd_intent(device_id: int, body: BfdIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's per-interface BFD intent mirror for this device atomically.

    Full-replace: rows not in the body are deleted. ``accepted_at`` defaults to now.
    If ``auto_apply`` is enabled, an apply job is enqueued; the single device Apply
    commits these via the bfd-reconciler.
    """
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")

    existing = await db.execute(select(BfdIntent).where(BfdIntent.device_id == device_id))
    existing_rows: dict[str, BfdIntent] = {r.interface_name: r for r in existing.scalars().all()}
    new_keys = {i.interface_name for i in body.interfaces}

    removed = [name for name in existing_rows if name not in new_keys]
    for name in removed:
        await db.delete(existing_rows[name])
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    cleared = False
    for item in body.interfaces:
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        row = existing_rows.get(item.interface_name)
        before = {f: getattr(row, f) for f in _STATE_FIELDS} if row is not None else None
        if row is None:
            row = BfdIntent(device_id=device_id, interface_name=item.interface_name)
            db.add(row)
        row.min_tx = item.min_tx
        row.min_rx = item.min_rx
        row.multiplier = item.multiplier
        row.micro_bfd = item.micro_bfd
        row.accepted_at = accepted
        if before is not None and any(is_cleared(before[f], getattr(row, f)) for f in _STATE_FIELDS):
            cleared = True
        count += 1

    await db.flush()
    settings = (
        await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    ).scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()

    replaced = False
    if removed or cleared:
        from nso_adapter.core.removal import replace_on_removal
        from nso_adapter.nso.apply import apply_bfd_config

        replaced = await replace_on_removal(db, device, removed, BfdIntent, apply_bfd_config, retract=cleared)

    return {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
