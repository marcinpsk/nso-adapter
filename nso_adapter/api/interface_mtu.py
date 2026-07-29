# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/interface-mtu — per-interface MTU, read (Phase 2b).

PUT /api/v1/devices/{id}/interface-mtu-intent — write path (Phase 2b).
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, IntentApplyResult, api_error
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import UtcInstant
from nso_adapter.core.removal import is_cleared
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, DeviceInterfaceMtu, DeviceSettings, InterfaceMtuIntent

router = APIRouter(prefix="/api/v1/devices", tags=["interface-mtu"])


# ── Read-mirror response models (GET /interface-mtu) ──────────────────────────
# Fixed/EMIT-NULL shape, NO top-level timestamp: mtu/ip_mtu/mpls_mtu always
# present (null when unset), bound_port coerced to "". No exclude_unset.


class InterfaceMtuEntryOut(BaseModel):
    interface_name: str
    mtu: int | None
    ip_mtu: int | None
    mpls_mtu: int | None
    bound_port: str


class InterfaceMtuOut(BaseModel):
    device_id: int
    read_state: FamilyReadState  # the S4 truth (this family never had legacy freshness fields)
    interfaces: list[InterfaceMtuEntryOut]


@router.get(
    "/{device_id}/interface-mtu",
    dependencies=[Depends(verify_token)],
    response_model=InterfaceMtuOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_interface_mtu(device_id: int, db: AsyncSession = Depends(get_read_db)):
    """Return the device's per-interface MTU (mtu / ip-mtu / mpls-mtu + bound-port)."""
    if (device := await db.get(Device, device_id)) is None:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "interface_mtu"), source_epoch=device.source_epoch
    )
    rows = (
        (
            await db.execute(
                select(DeviceInterfaceMtu)
                .where(DeviceInterfaceMtu.device_id == device_id)
                .order_by(DeviceInterfaceMtu.interface_name)
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
                "mtu": r.mtu,
                "ip_mtu": r.ip_mtu,
                "mpls_mtu": r.mpls_mtu,
                "bound_port": r.bound_port or "",
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/interface-mtu-intent  (Phase 2b write path)
# ---------------------------------------------------------------------------


class InterfaceMtuEntry(BaseModel):
    interface_name: str
    mtu: int | None = None
    ip_mtu: int | None = None
    mpls_mtu: int | None = None
    accepted_at: UtcInstant | None = None


# Scalars the writer emits only when set (`if row.mtu is not None:`, nso/apply.py) — a
# merge-PATCH apply can never drop one that goes back to unset, so clearing any of them
# must enqueue a PUT-replace retract. See core.removal.is_cleared.
_STATE_FIELDS = ("mtu", "ip_mtu", "mpls_mtu")


class InterfaceMtuIntentUpdate(BaseModel):
    interfaces: list[InterfaceMtuEntry]


@router.put(
    "/{device_id}/interface-mtu-intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentApplyResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def put_interface_mtu_intent(device_id: int, body: InterfaceMtuIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's per-interface MTU intent mirror for this device atomically.

    Full-replace: rows not in the body are deleted (and the device MTU reverted via
    a PUT-replace of the mtu-reconciler service). ``accepted_at`` defaults to now.
    If ``auto_apply`` is enabled on the device, an apply job is enqueued.
    """
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")

    existing = await db.execute(select(InterfaceMtuIntent).where(InterfaceMtuIntent.device_id == device_id))
    existing_rows: dict[str, InterfaceMtuIntent] = {r.interface_name: r for r in existing.scalars().all()}
    new_keys = {i.interface_name for i in body.interfaces}

    removed = [name for name in existing_rows if name not in new_keys]
    for name in removed:
        await db.delete(existing_rows[name])
    await db.flush()

    now = datetime.now(UTC)
    count = 0
    cleared = False
    for item in body.interfaces:
        accepted = item.accepted_at if item.accepted_at else now
        row = existing_rows.get(item.interface_name)
        before = {f: getattr(row, f) for f in _STATE_FIELDS} if row is not None else None
        if row is None:
            row = InterfaceMtuIntent(device_id=device_id, interface_name=item.interface_name)
            db.add(row)
        row.mtu = item.mtu
        row.ip_mtu = item.ip_mtu
        row.mpls_mtu = item.mpls_mtu
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
        from nso_adapter.nso.apply import apply_mtu_config

        replaced = await replace_on_removal(db, device, removed, InterfaceMtuIntent, apply_mtu_config, retract=cleared)

    return {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
