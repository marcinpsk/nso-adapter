# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/svi — L3 VLAN interfaces (SVIs / IRBs), read."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, IntentApplyResult, api_error
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.core.removal import is_cleared
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, DeviceSettings, DeviceSvi, SviIntent

router = APIRouter(prefix="/api/v1/devices", tags=["svi"])


class SviIfaceOut(BaseModel):
    interface_name: str
    vlan_id: int
    type: str
    vrf: str  # coerced to "" when unset
    source: str


class SviOut(BaseModel):
    device_id: int
    read_state: FamilyReadState  # the S4 truth (this family never had legacy freshness fields)
    interfaces: list[SviIfaceOut]


@router.get(
    "/{device_id}/svi",
    dependencies=[Depends(verify_token)],
    response_model=SviOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_svi(device_id: int, db: AsyncSession = Depends(get_read_db)):
    """Return the device's SVIs/IRBs (no IPs — those ride interface-ip)."""
    if await db.get(Device, device_id) is None:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(await outcome_store.get_current_outcome(db, device_id, "svi"))
    rows = (
        (await db.execute(select(DeviceSvi).where(DeviceSvi.device_id == device_id).order_by(DeviceSvi.vlan_id)))
        .scalars()
        .all()
    )
    return {
        "device_id": device_id,
        "read_state": read_state,
        "interfaces": [
            {
                "interface_name": r.interface_name,
                "vlan_id": r.vlan_id,
                "type": r.svi_type,
                "vrf": r.vrf or "",
                "source": "svi",
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/svi-intent (write path)
# ---------------------------------------------------------------------------


class SviEntry(BaseModel):
    interface_name: str
    vlan_id: int
    type: str = "svi"
    vrf: str = ""
    accepted_at: datetime | None = None


# Scalars the writer emits only when set — `if row.vrf:` (nso/apply.py). vrf is NOT NULL default='' so the clear is ''.
# A merge-PATCH apply can never drop one that goes back to unset, so clearing any of
# them must enqueue a PUT-replace retract. See core.removal.is_cleared.
_STATE_FIELDS = ("vrf",)


class SviIntentUpdate(BaseModel):
    interfaces: list[SviEntry]


@router.put(
    "/{device_id}/svi-intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentApplyResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def put_svi_intent(device_id: int, body: SviIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's SVI/IRB intent mirror for this device atomically.

    Full-replace: rows not in the body are deleted. ``accepted_at`` defaults to now.
    If ``auto_apply`` is enabled on the device, an apply job is enqueued.
    """
    device = await db.get(Device, device_id)
    if device is None:
        raise api_error(404, "not_found", "Device not found")

    existing = await db.execute(select(SviIntent).where(SviIntent.device_id == device_id))
    existing_rows: dict[str, SviIntent] = {r.interface_name: r for r in existing.scalars().all()}
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
            row = SviIntent(device_id=device_id, interface_name=item.interface_name)
            db.add(row)
        row.vlan_id = item.vlan_id
        row.svi_type = item.type
        row.vrf = item.vrf or None
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
        from nso_adapter.nso.apply import apply_svi_config

        replaced = await replace_on_removal(db, device, removed, SviIntent, apply_svi_config, retract=cleared)

    return {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
