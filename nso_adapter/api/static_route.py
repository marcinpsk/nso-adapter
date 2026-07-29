# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/static-routes and PUT /api/v1/devices/{id}/static-route-intent."""

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
from nso_adapter.api.timestamps import iso_z
from nso_adapter.core.removal import is_cleared
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, DeviceSettings, DeviceStaticRoute, StaticRouteIntent

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["static-routes"])


class StaticRouteOut(BaseModel):
    """One static route in the read mirror.

    The identity keys (``vrf``/``prefix``/``next_hop``) are always present; the
    rest are emitted only when set (``response_model_exclude_unset``), matching
    the hand-built dict the reader has always produced.
    """

    vrf: str
    prefix: str
    next_hop: str
    interface_next_hop: str | None = None
    next_hop_vrf: str | None = None
    metric: int | None = None
    permanent: bool | None = None
    tag: int | None = None
    name: str | None = None


class StaticRoutesOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # reader formats "<iso>Z"; None when never refreshed
    refresh_source: str  # legacy freshness (S5 retires it); read_state is the S4 truth
    read_state: FamilyReadState
    routes: list[StaticRouteOut]


@router.get(
    "/{device_id}/static-routes",
    dependencies=[Depends(verify_token)],
    response_model=StaticRoutesOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_static_routes(device_id: int, db: AsyncSession = Depends(get_read_db)):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer FIRST, rows second, one snapshot (D2): rows can only be same-or-newer than
    # the outcome they're paired with — the benign direction for the plugin gate.
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "static_route"), source_epoch=device.source_epoch
    )

    result = await db.execute(
        select(DeviceStaticRoute)
        .where(DeviceStaticRoute.device_id == device_id)
        .order_by(DeviceStaticRoute.vrf, DeviceStaticRoute.prefix, DeviceStaticRoute.next_hop)
    )
    rows = result.scalars().all()

    if not rows:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "read_state": read_state,
            "routes": [],
        }

    latest = max(rows, key=lambda r: r.last_refreshed_at or "")

    routes = []
    for row in rows:
        entry: dict = {
            "vrf": row.vrf,
            "prefix": row.prefix,
            "next_hop": row.next_hop,
        }
        if row.interface_next_hop is not None:
            entry["interface_next_hop"] = row.interface_next_hop
        if row.next_hop_vrf is not None:
            entry["next_hop_vrf"] = row.next_hop_vrf
        if row.metric is not None:
            entry["metric"] = row.metric
        if row.permanent is not None:
            entry["permanent"] = row.permanent
        if row.tag is not None:
            entry["tag"] = row.tag
        if row.name is not None:
            entry["name"] = row.name
        routes.append(entry)

    last_ts = latest.last_refreshed_at
    return {
        "device_id": device_id,
        "last_refreshed_at": iso_z(last_ts),
        "refresh_source": latest.refresh_source,
        "read_state": read_state,
        "routes": routes,
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/static-route-intent
# ---------------------------------------------------------------------------


class StaticRouteEntry(BaseModel):
    vrf: str = ""
    prefix: str
    next_hop: str
    interface_next_hop: str | None = None
    next_hop_vrf: str | None = None
    metric: int | None = None
    permanent: bool | None = None
    tag: int | None = None
    name: str | None = None
    accepted_at: datetime | None = None


# Scalars the writer emits only when set — `if row.metric is not None:` / `if getattr(row, 'interface_next_hop', None):` (nso/apply.py)
# A merge-PATCH apply can never drop one that goes back to unset, so clearing any of
# them must enqueue a PUT-replace retract. See core.removal.is_cleared.
_STATE_FIELDS = ("interface_next_hop", "next_hop_vrf", "metric", "permanent", "tag", "name")


class StaticRouteIntentUpdate(BaseModel):
    routes: list[StaticRouteEntry]


@router.put(
    "/{device_id}/static-route-intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentApplyResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def put_static_route_intent(device_id: int, body: StaticRouteIntentUpdate, db: AsyncSession = Depends(get_db)):
    """Replace the adapter's static-route intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is
    enabled on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    existing_result = await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))
    existing_rows: dict[tuple, StaticRouteIntent] = {
        (r.vrf, r.prefix, r.next_hop): r for r in existing_result.scalars().all()
    }

    # Determine which (vrf, prefix, next_hop) keys are in the new payload.
    new_keys: set[tuple] = {(item.vrf, item.prefix, item.next_hop) for item in body.routes}

    # Delete rows absent from the new payload.
    removed = [key for key in existing_rows if key not in new_keys]
    for key in removed:
        await db.delete(existing_rows[key])
    await db.flush()

    now = datetime.now(UTC).replace(tzinfo=None)
    count = 0
    cleared = False
    for item in body.routes:
        key = (item.vrf, item.prefix, item.next_hop)
        accepted = item.accepted_at.replace(tzinfo=None) if item.accepted_at else now
        if key in existing_rows:
            row = existing_rows[key]
            before = {f: getattr(row, f) for f in _STATE_FIELDS}
            row.accepted_at = accepted
            row.interface_next_hop = item.interface_next_hop
            row.next_hop_vrf = item.next_hop_vrf
            row.metric = item.metric
            row.permanent = item.permanent
            row.tag = item.tag
            row.name = item.name
            if any(is_cleared(before[f], getattr(row, f)) for f in _STATE_FIELDS):
                cleared = True
        else:
            row = StaticRouteIntent(
                device_id=device_id,
                vrf=item.vrf,
                prefix=item.prefix,
                next_hop=item.next_hop,
                interface_next_hop=item.interface_next_hop,
                next_hop_vrf=item.next_hop_vrf,
                metric=item.metric,
                permanent=item.permanent,
                tag=item.tag,
                name=item.name,
                accepted_at=accepted,
            )
            db.add(row)
        count += 1

    await db.flush()

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()

    replaced = False
    if removed or cleared:
        from nso_adapter.core.removal import replace_on_removal
        from nso_adapter.nso.apply import apply_static_routes

        replaced = await replace_on_removal(
            db, device, removed, StaticRouteIntent, apply_static_routes, retract=cleared
        )

    return {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
