# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Capability-matrix endpoints — refresh / read / pre-flight a device's route-policy support.

These let the plugin flag, at attach time, which parts of a route-map / community-list
won't apply on a device (instead of the operator discovering it only when it silently
didn't land). Backed by the persisted ``device_capability`` cache keyed by (ned, sw).
"""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.core import capability
from nso_adapter.core.importer import get_nso_client
from nso_adapter.store.models import Device

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["capability"])


class PreflightRequest(BaseModel):
    community_members: list[str] = []
    set_keys: list[str] = []
    match_keys: list[str] = []
    aspath_names: list[str] = []


async def _device_and_client(device_id: int, db: AsyncSession):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    try:
        client = get_nso_client(device.nso_instance)
    except RuntimeError:
        raise api_error(409, "no_nso_client", f"No NSO client for instance {device.nso_instance!r}")
    return device, client


@router.post("/{device_id}/capability/refresh", dependencies=[Depends(verify_token)])
async def refresh_capability(device_id: int, db: AsyncSession = Depends(get_db)):
    """Force a capability probe for this device now ('check now') and persist the result."""
    device, client = await _device_and_client(device_id, db)
    info = await capability.refresh_device_capability(db, client, device.nso_device_name, device)
    return info or {"ned_id": "", "sw_version": "", "count": 0}


@router.get("/{device_id}/capability", dependencies=[Depends(verify_token)])
async def get_capability(device_id: int, refresh: bool = False, db: AsyncSession = Depends(get_db)):
    """Return the cached capability verdict for this device's (ned_id, sw_version).

    ``refresh=true`` probes NSO now (the explicit "check now"); ``refresh=false`` (default —
    the cheap panel read) serves the device's last-learned key, returning an empty key when
    the device has never been probed.
    """
    device, client = await _device_and_client(device_id, db)
    info = await capability.resolve_capability_key(db, client, device, refresh=refresh)
    if not info:
        return {"known": False, "ned_id": "", "sw_version": "", "elements": []}
    rows = await capability.get_device_capability(db, info["ned_id"], info["sw_version"])
    return {
        "known": True,
        "ned_id": info["ned_id"],
        "sw_version": info["sw_version"],
        "coverage_unknown": capability.coverage_unknown(rows),
        "elements": [
            {"scope": r.scope, "name": r.name, "status": r.status, "detail": r.detail, "source": r.source} for r in rows
        ],
    }


@router.post("/{device_id}/route-policy/preflight", dependencies=[Depends(verify_token)])
async def preflight_route_policy(
    device_id: int, body: PreflightRequest, refresh: bool = True, db: AsyncSession = Depends(get_db)
):
    """Check a would-be attach against this device's capability matrix.

    ``refresh=true`` (default — the authoritative attach-time check) probes the device's
    (ned, sw) verdict once; ``refresh=false`` (the cheap panel read) uses the last-learned
    key. Reports which requested community members / route-map constructs won't fully apply.
    """
    device, client = await _device_and_client(device_id, db)
    info = await capability.resolve_capability_key(db, client, device, refresh=refresh)
    if not info:
        return {"known": False, "fully_supported": True, "unsupported": [], "coverage_unknown": False}
    rows = await capability.get_device_capability(db, info["ned_id"], info["sw_version"])
    result = capability.preflight(rows, body.community_members, body.set_keys, body.match_keys, body.aspath_names)
    return {"known": True, "ned_id": info["ned_id"], "sw_version": info["sw_version"], **result}
