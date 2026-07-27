# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Capability-matrix endpoints — refresh / read / pre-flight a device's route-policy support.

These let the plugin flag, at attach time, which parts of a route-map / community-list
won't apply on a device (instead of the operator discovering it only when it silently
didn't land). Backed by the persisted ``device_capability`` cache keyed by (ned, sw).
"""

from __future__ import annotations

from typing import Literal

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404, RESP_404_DEVICE, RESP_409, RESP_422_VALIDATION, api_error
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


class ApplyPreflightRequest(BaseModel):
    scopes: list[str] = []


class ReadCapabilityElement(BaseModel):
    scope: str
    status: Literal["native", "unsupported", "skipped", "unknown"]
    detail: str = ""


class ReadCapabilityReport(BaseModel):
    nso_device_name: str
    nso_instance: str = ""  # disambiguates when the same device name exists in several instances
    elements: list[ReadCapabilityElement] = []


# ── Response models ───────────────────────────────────────────────────────────
# The GET + both preflights are two-branch: the "known" branch adds a (ned_id,
# sw_version) key and coverage_unknown; the "unknown" branch omits them. Those keys
# are unset-by-default optionals + response_model_exclude_unset=True, so an omitted
# key stays absent (never null). The golden tests pin every branch's exact bytes.


class CapabilityElementOut(BaseModel):
    scope: str
    name: str
    status: str
    detail: str
    source: str


class CapabilityOut(BaseModel):
    known: bool
    ned_id: str
    sw_version: str
    coverage_unknown: bool | None = None  # present only in the known branch
    elements: list[CapabilityElementOut]


class CapabilityKeyCountOut(BaseModel):
    """{ned_id, sw_version, count} — shared by refresh + read-capability report."""

    ned_id: str
    sw_version: str
    count: int


class RoutePolicyPreflightUnsupported(BaseModel):
    scope: str
    element: str
    status: str
    detail: str


class RoutePolicyPreflightOut(BaseModel):
    known: bool
    ned_id: str | None = None  # present only in the known branch
    sw_version: str | None = None  # present only in the known branch
    fully_supported: bool
    unsupported: list[RoutePolicyPreflightUnsupported]
    coverage_unknown: bool


class ApplyPreflightUnsupported(BaseModel):
    scope: str
    name: str
    status: str
    detail: str


class ApplyPreflightOut(BaseModel):
    known: bool
    ned_id: str | None = None  # present only in the known branch
    sw_version: str | None = None  # present only in the known branch
    coverage_unknown: bool
    fully_supported: bool
    unsupported: list[ApplyPreflightUnsupported]


async def _device_and_client(device_id: int, db: AsyncSession):
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    try:
        client = get_nso_client(device.nso_instance)
    except RuntimeError:
        raise api_error(409, "no_nso_client", f"No NSO client for instance {device.nso_instance!r}")
    return device, client


@router.post(
    "/{device_id}/capability/refresh",
    dependencies=[Depends(verify_token)],
    response_model=CapabilityKeyCountOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
async def refresh_capability(device_id: int, db: AsyncSession = Depends(get_db)):
    """Force a capability probe for this device now ('check now') and persist the result."""
    device, client = await _device_and_client(device_id, db)
    info = await capability.refresh_device_capability(db, client, device.nso_device_name, device)
    return info or {"ned_id": "", "sw_version": "", "count": 0}


@router.get(
    "/{device_id}/capability",
    dependencies=[Depends(verify_token)],
    response_model=CapabilityOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
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


@router.post(
    "/read-capability/report",
    dependencies=[Depends(verify_token)],
    response_model=CapabilityKeyCountOut,
    responses={**RESP_401, **RESP_404, **RESP_409, **RESP_422_VALIDATION},
)
async def report_read_capability(body: ReadCapabilityReport, db: AsyncSession = Depends(get_db)):
    """Ingest the READ half of the capability matrix from an external read probe.

    The vendor-test harness posts per-scope read states (observed against a live device) by
    NSO device name; the adapter resolves the ``(ned_id, sw_version)`` key from the device
    row (kept fresh by the importer's per-sync NED refresh) and records ``source='read'``
    rows. This is how a brand-new NED emits capability signal before any apply has ever run.
    409 when the device has no learned NED id yet (no key to record under).
    """
    stmt = select(Device).where(Device.nso_device_name == body.nso_device_name)
    if body.nso_instance:
        stmt = stmt.where(Device.nso_instance == body.nso_instance)
    devices = (await db.execute(stmt)).scalars().all()
    if not devices:
        raise api_error(404, "not_found", f"No device named {body.nso_device_name!r}")
    if len(devices) > 1:
        raise api_error(
            409, "ambiguous_device", f"{body.nso_device_name!r} exists in several instances — pass nso_instance"
        )
    device = devices[0]
    ned_id = capability._clean_capability_key(device.ned_id)
    sw_version = capability._clean_capability_key(device.sw_version)
    if not ned_id:
        raise api_error(409, "no_ned_id", "Device has no learned NED id yet — sync or probe it first")
    count = await capability.record_read_capability(db, ned_id, sw_version, [el.model_dump() for el in body.elements])
    logger.info("capability.read_report", device=body.nso_device_name, ned_id=ned_id, sw_version=sw_version, rows=count)
    return {"ned_id": ned_id, "sw_version": sw_version, "count": count}


@router.post(
    "/{device_id}/route-policy/preflight",
    dependencies=[Depends(verify_token)],
    response_model=RoutePolicyPreflightOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
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


@router.post(
    "/{device_id}/apply/preflight",
    dependencies=[Depends(verify_token)],
    response_model=ApplyPreflightOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409, **RESP_422_VALIDATION},
)
async def preflight_apply(
    device_id: int, body: ApplyPreflightRequest, refresh: bool = False, db: AsyncSession = Depends(get_db)
):
    """Check the scopes the next apply will push against this device's capability matrix.

    The generic analog of ``/route-policy/preflight``: the plugin passes the scopes from its
    apply diff; any the matrix marks ``unsupported``/``skipped`` (recorded reactively on a prior
    apply failure, see ``_record_atomic_capability``) are returned so the operator is warned
    BEFORE a device write. ``refresh=false`` (default) is the cheap cache-only read; the device
    must have a known ``(ned, sw)`` key (probed at least once) or the result is fail-open.
    """
    device, client = await _device_and_client(device_id, db)
    info = await capability.resolve_capability_key(db, client, device, refresh=refresh)
    if not info:
        return {"known": False, "fully_supported": True, "unsupported": [], "coverage_unknown": False}
    rows = await capability.get_device_capability(db, info["ned_id"], info["sw_version"])
    result = capability.preflight_scopes(rows, body.scopes)
    return {
        "known": True,
        "ned_id": info["ned_id"],
        "sw_version": info["sw_version"],
        "coverage_unknown": capability.coverage_unknown(rows),
        **result,
    }
