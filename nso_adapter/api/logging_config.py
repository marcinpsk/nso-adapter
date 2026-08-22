# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Remote syslog servers: GET /logging-config (read) + PUT /logging-intent (write)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import (
    RESP_401,
    RESP_404_DEVICE,
    RESP_409_PUSH_SEQ,
    RESP_422_VALIDATION,
    IntentApplyResult,
    api_error,
)
from nso_adapter.api.intent_push import begin_delivery, get_intent_delivery
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import UtcInstant, iso_z
from nso_adapter.core.removal import is_cleared
from nso_adapter.store import outcome_store
from nso_adapter.store.models import (
    Device,
    DeviceLoggingHost,
    DeviceLoggingLevels,
    DeviceSettings,
    LoggingHostIntent,
    LoggingLevelsIntent,
)

router = APIRouter(prefix="/api/v1/devices", tags=["logging-config"])


class LoggingHostOut(BaseModel):
    """One remote syslog server; every key but ``address`` omitted when unset."""

    address: str
    port: int | None = None
    severity: str | None = None
    facility: str | None = None
    transport: str | None = None
    vrf: str | None = None
    source: str | None = None


class LocalLevelsOut(BaseModel):
    """Per-device local logging severities (OC grain); each key omitted when unset."""

    console_severity: str | None = None
    monitor_severity: str | None = None
    module_severity: str | None = None


class LoggingConfigOut(BaseModel):
    device_id: int
    last_refreshed_at: str | None = None  # reader formats "<iso>Z"; None when never refreshed
    refresh_source: str  # legacy freshness (S5 retires it); read_state is the S4 truth
    read_state: FamilyReadState
    hosts: list[LoggingHostOut]
    local_levels: LocalLevelsOut | None = None  # omitted entirely when the device sets no level


_LEVEL_FIELDS = ("console_severity", "monitor_severity", "module_severity")


@router.get(
    "/{device_id}/logging-config",
    dependencies=[Depends(verify_token)],
    response_model=LoggingConfigOut,
    response_model_exclude_unset=True,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_logging_config(device_id: int, db: AsyncSession = Depends(get_read_db)):
    """Return the device's remote syslog servers + local logging levels."""
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Pointer first, rows second, one snapshot (S4 D2 — benign direction).
    read_state = read_state_payload(
        await outcome_store.get_current_outcome(db, device_id, "logging"), source_epoch=device.source_epoch
    )

    rows = (
        (
            await db.execute(
                select(DeviceLoggingHost)
                .where(DeviceLoggingHost.device_id == device_id)
                .order_by(DeviceLoggingHost.address)
            )
        )
        .scalars()
        .all()
    )
    levels_row = (
        await db.execute(select(DeviceLoggingLevels).where(DeviceLoggingLevels.device_id == device_id))
    ).scalar_one_or_none()
    if not rows and levels_row is None:
        return {
            "device_id": device_id,
            "last_refreshed_at": None,
            "refresh_source": "never",
            "read_state": read_state,
            "hosts": [],
        }

    latest = max([*rows, *([levels_row] if levels_row else [])], key=lambda r: r.last_refreshed_at or "")
    hosts = []
    for r in rows:
        entry: dict = {"address": r.address}
        for attr in ("port", "severity", "facility", "transport", "vrf", "source"):
            val = getattr(r, attr)
            if val is not None:
                entry[attr] = val
        hosts.append(entry)
    ts = latest.last_refreshed_at
    out = {
        "device_id": device_id,
        "last_refreshed_at": iso_z(ts),
        "refresh_source": latest.refresh_source,
        "read_state": read_state,
        "hosts": hosts,
    }
    if levels_row is not None:
        out["local_levels"] = {f: getattr(levels_row, f) for f in _LEVEL_FIELDS if getattr(levels_row, f) is not None}
    return out


# ---------------------------------------------------------------------------
# PUT /{device_id}/logging-intent
# ---------------------------------------------------------------------------


class LoggingHostEntry(BaseModel):
    address: str
    port: int | None = None
    severity: str = ""
    facility: str = ""
    transport: str = ""
    vrf: str = ""
    source: str = ""
    accepted_at: UtcInstant | None = None


#: The closed OC severity vocabulary — the reconciler's log-severity-oc enum verbatim.
OcSeverity = Literal["EMERGENCY", "ALERT", "CRITICAL", "ERROR", "WARNING", "NOTICE", "INFORMATIONAL", "DEBUG"]


class LocalLevelsEntry(BaseModel):
    """Owned local-levels intent (NX-P4a); an omitted severity = that destination unmanaged."""

    console_severity: OcSeverity | None = None
    monitor_severity: OcSeverity | None = None
    module_severity: OcSeverity | None = None
    accepted_at: UtcInstant | None = None


# Scalars the writer emits only when set — `if row.port is not None:` / `if row.severity:` (nso/apply.py). Most are NOT NULL default='' so the clear is '' rather than None — is_cleared() covers both.
# A merge-PATCH apply can never drop one that goes back to unset, so clearing any of
# them must enqueue a PUT-replace retract. See core.removal.is_cleared.
_STATE_FIELDS = ("port", "severity", "facility", "transport", "vrf", "source")


class LoggingIntentUpdate(BaseModel):
    hosts: list[LoggingHostEntry]
    # Presence-sensitive (R2/F10): OMITTED = preserve the stored levels intent (an old
    # hosts-only client must never clear it); a dict = replace; an explicit null (or an
    # entry with every severity unset) = un-manage → delete the row + retract.
    local_levels: LocalLevelsEntry | None = None


async def _sync_local_levels(
    db: AsyncSession, device_id: int, entry: LocalLevelsEntry | None, now: datetime
) -> tuple[bool, int, int]:
    """Replace the levels singleton intent; return cleared, written, and removed counts.

    ``cleared`` reports any previously-set severity going back to unset — the #83
    cleared-owned-scalar shape a merge-PATCH can never revert, so the caller must
    enqueue the PUT-replace retract.
    """
    existing = (
        await db.execute(select(LoggingLevelsIntent).where(LoggingLevelsIntent.device_id == device_id))
    ).scalar_one_or_none()
    before = {f: getattr(existing, f) for f in _LEVEL_FIELDS} if existing is not None else None
    values = {f: (getattr(entry, f) if entry is not None else None) for f in _LEVEL_FIELDS}
    cleared = before is not None and any(is_cleared(before[f], values[f]) for f in _LEVEL_FIELDS)
    if not any(values.values()):
        removed = int(existing is not None)
        if existing is not None:
            await db.delete(existing)
        return cleared, 0, removed
    if existing is None:
        existing = LoggingLevelsIntent(device_id=device_id)
        db.add(existing)
    for f in _LEVEL_FIELDS:
        setattr(existing, f, values[f])
    existing.accepted_at = entry.accepted_at if entry.accepted_at else now
    return cleared, 1, 0


@router.put(
    "/{device_id}/logging-intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentApplyResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ, **RESP_422_VALIDATION},
)
async def put_logging_intent(
    device_id: int,
    body: LoggingIntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's remote-syslog + local-levels intent mirror atomically.

    Full-replace semantics for ``hosts``: rows not present in the request body are
    deleted. ``local_levels`` is presence-sensitive — see :class:`LoggingIntentUpdate`.
    ``accepted_at`` defaults to now if not supplied. If ``auto_apply`` is enabled
    on the device, an apply job is enqueued after the upsert.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    # Every accepted write records its projection revision, store-only and
    # auto-apply-off included, and takes the device's projection lock before anything is
    # read (#1522 §G2). Only a promotion authorizes a deployment.
    from nso_adapter.core.receipt import record_response

    if (replay := await begin_delivery(db, device_id, delivery)) is not None:
        return replay

    existing_result = await db.execute(select(LoggingHostIntent).where(LoggingHostIntent.device_id == device_id))
    existing_rows: dict[str, LoggingHostIntent] = {r.address: r for r in existing_result.scalars().all()}
    new_keys = {item.address for item in body.hosts}

    removed = [addr for addr in existing_rows if addr not in new_keys]
    for addr in removed:
        await db.delete(existing_rows[addr])
    await db.flush()

    now = datetime.now(UTC)
    count = 0
    cleared = False
    for item in body.hosts:
        accepted = item.accepted_at if item.accepted_at else now
        row = existing_rows.get(item.address)
        before = {f: getattr(row, f) for f in _STATE_FIELDS} if row is not None else None
        if row is None:
            row = LoggingHostIntent(device_id=device_id, address=item.address)
            db.add(row)
        row.port = item.port
        row.severity = item.severity
        row.facility = item.facility
        row.transport = item.transport
        row.vrf = item.vrf
        row.source = item.source
        row.accepted_at = accepted
        if before is not None and any(is_cleared(before[f], getattr(row, f)) for f in _STATE_FIELDS):
            cleared = True
        count += 1

    levels_cleared = False
    levels_count = 0
    levels_removed = 0
    if "local_levels" in body.model_fields_set:
        levels_cleared, levels_count, levels_removed = await _sync_local_levels(db, device_id, body.local_levels, now)

    await db.flush()

    replaced = False
    if removed or cleared or levels_cleared:
        from nso_adapter.core.removal import replace_on_removal
        from nso_adapter.nso.apply import apply_logging_config

        replaced = await replace_on_removal(
            db, device, removed, LoggingHostIntent, apply_logging_config, retract=cleared or levels_cleared
        )

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and (count > 0 or levels_count > 0):
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True, stream=delivery.stream)

    result = {
        "device_id": device_id,
        "count": count + levels_count,
        "removed": len(removed) + levels_removed,
        "replaced": replaced,
    }
    await record_response(db, device_id, delivery, result)
    await db.commit()
    return result
