# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S4: the per-(device, family) read-state wire contract.

``FamilyReadState`` is the SINGLE definition of the block every family GET carries
inline and the aggregate ``GET /devices/{id}/read-state`` serves per family. The plugin
gate authorizes reconciliation from the tuple (outcome, succeeded, result); everything
else fails closed there. A missing pointer NEVER serializes as null — the adapter
synthesizes ``unavailable/not_ready`` (key-absent means a pre-S4 adapter, D3). Every
block — real or synthesized — carries the store incarnation pair ``(incarnation,
incarnation_born)``: the plugin's only reliable store-reset signal (attempt ids restart
after a rebuild; numeric device ids can be reissued).
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel, PlainSerializer, WithJsonSchema
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_404_DEVICE, RESP_422_VALIDATION, api_error
from nso_adapter.api.timestamps import iso_z
from nso_adapter.core.families import ALL_FAMILY_KEYS, FAMILIES_VERSION
from nso_adapter.store import outcome_store
from nso_adapter.store.meta import get_store_incarnation
from nso_adapter.store.models import Device, RefreshOutcome

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1/devices", tags=["read-state"])


# Wire datetime (SA-2 round 2): serializes as the store's "<iso>Z" (valid RFC 3339) while
# the OpenAPI schema stays an honest non-nullable string/format=date-time — a bare
# field_serializer returning `str | None` published unformatted NULLABLE strings, letting
# a schema-generated consumer accept a null incarnation_born (load-bearing for adoption).
IsoZDateTime = Annotated[
    datetime,
    PlainSerializer(iso_z, return_type=str, when_used="json"),
    WithJsonSchema({"type": "string", "format": "date-time"}),
]


class FamilyReadState(BaseModel):
    """The declared truth of one family's newest terminal read attempt."""

    outcome: Literal["present", "absent_authoritative", "unavailable"]
    reason: Literal["export_down", "read_error", "not_authoritative", "unsupported", "not_ready"] | None = None
    freshness: Literal["fresh", "stale"] | None = None
    # 'error' included deliberately: the engine terminalizes materializer failures as
    # result="error"/succeeded=False (refresh_engine) — a narrower literal would
    # response-validation-fail those real rows (codex R1-F3).
    result: Literal["replaced", "cleared", "kept", "error", "superseded"] | None = None
    succeeded: bool | None = None
    # Phase-1 started_at — WHEN THE READ HAPPENED (SA-2: completed_at would make data look
    # newer than its read under a slow materializer). Display-only, never an ordering key.
    read_at: IsoZDateTime | None = None
    attempt_id: int | None = None  # null = synthesized (no pointer); sorts below every real attempt
    source_epoch: int
    payload_revision: int | None = None
    incarnation: str
    incarnation_born: IsoZDateTime  # adoption orders on this; adapter clock domain only — NEVER null


def read_state_payload(row: RefreshOutcome | None, *, source_epoch: int) -> dict:
    """Build the wire block from the pointed terminal attempt, or synthesize not_ready."""
    incarnation, born = get_store_incarnation()
    base = {
        "incarnation": incarnation,
        "incarnation_born": born,
        "source_epoch": source_epoch,
        "payload_revision": getattr(row, "payload_revision", None),
    }
    if row is None:
        return {
            **base,
            "outcome": "unavailable",
            "reason": "not_ready",
            "freshness": None,
            "result": None,
            "succeeded": None,
            "read_at": None,
            "attempt_id": None,
        }
    return {
        **base,
        "outcome": row.read_outcome,
        "reason": row.read_reason,
        "freshness": row.freshness,
        "result": row.result,
        "succeeded": row.succeeded,
        "read_at": row.started_at,
        "attempt_id": row.id,
    }


class DeviceReadStateOut(BaseModel):
    device_id: int
    families_version: int
    families: dict[str, FamilyReadState]


@router.get(
    "/{device_id}/read-state",
    dependencies=[Depends(verify_token)],
    response_model=DeviceReadStateOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_device_read_state(device_id: int, db: AsyncSession = Depends(get_read_db)):
    """Every canonical family's current read state in one pointer-join query.

    Pointerless families synthesize ``not_ready`` — the full 19-key vocabulary is always
    present, so the plugin can render a complete per-family picture from one call.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")
    by_family = await outcome_store.get_current_outcomes(db, device_id)
    return {
        "device_id": device_id,
        "families_version": FAMILIES_VERSION,
        "families": {
            key: read_state_payload(by_family.get(key), source_epoch=device.source_epoch) for key in ALL_FAMILY_KEYS
        },
    }
