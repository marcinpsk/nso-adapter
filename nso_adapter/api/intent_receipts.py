# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/intent-receipts — what the pusher's restore path reads (#1503 §4.4/§4.6).

The receipt is the only durable record of which logical operation the adapter last admitted
for a ``(device, stream)``. A pusher restored from a snapshot has lost that knowledge on its
own side, so before it resolves a single outstanding claim it reads this surface and learns:

* every per-key receipt — its sequence, the body digest, the request mode and the STORED
  RESPONSE, which is what §4.6's same-sequence arm re-validates the claim's exact set
  against;
* ``global_max_push_seq``, the fleet-wide highest admitted sequence, so the restored pusher
  allocates ABOVE it and never re-uses one;
* ``global_max_route_id``, the highest NetBox route pk the adapter holds anywhere. A
  plugin-only restore rewinds ``StaticRoute``'s pk sequence too, so a snapshot taken before
  pk R existed can re-allocate R while the adapter still holds an acknowledged, unrelated row
  carrying ``route_id = R``. The deletion partition's first pass would then bind that row as
  GENUINE and authorize removing it: a device write with no authority behind it. Advancing the
  pk sequence past this value is what closes it (R9-B4), and the value therefore counts both
  TOMBSTONES and receipt-held promotion deletions. Either carrier can hold the pk of a route
  whose deletion is still in flight.

Both maxima stay fleet-wide under a filter: the pusher advances ONE sequence for the whole
fleet, and a per-key answer would let it advance past its own key while another key's receipt
still names a higher one.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_read_db, verify_token
from nso_adapter.api.errors import RESP_401, RESP_422_VALIDATION, api_error
from nso_adapter.api.timestamps import iso_z
from nso_adapter.core.intent_protocol import INTENT_STREAMS
from nso_adapter.store.models import IntentPushReceipt, StaticRouteIntent, StaticRouteTombstone

router = APIRouter(prefix="/api/v1/intent-receipts", tags=["intent-receipts"])


class IntentReceiptOut(BaseModel):
    """One ``(device, section)`` receipt — EMIT-NULL, every key always present.

    ``section`` is the adapter's own stream vocabulary (:mod:`core.intent_protocol`). The
    pusher's delivery key for the interface family is ``interface`` and maps onto
    ``interface_config`` here; the other fifteen names are identical on both sides.
    """

    device_id: int
    section: str
    push_seq: int
    request_digest: str
    store_only: bool
    delete_origin: bool
    backfill_only: bool
    status_code: int
    response: dict | None
    generation_id: int | None
    created_at: str
    updated_at: str | None


class IntentReceiptsOut(BaseModel):
    receipts: list[IntentReceiptOut]
    #: NULL when the adapter has admitted no keyed push at all, which is not the same as 0.
    global_max_push_seq: int | None
    global_max_route_id: int | None


def _receipt_out(row: IntentPushReceipt) -> dict:
    response = row.response
    if isinstance(response, dict):
        response = {key: value for key, value in response.items() if not key.startswith("_")}
    return {
        "device_id": row.device_id,
        "section": row.section,
        "push_seq": row.push_seq,
        "request_digest": row.request_digest,
        "store_only": row.store_only,
        "delete_origin": row.delete_origin,
        "backfill_only": row.backfill_only,
        "status_code": row.status_code,
        "response": response,
        "generation_id": row.generation_id,
        "created_at": iso_z(row.created_at),
        "updated_at": iso_z(row.updated_at),
    }


@router.get(
    "",
    dependencies=[Depends(verify_token)],
    response_model=IntentReceiptsOut,
    responses={**RESP_401, **RESP_422_VALIDATION},
)
async def list_intent_receipts(
    device_id: int | None = None,
    section: str | None = None,
    db: AsyncSession = Depends(get_read_db),
):
    """Serve the per-key receipts, filterable, plus the two fleet-wide maxima."""
    if section is not None and section not in INTENT_STREAMS:
        # An empty page for a mis-spelled name reads as "this key has no receipt", and the
        # restore resolves that by replaying the claim normally — the wrong branch entirely.
        raise api_error(
            422,
            "validation_error",
            f"Unknown intent section {section!r}",
            {"reason": "unknown_section", "sections": sorted(INTENT_STREAMS)},
        )

    query = select(IntentPushReceipt).order_by(IntentPushReceipt.device_id, IntentPushReceipt.section)
    if device_id is not None:
        query = query.where(IntentPushReceipt.device_id == device_id)
    if section is not None:
        query = query.where(IntentPushReceipt.section == section)
    rows = (await db.execute(query)).scalars().all()

    # Both maxima are read UNFILTERED: see the module docstring.
    max_push_seq = await db.scalar(select(func.max(IntentPushReceipt.push_seq)))
    max_live_route_id = await db.scalar(select(func.max(StaticRouteIntent.route_id)))
    max_tombstoned_route_id = await db.scalar(select(func.max(StaticRouteTombstone.route_id)))
    receipt_responses = (await db.execute(select(IntentPushReceipt.response))).scalars().all()
    receipt_route_ids = [
        record["route_id"]
        for response in receipt_responses
        if isinstance(response, dict)
        for record in response.get("_promotion_deletions") or []
        if isinstance(record, dict) and isinstance(record.get("route_id"), int)
    ]
    held_route_ids = [
        value for value in (max_live_route_id, max_tombstoned_route_id, *receipt_route_ids) if value is not None
    ]

    return {
        "receipts": [_receipt_out(row) for row in rows],
        "global_max_push_seq": max_push_seq,
        "global_max_route_id": max(held_route_ids) if held_route_ids else None,
    }
