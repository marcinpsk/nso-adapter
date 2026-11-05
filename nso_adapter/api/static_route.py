# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/static-routes and PUT /api/v1/devices/{id}/static-route-intent."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.candidate_ingestion import candidate_ingester
from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import (
    RESP_401,
    RESP_404_DEVICE,
    RESP_409_PUSH_SEQ_OR_DEVICE_BUSY,
    RESP_422_VALIDATION,
    IntentApplyResult,
    api_error,
)
from nso_adapter.api.intent_push import get_intent_delivery
from nso_adapter.api.observation import StaticRouteObservationOut
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import UtcInstant, iso_z, latest_refreshed
from nso_adapter.config import get_config
from nso_adapter.core.claim import ClaimUnavailableError, held_claim, lock_claim
from nso_adapter.core.static_route_ingestion import (
    _STATE_FIELDS as _STATE_FIELDS,
)
from nso_adapter.core.static_route_ingestion import (
    _apply_static_route_intent,
    _echo,
    _reject_deletion_payload,
    _reject_payload_duplicates,
)
from nso_adapter.core.static_route_ingestion import (
    _double_claimed_triple as _double_claimed_triple,
)
from nso_adapter.store import outcome_store
from nso_adapter.store.models import (
    Device,
    DeviceStaticRoute,
    StaticRouteIntent,
)

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
    observation: StaticRouteObservationOut | None


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
    outcome, observation = await outcome_store.get_current_publication(
        db, device_id, "static_route", source_epoch=device.source_epoch
    )
    read_state = read_state_payload(outcome, source_epoch=device.source_epoch)

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
            "observation": observation,
            "routes": [],
        }

    latest = latest_refreshed(rows)

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
        "observation": observation,
        "routes": routes,
    }


# ---------------------------------------------------------------------------
# PUT /{device_id}/static-route-intent
# ---------------------------------------------------------------------------


class StaticRouteEntry(BaseModel):
    # The NetBox routing.StaticRoute pk, when the pusher knows it. Optional: a pre-R3
    # plugin omits it entirely, and its absence is what keeps the rollout fence shut.
    route_id: int | None = None
    # The pusher's intent generation for this route — the token an apply result is
    # correlated against. Optional and adopted only when non-null, for route_id's reason:
    # a pusher that never learned the field must not erase a newer pusher's correlation.
    generation: int | None = None
    vrf: str = ""
    prefix: str
    next_hop: str
    interface_next_hop: str | None = None
    next_hop_vrf: str | None = None
    metric: int | None = None
    permanent: bool | None = None
    tag: int | None = None
    name: str | None = None
    accepted_at: UtcInstant | None = None


class RouteTriple(BaseModel):
    """One ``(vrf, prefix, next_hop)`` identity.

    The only thing the adapter can match a deleted NetBox route by once its intent row is gone.
    """

    vrf: str = ""
    prefix: str
    next_hop: str


class DeletedRoute(BaseModel):
    """One NetBox route pk the operator deleted, and where the adapter may be holding it.

    ``triples`` is the route's LINEAGE, most-authoritative-first (last acknowledged, then
    current): a content edit whose push never landed leaves the adapter on the older triple,
    so an id carrying only the current one would match nothing and be called moot (§4.1).
    An empty list restores the undecidable degraded-versus-moot partition and is a 422.

    Two is the ceiling, not a convention (R9-M2): the adapter can only hold a triple that was
    sent, and at most one claim is ever unresolved. A third distinct triple would be
    classification evidence the contract never grants — a ``route_id IS NULL`` row matching
    only it flips the acknowledgement from moot to degraded — so it is a 422 too.

    ``unverified`` is declared, never inferred from the lineage's shape: a verified ``[C, C]``
    deduplicates to exactly what a genuinely unverified ``[C]`` produces (R10-B1).
    """

    route_id: int
    triples: list[RouteTriple] = Field(min_length=1, max_length=2)
    unverified: bool


class StaticRouteIntentUpdate(BaseModel):
    routes: list[StaticRouteEntry]
    #: The required deletion authority this push carries (§4.4). An empty list means no
    #: deletions are carried. Every push marks removals per object.
    deleted_routes: list[DeletedRoute]


class StaticRouteIntentEcho(BaseModel):
    """One stored row's settlement coordinates — what the pusher records as its expectation.

    ``fingerprint`` is the hash of the exact wire entry this row renders, so it moves with
    the content the adapter really holds and not with the payload that was sent. Both
    nullables stay null for a pre-R3 row: reporting a placeholder would let a result that
    correlates with nothing settle something.
    """

    route_id: int | None
    generation: int | None
    fingerprint: str


class StaticRouteIntentResult(IntentApplyResult):
    """The static-route PUT's 2xx body: the shared summary, the per-route echo, the ack.

    The three id lists PARTITION the ``deleted_routes`` the request carried — unique within
    each list, pairwise disjoint, no id the request did not send, and exact coverage. The
    pusher validates all four properties and treats anything else as a protocol violation, so
    a union of the three is not enough: executed ``[1]`` with degraded ``[1]`` would otherwise
    drive both a degradation record and a restore decision for one id.

    ``removed_uncorrelated`` is emitted on EVERY mode, normal, store-only and backfill-only
    alike (R11-B2): a normal push detaches a ``route_id IS NULL`` row nobody claimed exactly
    as a backfill pass prunes one, and the pusher's conservative attribution rule reads it.
    """

    routes: list[StaticRouteIntentEcho]
    deleted_executed_ids: list[int]
    deleted_degraded_ids: list[int]
    deleted_moot_ids: list[int]
    removed_uncorrelated: list[RouteTriple]


class StaticRouteIntentOut(BaseModel):
    """The read-back of the same echo, for a PUT whose response was lost after it committed."""

    device_id: int
    routes: list[StaticRouteIntentEcho]


# Logged on every request to a device that still has a route_id-less intent row: that
# device is classified by today's rules, so a deletion there detaches instead of
# producing a correlated tombstone. Loud on purpose — it is the rollout's progress meter.


@router.get(
    "/{device_id}/static-route-intent",
    dependencies=[Depends(verify_token)],
    response_model=StaticRouteIntentOut,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_422_VALIDATION},
)
async def get_static_route_intent(device_id: int, db: AsyncSession = Depends(get_read_db)):
    """Re-serve the settlement triples the last PUT echoed — the lost-response recovery path.

    The PUT commits its store write before returning (and a terminal apply can notify the
    pusher before the response arrives), so a response lost in flight leaves the pusher with
    a committed intent it recorded no expectation for, and no other way to obtain one. This
    is that way: the same ``{route_id, generation, fingerprint}`` per stored row, read from
    the rows themselves, so it cannot drift from what the PUT reported.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    rows = (
        (
            await db.execute(
                select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id).order_by(StaticRouteIntent.id)
            )
        )
        .scalars()
        .all()
    )
    return {"device_id": device_id, "routes": [_echo(row) for row in rows]}


@router.put(
    "/{device_id}/static-route-intent",
    dependencies=[Depends(verify_token)],
    response_model=StaticRouteIntentResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_PUSH_SEQ_OR_DEVICE_BUSY, **RESP_422_VALIDATION},
)
@candidate_ingester(_apply_static_route_intent)
async def put_static_route_intent(
    device_id: int,
    body: StaticRouteIntentUpdate,
    db: AsyncSession = Depends(get_db),
    delivery=Depends(get_intent_delivery),
):
    """Replace the adapter's static-route intent mirror for this device atomically.

    Full-replace semantics: rows not present in the request body are deleted.
    ``accepted_at`` defaults to now if not supplied.  If ``auto_apply`` is
    enabled on the device, an apply job is enqueued after the upsert.

    Matching is ``route_id``-first, falling back to the ``(vrf, prefix, next_hop)``
    triple: an operator editing a route's prefix or next-hop therefore updates the
    intent row **in place** instead of appearing as an unrelated delete plus insert.
    A deletion on a device whose rows all carry a ``route_id`` also writes a
    tombstone — the only carrier of that deletion once the row is gone.

    Everything the plan depends on is read **under the device claim** (Q8): payload-internal
    refusals first because they need no store read, then acquisition, then the reload. Read
    before claiming and two concurrent pushes both plan against the same snapshot — the
    second then applies a plan whose premise is gone, and the deferred identity constraint
    surfaces it as a 500 instead of the sequentially correct answer.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise api_error(404, "not_found", "Device not found")

    _reject_payload_duplicates(body.routes)
    _reject_deletion_payload(body.deleted_routes)

    # Nothing of ours is pending, and the wait must not sit inside an open transaction.
    await db.rollback()
    try:
        async with held_claim(
            device_id, "intent_put", timeout_s=get_config().intent_claim_wait_seconds, guard_db=db
        ) as claim_reg:
            # The guard, before the first effectful statement and held to COMMIT: it is
            # what makes a concurrent revoke serialize against this transaction instead of
            # racing it.
            await lock_claim(db, claim_reg)
            return await _apply_static_route_intent(device_id, body, db, delivery)
    except ClaimUnavailableError:
        logger.warning("static_route.intent_claim_timeout", device_id=device_id)
        busy = api_error(
            409,
            "conflict",
            "The device is busy with another operation; retry",
            {"reason": "device_claimed"},
        )
    raise busy
