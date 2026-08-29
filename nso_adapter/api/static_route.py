# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""GET /api/v1/devices/{id}/static-routes and PUT /api/v1/devices/{id}/static-route-intent."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, get_read_db, verify_token
from nso_adapter.api.errors import (
    RESP_401,
    RESP_404_DEVICE,
    RESP_409_PUSH_SEQ_OR_DEVICE_BUSY,
    RESP_422_VALIDATION,
    IntentApplyResult,
    api_error,
)
from nso_adapter.api.intent_push import begin_delivery, get_intent_delivery
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import UtcInstant, iso_z, latest_refreshed
from nso_adapter.config import get_config
from nso_adapter.core.claim import ClaimUnavailableError, held_claim, lock_claim
from nso_adapter.core.deleted_routes import DeletionPartition, DeletionRecord, classify_deletions
from nso_adapter.core.request_flags import (
    BACKFILL_ONLY,
    DELETE_ORIGIN_MARKING,
    DETACH_MARKING,
    STORE_ONLY,
)
from nso_adapter.core.static_route_plan import (
    SR_CLEAR_FIELDS,
    null_route_id_count,
    sr_is_cleared,
    update_pending_clear,
    wire_set,
)
from nso_adapter.core.static_route_plan import fence_open as sr_fence_open
from nso_adapter.store import outcome_store
from nso_adapter.store.models import (
    Device,
    DeviceStaticRoute,
    StaticRouteIntent,
    StaticRouteTombstone,
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


# Scalars the writer emits only when set — `if row.metric is not None:` / `if getattr(row, 'interface_next_hop', None):` (nso/apply.py)
# A merge-PATCH apply can never drop one that goes back to unset, so clearing any of
# them must enqueue a PUT-replace retract. Clear DETECTION runs over the subset that has a
# wire leaf (``SR_CLEAR_FIELDS``); ``name`` stays here for the before-image and nothing else.
_STATE_FIELDS = ("interface_next_hop", "next_hop_vrf", "metric", "permanent", "tag", "name")


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


def _echo(row) -> dict:
    """Render one stored row as its settlement triple. The ONE renderer both paths call."""
    from nso_adapter.core.apply import static_route_fingerprint

    return {
        "route_id": row.route_id,
        "generation": row.intent_generation,
        "fingerprint": static_route_fingerprint(row),
    }


# Logged on every request to a device that still has a route_id-less intent row: that
# device is classified by today's rules, so a deletion there detaches instead of
# producing a correlated tombstone. Loud on purpose — it is the rollout's progress meter.
_FALLBACK_EVENT = "static_route.null_route_id_fallback"


def _triple(item: StaticRouteEntry) -> tuple[str, str, str]:
    return (item.vrf, item.prefix, item.next_hop)


def _reject_payload_duplicates(routes: list[StaticRouteEntry]) -> None:
    """Payload-internal refusals. They need no store read, so they run first."""
    seen_triples: set[tuple[str, str, str]] = set()
    for item in routes:
        triple = _triple(item)
        if triple in seen_triples:
            raise api_error(
                422,
                "validation_error",
                "Two routes in the payload carry the same (vrf, prefix, next_hop)",
                {"reason": "duplicate_triple", "triple": list(triple)},
            )
        seen_triples.add(triple)

    seen_route_ids: set[int] = set()
    for item in routes:
        # NULL is excluded, and that is load-bearing: a pre-R3 push of two distinct routes
        # carries [None, None], and rejecting it would break the fence-shut path the whole
        # rollout depends on.
        if item.route_id is None:
            continue
        if item.route_id in seen_route_ids:
            raise api_error(
                422,
                "validation_error",
                "Two routes in the payload claim the same route_id",
                {"reason": "duplicate_route_id", "route_id": item.route_id},
            )
        seen_route_ids.add(item.route_id)


def _reject_deletion_payload(records: list[DeletedRoute]) -> None:
    """Payload-internal refusals for the deletion authority. No store read, so they run first.

    A backfill-only pass carries no authority AT ALL: its whole purpose is to open a device's
    fence without touching anything else, and it writes neither tombstone nor job, so a
    deletion in that body could only ever be dropped silently.
    """
    if BACKFILL_ONLY.get() and records:
        raise api_error(
            422,
            "validation_error",
            "A backfill-only pass carries no deletion authority; deleted_routes must be empty",
            {"reason": "backfill_carries_deletions", "route_ids": [r.route_id for r in records]},
        )
    seen: set[int] = set()
    for record in records:
        if record.route_id in seen:
            # Emission is id-oriented, exactly one outcome per id: two records for one pk
            # cannot both be answered, and a partition that names it twice is rejected.
            raise api_error(
                422,
                "validation_error",
                "Two deleted_routes records claim the same route_id",
                {"reason": "duplicate_deleted_route_id", "route_id": record.route_id},
            )
        seen.add(record.route_id)


def _deletion_records(records: list[DeletedRoute]) -> list[DeletionRecord]:
    """Wire records → the classifier's own shape, with each lineage deduplicated in order."""
    out: list[DeletionRecord] = []
    for record in records:
        triples: list[tuple[str, str, str]] = []
        for triple in record.triples:
            key = (triple.vrf, triple.prefix, triple.next_hop)
            if key not in triples:
                triples.append(key)
        out.append(DeletionRecord(record.route_id, tuple(triples), record.unverified))
    return out


def _double_claimed_triple(planned: list[tuple[tuple[str, str, str], int | None]]) -> tuple | None:
    """Return the first triple two distinct route_ids claim in the planned outcome.

    Under full-replace the planned triples are exactly the payload triples, so this is
    unreachable once payload-internal duplicates are refused — defense-in-depth beside
    the deferred unique constraint, kept because the authority states the rule on the
    final state rather than on the payload.
    """
    claims: dict[tuple[str, str, str], set[int]] = {}
    for triple, route_id in planned:
        if route_id is not None:
            claims.setdefault(triple, set()).add(route_id)
    for triple, route_ids in claims.items():
        if len(route_ids) > 1:
            return triple
    return None


def _match_payload_to_rows(
    routes: list[StaticRouteEntry], existing: list[StaticRouteIntent]
) -> tuple[dict[int, StaticRouteIntent], list[StaticRouteIntent]]:
    """Pair payload entries with store rows: route_id first, then the triple.

    Two passes, not one: a single in-order pass lets an earlier route_id-less entry
    triple-claim the very row a later entry owns by route_id. Returns
    ``({payload index: row}, rows nothing claimed)``.
    """
    by_route_id = {r.route_id: r for r in existing if r.route_id is not None}
    by_triple = {(r.vrf, r.prefix, r.next_hop): r for r in existing}
    matched: dict[int, StaticRouteIntent] = {}
    claimed: set[int] = set()

    for index, item in enumerate(routes):
        row = by_route_id.get(item.route_id) if item.route_id is not None else None
        if row is not None:
            matched[index] = row
            claimed.add(row.id)

    for index, item in enumerate(routes):
        if index in matched:
            continue
        row = by_triple.get(_triple(item))
        if row is None or row.id in claimed:
            continue
        # An entry that names a route_id and did not match one is a DIFFERENT route from
        # any row that already carries a route_id, even when they share a triple: that
        # row was dropped from the payload and its deletion has to be recorded. Only a
        # row with no provenance at all may be adopted. An entry naming no route_id
        # asserts nothing, so it matches by triple and leaves any backfilled id alone —
        # that is the pre-R3 push, which must not undo the fleet's backfill.
        if item.route_id is not None and row.route_id is not None:
            continue
        matched[index] = row
        claimed.add(row.id)

    return matched, [r for r in existing if r.id not in claimed]


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
    delivery = replace(delivery, identity=replace(delivery.identity, delete_origin=False))
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
        raise api_error(
            409,
            "conflict",
            "The device is busy with another operation; retry",
            {"reason": "device_claimed"},
        ) from None


def _write_tombstones(
    db: AsyncSession,
    device_id: int,
    removed_rows: list[StaticRouteIntent],
    *,
    fence_open: bool,
    markings: dict[int, str],
) -> list[StaticRouteTombstone]:
    """Add a carrier for every row this push deletes; the caller stamps the job id.

    *markings* gives each removed row's deletion provenance by intent-row id, rather than
    this function reading the request flag: the marking is per OBJECT (§4.5), and a carrier
    marked here is what its removal job's authority is read from later.

    Written BEFORE the delete, because the delete expires the attributes they copy, and
    gated on the same ``STORE_ONLY`` check the job enqueue is — a tombstone with no job
    would be swept into one, which is exactly the device write store-only prevents.
    """
    if not removed_rows or not fence_open or STORE_ONLY.get():
        return []
    tombstones = [
        StaticRouteTombstone(
            device_id=device_id,
            route_id=row.route_id,
            vrf=row.vrf,
            prefix=row.prefix,
            next_hop=row.next_hop,
            deployed_key=row.deployed_key,
            marking=markings[row.id],
            # Stamped by the caller with the removal job enqueued in THIS transaction. A
            # tombstone left NULL here is, to the sweeper, a deletion whose job never got
            # created.
            job_id=None,
        )
        for row in removed_rows
    ]
    for tombstone in tombstones:
        db.add(tombstone)
    return tombstones


def _reject_double_claimed_triple(routes: list[StaticRouteEntry], matched: dict[int, StaticRouteIntent]) -> None:
    """Refuse a push whose PLANNED outcome gives one triple two distinct route_id claimants.

    The planned outcome pairs each entry's triple with the route_id that will claim it. An
    entry naming no route_id keeps whatever its matched row already carries.
    """
    planned: list[tuple[tuple[str, str, str], int | None]] = []
    for index, item in enumerate(routes):
        row = matched.get(index)
        claimant = item.route_id if item.route_id is not None else (row.route_id if row is not None else None)
        planned.append((_triple(item), claimant))
    conflict = _double_claimed_triple(planned)
    if conflict is not None:
        raise api_error(
            422,
            "validation_error",
            "Two routes would claim the same (vrf, prefix, next_hop)",
            {"reason": "duplicate_triple", "triple": list(conflict)},
        )


async def _apply_static_route_intent(
    device_id: int, body: StaticRouteIntentUpdate, db: AsyncSession, delivery
) -> dict | JSONResponse:
    """Run steps 3-9 of Q8, all under the claim the caller acquired and guard-locked."""
    device = await db.get(Device, device_id)
    if not device:
        # Offboarded between the 404 check and the claim: nothing left to write intent for.
        raise api_error(404, "not_found", "Device not found")

    # Every accepted write records its projection revision, store-only and
    # auto-apply-off included, and takes the device's projection lock before anything is
    # read (#1522 §G2). Only a promotion authorizes a deployment.
    from nso_adapter.core.receipt import record_response

    if (replay := await begin_delivery(db, device_id, delivery)) is not None:
        return replay

    if BACKFILL_ONLY.get():
        return await _apply_backfill_only(device_id, body, db, delivery)

    existing_result = await db.execute(
        select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id).order_by(StaticRouteIntent.id)
    )
    existing = list(existing_result.scalars().all())

    # The fence is evaluated on the PRE-mutation row set. Post-payload evaluation would
    # read "open" on the very request that fills the last NULL route_id, and then claim
    # deletion authority for a triple nothing ever correlated with a NetBox route pk.
    null_route_ids = null_route_id_count(existing)
    fence_open = sr_fence_open(existing)
    if not fence_open:
        logger.warning(_FALLBACK_EVENT, device_id=device_id, null_route_id_count=null_route_ids)

    matched, removed_rows = _match_payload_to_rows(body.routes, existing)
    _reject_double_claimed_triple(body.routes, matched)

    # The partition, over the PRE-deletion rows: their route_id and triple are what the
    # requested ids bind against, and both are gone once the deletes run.
    partition = classify_deletions(_deletion_records(body.deleted_routes), removed_rows)
    _refuse_unmarkable_deletions(partition, fence_open=fence_open)

    markings = _removal_markings(removed_rows, partition)
    promotion_deletions = [
        {
            "table": "static_route_intent",
            "route_id": row.route_id,
            "key": [row.vrf, row.prefix, row.next_hop],
            "marking": markings[row.id],
        }
        for row in removed_rows
    ]
    removed_by_marking: dict[str, list] = {}
    for row in removed_rows:
        removed_by_marking.setdefault(markings[row.id], []).append((row.vrf, row.prefix, row.next_hop))
    tombstones = _write_tombstones(db, device_id, removed_rows, fence_open=fence_open, markings=markings)
    for row in removed_rows:
        await db.delete(row)

    now = datetime.now(UTC)
    count = 0
    cleared = False
    # The rows the response echoes, in payload order, so the pusher can pair them with the
    # entries it sent. Collected as ORM objects and rendered once at the end, off the stored
    # rows rather than off the payload.
    echoed: list[StaticRouteIntent] = []
    for index, item in enumerate(body.routes):
        accepted = item.accepted_at if item.accepted_at else now
        route_row = matched.get(index)
        if route_row is not None:
            before = {f: getattr(route_row, f) for f in _STATE_FIELDS}
            # The identity edit: same row, new triple. Legal transient collisions (a
            # same-payload swap, a delete-then-reclaim) are why the identity constraint
            # is DEFERRABLE INITIALLY DEFERRED.
            route_row.vrf = item.vrf
            route_row.prefix = item.prefix
            route_row.next_hop = item.next_hop
            if item.route_id is not None:
                route_row.route_id = item.route_id  # adopt/backfill; a pre-R3 push never clears it
            if item.generation is not None:
                route_row.intent_generation = item.generation  # same rule, same reason
            route_row.accepted_at = accepted
            route_row.interface_next_hop = item.interface_next_hop
            route_row.next_hop_vrf = item.next_hop_vrf
            route_row.metric = item.metric
            route_row.permanent = item.permanent
            route_row.tag = item.tag
            route_row.name = item.name
            # Clear detection is static-route-specific, not the shared `is_cleared`:
            # `permanent True -> False` IS a clear here because the renderer never emits
            # `permanent: false` (the other twelve scopes' writers do emit False, which is
            # why the shared predicate is right for them), and `name` is excluded outright
            # because it has no wire leaf.
            cleared_fields = {f for f in SR_CLEAR_FIELDS if sr_is_cleared(f, before[f], getattr(route_row, f))}
            # The carrier is written for EVERY detected clear, before job classification and
            # unconditionally — a pure clear and a delete-origin+clear both enqueue a
            # networked job, and neither `retract` nor the cleared fields survive into its
            # context, so a carrier written only on the detach path leaves those jobs with
            # no clear to find.
            update_pending_clear(
                route_row,
                cleared=cleared_fields,
                reset={f for f in SR_CLEAR_FIELDS if wire_set(f, getattr(route_row, f))},
                store_only=STORE_ONLY.get(),
            )
            if cleared_fields:
                cleared = True
            echoed.append(route_row)
        else:
            # deployed_key stays NULL: R1 never writes it at runtime — nothing is proven
            # committed until R2's CAS writer runs.
            route_row = StaticRouteIntent(
                device_id=device_id,
                route_id=item.route_id,
                intent_generation=item.generation,
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
            db.add(route_row)
            echoed.append(route_row)
        count += 1

    # Flushes the tombstone INSERTs and the row DELETEs, so this transaction holds those
    # rows before it touches `jobs` — the §3.9 order, `intent + tombstone -> jobs`.
    await db.flush()

    removal_generation_count = len(removed_by_marking) if removed_rows else int(cleared)
    from nso_adapter.core.generation import prepare_request_settlement

    auto_apply, settlement_cohort = await prepare_request_settlement(
        db,
        device_id,
        mutation_count=count,
        removal_generation_count=removal_generation_count,
    )

    # Removal BEFORE apply, and both inside this transaction. The order is the contract:
    # the removal must carry the lower (created_at, id) so the worker's per-device head
    # claim runs it first — a retract that lands after the re-apply undoes the apply.
    replaced = False
    if removed_rows or cleared:
        from nso_adapter.core.removal import enqueue_static_route_removals

        # Direct, not via `replace_on_removal`: that shim commits first and enqueues
        # afterwards, which is what put the apply ahead of the removal and left the
        # tombstone with no job to point at. One job per marking, each stamping its own
        # carriers (§4.5). A homogeneous push, which is every push today, gets exactly one.
        replaced = bool(
            await enqueue_static_route_removals(
                db,
                device_id,
                promotes=(delivery.stream,),
                removed=removed_by_marking,
                tombstones=tombstones,
                retract=cleared,
                settlement_cohort=settlement_cohort,
            )
        )

    if auto_apply:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(
            db,
            device_id,
            force=True,
            stream=delivery.stream,
            settlement_cohort=settlement_cohort,
        )

    # Rendered BEFORE the commit, off the rows this transaction just wrote: the echo is the
    # pusher's settlement expectation, so it must describe exactly what was stored, not what
    # was asked for.
    routes = [_echo(row) for row in echoed]

    result = {
        "device_id": device_id,
        "count": count,
        "removed": len(removed_rows),
        "replaced": replaced,
        "routes": routes,
        # The public response model filters this receipt-only carrier from the wire.
        "_promotion_deletions": promotion_deletions,
        **_acknowledgement(partition),
    }
    await record_response(db, device_id, delivery, result)
    await db.commit()
    return result


async def _apply_backfill_only(device_id: int, body: StaticRouteIntentUpdate, db: AsyncSession, delivery) -> dict:
    """Open this device's replacement fence, and do nothing else (OQ-O-8(a), §4.4).

    A key holding a pending genuine deletion cannot open its own fence: any ordinary push that
    omits the deleted route destroys the before-image the deletion depends on (`:476-477`,
    O-A4). This mode is the way out, and it is deliberately the narrowest one that works:

    * it ADOPTS ``route_id`` and ``generation`` from every payload entry onto its matched row,
      filling the NULL ids of every row the payload still names — and nothing else. Content is
      not written, so a row whose adapter state has drifted stays drifted and the pusher
      records no acknowledgement of it (O1.32/O1.35);
    * it LEAVES every omitted row that carries a ``route_id`` exactly as it is. That is the
      before-image protection, and it is what makes the mode safe for a pending deletion;
    * it PRUNES every omitted row whose ``route_id`` is NULL, reporting each in
      ``removed_uncorrelated`` (R6-B4). A matched NULL row must receive an id from the
      payload, or the pass is refused. The fence predicate is exactly
      ``null_route_id_count(rows) == 0``, so these two operations are necessary and
      sufficient — and pruning is safe by definition, because a NULL row correlates with no
      NetBox pk and can never be the subject of a genuine deletion;
    * it SPAWNS nothing: no removal job, no tombstone, no auto-apply, so the pass can never
      cause a device write. A payload entry matching no row creates no row either — an
      ordinary push does that.

    It takes an ``X-Push-Seq`` and writes a receipt like any other accepted operation, so it
    is replayable and cannot be re-applied at a stale sequence.
    """
    existing = list(
        (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))).scalars().all()
    )
    matched, omitted = _match_payload_to_rows(body.routes, existing)

    residual_nulls = [
        _triple(item)
        for index, item in enumerate(body.routes)
        if (row := matched.get(index)) is not None and row.route_id is None and item.route_id is None
    ]
    if residual_nulls:
        raise api_error(
            422,
            "validation_error",
            "A backfill-only pass must assign a route_id to every matched uncorrelated row",
            {
                "reason": "backfill_missing_route_id",
                "routes": [
                    {"vrf": vrf, "prefix": prefix, "next_hop": next_hop}
                    for vrf, prefix, next_hop in sorted(residual_nulls)
                ],
            },
        )

    adopted: list[StaticRouteIntent] = []
    for index in range(len(body.routes)):
        row = matched.get(index)
        if row is None:
            continue
        item = body.routes[index]
        if item.route_id is None and item.generation is None:
            continue  # nothing to adopt, so nothing to acknowledge
        if item.route_id is not None:
            row.route_id = item.route_id
        if item.generation is not None:
            row.intent_generation = item.generation
        adopted.append(row)

    pruned = [row for row in omitted if row.route_id is None]
    uncorrelated = tuple(sorted((row.vrf or "", row.prefix or "", row.next_hop or "") for row in pruned))
    for row in pruned:
        await db.delete(row)
    await db.flush()

    logger.info(
        "static_route.backfill_only",
        device_id=device_id,
        adopted=len(adopted),
        pruned=[list(triple) for triple in uncorrelated],
    )
    result = {
        "device_id": device_id,
        # The rows the pass adopted an id onto, not the payload length: an entry that matched
        # nothing wrote nothing and has no settlement coordinates to echo.
        "count": len(adopted),
        "removed": len(pruned),
        "replaced": False,
        "routes": [_echo(row) for row in adopted],
        **_acknowledgement(
            DeletionPartition(
                executed=(),
                degraded=(),
                moot=(),
                uncorrelated=uncorrelated,
                genuine_row_ids=frozenset(),
            )
        ),
    }
    from nso_adapter.core.receipt import record_response

    await record_response(db, device_id, delivery, result)
    await db.commit()
    return result


def _acknowledgement(partition) -> dict:
    """Render the four fields every mode reports.

    Sorted, so request order cannot reach the wire.
    """
    return {
        "deleted_executed_ids": list(partition.executed),
        "deleted_degraded_ids": list(partition.degraded),
        "deleted_moot_ids": list(partition.moot),
        "removed_uncorrelated": [
            {"vrf": vrf, "prefix": prefix, "next_hop": next_hop} for vrf, prefix, next_hop in partition.uncorrelated
        ],
    }


def _refuse_unmarkable_deletions(partition, *, fence_open: bool) -> None:
    """Refuse, before ANY effect, a request whose genuine deletions cannot be marked (§4.4).

    A genuine id needs a tombstone on an immediately promoted request. A store-only request
    preserves the removed row and its marking in the durable receipt for a later selected
    Apply. The fence remains mandatory for an immediate removal job.

    Nothing is written yet — the caller's ``held_claim`` rolls this transaction back — so the
    sequence is NOT burned and the pusher can abandon the claim and re-send at a new one once
    a backfill-only pass has opened the fence.
    """
    if not partition.executed:
        return
    if not STORE_ONLY.get() and not fence_open:
        reason = "fence_shut"
        logger.warning("static_route.deletion_refused", reason=reason, route_ids=list(partition.executed))
        raise api_error(
            409,
            "conflict",
            f"This device cannot record a route deletion right now ({reason}); nothing was applied",
            {"reason": reason, "route_ids": list(partition.executed)},
        )


def _removal_markings(removed_rows, partition) -> dict[int, str]:
    """Each removed row's deletion provenance, by intent-row id.

    Every push carries ``deleted_routes`` (§4.5), so each row is marked by whether a NetBox
    deletion authorized it. The request-wide ``?delete_origin`` flag does not apply here.
    """
    return {
        row.id: (DELETE_ORIGIN_MARKING if row.id in partition.genuine_row_ids else DETACH_MARKING)
        for row in removed_rows
    }
