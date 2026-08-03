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
from nso_adapter.api.errors import (
    RESP_401,
    RESP_404_DEVICE,
    RESP_409_ACTIVE_JOB,
    RESP_422_VALIDATION,
    IntentApplyResult,
    api_error,
)
from nso_adapter.api.read_state import FamilyReadState, read_state_payload
from nso_adapter.api.timestamps import UtcInstant, iso_z
from nso_adapter.config import get_config
from nso_adapter.core.claim import ClaimUnavailableError, held_claim, lock_claim
from nso_adapter.core.request_flags import DELETE_ORIGIN, STORE_ONLY
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
    DeviceSettings,
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
    # The NetBox routing.StaticRoute pk, when the pusher knows it. Optional: a pre-R3
    # plugin omits it entirely, and its absence is what keeps the rollout fence shut.
    route_id: int | None = None
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


class StaticRouteIntentUpdate(BaseModel):
    routes: list[StaticRouteEntry]


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


@router.put(
    "/{device_id}/static-route-intent",
    dependencies=[Depends(verify_token)],
    response_model=IntentApplyResult,
    responses={**RESP_401, **RESP_404_DEVICE, **RESP_409_ACTIVE_JOB, **RESP_422_VALIDATION},
)
async def put_static_route_intent(device_id: int, body: StaticRouteIntentUpdate, db: AsyncSession = Depends(get_db)):
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
            return await _apply_static_route_intent(device_id, body, db)
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
) -> list[StaticRouteTombstone]:
    """Add a carrier for every row this push deletes; the caller stamps the job id.

    Written BEFORE the delete, because the delete expires the attributes they copy, and
    gated on the same ``STORE_ONLY`` check the job enqueue is — a tombstone with no job
    would be swept into one, which is exactly the device write store-only prevents.
    """
    if not removed_rows or not fence_open or STORE_ONLY.get():
        return []
    marking = "delete_origin" if DELETE_ORIGIN.get() else "detach"
    tombstones = [
        StaticRouteTombstone(
            device_id=device_id,
            route_id=row.route_id,
            vrf=row.vrf,
            prefix=row.prefix,
            next_hop=row.next_hop,
            deployed_key=row.deployed_key,
            marking=marking,
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


async def _apply_static_route_intent(device_id: int, body: StaticRouteIntentUpdate, db: AsyncSession) -> dict:
    """Run steps 3-9 of Q8, all under the claim the caller acquired and guard-locked."""
    device = await db.get(Device, device_id)
    if not device:
        # Offboarded between the 404 check and the claim: nothing left to write intent for.
        raise api_error(404, "not_found", "Device not found")

    existing_result = await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))
    existing = list(existing_result.scalars().all())

    # The fence is evaluated on the PRE-mutation row set. Post-payload evaluation would
    # read "open" on the very request that fills the last NULL route_id, and then claim
    # deletion authority for a triple nothing ever correlated with a NetBox route pk.
    null_route_ids = null_route_id_count(existing)
    fence_open = sr_fence_open(existing)
    if not fence_open:
        logger.warning(_FALLBACK_EVENT, device_id=device_id, null_route_id_count=null_route_ids)

    matched, removed_rows = _match_payload_to_rows(body.routes, existing)

    # The planned outcome: each entry's triple paired with the route_id that will claim it.
    # An entry naming no route_id keeps whatever its matched row already carries.
    planned: list[tuple[tuple[str, str, str], int | None]] = []
    for index, item in enumerate(body.routes):
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

    removed = [(r.vrf, r.prefix, r.next_hop) for r in removed_rows]
    tombstones = _write_tombstones(db, device_id, removed_rows, fence_open=fence_open)
    for row in removed_rows:
        await db.delete(row)

    now = datetime.now(UTC)
    count = 0
    cleared = False
    for index, item in enumerate(body.routes):
        accepted = item.accepted_at if item.accepted_at else now
        row = matched.get(index)
        if row is not None:
            before = {f: getattr(row, f) for f in _STATE_FIELDS}
            # The identity edit: same row, new triple. Legal transient collisions (a
            # same-payload swap, a delete-then-reclaim) are why the identity constraint
            # is DEFERRABLE INITIALLY DEFERRED.
            row.vrf = item.vrf
            row.prefix = item.prefix
            row.next_hop = item.next_hop
            if item.route_id is not None:
                row.route_id = item.route_id  # adopt/backfill; a pre-R3 push never clears it
            row.accepted_at = accepted
            row.interface_next_hop = item.interface_next_hop
            row.next_hop_vrf = item.next_hop_vrf
            row.metric = item.metric
            row.permanent = item.permanent
            row.tag = item.tag
            row.name = item.name
            # Clear detection is static-route-specific, not the shared `is_cleared`:
            # `permanent True -> False` IS a clear here because the renderer never emits
            # `permanent: false` (the other twelve scopes' writers do emit False, which is
            # why the shared predicate is right for them), and `name` is excluded outright
            # because it has no wire leaf.
            cleared_fields = {f for f in SR_CLEAR_FIELDS if sr_is_cleared(f, before[f], getattr(row, f))}
            # The carrier is written for EVERY detected clear, before job classification and
            # unconditionally — a pure clear and a delete-origin+clear both enqueue a
            # networked job, and neither `retract` nor the cleared fields survive into its
            # context, so a carrier written only on the detach path leaves those jobs with
            # no clear to find.
            update_pending_clear(
                row,
                cleared=cleared_fields,
                reset={f for f in SR_CLEAR_FIELDS if wire_set(f, getattr(row, f))},
                store_only=STORE_ONLY.get(),
            )
            if cleared_fields:
                cleared = True
        else:
            # deployed_key stays NULL: R1 never writes it at runtime — nothing is proven
            # committed until R2's CAS writer runs.
            db.add(
                StaticRouteIntent(
                    device_id=device_id,
                    route_id=item.route_id,
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
            )
        count += 1

    # Flushes the tombstone INSERTs and the row DELETEs, so this transaction holds those
    # rows before it touches `jobs` — the §3.9 order, `intent + tombstone -> jobs`.
    await db.flush()

    # Removal BEFORE apply, and both inside this transaction. The order is the contract:
    # the removal must carry the lower (created_at, id) so the worker's per-device head
    # claim runs it first — a retract that lands after the re-apply undoes the apply.
    replaced = False
    if removed or cleared:
        from nso_adapter.core.removal import enqueue_removal, removed_map

        # Direct, not via `replace_on_removal`: that shim commits first and enqueues
        # afterwards, which is what put the apply ahead of the removal and left the
        # tombstone with no job to point at.
        removal_job = await enqueue_removal(
            db,
            device_id,
            "static_route",
            removed=removed_map("static_route", removed) if removed else None,
            retract=cleared,
            shrank=bool(removed),
        )
        if removal_job is not None:
            replaced = True
            for tombstone in tombstones:
                tombstone.job_id = removal_job.id

    settings_result = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
    settings = settings_result.scalar_one_or_none()
    if settings and settings.auto_apply and count > 0:
        from nso_adapter.core.apply import enqueue_apply

        await enqueue_apply(db, device_id, force=True)

    await db.commit()

    return {"device_id": device_id, "count": count, "removed": len(removed), "replaced": replaced}
