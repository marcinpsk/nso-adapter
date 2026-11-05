# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Static-route ingestion shared by claimed delivery and rollback-only planning."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import structlog
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.errors import api_error
from nso_adapter.api.intent_push import begin_delivery
from nso_adapter.core.deleted_routes import DeletionPartition, DeletionRecord, classify_deletions
from nso_adapter.core.request_flags import BACKFILL_ONLY, DELETE_ORIGIN_MARKING, DETACH_MARKING, STORE_ONLY
from nso_adapter.core.static_route_plan import (
    SR_CLEAR_FIELDS,
    null_route_id_count,
    sr_is_cleared,
    update_pending_clear,
    wire_set,
)
from nso_adapter.core.static_route_plan import fence_open as sr_fence_open
from nso_adapter.store.models import Device, StaticRouteIntent, StaticRouteTombstone

if TYPE_CHECKING:
    from nso_adapter.api.static_route import DeletedRoute, StaticRouteEntry, StaticRouteIntentUpdate

logger = structlog.get_logger(__name__)

_STATE_FIELDS = ("interface_next_hop", "next_hop_vrf", "metric", "permanent", "tag", "name")


def _echo(row) -> dict:
    """Render one stored row as its settlement triple. The ONE renderer both paths call."""
    from nso_adapter.core.apply import static_route_fingerprint

    return {
        "route_id": row.route_id,
        "generation": row.intent_generation,
        "fingerprint": static_route_fingerprint(row),
    }


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
        # Missing route ids do not participate in duplicate-id detection.
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
            # Each route id must have exactly one deletion outcome.
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
    unreachable once payload-internal duplicates are refused, defense-in-depth beside
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
        # Adopt only unclaimed rows by triple; an entry without a route id preserves the stored id.
        if item.route_id is not None and row.route_id is not None:
            continue
        matched[index] = row
        claimed.add(row.id)

    return matched, [r for r in existing if r.id not in claimed]


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
    gated on the same ``STORE_ONLY`` check the job enqueue is, a tombstone with no job
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
            # The enqueue step stamps the job id on each new tombstone.
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
    """Ingest under the caller's device claim or rollback-only projection lock."""
    delivery = replace(delivery, identity=replace(delivery.identity, delete_origin=False))
    _reject_payload_duplicates(body.routes)
    _reject_deletion_payload(body.deleted_routes)
    device = await db.get(Device, device_id)
    if not device:
        # Offboarded between the 404 check and the claim: nothing left to write intent for.
        raise api_error(404, "not_found", "Device not found")

    # Record the revision under the projection lock; only promotion authorizes deployment.
    from nso_adapter.core.receipt import record_response

    if (replay := await begin_delivery(db, device_id, delivery)) is not None:
        return replay

    if BACKFILL_ONLY.get():
        return await _apply_backfill_only(device_id, body, db, delivery)

    existing_result = await db.execute(
        select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id).order_by(StaticRouteIntent.id)
    )
    existing = list(existing_result.scalars().all())

    # Check the replacement fence before mutation so this push cannot authorize its own deletion.
    null_route_ids = null_route_id_count(existing)
    fence_open = sr_fence_open(existing)
    if not fence_open:
        logger.warning(_FALLBACK_EVENT, device_id=device_id, null_route_id_count=null_route_ids)

    matched, removed_rows = _match_payload_to_rows(body.routes, existing)
    _reject_double_claimed_triple(body.routes, matched)

    # Classify deletions before their route ids and triples leave the store.
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
    # Echo the stored rows in payload order.
    echoed: list[StaticRouteIntent] = []
    for index, item in enumerate(body.routes):
        accepted = item.accepted_at if item.accepted_at else now
        route_row = matched.get(index)
        if route_row is not None:
            before = {f: getattr(route_row, f) for f in _STATE_FIELDS}
            # Deferred uniqueness permits triple swaps and delete-then-reclaim updates within this transaction.
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
            # Detect clears that remove emitted wire leaves, including permanent changing from true to false.
            cleared_fields = {f for f in SR_CLEAR_FIELDS if sr_is_cleared(f, before[f], getattr(route_row, f))}
            # Record every clear before job classification so both networked removal paths keep its authority.
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
            # A new row has no deployed key until execution certifies a committed write.
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

    # Lock intent rows and tombstones before jobs.
    await db.flush()

    removal_generation_count = len(removed_by_marking) if removed_rows else int(cleared)
    from nso_adapter.core.generation import prepare_request_settlement

    auto_apply, settlement_cohort = await prepare_request_settlement(
        db,
        device_id,
        mutation_count=count,
        removal_generation_count=removal_generation_count,
    )

    # Enqueue removal before apply so the worker cannot retract a later re-apply.
    replaced = False
    if removed_rows or cleared:
        from nso_adapter.core.removal import enqueue_static_route_removals

        # Split removals by marking and stamp each job's carriers.
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

    # Build settlement coordinates from stored rows before commit.
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
      filling the NULL ids of every row the payload still names, and nothing else. Content is
      not written, so a row whose adapter state has drifted stays drifted and the pusher
      records no acknowledgement of it (O1.32/O1.35);
    * it LEAVES every omitted row that carries a ``route_id`` exactly as it is. That is the
      before-image protection, and it is what makes the mode safe for a pending deletion;
    * it PRUNES every omitted row whose ``route_id`` is NULL, reporting each in
      ``removed_uncorrelated`` (R6-B4). A matched NULL row must receive an id from the
      payload, or the pass is refused. The fence predicate is exactly
      ``null_route_id_count(rows) == 0``, so these two operations are necessary and
      sufficient, and pruning is safe by definition, because a NULL row correlates with no
      NetBox pk and can never be the subject of a genuine deletion;
    * it SPAWNS nothing: no removal job, no tombstone, no auto-apply, so the pass can never
      cause a device write. A payload entry matching no row creates no row either, an
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
        # Count only rows that received settlement coordinates.
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

    Nothing is written yet, the caller's ``held_claim`` rolls this transaction back, so the
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
