# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The ``deployed_key`` compare-and-set — #1396 R2 §4.6.

``deployed_key`` is deletion authority: it is the triple a later removal is allowed to drop
from the device on this row's behalf. Writing it is therefore only ever legal on a
CONCLUSIVE proof (§4.4), and only against the value the plan snapshotted before the network
call — another session may have moved the row while the PUT was in flight.

Every function here is a plain statement runner: the caller owns the transaction, takes the
claim lock and commits. That is deliberate — the CAS has to land in the SAME transaction as
the row stamps, the per-route results and the terminal job status, or a crash between them
leaves a closed replacement under a failed apply (§4.6 atomicity).
"""

from __future__ import annotations

from typing import Any
from typing import cast as type_cast

import structlog
from sqlalchemy import cast, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

#: What :func:`cas_deployed_key` did. Never a boolean — "the row moved on" and "there was no
#: unambiguous carrier to write" are different facts and drive different follow-ups.
CAS_ROW = "row"  # the live intent row took the new triple
CAS_STOPPED = "stopped"  # the row is still there with a different deployed_key (a B->C edit landed)
CAS_TOMBSTONE = "tombstone"  # the row is gone; exactly one post-watermark tombstone carried it
CAS_ABSTAINED = "abstained"  # the row is gone and no single tombstone could be identified


#: ``none_as_null`` is load-bearing, not tidiness: a plain ``JSONB`` binds Python ``None`` as
#: the JSON encoding ``'null'::jsonb``, which is DISTINCT FROM SQL NULL — so the CAS of a
#: never-applied row (``deployed_key IS NULL``) would miss every time and never bootstrap.
_JSONB = JSONB(none_as_null=True)


def _jsonb(value):
    """Bind a Python triple/None as jsonb so ``IS NOT DISTINCT FROM`` compares like with like."""
    return cast(value, _JSONB)


async def cas_deployed_key(
    db: AsyncSession,
    *,
    device_id: int,
    row_id: int,
    route_id: int | None,
    sent_triple: tuple[str, str, str],
    expected_old,
    tombstone_id_watermark: int,
) -> str:
    """Move this row's ``deployed_key`` to *sent_triple*, but only from *expected_old*.

    Identity is the intent row ``id``, not ``route_id``: the row id is defined for
    fence-shut rows (``route_id IS NULL``), is equivalent to ``route_id`` for a live row,
    and cannot be mis-hit by a delete-and-recreate of the same NetBox pk — that case must
    land on the tombstone, which is what the fallback below is for.

    On a miss the row is RE-READ under lock and the branch is taken on existence, never on
    the miss alone. A row that still exists with a different ``deployed_key`` is a later
    identity edit landing first (C-over-B stays open) and we stop; only an ABSENT row falls
    back to the tombstone. Falling back on any miss would rewrite an unrelated older
    tombstone that happens to share the ``route_id`` — the schema permits exactly that.
    """
    from nso_adapter.store.models import StaticRouteIntent

    sent = list(sent_triple)
    result = type_cast(
        CursorResult[Any],
        await db.execute(
            update(StaticRouteIntent)
            .where(
                StaticRouteIntent.device_id == device_id,
                StaticRouteIntent.id == row_id,
                StaticRouteIntent.deployed_key.is_not_distinct_from(_jsonb(expected_old)),
            )
            .values(deployed_key=sent)
        ),
    )
    if result.rowcount:
        return CAS_ROW

    still_there = await db.scalar(select(StaticRouteIntent.id).where(StaticRouteIntent.id == row_id).with_for_update())
    if still_there is not None:
        logger.info(
            "static_route.cas_stopped",
            device_id=device_id,
            row_id=row_id,
            sent=sent,
            expected_old=expected_old,
        )
        return CAS_STOPPED
    return await _cas_onto_tombstone(
        db,
        device_id=device_id,
        route_id=route_id,
        sent=sent,
        expected_old=expected_old,
        watermark=tombstone_id_watermark,
    )


async def _cas_onto_tombstone(db: AsyncSession, *, device_id, route_id, sent, expected_old, watermark: int) -> str:
    """Write the sent triple onto the tombstone that carries it — the X2 belt.

    The row was deleted mid-flight, so its carrier is a tombstone rather than an intent row.

    Unreachable in production — R1's per-device claim makes an intent PUT wait and 409
    rather than commit a delete while an apply job runs — and built anyway (OQ-R2-2), so it
    survives any later weakening of that serialization.

    NOT oldest-first. Several tombstones can share a ``route_id`` AND an ``expected_old``
    (they carry no source-row id and no uniqueness), and picking the oldest writes the sent
    triple onto a stale carrier while the real one keeps the old value — its removal could
    then not authorize what this apply actually sent. So: only candidates created after the
    plan's snapshot, exactly one of them, or ABSTAIN. Abstaining grants no authority at all;
    the carrier keeps its old ``deployed_key`` and its removal authorizes ``{triple}`` alone.
    """
    from nso_adapter.store.models import StaticRouteTombstone

    if route_id is None:
        # A fence-shut row has no NetBox pk to correlate a tombstone by — and a fence-shut
        # device has no tombstones either (they are only written with the fence open).
        return CAS_ABSTAINED
    candidates = (
        (
            await db.execute(
                select(StaticRouteTombstone)
                .where(
                    StaticRouteTombstone.device_id == device_id,
                    StaticRouteTombstone.route_id == route_id,
                    StaticRouteTombstone.id > watermark,
                    StaticRouteTombstone.deployed_key.is_not_distinct_from(_jsonb(expected_old)),
                )
                .order_by(StaticRouteTombstone.id)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    if len(candidates) != 1:
        logger.warning(
            "static_route.cas_abstained",
            device_id=device_id,
            route_id=route_id,
            candidates=len(candidates),
            watermark=watermark,
        )
        return CAS_ABSTAINED
    candidates[0].deployed_key = sent
    logger.info("static_route.cas_tombstone", device_id=device_id, route_id=route_id, tombstone_id=candidates[0].id)
    return CAS_TOMBSTONE


__all__ = [
    "CAS_ABSTAINED",
    "CAS_ROW",
    "CAS_STOPPED",
    "CAS_TOMBSTONE",
    "cas_deployed_key",
]
