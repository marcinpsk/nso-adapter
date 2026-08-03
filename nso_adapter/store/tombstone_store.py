# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Repository primitives over ``static_route_tombstone``.

R1 ships and tests the snapshotted delete; **R2** supplies the consumption proof that
decides when to call it. Nothing here decides whether a deletion is proven.
"""

from __future__ import annotations

from collections.abc import Iterable

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import ClaimRegistration, lock_claim
from nso_adapter.store.models import StaticRouteTombstone

logger = structlog.get_logger(__name__)


async def delete_tombstones(
    db: AsyncSession,
    ids: Iterable[int],
    *,
    device_id: int,
    claim_token: str,
) -> int:
    """Delete exactly the supplied snapshotted ids, under the caller's claim.

    Takes the claim's row lock first — a subquery predicate would prove the token was
    valid at statement time and nothing would make it valid at COMMIT time — and raises
    ``ClaimLostError`` if the claim is no longer ours, deleting nothing.

    The supplied ids are then pre-locked in sorted order: a bulk
    ``DELETE … id = ANY(...)`` has no guaranteed per-row lock order, so without it two
    callers can interleave against each other's sorted pre-lock. Per-device claim
    exclusion already makes that unreachable; the pre-lock is what makes the ordering
    property local and checkable instead of an argument about exclusion.

    Deletes only what the caller snapshotted: a tombstone inserted after the snapshot
    survives, because nothing has proven anything about it. The caller owns the commit.
    """
    await lock_claim(db, ClaimRegistration(device_id, claim_token))
    ordered = sorted(set(ids))
    if not ordered:
        return 0
    await db.execute(
        select(StaticRouteTombstone.id)
        .where(StaticRouteTombstone.id.in_(ordered))
        .order_by(StaticRouteTombstone.id)
        .with_for_update()
    )
    result = await db.execute(
        delete(StaticRouteTombstone).where(
            StaticRouteTombstone.id.in_(ordered),
            StaticRouteTombstone.device_id == device_id,
        )
    )
    logger.info("tombstone.deleted", device_id=device_id, count=result.rowcount)
    return result.rowcount
