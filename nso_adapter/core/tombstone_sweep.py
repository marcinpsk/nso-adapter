# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The tombstone sweeper: give every uncarried deletion a removal job.

A tombstone is the only surviving record of a deleted static-route intent row. If the
push that wrote it died before its removal job was committed — or that job later failed —
nothing else will ever retract the route from the device. The sweeper is the recovery
path, and it runs at startup and periodically, in a deployment that deliberately allows
two adapter processes to overlap.

Exclusion is the ordinary per-device claim, acquired with ``purpose='sweep'``. It is what
makes two sweepers, a teardown, an intent PUT and ``delete_tombstones`` mutually exclusive
on a device — the loser simply fails to acquire and skips. A ``NOT EXISTS (SELECT 1 FROM
device_claim …)`` predicate cannot do that: absence cannot be row-locked, and under READ
COMMITTED a claimant can insert and commit after the sweep's statement snapshot,
invisibly to it.
"""

from __future__ import annotations

import structlog
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import acquire_claim, claim_session, lock_claim, release_claim
from nso_adapter.core.request_flags import DETACH_MARKING
from nso_adapter.core.static_route_plan import as_triple, triple_of
from nso_adapter.store.models import Job, JobStatus, JobType, StaticRouteTombstone

logger = structlog.get_logger(__name__)


def _eligible():
    """Tombstones with no job, or whose job failed. Nothing else.

    A tombstone owned by a SUCCEEDED job is deliberately left alone: re-sweeping it would
    re-issue a removal for config that was already removed. Those rows accumulate for the
    length of R1's deployment and are R2's handoff set — R1 must not garbage-collect them
    and must not widen this predicate to ``status <> 'failed'``.

    Debt for whoever adds job GC: ``job_id`` is ``ON DELETE SET NULL``, so deleting a
    terminal job turns its tombstone back into a ``job_id IS NULL`` row and this predicate
    re-sweeps it. No production job-deletion path exists today; before one lands, either
    make that FK restrictive for tombstone-referenced jobs or record the succeeded outcome
    on the tombstone itself.
    """
    owner_failed = select(Job.id).where(Job.id == StaticRouteTombstone.job_id, Job.status == JobStatus.failed).exists()
    return or_(StaticRouteTombstone.job_id.is_(None), owner_failed)


def _removal_context(row: StaticRouteTombstone) -> dict:
    """Build the removal job's context from the TOMBSTONE, never from request state.

    One job per tombstone, at ITS OWN marking: the carriers of one push can be marked
    differently (§4.5), and a re-issue that took a marking from anywhere else would convert
    a failed **delete-origin** retract into a no-networking retry: a destructive-semantics
    flip, not a cosmetic default.

    Deliberately not ``enqueue_removal``: a re-issue promotes nothing (there is no new
    operator intent behind it), so it builds its own generation.
    """
    triple = triple_of(row)
    removed = [triple]
    deployed = as_triple(row.deployed_key)
    if deployed is not None and deployed != triple:
        removed.append(deployed)
    return {
        "scope": "static_route",
        "removed": {"route": [list(key) for key in removed]},
        "detach": row.marking == DETACH_MARKING,
        "tombstone_ids": [row.id],
    }


async def reissue_removal_job(conn: AsyncSession, device_id: int, row: StaticRouteTombstone) -> Job:
    """Re-issue *row*'s removal as a generation and the job that carries it. Caller commits.

    Shared by the sweeper and the reclaimer, which are the same operation reached from two
    directions: a deletion an earlier push already authorized still has no proof of delivery.
    It PROMOTES NOTHING — there is no new operator intent — but it does take a place in the
    device's ordered chain, so it cannot cross a blocked head and a later push cannot cross
    it (#1522 §H2).
    """
    from nso_adapter.core.generation import create_reissue_generation
    from nso_adapter.store.models import GenerationMode

    context = _removal_context(row)
    generation = await create_reissue_generation(
        conn,
        device_id,
        mode=GenerationMode.detach if context["detach"] else GenerationMode.networked,
        removal_context=context,
        allowed_removal_keys=context["removed"],
    )
    job = Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.queued, context=context)
    conn.add(job)
    await conn.flush()
    generation.job_id = job.id
    row.job_id = job.id
    await conn.flush()
    return job


async def _devices_with_eligible_tombstones(db: AsyncSession | None = None) -> list[int]:
    async with claim_session(db) as conn:
        rows = await conn.execute(
            select(StaticRouteTombstone.device_id)
            .where(_eligible())
            .distinct()
            .order_by(StaticRouteTombstone.device_id)
        )
        return list(rows.scalars().all())


async def sweep_one_device(device_id: int, *, db: AsyncSession | None = None) -> int:
    """Give every eligible tombstone on *device_id* a removal job. Returns the job count.

    The claim is acquired FIRST, in its own committed transaction; a conflict means some
    other holder owns the device and this pass skips it entirely. Released in a ``finally``
    that also covers the nothing-eligible early return — a skipped device left claimed
    would stay claimed until the reaper.
    """
    reg = await acquire_claim(device_id, "sweep", db=db)
    if reg is None:
        logger.debug("tombstone_sweep.skipped_claimed", device_id=device_id)
        return 0
    created = 0
    try:
        async with claim_session(db) as conn:
            # Guard first, then tombstones, then jobs — §3.9's order. No `devices` row
            # lock: the claim is the exclusion, and a same-device rival already lost.
            await lock_claim(conn, reg)
            ids = sorted(
                (
                    await conn.execute(
                        select(StaticRouteTombstone.id).where(StaticRouteTombstone.device_id == device_id, _eligible())
                    )
                )
                .scalars()
                .all()
            )
            if not ids:
                await conn.rollback()
                return 0
            # Explicit sorted() and an explicit ordered pre-lock: PostgreSQL gives no
            # ordering guarantee for a bulk statement's RETURNING or scan order, so
            # nothing but this makes job creation follow tombstone id order.
            await conn.execute(
                select(StaticRouteTombstone.id)
                .where(StaticRouteTombstone.id.in_(ids))
                .order_by(StaticRouteTombstone.id)
                .with_for_update()
            )
            rows = (
                (
                    await conn.execute(
                        select(StaticRouteTombstone)
                        .where(StaticRouteTombstone.id.in_(ids))
                        .order_by(StaticRouteTombstone.id)
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                await reissue_removal_job(conn, device_id, row)
                created += 1
            await conn.commit()
    finally:
        await release_claim(reg, db=db)
    if created:
        logger.warning("tombstone_sweep.jobs_created", device_id=device_id, count=created)
    return created


async def sweep_tombstones(*, db: AsyncSession | None = None) -> int:
    """One full pass over every device holding an uncarried deletion."""
    created = 0
    for device_id in await _devices_with_eligible_tombstones(db):
        created += await sweep_one_device(device_id, db=db)
    return created
