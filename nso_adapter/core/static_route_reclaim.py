# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The R1→R2 activation reclaimer — #1396 R2 §4.10, mandated by R1 (G18).

R1's sweeper deliberately leaves a tombstone whose owning job SUCCEEDED alone: re-issuing it
would re-remove config that was already removed. Those rows are R1's handoff set — deletions
whose job reported success without ever proving anything, because R1 had no proof to give.
R2 is what proves them, so R2 owes them a pass: for each, either consume the carrier or
re-issue a removal job that will do the proving.

Three properties are load-bearing, and each one closes a real failure:

* **It is a drain, not a single pass.** A rival holder (an apply, a teardown, the sweeper)
  makes the device unclaimable and this tick skips it. A startup-only reclaimer would abandon
  that device forever, because R1's periodic sweep predicate excludes succeeded owners (G17)
  and never revisits it.
* **It is bounded and off the critical path.** The proof work is a device-state read per
  device plus a ``sync_from`` per detach, and the succeeded-owner predicate is a cross-table
  join with no supporting index (G34). So: its own scheduled job with ``max_instances=1``
  (never appended to the sequential orphan-reap tick, which ends in ``ensure_workers()``), a
  per-tick device budget with a resume cursor, and one snapshot per device shared by all its
  tombstones.
* **Re-issue is atomic.** Under the claim, in ONE transaction: lock the tombstones, insert the
  job, stamp ``job_id``. A split leaves an ownerless job, which falls back to
  ``context["removed"]`` and silently loses the ``deployed_key`` half of its authorization.

R1's sweep predicate is NOT widened — this is a separate reader with its own predicate.
"""

from __future__ import annotations

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import acquire_claim, claim_session, lock_claim, release_claim
from nso_adapter.store.models import Device, Job, JobStatus, StaticRouteTombstone

logger = structlog.get_logger(__name__)

#: Resume point for the per-tick device budget. Module-level rather than a column because the
#: reclaimer is idempotent: a restart that rewinds it re-proves devices it already drained,
#: which costs a read and consumes nothing twice.
_cursor: int = 0


def reset_cursor() -> None:
    """Rewind the drain cursor — tests and a fresh process start from the beginning."""
    global _cursor
    _cursor = 0


def _eligible():
    """Tombstones whose owning job SUCCEEDED — precisely what R1's sweeper will never re-issue."""
    return (
        StaticRouteTombstone.job_id.is_not(None),
        select(Job.id).where(Job.id == StaticRouteTombstone.job_id, Job.status == JobStatus.succeeded).exists(),
    )


async def _devices_with_succeeded_owner_tombstones(*, after: int, limit: int, db: AsyncSession | None = None):
    async with claim_session(db) as conn:
        rows = await conn.execute(
            select(StaticRouteTombstone.device_id)
            .where(StaticRouteTombstone.device_id > after, *_eligible())
            .distinct()
            .order_by(StaticRouteTombstone.device_id)
            .limit(limit)
        )
        return list(rows.scalars().all())


def _authorized(row: StaticRouteTombstone) -> set[tuple[str, str, str]]:
    """``{triple} ∪ {deployed_key}`` — both are the deleted row's own keys (X6)."""
    keys = {(row.vrf or "", row.prefix or "", row.next_hop or "")}
    if row.deployed_key:
        vrf, prefix, next_hop = row.deployed_key
        keys.add((vrf or "", prefix or "", next_hop or ""))
    return keys


class _DeviceProof:
    """The ONE snapshot per device every tombstone of that device is judged against."""

    def __init__(self) -> None:
        self.device_status: str = "error"
        self.device_entries: dict = {}
        self.service_state = None
        self.sync_ok: bool | None = None


async def _read_device(client, device, *, need_device_state: bool, need_service: bool) -> _DeviceProof:
    from nso_adapter.core.apply import _static_route_device_state
    from nso_adapter.nso.apply import _STATIC_ROUTE_SERVICE_PATH

    proof = _DeviceProof()
    if need_device_state:
        proof.device_status, proof.device_entries = await _static_route_device_state(client, device)
    if need_service:
        proof.service_state = await client.service_instance_state(_STATIC_ROUTE_SERVICE_PATH, device.nso_device_name)
    return proof


def _delete_origin_proven(row: StaticRouteTombstone, proof: _DeviceProof) -> bool:
    """Whether a delete-origin deletion is proven — none of its keys is on the DEVICE."""
    if proof.device_status != "ok":
        return False
    return not (_authorized(row) & set(proof.device_entries))


def _detach_proven(row: StaticRouteTombstone, proof: _DeviceProof) -> bool:
    """Whether a detach is proven — by the SERVICE, plus a successful ``sync_from``.

    Device presence is the EXPECTED state after an un-own — that is the entire point of
    ``no-networking`` — so it is never a failure here. Only a certified service read counts:
    an inconclusive one proves nothing and re-issues.
    """
    from nso_adapter.nso.apply import static_route_entry_key

    state = proof.service_state
    if state is None or state.inconclusive or proof.sync_ok is not True:
        return False
    if not state.entry:
        return True
    live = {static_route_entry_key(entry) for entry in (state.entry.get("route") or [])}
    return not (live & _authorized(row))


async def reclaim_one_device(device_id: int, *, db: AsyncSession | None = None) -> tuple[int, int]:
    """Prove or re-issue every succeeded-owner tombstone on *device_id* → ``(consumed, reissued)``.

    Skips the device entirely when the claim is unavailable — a rival owns it, and a later
    tick will find it again. Consumption and re-issue share ONE transaction under the claim,
    so a kill mid-pass can neither strand a consumed carrier nor leave an ownerless job.
    """
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.tombstone_sweep import reissue_removal_job
    from nso_adapter.store.tombstone_store import delete_tombstones

    reg = await acquire_claim(device_id, "sweep", db=db)
    if reg is None:
        logger.debug("static_route_reclaim.skipped_claimed", device_id=device_id)
        return 0, 0
    consumed = reissued = 0
    try:
        async with claim_session(db) as conn:
            await lock_claim(conn, reg)
            device = await conn.get(Device, device_id)
            rows = list(
                (
                    await conn.execute(
                        select(StaticRouteTombstone)
                        .where(StaticRouteTombstone.device_id == device_id, *_eligible())
                        .order_by(StaticRouteTombstone.id)
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            if device is None or not rows:
                await conn.rollback()
                return 0, 0

            client = get_nso_client(device.nso_instance)
            markings = {row.marking for row in rows}
            proof = await _read_device(
                client,
                device,
                need_device_state="delete_origin" in markings,
                need_service="detach" in markings,
            )
            if "detach" in markings:
                from nso_adapter.core.removal import _sr_sync_from

                proof.sync_ok = await _sr_sync_from(client, device, {}, job_id=0)

            consumable: list[int] = []
            for row in rows:
                if _delete_origin_proven(row, proof) if row.marking == "delete_origin" else _detach_proven(row, proof):
                    consumable.append(row.id)
                else:
                    await reissue_removal_job(conn, device_id, row)
                    reissued += 1
            if consumable:
                consumed = await delete_tombstones(conn, consumable, device_id=device_id, claim_token=reg.token)
            await conn.commit()
    finally:
        await release_claim(reg, db=db)
    if consumed or reissued:
        logger.warning("static_route_reclaim.device_drained", device_id=device_id, consumed=consumed, reissued=reissued)
    return consumed, reissued


async def reclaim_succeeded_tombstones(*, budget: int | None = None, db: AsyncSession | None = None) -> tuple[int, int]:
    """One bounded tick of the drain → ``(consumed, reissued)``.

    Takes at most *budget* devices, in id order, resuming after the last one it looked at.
    When the batch comes up short the cursor rewinds, so the drain wraps instead of stopping
    at the highest id it ever saw.
    """
    global _cursor
    from nso_adapter.config import get_config

    if budget is None:
        budget = get_config().scheduler.static_route_reclaim_devices_per_tick
    if budget <= 0:
        return 0, 0
    device_ids = await _devices_with_succeeded_owner_tombstones(after=_cursor, limit=budget, db=db)
    if not device_ids:
        _cursor = 0
        return 0, 0
    consumed = reissued = 0
    for device_id in device_ids:
        try:
            got, made = await reclaim_one_device(device_id, db=db)
        except Exception as exc:  # noqa: BLE001 — one wedged device must not stop the drain
            logger.warning("static_route_reclaim.device_failed", device_id=device_id, error=repr(exc))
            got = made = 0
        consumed += got
        reissued += made
    _cursor = 0 if len(device_ids) < budget else device_ids[-1]
    return consumed, reissued
