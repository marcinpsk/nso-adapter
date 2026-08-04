# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Two-phase refresh-outcome store (READSEM §2.4).

Every read-mirror refresh attempt records its truth here, in two phases on the CALLER's session:

* **Phase 1** (:func:`record_read_outcome`) — the classified read outcome, BEFORE the family
  materializer runs. Flushed (not committed) to assign and return the immutable **attempt id**
  (the row PK, also the start-order key); it rides the caller's transaction.
* **Phase 2** (:func:`record_result`) — the materialization result + returned success flag, AFTER
  the materializer. Terminalizes the attempt, advances the per-(device, family) pointer, and
  commits — so the outcome + pointer persist regardless of whether the caller commits again.
  For engine families, the preceding engine mirror commit already made phase 1 and the staged
  mirror durable.

**Why the caller's session, not an independent one.** Two load-bearing reasons, both about
visibility inside one transaction:

1. Phase 1 ends in ``flush()``, not ``commit()`` — the row has a PK but is not yet committed.
   An independent session reading under READ COMMITTED cannot see it, so
   :func:`record_result`'s ``db.get(RefreshOutcome, attempt_id)`` would return ``None`` and
   the whole phase degrades to the "unknown_attempt" no-op.
2. The atomic-publication invariant (1332): the mirror rows, the pointer and
   ``payload_revision`` must become visible together, so a reader never sees a new pointer
   over old rows. Only the session that holds the staged mirror writes can commit all three
   in one transaction — see ``tests/store/test_pointer_concurrency.py``'s publication test.

This is a contract, not a workaround: every caller passes the session it materializes on.

The pointer advances to the **newest TERMINAL attempt by start order** (attempt id): a terminal
attempt only advances the pointer when its id exceeds the pointed-to one, so a newest failure
stays visible and an older attempt finishing late can never regress the pointer onto a stale
result.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.families import ALL_FAMILY_KEYS
from nso_adapter.nso.read_outcome import AbsentAuthoritative, Present, ReadOutcome, Unavailable
from nso_adapter.store.models import Device, RefreshOutcome, RefreshOutcomePointer

logger = structlog.get_logger(__name__)
_FAMILY_ORDINAL = {family: index + 1 for index, family in enumerate(ALL_FAMILY_KEYS)}


def _decompose(outcome: ReadOutcome) -> tuple[str, str | None, str | None]:
    """Flatten a :data:`ReadOutcome` into (read_outcome, read_reason, freshness) columns."""
    if isinstance(outcome, Present):
        return "present", None, outcome.freshness.value
    if isinstance(outcome, AbsentAuthoritative):
        return "absent_authoritative", None, None
    assert isinstance(outcome, Unavailable)
    return "unavailable", outcome.reason.value, None


async def record_read_outcome(
    db: AsyncSession,
    device_id: int,
    family: str,
    outcome: ReadOutcome,
    *,
    refresh_source: str,
    source_epoch: int | None = None,
) -> int:
    """Phase 1: persist the classified read outcome; return the new attempt id.

    Flushed (not committed) on the caller's session, so it is assigned its PK and rides the
    caller's transaction — :func:`record_result` (or the refresh engine's preceding mirror
    commit) makes it durable.
    """
    read_outcome, read_reason, freshness = _decompose(outcome)
    if source_epoch is None:
        source_epoch = await db.scalar(select(Device.source_epoch).where(Device.id == device_id))
    if source_epoch is None:
        raise LookupError(f"device {device_id} disappeared before read outcome recording")
    row = RefreshOutcome(
        device_id=device_id,
        family=family,
        refresh_source=refresh_source,
        read_outcome=read_outcome,
        read_reason=read_reason,
        freshness=freshness,
        source_epoch=source_epoch,
    )
    db.add(row)
    await db.flush()  # assign the PK (the attempt id) without committing
    return row.id


async def record_result(
    db: AsyncSession,
    attempt_id: int,
    *,
    result: str,
    succeeded: bool,
    row_count: int | None = None,
    publish_payload: bool | None = None,
) -> bool | None:
    """Phase 2: terminalize *attempt_id*, advance the per-(device, family) pointer, and commit.

    The pointer advances only when this attempt's id exceeds the currently-pointed one (newest
    TERMINAL by start order) — so an older attempt terminalizing late can't overwrite a newer
    result, and a newest failure stays visible. Commits on the caller's session, so the outcome +
    pointer are durable even if the caller never commits again.
    """
    row = await db.get(RefreshOutcome, attempt_id)
    if row is None:
        logger.warning("outcome_store.record_result.unknown_attempt", attempt_id=attempt_id)
        return None
    await acquire_family_fence(db, row.device_id, row.family)
    selected = await stage_result(
        db,
        row,
        result=result,
        succeeded=succeeded,
        row_count=row_count,
        publish_payload=publish_payload,
    )
    await db.commit()
    return selected


async def acquire_family_fence(db: AsyncSession, device_id: int, family: str) -> None:
    """Serialize every terminal writer for one family at the database boundary."""
    try:
        ordinal = _FAMILY_ORDINAL[family]
    except KeyError as exc:
        raise ValueError(f"unknown read family {family!r}") from exc
    # The one genuinely unbounded wait on a claimed path: PostgreSQL blocks on a conflicting
    # advisory lock forever by default, and this runs inside a cancellation-absorbing span
    # whose drain the worker has to bound. The bound must therefore be the server's.
    from nso_adapter.store.db import apply_absorbed_span_bounds

    await apply_absorbed_span_bounds(db)
    await db.execute(
        text("SELECT pg_advisory_xact_lock(:device_id, :family_ordinal)"),
        {"device_id": device_id, "family_ordinal": ordinal},
    )


async def stage_result(
    db: AsyncSession,
    row: RefreshOutcome,
    *,
    result: str,
    succeeded: bool,
    row_count: int | None,
    publish_payload: bool | None = None,
) -> bool:
    """Stage terminal truth + pointer without committing; return whether it selected the pointer.

    Caller holds the family fence. A source-rekeyed or older attempt becomes historical
    ``superseded`` and never creates/advances a pointer.
    """
    if publish_payload is None:
        publish_payload = result in {"replaced", "cleared"}
    current_epoch = await db.scalar(select(Device.source_epoch).where(Device.id == row.device_id))
    current_attempt = await db.scalar(
        select(RefreshOutcomePointer.attempt_id).where(
            RefreshOutcomePointer.device_id == row.device_id,
            RefreshOutcomePointer.family == row.family,
        )
    )
    superseded = current_epoch != row.source_epoch or (current_attempt is not None and current_attempt > row.id)
    if superseded:
        result, succeeded, row_count, publish_payload = "superseded", True, None, False

    row.result = result
    row.succeeded = succeeded
    row.row_count = row_count
    row.completed_at = datetime.now(UTC)
    if superseded:
        return False

    await db.execute(
        _pointer_advance_stmt(
            row.device_id,
            row.family,
            row.id,
            publish_payload=publish_payload,
        )
    )
    selected = await db.scalar(
        select(RefreshOutcomePointer.attempt_id).where(
            RefreshOutcomePointer.device_id == row.device_id,
            RefreshOutcomePointer.family == row.family,
        )
    )
    return selected == row.id


async def publication_is_current(db: AsyncSession, row: RefreshOutcome) -> bool:
    """Return whether *row* may still publish after the caller acquired its family fence."""
    current_epoch = await db.scalar(select(Device.source_epoch).where(Device.id == row.device_id))
    current_attempt = await db.scalar(
        select(RefreshOutcomePointer.attempt_id).where(
            RefreshOutcomePointer.device_id == row.device_id,
            RefreshOutcomePointer.family == row.family,
        )
    )
    return current_epoch == row.source_epoch and (current_attempt is None or current_attempt <= row.id)


def _pointer_advance_stmt(
    device_id: int,
    family: str,
    attempt_id: int,
    *,
    publish_payload: bool,
):
    """Build the database-atomic monotonic pointer upsert (READSEM S4 D6).

    ``INSERT … ON CONFLICT (device_id, family) DO UPDATE … WHERE excluded.attempt_id >
    attempt_id`` — the monotonic guard evaluates INSIDE the database at write time, so a
    session that decided from a pre-window snapshot cannot regress the pointer (the old
    SELECT→compare-in-Python→UPDATE protocol lost that update; proven red-first by
    ``tests/store/test_pointer_concurrency.py``). ``updated_at`` is set explicitly — ORM
    ``onupdate`` does not fire for core statements.
    """
    payload_revision = attempt_id if publish_payload else None
    stmt = pg_insert(RefreshOutcomePointer).values(
        device_id=device_id,
        family=family,
        attempt_id=attempt_id,
        payload_revision=payload_revision,
    )
    revision_update = stmt.excluded.payload_revision if publish_payload else RefreshOutcomePointer.payload_revision
    return stmt.on_conflict_do_update(
        index_elements=["device_id", "family"],
        set_={
            "attempt_id": stmt.excluded.attempt_id,
            "payload_revision": revision_update,
            "updated_at": func.now(),
        },
        where=stmt.excluded.attempt_id > RefreshOutcomePointer.attempt_id,
    )


# ── S4 read accessors (the pointer join the API serves) ──────────────────────────────────


async def get_current_outcome(db: AsyncSession, device_id: int, family: str) -> RefreshOutcome | None:
    """Return the newest TERMINAL attempt for (device, family) via the pointer, or None.

    None means the family was never terminalized for this device (no pointer row) — the
    API synthesizes a ``not_ready`` read_state from it; it must never be conflated with a
    recorded ``unavailable``.
    """
    result = (
        await db.execute(
            select(RefreshOutcome, RefreshOutcomePointer.payload_revision)
            .join(RefreshOutcomePointer, RefreshOutcomePointer.attempt_id == RefreshOutcome.id)
            .where(
                RefreshOutcomePointer.device_id == device_id,
                RefreshOutcomePointer.family == family,
            )
        )
    ).one_or_none()
    if result is None:
        return None
    row, payload_revision = result
    row.payload_revision = payload_revision
    return row


async def get_current_outcomes(db: AsyncSession, device_id: int) -> dict[str, RefreshOutcome]:
    """Return every pointed family's newest terminal attempt for *device_id*, in ONE query."""
    rows = (
        await db.execute(
            select(RefreshOutcome, RefreshOutcomePointer.payload_revision)
            .join(RefreshOutcomePointer, RefreshOutcomePointer.attempt_id == RefreshOutcome.id)
            .where(RefreshOutcomePointer.device_id == device_id)
        )
    ).all()
    by_family = {}
    for row, payload_revision in rows:
        row.payload_revision = payload_revision
        by_family[row.family] = row
    return by_family
