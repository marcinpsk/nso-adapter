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

Why the caller's session, not an independent one: a second connection to the caller's engine
deadlocks on file-backed SQLite (the unit-test store). The two aiosqlite threads share one
asyncio task, so when the caller's ``db`` holds an uncommitted write (e.g. mid-Apply,
``apply._post_apply_refresh_and_notify``), the second connection's commit blocks on the file lock
that only the caller — now parked awaiting this coroutine — can release. Reusing ``db`` sidesteps
that entirely; the outcome rides the family's own commit boundary. (On PostgreSQL a separate
connection would be safe; this trades that theoretical independence for a deadlock-free store.)

The pointer advances to the **newest TERMINAL attempt by start order** (attempt id): a terminal
attempt only advances the pointer when its id exceeds the pointed-to one, so a newest failure
stays visible and an older attempt finishing late can never regress the pointer onto a stale
result.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.read_outcome import AbsentAuthoritative, Present, ReadOutcome, Unavailable
from nso_adapter.store.models import RefreshOutcome, RefreshOutcomePointer

logger = structlog.get_logger(__name__)


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
) -> int:
    """Phase 1: persist the classified read outcome; return the new attempt id.

    Flushed (not committed) on the caller's session, so it is assigned its PK and rides the
    caller's transaction — :func:`record_result` (or the family materializer's own commit) makes
    it durable.
    """
    read_outcome, read_reason, freshness = _decompose(outcome)
    row = RefreshOutcome(
        device_id=device_id,
        family=family,
        refresh_source=refresh_source,
        read_outcome=read_outcome,
        read_reason=read_reason,
        freshness=freshness,
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
) -> None:
    """Phase 2: terminalize *attempt_id*, advance the per-(device, family) pointer, and commit.

    The pointer advances only when this attempt's id exceeds the currently-pointed one (newest
    TERMINAL by start order) — so an older attempt terminalizing late can't overwrite a newer
    result, and a newest failure stays visible. Commits on the caller's session, so the outcome +
    pointer are durable even if the caller never commits again.
    """
    row = await db.get(RefreshOutcome, attempt_id)
    if row is None:
        logger.warning("outcome_store.record_result.unknown_attempt", attempt_id=attempt_id)
        return
    row.result = result
    row.succeeded = succeeded
    row.row_count = row_count
    row.completed_at = datetime.now(UTC)

    pointer = (
        await db.execute(
            select(RefreshOutcomePointer).where(
                RefreshOutcomePointer.device_id == row.device_id,
                RefreshOutcomePointer.family == row.family,
            )
        )
    ).scalar_one_or_none()
    if pointer is None:
        db.add(RefreshOutcomePointer(device_id=row.device_id, family=row.family, attempt_id=attempt_id))
    elif attempt_id > pointer.attempt_id:
        pointer.attempt_id = attempt_id
    await db.commit()


# ── S4 read accessors (the pointer join the API serves) ──────────────────────────────────


async def get_current_outcome(db: AsyncSession, device_id: int, family: str) -> RefreshOutcome | None:
    """Return the newest TERMINAL attempt for (device, family) via the pointer, or None.

    None means the family was never terminalized for this device (no pointer row) — the
    API synthesizes a ``not_ready`` read_state from it; it must never be conflated with a
    recorded ``unavailable``.
    """
    return (
        await db.execute(
            select(RefreshOutcome)
            .join(RefreshOutcomePointer, RefreshOutcomePointer.attempt_id == RefreshOutcome.id)
            .where(
                RefreshOutcomePointer.device_id == device_id,
                RefreshOutcomePointer.family == family,
            )
        )
    ).scalar_one_or_none()


async def get_current_outcomes(db: AsyncSession, device_id: int) -> dict[str, RefreshOutcome]:
    """Return every pointed family's newest terminal attempt for *device_id*, in ONE query."""
    rows = (
        await db.execute(
            select(RefreshOutcome)
            .join(RefreshOutcomePointer, RefreshOutcomePointer.attempt_id == RefreshOutcome.id)
            .where(RefreshOutcomePointer.device_id == device_id)
        )
    ).scalars()
    return {row.family: row for row in rows}
