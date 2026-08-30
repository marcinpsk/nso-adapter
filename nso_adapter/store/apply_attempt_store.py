# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Durable admission identity and replay storage for manual Apply."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.store.db import execute_dml
from nso_adapter.store.models import DeploymentApplyAttempt


def canonical_selected(selected: dict[str, int]) -> dict[str, int]:
    """Return the complete stable identity of an Apply selection."""
    return {stream: selected[stream] for stream in sorted(selected)}


@dataclass(frozen=True)
class StoredApplyResponse:
    http_status: int
    response: dict


class ApplyAttemptIdentityMismatch(RuntimeError):
    """An existing UUID belongs to a different request identity."""

    def __init__(self, mismatch: str):
        super().__init__(f"Apply attempt UUID does not match {mismatch}")
        self.mismatch = mismatch


async def replay_apply_attempt(
    db: AsyncSession,
    attempt_id: UUID,
    device_id: int,
    selected: dict[str, int],
) -> StoredApplyResponse | None:
    """Return a completed replay, or ``None`` when the UUID is unknown."""
    attempt = await db.scalar(select(DeploymentApplyAttempt).where(DeploymentApplyAttempt.id == attempt_id))
    if attempt is None:
        return None
    if attempt.device_id != device_id:
        raise ApplyAttemptIdentityMismatch("device_id")
    if canonical_selected(attempt.selected) != canonical_selected(selected):
        raise ApplyAttemptIdentityMismatch("selected")
    return StoredApplyResponse(http_status=attempt.http_status, response=attempt.response)


async def begin_apply_attempt(
    db: AsyncSession,
    attempt_id: UUID,
    device_id: int,
    selected: dict[str, int],
) -> StoredApplyResponse | None:
    """Reserve a new UUID in this transaction, or return its completed replay."""
    identity = canonical_selected(selected)
    inserted_id = await db.scalar(
        pg_insert(DeploymentApplyAttempt)
        .values(
            id=attempt_id,
            device_id=device_id,
            selected=identity,
            admission_state="admitted",
            http_status=500,
            response={},
        )
        .on_conflict_do_nothing(index_elements=[DeploymentApplyAttempt.id])
        .returning(DeploymentApplyAttempt.id)
    )
    if inserted_id is not None:
        return None

    stored = await replay_apply_attempt(db, attempt_id, device_id, identity)
    if stored is None:  # pragma: no cover - INSERT conflict makes the row visible at READ COMMITTED
        raise RuntimeError(f"Apply attempt {attempt_id} conflicted but is not readable")
    return stored


async def complete_apply_attempt(
    db: AsyncSession,
    attempt_id: UUID,
    *,
    admission_state: str,
    http_status: int,
    response: dict,
) -> None:
    """Store the final replay body before the caller commits the admission."""
    result = await execute_dml(
        db,
        update(DeploymentApplyAttempt)
        .where(DeploymentApplyAttempt.id == attempt_id)
        .values(
            admission_state=admission_state,
            http_status=http_status,
            response=response,
        ),
    )
    if result.rowcount != 1:
        raise RuntimeError(f"Apply attempt {attempt_id} disappeared before completion")


__all__ = [
    "ApplyAttemptIdentityMismatch",
    "StoredApplyResponse",
    "begin_apply_attempt",
    "canonical_selected",
    "complete_apply_attempt",
    "replay_apply_attempt",
]
