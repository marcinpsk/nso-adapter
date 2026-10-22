# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Runtime transactions over the shared execution-attempt store."""

from collections.abc import Iterable, Sequence
from functools import partial
from typing import Literal, NamedTuple

import anyio.to_thread
import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.claim import ClaimLostError, JobError, lock_claim
from nso_adapter.core.static_route_reader import CertifiedSection, certified_static_route_section
from nso_adapter.domain.diagnostics import device_fields
from nso_adapter.nso.apply import ResponseCheckpoint
from nso_adapter.nso.client import failure_detail
from nso_adapter.nso.origins import LogicalRow
from nso_adapter.store.execution_attempt_store import (
    AttemptOwnershipLost,
    bind_historical_origins,
    dispose_execution_attempt,
    prepare_execution_attempt,
    record_classified_response,
    unsent_execution_result,
    validate_logical_membership,
)
from nso_adapter.store.models import ApplyExecutionAttempt, DeploymentGeneration, Job, JobType

logger = structlog.get_logger(__name__)


class Unresolved(NamedTuple):
    """Why a run that prepared no attempt gets no not-applied evidence (#1825)."""

    reason: Literal["attempt_exists", "no_generation", "generation_tampered", "derivation_failed"]
    exception_type: str | None = None


def _unprepared_membership(job_type: JobType, generations: Sequence[DeploymentGeneration]) -> tuple[LogicalRow, ...]:
    from nso_adapter.core.apply import (
        WORKER_APPLY_FORCE,
        eligible_membership,
        encode_device_document,
        select_frozen_apply,
    )
    from nso_adapter.core.generation import execution_context, execution_streams, validate_generation_execution
    from nso_adapter.nso.origins import current_membership

    streams = execution_streams(generations)
    context = execution_context(generations)
    generation = generations[-1]
    validate_generation_execution(generation)
    if job_type is JobType.removal:
        return current_membership(encode_device_document(generation.document, origin_context=context).origins, context)
    selection = select_frozen_apply(generation.document, streams, force=WORKER_APPLY_FORCE)
    encoded = encode_device_document(
        generation.document,
        rows_by_section=selection.rows,
        proof_by_section={"interface_config": selection.interface},
        origin_context=context,
    )
    return eligible_membership(current_membership(encoded.origins, context), selection)


def derive_unprepared_evidence(
    job_type: JobType, generations: Sequence[DeploymentGeneration], error: dict | None, *, attempt_exists: bool
) -> dict | Unresolved:
    """Derive the not-applied result of a run that reached no device, from loaded rows alone.

    No session and no I/O: every exception here is a property of the frozen document or of
    the code, so a retry cannot change it, and the caller keeps its own result instead.
    """
    from nso_adapter.core.generation import GenerationTampered, verify_generation_digest

    if attempt_exists:
        return Unresolved("attempt_exists")
    if not generations:
        return Unresolved("no_generation")
    try:
        verify_generation_digest(generations[-1])
        membership = _unprepared_membership(job_type, generations)
        validate_logical_membership(generations, membership)
        return unsent_execution_result(generations[-1], membership, error)
    except GenerationTampered:
        return Unresolved("generation_tampered")
    except Exception as exc:  # noqa: BLE001 - fail closed to the lane's own result; the type is logged
        return Unresolved("derivation_failed", type(exc).__name__)


async def unprepared_evidence(
    db: AsyncSession, job_id: int, job_type: JobType, run_attempt: int, error: dict | None
) -> dict | None:
    """Load the locked run's frozen rows and return its not-applied evidence, or ``None``.

    The caller holds the job row lock, so a missing attempt proves that nothing was sent:
    preparation commits before delivery and needs that lock. Database errors propagate.
    """
    attempt_id = await db.scalar(
        select(ApplyExecutionAttempt.id)
        .where(ApplyExecutionAttempt.job_correlation_id == job_id, ApplyExecutionAttempt.run_attempt == run_attempt)
        .limit(1)
    )
    generations = list(
        await db.scalars(
            select(DeploymentGeneration).where(DeploymentGeneration.job_id == job_id).order_by(DeploymentGeneration.seq)
        )
    )
    # CPU-bound encoding: keep the event loop free while the caller holds the job row lock.
    derived = await anyio.to_thread.run_sync(
        partial(derive_unprepared_evidence, job_type, generations, error, attempt_exists=attempt_id is not None)
    )
    if isinstance(derived, Unresolved):
        logger.error(
            "execution.unprepared_evidence_unresolved",
            job_id=job_id,
            reason=derived.reason,
            exception_type=derived.exception_type,
        )
        return None
    return derived


async def prepare_send(
    db: AsyncSession, generation: DeploymentGeneration, origins, logicals: Iterable[LogicalRow], reg
) -> ResponseCheckpoint:
    """Commit preparation before delivery and commit each sender checkpoint separately."""
    if reg is None or not reg.registered:
        raise JobError("execution_claim_required", "Execution requires a REGISTERED claim")
    if generation.job_id is None:
        raise ValueError("Execution requires its job correlation")
    await lock_claim(db, reg)
    await bind_historical_origins(db, generation.device_id, origins, generation)
    attempt = None
    try:
        attempt = await prepare_execution_attempt(
            db, generation.job_id, reg.run_attempt, reg.token, generation.id, origins=origins, logical_rows=logicals
        )
    except AttemptOwnershipLost:
        await db.rollback()
    if attempt is None:
        raise ClaimLostError("Execution preparation ownership was lost")
    await db.commit()
    identity = attempt.id, generation.job_id, reg.run_attempt, reg.token

    async def checkpoint(phase, classification, acknowledged):
        try:
            await record_classified_response(
                db, attempt.id, *identity[1:], classification, sender_phase=phase, commit_acknowledged=acknowledged
            )
            await db.commit()
        except AttemptOwnershipLost:
            await db.rollback()
            lost = True
        else:
            lost = False
        if lost:
            raise ClaimLostError("Execution checkpoint ownership was lost")

    return checkpoint


async def dispose_run(
    db: AsyncSession, job_id: int, reg, reason="runner_failed", *, terminal_error: dict | None = None
) -> bool:
    """Stage attempt settlement before a generic terminal write; caller commits once."""
    if reg is not None and reg.registered:
        await lock_claim(db, reg)
    attempt = await db.scalar(select(Job.run_attempt).where(Job.id == job_id)) if reg is None else reg.run_attempt
    token = reg.token if reg is not None and reg.registered else None
    result = None
    try:
        result = await dispose_execution_attempt(db, job_id, attempt, token, reason, terminal_error=terminal_error)
    except AttemptOwnershipLost:
        await db.rollback()
    if result is None:
        raise ClaimLostError("Execution settlement ownership was lost")
    return result != "no_attempt"


async def active_attempt(db: AsyncSession, job_id: int) -> ApplyExecutionAttempt | None:
    return await db.scalar(
        select(ApplyExecutionAttempt)
        .where(ApplyExecutionAttempt.job_id == job_id)
        .order_by(ApplyExecutionAttempt.id.desc())
        .limit(1)
    )


async def settle_unsent(db: AsyncSession, generation, origins, logicals, reg, error: dict) -> None:
    """Settle known unsent membership with the existing attempt and disposition helpers."""
    from nso_adapter.nso.refusals import OrdinaryRejection

    await lock_claim(db, reg)
    attempt = await prepare_execution_attempt(
        db, generation.job_id, reg.run_attempt, reg.token, generation.id, origins=origins, logical_rows=logicals
    )
    await record_classified_response(
        db,
        attempt.id,
        generation.job_id,
        reg.run_attempt,
        reg.token,
        OrdinaryRejection(),
        sender_phase="pre_commit_dry_run",
    )
    await dispose_execution_attempt(db, generation.job_id, reg.run_attempt, reg.token, "disposed", terminal_error=error)


async def read_send_history(db: AsyncSession, job_id: int, client, device) -> CertifiedSection | None:
    """Read optional certified history before terminal settlement takes locks."""
    # no_autoflush: a flushed dirty row here would take row locks before the claim lock.
    with db.no_autoflush:
        attempt = await active_attempt(db, job_id)
        if attempt is None or not attempt.commit_acknowledged:
            return None
        generation = await db.get(DeploymentGeneration, attempt.generation_id)
    if generation is None or "static_route" not in generation.document:
        return None
    try:
        return await certified_static_route_section(client, device)
    except httpx.HTTPError as exc:
        logger.warning("execution.history_read_failed", **device_fields(device_id=device.id), error=failure_detail(exc))
        return CertifiedSection("inconclusive", None)


async def record_send_history(db: AsyncSession, job_id: int, reg, certified: CertifiedSection | None) -> None:
    """Record pre-read acknowledged origin history in the terminal transaction."""
    from nso_adapter.store.execution_attempt_store import record_acknowledged_origins

    attempt = await active_attempt(db, job_id)
    if attempt is None or not attempt.commit_acknowledged:
        return
    await record_acknowledged_origins(db, attempt.id, job_id, reg.run_attempt, reg.token, certified)
