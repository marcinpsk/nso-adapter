# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Inert execution evidence helpers in a caller-owned transaction.

The caller commits preparation before sending. It commits response checkpoints before
settlement. The terminal writer owns outcomes, restoration, stamps, and lifecycle writes.
"""

from collections.abc import Iterable
from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.projection import section_operation, section_registry
from nso_adapter.nso.origins import AttributionFailure, LogicalRow, OriginIndex, resolve_refusals
from nso_adapter.nso.refusal_protocol import Refusal
from nso_adapter.nso.refusals import DefiniteRefusal, ErrorClassification, OrdinaryRejection, ProtocolFault, Unavailable
from nso_adapter.store.execution_bookkeeping import restore_bookkeeping, snapshot_bookkeeping, stamp_refused_bookkeeping
from nso_adapter.store.execution_evidence import (
    catalog_records,
    logical_from_record,
    logical_record,
    origin_records,
    origins_from_records,
    refusals_from_payload,
)
from nso_adapter.store.execution_history import (
    automatic_removal_reissue_blocked as automatic_removal_reissue_blocked,
)
from nso_adapter.store.execution_history import (
    bind_historical_origins as bind_historical_origins,
)
from nso_adapter.store.execution_history import (
    record_acknowledged_origins as record_acknowledged_origins,
)
from nso_adapter.store.models import (
    ApplyExecutionAttempt,
    DeploymentGeneration,
    Device,
    DeviceClaim,
    GenerationStatus,
    Job,
    JobStatus,
    JobType,
    RowOutcome,
    StaticRouteTombstone,
)


class AttemptOwnershipLost(RuntimeError):
    """The caller must roll back all staged settlement effects."""


async def _lock_run(
    db: AsyncSession, job_id: int, run_attempt: int, token: str | None, generation_id: int
) -> tuple[Job, list[DeploymentGeneration]]:
    if token is None:
        device_id = await db.scalar(select(Job.device_id).where(Job.id == job_id))
        # The device lock excludes a new claim insertion through its foreign key.
        await db.scalar(select(Device.id).where(Device.id == device_id).with_for_update())
    claim = await db.scalar(
        select(DeviceClaim)
        .where(DeviceClaim.device_id == select(Job.device_id).where(Job.id == job_id).scalar_subquery())
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    job = await db.scalar(
        select(Job)
        .where(Job.id == job_id, Job.status == JobStatus.running, Job.run_attempt == run_attempt)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    generations = list(
        await db.scalars(
            select(DeploymentGeneration)
            .where(DeploymentGeneration.job_id == job_id)
            .order_by(DeploymentGeneration.seq)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    if (
        (claim is not None if token is None else claim is None or claim.claim_token != token or claim.job_id != job_id)
        or job is None
        or not generations
        or generations[-1].id != generation_id
        or (
            any(generation.device_id != job.device_id for generation in generations)
            or job.job_type not in (JobType.apply, JobType.removal)
        )
    ):
        raise AttemptOwnershipLost("Execution attempt no longer owns this job and generation")
    return job, generations


def _generation_authorities(generations: list[DeploymentGeneration]) -> list[dict]:
    return [
        {
            "generation_id": generation.id,
            "document_digest": generation.digest,
            "stream_revisions": dict(generation.stream_revisions),
            "source_push_seq": dict(generation.source_push_seq),
            "settlement_cohort": generation.settlement_cohort,
        }
        for generation in generations
    ]


async def prepare_execution_attempt(
    db: AsyncSession,
    job_id: int,
    run_attempt: int,
    claim_token: str,
    generation_id: int,
    *,
    origins: OriginIndex,
    logical_rows: Iterable[LogicalRow],
) -> ApplyExecutionAttempt:
    """Freeze all carried authorities and the highest document without committing."""
    job, generations = await _lock_run(db, job_id, run_attempt, claim_token, generation_id)
    generation = generations[-1]
    by_id = {carried.id: carried for carried in generations}
    logicals = tuple(logical_rows)
    if len({(logical.stream, logical.row, logical.settlement_cohort) for logical in logicals}) != len(logicals):
        raise ValueError("Attempted logical membership must be unique")
    carriers = list(dict.fromkeys(logical.carrier for logical in logicals))
    for logical in logicals:
        authority = by_id.get(logical.generation_id) if logical.generation_id is not None else None
        if (
            authority is None
            or logical.stream not in authority.stream_revisions
            or logical.frozen_revision != authority.stream_revisions.get(logical.stream)
            or logical.source_push_seq != authority.source_push_seq.get(logical.stream)
            or logical.settlement_cohort != authority.settlement_cohort
            or logical.carrier.generation_id != authority.id
            or logical.carrier.stream != logical.stream
            or logical.carrier.frozen_revision != logical.frozen_revision
            or logical.carrier.settlement_cohort != logical.settlement_cohort
            or logical.carrier.source_push_seq != logical.source_push_seq
        ):
            raise ValueError("Logical membership does not match the frozen generation")
    tombstone_ids = (
        sorted(
            {
                identity
                for section in generation.document
                if section in section_registry()
                for identity in section_operation(generation.document, section).get("tombstone_ids", [])
            }
        )
        if job.job_type == JobType.removal
        else []
    )
    if any(type(identity) is not int or identity <= 0 for identity in tombstone_ids):
        raise ValueError("Removal authority requires concrete tombstone identities")
    if tombstone_ids:
        owned = list(
            await db.scalars(
                select(StaticRouteTombstone.id)
                .where(
                    StaticRouteTombstone.id.in_(tombstone_ids),
                    StaticRouteTombstone.device_id == job.device_id,
                    StaticRouteTombstone.job_id == job_id,
                )
                .with_for_update()
            )
        )
        if set(owned) != set(tombstone_ids):
            raise AttemptOwnershipLost("Removal attempt no longer owns its tombstones")
    attempt = ApplyExecutionAttempt(
        device_id=job.device_id,
        generation_id=generation.id,
        job_id=job.id,
        job_correlation_id=job.id,
        run_attempt=run_attempt,
        operation_kind=job.job_type.value,
        document_digest=generation.digest,
        claim_token=claim_token,
        stream_revisions={
            stream: revision for carried in generations for stream, revision in carried.stream_revisions.items()
        },
        generation_authorities=_generation_authorities(generations),
        manual_attempt_id=generation.apply_attempt_id,
        removal_tombstone_ids=tombstone_ids,
        logical_membership=[logical_record(logical) for logical in logicals],
        attempted_carriers=[
            {
                **carrier.public_record(),
                "source_table": carrier.source_table,
                "content_identity": carrier.content_identity,
            }
            for carrier in carriers
        ],
        safe_origins=origin_records(origins),
        prior_bookkeeping=await snapshot_bookkeeping(db, generation, origins, logicals),
    )
    db.add(attempt)
    await db.flush()
    return attempt


async def _owned_attempt(
    db: AsyncSession, attempt_id: int, job_id: int, run_attempt: int, token: str
) -> ApplyExecutionAttempt:
    generation_id = await db.scalar(
        select(ApplyExecutionAttempt.generation_id).where(ApplyExecutionAttempt.id == attempt_id)
    )
    if generation_id is None:
        raise AttemptOwnershipLost("Execution attempt is absent")
    job, generations = await _lock_run(db, job_id, run_attempt, token, generation_id)
    attempt = await db.scalar(
        select(ApplyExecutionAttempt)
        .where(ApplyExecutionAttempt.id == attempt_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        attempt is None
        or (attempt.job_id, attempt.run_attempt, attempt.claim_token, attempt.document_digest)
        != (job.id, run_attempt, token, generations[-1].digest)
        or attempt.generation_authorities != _generation_authorities(generations)
        or attempt.phase == "terminal"
    ):
        raise AttemptOwnershipLost("Execution attempt no longer owns this settlement")
    return attempt


async def record_classified_response(
    db: AsyncSession,
    attempt_id: int,
    job_id: int,
    run_attempt: int,
    claim_token: str,
    classification: ErrorClassification | None,
    *,
    sender_phase: Literal["pre_commit_dry_run", "commit", "post_commit_verification"],
    commit_acknowledged: bool = False,
) -> None:
    """Checkpoint safe classification and a separate commit acknowledgement."""
    attempt = await _owned_attempt(db, attempt_id, job_id, run_attempt, claim_token)
    if isinstance(classification, DefiniteRefusal):
        if not classification.records:
            raise ValueError("A refusal checkpoint requires codec catalog records")
        checkpoint = {"kind": "refused", "refusals": catalog_records(classification.records)}
    elif isinstance(classification, ProtocolFault):
        checkpoint = {"kind": "protocol_fault"}
    elif isinstance(classification, Unavailable):
        checkpoint = {"kind": "unavailable"}
    elif isinstance(classification, OrdinaryRejection):
        checkpoint = {"kind": "rejected"}
    elif classification is None and sender_phase == "commit" and commit_acknowledged:
        checkpoint = {"kind": "acknowledged"}
    else:
        raise ValueError("A response checkpoint requires safe classification")
    if sender_phase in attempt.response_classification:
        raise ValueError("A recorded response checkpoint is immutable")
    if commit_acknowledged and checkpoint["kind"] != "acknowledged":
        raise ValueError("Commit acknowledgement requires a successful commit response")
    if sender_phase == "post_commit_verification" and not attempt.commit_acknowledged:
        raise ValueError("Verification requires a recorded commit acknowledgement")
    attempt.response_classification = {**attempt.response_classification, sender_phase: checkpoint}
    attempt.response_phase = sender_phase
    attempt.commit_acknowledged = attempt.commit_acknowledged or commit_acknowledged
    attempt.phase = "response_recorded"
    await db.flush()


async def stage_row_outcome(
    db: AsyncSession,
    attempt_id: int,
    job_id: int,
    run_attempt: int,
    claim_token: str,
    logical: LogicalRow,
    disposition: Literal["in_sync", "refused", "not_applied", "unavailable"],
    *,
    refusals: Iterable[Refusal] = (),
) -> RowOutcome:
    """Stage one frozen logical outcome without changing its carrier or committing."""
    attempt = await _owned_attempt(db, attempt_id, job_id, run_attempt, claim_token)
    return await _stage_outcome(db, attempt, logical, disposition, refusals)


async def _stage_outcome(
    db: AsyncSession,
    attempt: ApplyExecutionAttempt,
    logical: LogicalRow,
    disposition: Literal["in_sync", "refused", "not_applied", "unavailable"],
    refusals: Iterable[Refusal],
) -> RowOutcome:
    identity = logical_record(logical)
    if identity not in attempt.logical_membership:
        raise ValueError("Row outcome is outside attempted logical membership")
    causes = catalog_records(refusals)
    for cause in causes:
        if not any(
            origin.get("family") == cause["family"]
            and origin.get("row") == cause["row"]
            and origin.get("field") == cause["field"]
            and identity in origin["logical_rows"]
            for origin in attempt.safe_origins
        ):
            raise ValueError("Refusal site does not belong to this logical outcome")
    outcome = RowOutcome(
        execution_attempt_id=attempt.id,
        stream=logical.stream,
        row_subject=logical.row.subject,
        row_key=logical.row.key,
        settlement_cohort=logical.settlement_cohort,
        source_identity=identity["sources"],
        logical_identity=logical.public_record(),
        carrier_identity=logical.carrier.public_record(),
        disposition=disposition,
        refusals=causes,
    )
    db.add(outcome)
    await db.flush()
    return outcome


async def restore_prior_bookkeeping(
    db: AsyncSession, attempt_id: int, job_id: int, run_attempt: int, claim_token: str
) -> set[tuple[str, int]]:
    """Keep frozen outcomes on a row CAS miss; ownership loss requires caller rollback."""
    attempt = await _owned_attempt(db, attempt_id, job_id, run_attempt, claim_token)
    return await _restore_attempt_bookkeeping(db, attempt)


async def _restore_attempt_bookkeeping(db: AsyncSession, attempt: ApplyExecutionAttempt) -> set[tuple[str, int]]:
    outcomes = list(await db.scalars(select(RowOutcome).where(RowOutcome.execution_attempt_id == attempt.id)))
    dispositions = {
        (outcome.stream, outcome.row_subject, outcome.row_key, outcome.settlement_cohort): outcome.disposition
        for outcome in outcomes
    }
    membership = {
        (logical["stream"], logical["row"]["subject"], logical["row"]["key"], logical["settlement_cohort"])
        for logical in attempt.logical_membership
    }
    if set(dispositions) != membership:
        raise ValueError("Restoration requires the complete staged logical outcome set")
    unnamed = [
        snapshot
        for snapshot in attempt.prior_bookkeeping
        if all(
            dispositions[
                (logical["stream"], logical["row"]["subject"], logical["row"]["key"], logical["settlement_cohort"])
            ]
            in ("not_applied", "unavailable")
            for logical in snapshot["contributors"]
        )
    ]
    return await restore_bookkeeping(db, attempt.device_id, unnamed)


ExitReason = Literal["cancelled", "timeout", "runner_failed", "removal_failed", "disposed", "orphaned", "claim_expired"]
_EXIT_MESSAGES = {
    "cancelled": "Execution cancelled",
    "timeout": "Execution budget expired",
    "runner_failed": "Execution runner failed",
    "removal_failed": "Removal runner failed",
    "disposed": "Execution ownership disposed",
    "orphaned": "Execution interrupted by restart",
    "claim_expired": "Execution claim expired",
}


def _identity(logical: dict) -> tuple:
    return logical["stream"], logical["row"]["subject"], logical["row"]["key"], logical["settlement_cohort"]


def _disposition_evidence(attempt: ApplyExecutionAttempt) -> tuple[str, dict, list]:
    """Resolve complete durable attribution without reading live intent rows."""
    phase = "commit" if "commit" in attempt.response_classification else "pre_commit_dry_run"
    checkpoint = attempt.response_classification.get(phase, {})
    if not attempt.commit_acknowledged and checkpoint.get("kind") == "refused":
        records = refusals_from_payload(checkpoint["refusals"])
        attributed = resolve_refusals(records, origins_from_records(attempt.safe_origins))
        if isinstance(attributed, AttributionFailure):
            return "attribution_failed", {}, []
        causes: dict = {}
        for site in attributed:
            for logical in site.logical_rows:
                causes.setdefault(_identity(logical.public_record()), []).extend(catalog_records((site.refusal,)))
        return "refused", causes, catalog_records(records)
    if attempt.commit_acknowledged:
        return "post_commit_unavailable", {}, []
    if checkpoint.get("kind") == "rejected":
        return "rejected", {}, []
    return "unavailable", {}, []


def _carrier_results(attempt: ApplyExecutionAttempt, outcomes: list[RowOutcome]) -> tuple[list, dict]:
    carriers: list[dict] = []
    counts: dict[str, dict[str, int]] = {}
    priority = {"in_sync": 0, "not_applied": 1, "unavailable": 2, "refused": 3}
    for carrier in attempt.attempted_carriers:
        public = {key: value for key, value in carrier.items() if key not in {"source_table", "content_identity"}}
        members = [outcome for outcome in outcomes if outcome.carrier_identity == public]
        if not members:
            raise ValueError("An attempted carrier requires logical outcomes")
        disposition = max((outcome.disposition for outcome in members), key=priority.__getitem__)
        carriers.append({**public, "disposition": disposition})
        key = "attribute" if carrier["stream"] == "interface_config" else carrier["stream"]
        count = counts.setdefault(
            f"{key}_count_by_outcome", {"in_sync": 0, "apply_failed": 0, "not_applied": 0, "unavailable": 0}
        )
        count["apply_failed" if disposition == "refused" else disposition] += 1
    return carriers, counts


async def dispose_execution_attempt(
    db: AsyncSession,
    job_id: int,
    run_attempt: int,
    claim_token: str | None,
    exit_reason: ExitReason,
) -> Literal["no_attempt", "terminal", "settled"]:
    """Stage one terminal transaction. Caller commits once, then disposes ownership.

    A null token requires an absent device claim and locks out acquisition. Expired-claim
    recovery calls this while it still holds the claim row, before deleting that row.
    Unknown commit acknowledgement requires durable recovery, never a second write.
    """
    if exit_reason not in _EXIT_MESSAGES:
        raise ValueError("Execution disposition requires a safe exit reason")
    attempt = await db.scalar(
        select(ApplyExecutionAttempt)
        .where(
            ApplyExecutionAttempt.job_correlation_id == job_id,
            ApplyExecutionAttempt.run_attempt == run_attempt,
        )
        .execution_options(populate_existing=True)
    )
    if attempt is None:
        return "no_attempt"
    if attempt.phase == "terminal":
        return "terminal"
    job, generations = await _lock_run(db, job_id, run_attempt, claim_token, attempt.generation_id)
    attempt = await db.scalar(
        select(ApplyExecutionAttempt)
        .where(ApplyExecutionAttempt.id == attempt.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        attempt.job_id != job_id
        or (claim_token is not None and attempt.claim_token != claim_token)
        or attempt.document_digest != generations[-1].digest
        or attempt.generation_authorities != _generation_authorities(generations)
    ):
        raise AttemptOwnershipLost("Execution attempt no longer owns this settlement")
    if attempt.phase == "terminal":
        return "terminal"
    kind, causes, records = _disposition_evidence(attempt)
    if kind in {"unavailable", "attribution_failed"} and "delivery" not in attempt.response_classification:
        attempt.response_classification = {
            **attempt.response_classification,
            "delivery": {"kind": "unavailable", "reason": exit_reason},
        }
        attempt.phase = "response_recorded"
    outcomes = []
    for frozen in attempt.logical_membership:
        row_causes = causes.get(_identity(frozen), [])
        disposition: Literal["in_sync", "refused", "not_applied", "unavailable"] = (
            "refused" if row_causes else "not_applied" if kind in {"refused", "rejected"} else "unavailable"
        )
        logical = logical_from_record(frozen)
        outcome = await _stage_outcome(db, attempt, logical, disposition, refusals_from_payload(row_causes))
        outcomes.append(outcome)
    await _restore_attempt_bookkeeping(db, attempt)
    await stamp_refused_bookkeeping(db, attempt.device_id, attempt.prior_bookkeeping, causes)
    carriers, counts = _carrier_results(attempt, outcomes)
    result = {
        **counts,
        "row_outcomes": [
            {**outcome.logical_identity, "disposition": outcome.disposition, "refusals": outcome.refusals}
            for outcome in outcomes
        ],
        "refusals": records,
        "settlement_owners": carriers,
        "commit_acknowledged": attempt.commit_acknowledged,
    }
    from nso_adapter.core.claim import terminalize

    write = await terminalize(
        db,
        job_id,
        status=JobStatus.failed,
        expect=JobStatus.running,
        run_attempt=run_attempt,
        result=result,
        error={"code": kind, "message": _EXIT_MESSAGES[exit_reason], "detail": {}},
        generation_outcome=GenerationStatus.failed
        if kind in {"refused", "rejected"}
        else GenerationStatus.outcome_unknown,
    )
    if write is None:
        raise AttemptOwnershipLost("Execution attempt terminal ownership was lost")
    attempt.phase = "terminal"
    await db.flush()
    return "settled"
