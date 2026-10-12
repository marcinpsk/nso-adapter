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
from nso_adapter.nso.origins import LogicalRow, OriginIndex
from nso_adapter.nso.refusal_protocol import Refusal
from nso_adapter.nso.refusals import DefiniteRefusal, ErrorClassification, OrdinaryRejection, ProtocolFault, Unavailable
from nso_adapter.store.execution_bookkeeping import restore_bookkeeping, snapshot_bookkeeping
from nso_adapter.store.execution_evidence import catalog_records, source_record
from nso_adapter.store.models import (
    ApplyExecutionAttempt,
    DeploymentGeneration,
    DeviceClaim,
    Job,
    JobStatus,
    JobType,
    RowOutcome,
    StaticRouteTombstone,
)


class AttemptOwnershipLost(RuntimeError):
    """The caller must roll back all staged settlement effects."""


async def _lock_run(
    db: AsyncSession, job_id: int, run_attempt: int, token: str, generation_id: int
) -> tuple[Job, list[DeploymentGeneration]]:
    claim = await db.scalar(
        select(DeviceClaim)
        .where(DeviceClaim.job_id == job_id, DeviceClaim.claim_token == token)
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
        claim is None
        or job is None
        or not generations
        or generations[-1].id != generation_id
        or claim.device_id != job.device_id
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


def _logical_record(logical: LogicalRow) -> dict:
    return {
        **logical.public_record(),
        "sources": [source_record(source) for source in logical.source_refs],
        "carrier_content_identity": logical.carrier.content_identity,
    }


def _origin_records(origins: OriginIndex) -> list[dict]:
    records = [
        {
            "family": key.family,
            "row": {"subject": key.row.subject, "key": key.row.key},
            "field": key.field,
            "stream": binding.stream,
            "construct": binding.construct,
            "provenance": binding.provenance,
            "sources": [source_record(source) for source in binding.sources],
            "logical_rows": [_logical_record(logical) for logical in binding.logical_rows],
        }
        for key, binding in origins.bindings.items()
    ]
    records.extend(
        {
            "family": key.family,
            "row": {"subject": key.row.subject, "key": key.row.key},
            "field": key.field,
            "unsupported_reason": unsupported.reason,
            "logical_rows": [],
        }
        for key, unsupported in origins.unsupported.items()
    )
    records.extend(
        {"subject": subject, "unsupported_reason": unsupported.reason, "logical_rows": []}
        for subject, unsupported in origins.unsupported_subjects.items()
    )
    return records


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
        logical_membership=[_logical_record(logical) for logical in logicals],
        attempted_carriers=[
            {
                **carrier.public_record(),
                "source_table": carrier.source_table,
                "content_identity": carrier.content_identity,
            }
            for carrier in carriers
        ],
        safe_origins=_origin_records(origins),
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
    identity = _logical_record(logical)
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
