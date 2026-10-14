# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Lifecycle, catalog secrecy, and ownership of durable send evidence."""

import pytest
import sqlalchemy as sa

from nso_adapter.nso.origins import OriginIndex
from nso_adapter.nso.refusal_protocol import refusal
from nso_adapter.nso.refusals import DefiniteRefusal, ProtocolFault
from nso_adapter.store.execution_attempt_store import (
    AttemptOwnershipLost,
    prepare_execution_attempt,
    record_classified_response,
    restore_prior_bookkeeping,
    stage_row_outcome,
)
from nso_adapter.store.models import (
    ApplyExecutionAttempt,
    DeploymentGeneration,
    DeviceClaim,
    GenerationMode,
    GenerationStatus,
    Job,
    JobStatus,
    JobType,
    RowOutcome,
    StaticRouteTombstone,
    SyncState,
)
from tests.store.test_execution_attempts import _prepare, _seed


@pytest.mark.anyio
async def test_restoration_excludes_named_logical_rows(db_session):
    seeded = await _seed(db_session, attribute=True)
    attempt = await _prepare(db_session, seeded)
    job, _generation, row, state, token, _origins, logical = seeded
    await stage_row_outcome(
        db_session,
        attempt.id,
        job.id,
        1,
        token,
        logical,
        "refused",
        refusals=(refusal("interface", logical.row, "write-blocked"),),
    )
    assert await restore_prior_bookkeeping(db_session, attempt.id, job.id, 1, token) == set()
    assert state.sync_state == SyncState.deploying
    await db_session.commit()


@pytest.mark.anyio
async def test_removal_authority_is_searchable_after_job_pruning(db_session):
    seeded = await _seed(db_session)
    job, generation, _row, _state, token, _origins, _logical = seeded
    job.job_type = JobType.removal
    tombstone = StaticRouteTombstone(
        device_id=job.device_id,
        route_id=1,
        vrf="",
        prefix="192.0.2.0/24",
        next_hop="192.0.2.1",
        marking="delete_origin",
        job_id=job.id,
    )
    db_session.add(tombstone)
    await db_session.flush()
    generation = DeploymentGeneration(
        device_id=job.device_id,
        job_id=job.id,
        seq=2,
        mode=GenerationMode.networked,
        document={
            "static_route": {
                "_execution": {
                    "context": {"ned_id": None, "dialect": "identity"},
                    "operation": {"tombstone_ids": [tombstone.id]},
                }
            }
        },
        digest="b" * 64,
        stream_revisions={"static_route": 7},
        removal_context={"scope": "static_route"},
    )
    db_session.add(generation)
    await db_session.flush()
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, generation.id, origins=OriginIndex(), logical_rows=()
    )
    assert attempt.removal_tombstone_ids == [tombstone.id], "removal evidence must retain the tombstone authority"
    await record_classified_response(db_session, attempt.id, job.id, 1, token, ProtocolFault(), sender_phase="commit")
    attempt_id, job_id, tombstone_id = attempt.id, job.id, tombstone.id
    await db_session.execute(sa.delete(DeviceClaim))
    await db_session.execute(sa.delete(Job).where(Job.id == job_id))
    await db_session.commit()
    db_session.expire_all()
    saved = await db_session.scalar(
        sa.select(ApplyExecutionAttempt).where(ApplyExecutionAttempt.removal_tombstone_ids.contains([tombstone_id]))
    )
    assert saved.id == attempt_id and saved.job_id is None and saved.job_correlation_id == job_id
    assert saved.response_classification == {"commit": {"kind": "protocol_fault"}}


@pytest.mark.anyio
@pytest.mark.parametrize("loss", ["claim", "run", "generation"])
async def test_ownership_loss_rolls_back_staged_evidence(db_session, loss):
    seeded = await _seed(db_session, attribute=True)
    attempt = await _prepare(db_session, seeded)
    job, generation, _row, state, token, _origins, logical = seeded
    attempt_id, job_id, generation_id, state_id = attempt.id, job.id, generation.id, state.id
    await db_session.commit()
    with pytest.raises(AttemptOwnershipLost):
        async with db_session.begin_nested():
            await stage_row_outcome(db_session, attempt_id, job_id, 1, token, logical, "not_applied")
            if loss == "claim":
                await db_session.execute(sa.delete(DeviceClaim))
            elif loss == "run":
                await db_session.execute(sa.update(Job).where(Job.id == job_id).values(run_attempt=2))
            else:
                await db_session.execute(
                    sa.update(DeploymentGeneration).where(DeploymentGeneration.id == generation_id).values(job_id=None)
                )
            await restore_prior_bookkeeping(db_session, attempt_id, job_id, 1, token)
    assert await db_session.scalar(sa.select(sa.func.count()).select_from(RowOutcome)) == 0
    await db_session.refresh(state)
    assert state.id == state_id and state.sync_state == SyncState.deploying


@pytest.mark.anyio
async def test_post_commit_refusal_keeps_acknowledgement(db_session):
    seeded = await _seed(db_session)
    attempt = await _prepare(db_session, seeded)
    job, _generation, _row, _state, token, _origins, logical = seeded
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, None, sender_phase="commit", commit_acknowledged=True
    )
    await record_classified_response(
        db_session,
        attempt.id,
        job.id,
        1,
        token,
        DefiniteRefusal((refusal("vlan", logical.row, "write-blocked"),)),
        sender_phase="post_commit_verification",
    )
    await db_session.commit()
    await db_session.refresh(attempt)
    assert attempt.commit_acknowledged
    assert attempt.response_classification["commit"] == {"kind": "acknowledged"}
    assert attempt.response_classification["post_commit_verification"]["kind"] == "refused"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "change",
    [
        {"prior_bookkeeping": []},
        {"document_digest": "b" * 64},
        {"safe_origins": []},
        {"generation_authorities": []},
        {"job_correlation_id": 999},
        {"run_attempt": 2},
    ],
)
async def test_attempt_preparation_is_immutable_in_postgresql(db_session, change):
    seeded = await _seed(db_session)
    attempt = await _prepare(db_session, seeded)
    await db_session.commit()
    with pytest.raises(sa.exc.IntegrityError, match="immutable"):
        async with db_session.begin_nested():
            await db_session.execute(
                sa.update(ApplyExecutionAttempt).where(ApplyExecutionAttempt.id == attempt.id).values(**change)
            )


@pytest.mark.anyio
async def test_recorded_response_and_outcomes_are_immutable(db_session):
    seeded = await _seed(db_session)
    attempt = await _prepare(db_session, seeded)
    job, _generation, _row, _state, token, _origins, logical = seeded
    await record_classified_response(db_session, attempt.id, job.id, 1, token, ProtocolFault(), sender_phase="commit")
    outcome = await stage_row_outcome(db_session, attempt.id, job.id, 1, token, logical, "unavailable")
    await db_session.commit()
    for statement in (
        sa.update(ApplyExecutionAttempt)
        .where(ApplyExecutionAttempt.id == attempt.id)
        .values(response_classification={}),
        sa.update(RowOutcome).where(RowOutcome.id == outcome.id).values(disposition="in_sync"),
    ):
        with pytest.raises(sa.exc.IntegrityError, match="immutable"):
            async with db_session.begin_nested():
                await db_session.execute(statement)


@pytest.mark.anyio
async def test_caller_rollback_removes_preparation_and_deploying_state(db_session):
    seeded = await _seed(db_session, attribute=True)
    state = seeded[3]
    await _prepare(db_session, seeded)
    assert state.sync_state == SyncState.deploying
    await db_session.rollback()
    assert await db_session.scalar(sa.select(sa.func.count()).select_from(ApplyExecutionAttempt)) == 0
    await db_session.refresh(state)
    assert state.sync_state == SyncState.in_sync


@pytest.mark.anyio
async def test_cutover_preserves_execution_evidence(db_session):
    from nso_adapter.core.cutover import deauthorize_for_cutover
    from nso_adapter.core.projection import _row_dict

    seeded = await _seed(db_session)
    attempt = await _prepare(db_session, seeded)
    job, generation, _row, _state, token, _origins, logical = seeded
    await record_classified_response(db_session, attempt.id, job.id, 1, token, ProtocolFault(), sender_phase="commit")
    outcome = await stage_row_outcome(db_session, attempt.id, job.id, 1, token, logical, "unavailable")
    attempt.phase = "terminal"
    job.status = JobStatus.failed
    generation.status = GenerationStatus.settled
    await db_session.commit()
    before = (_row_dict(attempt), _row_dict(outcome))
    await deauthorize_for_cutover(db_session)
    await db_session.commit()
    await db_session.refresh(attempt)
    await db_session.refresh(outcome)
    assert (_row_dict(attempt), _row_dict(outcome)) == before


def test_execution_evidence_migration_round_trip(pg_provisioner):
    from tests.store.migration_harness import alembic, assert_single_head_containing, engine_on, private_database

    revision, parent = "c39b7d2e4a61", "8a86079e0dfe"
    assert_single_head_containing(revision)
    with private_database(pg_provisioner, "execution_evidence") as url:
        alembic(url, "upgrade", revision)
        with engine_on(url) as engine:
            assert {"apply_execution_attempt", "row_outcome"} <= set(sa.inspect(engine).get_table_names())
        alembic(url, "downgrade", parent)
        with engine_on(url) as engine:
            assert not {"apply_execution_attempt", "row_outcome"} & set(sa.inspect(engine).get_table_names())
        alembic(url, "upgrade", revision)
