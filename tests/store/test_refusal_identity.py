# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""A document refusal can affect only its complete attempted logical identity."""

from dataclasses import replace
from datetime import timedelta

import pytest
import sqlalchemy as sa

from nso_adapter.nso.refusal_protocol import refusal
from nso_adapter.nso.refusals import DefiniteRefusal
from nso_adapter.store.device_settle import create_counter
from nso_adapter.store.execution_attempt_store import (
    dispose_execution_attempt,
    prepare_execution_attempt,
    record_classified_response,
)
from nso_adapter.store.models import RowOutcome, SyncState
from tests.store.test_execution_attempts import ACCEPTED, APPLIED, _seed

pytestmark = pytest.mark.anyio


def _other_fence(logical, fence):
    if fence.startswith("carrier."):
        field = fence.removeprefix("carrier.")
        value = getattr(logical.carrier, field)
        changed = ACCEPTED + timedelta(seconds=1) if field == "accepted_at" else (value or 1) + 1
        return replace(logical, carrier=replace(logical.carrier, **{field: changed}))
    value = getattr(logical, fence)
    changed = (ACCEPTED + timedelta(seconds=1)).isoformat() if fence == "acceptance_identity" else (value or 1) + 1
    return replace(logical, **{fence: changed})


@pytest.mark.parametrize(
    "fence",
    [
        "source_push_seq",
        "frozen_revision",
        "acceptance_identity",
        "generation_id",
        "settlement_cohort",
        "carrier.source_push_seq",
        "carrier.frozen_revision",
        "carrier.accepted_at",
        "carrier.generation_id",
        "carrier.settlement_cohort",
    ],
)
async def test_retained_refusal_cannot_cross_an_attempted_identity_fence(db_session, fence):
    job, generation, row, state, token, origins, logical = await _seed(db_session, attribute=True)
    await create_counter(db_session, job.device_id)
    foreign = _other_fence(logical, fence)
    origins.bindings = {
        key: replace(
            binding,
            provenance="retained",
            logical_rows=(foreign,),
            baseline_attempt_id=1,
            entry_fingerprint="b" * 64,
        )
        for key, binding in origins.bindings.items()
    }
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, generation.id, origins=origins, logical_rows=(logical,)
    )
    await db_session.commit()
    cause = refusal("interface", logical.row, "write-blocked")
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, DefiniteRefusal((cause,)), sender_phase="commit"
    )
    await db_session.commit()

    assert await dispose_execution_attempt(db_session, job.id, 1, token, "cancelled") == "settled"
    await db_session.commit()
    await db_session.refresh(job)
    await db_session.refresh(row)
    await db_session.refresh(state)
    outcomes = list(await db_session.scalars(sa.select(RowOutcome)))
    assert [(outcome.disposition, outcome.refusals) for outcome in outcomes] == [("not_applied", [])]
    assert job.result["row_outcomes"][0]["disposition"] == "not_applied"
    assert job.result["refusals"][0]["logical_rows"] == [foreign.public_record()]
    assert job.result["refusals"][0]["message"] == cause.message
    assert state.sync_state == SyncState.in_sync
    assert (row.last_apply_at, row.last_apply_error) == (APPLIED, {"code": "prior"})


async def test_refused_bookkeeping_contains_only_exactly_staged_causes(db_session):
    job, generation, row, state, token, origins, logical = await _seed(db_session, attribute=True)
    await create_counter(db_session, job.device_id)
    foreign = replace(logical, generation_id=logical.generation_id + 1)
    for key, binding in tuple(origins.bindings.items()):
        if key.field is None:
            origins.bindings[key] = replace(
                binding,
                provenance="retained",
                logical_rows=(foreign,),
                baseline_attempt_id=1,
                entry_fingerprint="b" * 64,
            )
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, generation.id, origins=origins, logical_rows=(logical,)
    )
    await db_session.commit()
    exact = refusal("interface", logical.row, "write-blocked", field="interface.attrs.description")
    foreign_cause = refusal("interface", logical.row, "write-blocked")
    await record_classified_response(
        db_session,
        attempt.id,
        job.id,
        1,
        token,
        DefiniteRefusal((exact, foreign_cause)),
        sender_phase="pre_commit_dry_run",
    )
    await db_session.commit()

    await dispose_execution_attempt(db_session, job.id, 1, token, "cancelled")
    await db_session.commit()
    await db_session.refresh(job)
    await db_session.refresh(row)
    await db_session.refresh(state)
    assert len(job.result["refusals"]) == 2
    assert [cause["field"] for cause in job.result["row_outcomes"][0]["refusals"]] == [exact.field]
    assert [cause["field"] for cause in row.last_apply_error["refusals"]] == [exact.field]
    assert state.sync_state == SyncState.apply_failed
    assert row.last_apply_at == APPLIED
