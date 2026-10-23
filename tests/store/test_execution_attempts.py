# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Durable execution evidence and restoration against the migrated PostgreSQL store."""

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.core.projection import _row_dict, build_interface_proof, section_registry, section_rows_by_table
from nso_adapter.nso.apply import SectionExecution
from nso_adapter.nso.origins import OriginContext, OriginRecorder
from nso_adapter.nso.refusal_protocol import refusal
from nso_adapter.nso.refusals import ProtocolFault
from nso_adapter.store.models import (
    ApplyExecutionAttempt,
    DbInterface,
    DeploymentGeneration,
    Device,
    DeviceClaim,
    DeviceProjectionStream,
    GenerationMode,
    InterfaceAttrState,
    InterfaceIntent,
    Job,
    JobStatus,
    JobType,
    RowOutcome,
    SyncState,
    VlanIntent,
)
from tests.store.test_schema_parity import test_alembic_baseline_matches_create_all as assert_schema_parity

ACCEPTED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
APPLIED = ACCEPTED - timedelta(days=1)


def test_refusal_schema_parity(pg_provisioner, pg_sync_session):
    tables = set(sa.inspect(pg_sync_session.bind).get_table_names())
    assert {"apply_execution_attempt", "row_outcome"} <= tables, "deployment evidence needs durable logical outcomes"
    assert_schema_parity(pg_provisioner)


async def _seed(db, *, attribute=False, prior_state=SyncState.in_sync, coalescible=False, attributes=("description",)):
    device = Device(nso_instance="test", nso_device_name="placeholder-device")
    db.add(device)
    await db.flush()
    job = Job(
        device_id=device.id, job_type=JobType.apply, status=JobStatus.running, run_attempt=1, coalescible=coalescible
    )
    db.add(job)
    await db.flush()
    token = str(uuid4())
    db.add(DeviceClaim(device_id=device.id, job_id=job.id, claim_token=token, purpose="job"))
    if attribute:
        interface = DbInterface(device_id=device.id, name="Ethernet1")
        db.add(interface)
        await db.flush()
        rows = [
            InterfaceIntent(
                interface_id=interface.id,
                attribute=name,
                intent_value="placeholder intent",
                accepted_at=ACCEPTED,
                last_apply_at=APPLIED,
                last_apply_error={"code": "prior"},
            )
            for name in attributes
        ]
        states = [
            InterfaceAttrState(
                interface_id=interface.id,
                attribute=name,
                sync_state=prior_state,
                last_checked_at=APPLIED,
                nso_value="placeholder observation",
                netbox_value="placeholder intent",
            )
            for name in attributes
        ]
        db.add_all(states)
        row, state = rows[0], states[0]
        section = "interface_config"
    else:
        row = VlanIntent(
            device_id=device.id,
            vlan_id=10,
            name="placeholder VLAN",
            accepted_at=ACCEPTED,
            last_apply_at=APPLIED,
            last_apply_error={"code": "prior"},
        )
        rows = [row]
        state = None
        section = "vlan"
    db.add_all(rows)
    await db.flush()
    tables = {row.__tablename__: [_row_dict(each) for each in rows]}
    execution = {"context": {"ned_id": None, "dialect": community_dialect_for(None).name}}
    if attribute:
        execution["proof"] = build_interface_proof(
            section, tables, [interface], {(interface.id, name): True for name in attributes}
        )
    db.add(DeviceProjectionStream(device_id=device.id, stream=section, desired_revision=7, authorized_revision=7))
    generation = DeploymentGeneration(
        device_id=device.id,
        job_id=job.id,
        seq=1,
        mode=GenerationMode.networked,
        document={section: {**tables, "_execution": execution}},
        digest="a" * 64,
        stream_revisions={section: 7},
        source_push_seq={section: 41},
    )
    db.add(generation)
    await db.flush()
    recorder = OriginRecorder("interface" if attribute else "vlan", OriginContext.from_generation(generation))
    subject = "interface.description" if attribute else "vlan.vlan"
    path = (("interface", ("Ethernet1",)),) if attribute else (("vlan", (10,)),)
    logical = recorder.logical(subject, path, recorder.source(row, row.__tablename__, section), section)
    recorder.emit(subject, path, (logical,))
    for extra in rows[1:]:
        extra_subject = "interface." + extra.attribute
        source = recorder.source(extra, extra.__tablename__, section)
        recorder.emit(extra_subject, path, (recorder.logical(extra_subject, path, source, section),))
    origins = recorder.finish({}).origins
    await db.commit()
    return job, generation, row, state, token, origins, logical


async def _prepare(db, seeded):
    from nso_adapter.store.execution_attempt_store import prepare_execution_attempt

    job, generation, _row, _state, token, origins, logical = seeded
    return await prepare_execution_attempt(
        db, job.id, job.run_attempt, token, generation.id, origins=origins, logical_rows=(logical,)
    )


async def _seed_coalesced(db):
    from nso_adapter.core.generation import (
        attach_to_job,
        digest_document,
        executing_generation,
        generation_execution_streams,
    )
    from nso_adapter.store.models import BgpRouterIntent

    seeded = await _seed(db, coalescible=True)
    job, first, vlan, _state, token, origins, vlan_logical = seeded
    router = BgpRouterIntent(
        device_id=job.device_id,
        asn="64512",
        router_id="192.0.2.254",
        accepted_at=ACCEPTED,
        last_apply_at=APPLIED,
        last_apply_error={"code": "prior BGP"},
    )
    db.add(router)
    db.add(DeviceProjectionStream(device_id=job.device_id, stream="bgp", desired_revision=9, authorized_revision=9))
    await db.flush()
    document = {
        **first.document,
        "bgp": {
            router.__tablename__: [_row_dict(router)],
            "_execution": {"context": {"ned_id": None, "dialect": community_dialect_for(None).name}},
        },
    }
    second = DeploymentGeneration(
        device_id=job.device_id,
        seq=2,
        mode=GenerationMode.networked,
        document=document,
        digest=digest_document(GenerationMode.networked, document, {}),
        stream_revisions={"bgp": 9},
        source_push_seq={"bgp": 52},
        settlement_cohort=202,
    )
    db.add(second)
    await db.flush()
    assert await attach_to_job(db, second, job)
    assert (await executing_generation(db, job.id)).id == second.id
    assert await generation_execution_streams(db, job.id) == frozenset({"vlan", "bgp"})
    recorder = OriginRecorder("bgp", OriginContext.from_generation(second))
    path = (("router", ("64512",)),)
    bgp_logical = recorder.logical("bgp.router", path, recorder.source(router, router.__tablename__, "bgp"), "bgp")
    recorder.emit("bgp.router", path, (bgp_logical,))
    origins.merge(recorder.finish({}).origins)
    await db.commit()
    return job, (first, second), (vlan, router), token, origins, (vlan_logical, bgp_logical)


@pytest.mark.anyio
@pytest.mark.parametrize("successor", [None, "authorization", "content", "bookkeeping"])
@pytest.mark.parametrize("bgp_refused", [False, True])
async def test_coalesced_attempt_keeps_all_membership_outcomes_and_restoration_fences(
    db_session, rival_engine, successor, bgp_refused
):
    from nso_adapter.store.execution_attempt_store import (
        prepare_execution_attempt,
        restore_prior_bookkeeping,
        stage_row_outcome,
    )

    job, generations, rows, token, origins, logicals = await _seed_coalesced(db_session)
    first, second = generations
    vlan, router = rows
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, second.id, origins=origins, logical_rows=logicals
    )
    assert attempt.generation_id == second.id and attempt.document_digest == second.digest
    assert attempt.stream_revisions == {"vlan": 7, "bgp": 9}
    assert attempt.generation_authorities == [
        {
            "generation_id": first.id,
            "document_digest": first.digest,
            "stream_revisions": {"vlan": 7},
            "source_push_seq": {"vlan": 41},
            "settlement_cohort": None,
        },
        {
            "generation_id": second.id,
            "document_digest": second.digest,
            "stream_revisions": {"bgp": 9},
            "source_push_seq": {"bgp": 52},
            "settlement_cohort": 202,
        },
    ]
    assert {row["generation_id"] for row in attempt.logical_membership} == {first.id, second.id}
    assert len(attempt.attempted_carriers) == 2
    assert {snapshot["stream"] for snapshot in attempt.prior_bookkeeping} == {"vlan", "bgp"}
    assert {snapshot["stream"]: snapshot["frozen_revision"] for snapshot in attempt.prior_bookkeeping} == {
        "vlan": 7,
        "bgp": 9,
    }
    await db_session.commit()
    factory = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with factory() as restarted:
        if successor == "authorization":
            await restarted.execute(
                sa.update(DeviceProjectionStream)
                .where(DeviceProjectionStream.device_id == job.device_id, DeviceProjectionStream.stream == "vlan")
                .values(desired_revision=8, authorized_revision=8)
            )
        elif successor == "content":
            await restarted.execute(
                sa.update(VlanIntent).where(VlanIntent.id == vlan.id).values(name="placeholder successor")
            )
        elif successor == "bookkeeping":
            await restarted.execute(
                sa.update(VlanIntent)
                .where(VlanIntent.id == vlan.id)
                .values(last_apply_at=ACCEPTED, last_apply_error={"code": "successor"})
            )
        await restarted.commit()
        await stage_row_outcome(restarted, attempt.id, job.id, 1, token, logicals[0], "not_applied")
        with pytest.raises(ValueError, match="complete staged logical outcome set"):
            await restore_prior_bookkeeping(restarted, attempt.id, job.id, 1, token)
        await stage_row_outcome(
            restarted,
            attempt.id,
            job.id,
            1,
            token,
            logicals[1],
            "refused" if bgp_refused else "not_applied",
            refusals=(refusal("bgp", logicals[1].row, "write-blocked"),) if bgp_refused else (),
        )
        expected = set() if bgp_refused else {(router.__tablename__, router.id)}
        if successor is None:
            expected.add((vlan.__tablename__, vlan.id))
        assert await restore_prior_bookkeeping(restarted, attempt.id, job.id, 1, token) == expected
        await restarted.commit()
    outcomes = list(await db_session.scalars(sa.select(RowOutcome).order_by(RowOutcome.id)))
    assert [(outcome.stream, outcome.disposition) for outcome in outcomes] == [
        ("vlan", "not_applied"),
        ("bgp", "refused" if bgp_refused else "not_applied"),
    ]
    assert [outcome.logical_identity["generation_id"] for outcome in outcomes] == [first.id, second.id]
    await db_session.refresh(vlan)
    await db_session.refresh(router)
    assert (router.last_apply_at, router.last_apply_error) == (APPLIED, {"code": "prior BGP"})
    assert (vlan.last_apply_at, vlan.last_apply_error) == (
        (ACCEPTED, {"code": "successor"}) if successor == "bookkeeping" else (APPLIED, {"code": "prior"})
    )
    assert vlan.name == ("placeholder successor" if successor == "content" else "placeholder VLAN")


@pytest.mark.anyio
@pytest.mark.parametrize(
    "field", ["generation_id", "frozen_revision", "source_push_seq", "settlement_cohort", "carrier"]
)
async def test_coalesced_membership_requires_its_own_generation_authority(db_session, field):
    from nso_adapter.store.execution_attempt_store import prepare_execution_attempt

    job, generations, _rows, token, origins, logicals = await _seed_coalesced(db_session)
    first, second = generations
    changes = {
        "generation_id": second.id,
        "frozen_revision": 9,
        "source_push_seq": 52,
        "settlement_cohort": 202,
        "carrier": replace(logicals[0].carrier, generation_id=second.id),
    }
    invalid = replace(logicals[0], **{field: changes[field]})
    with pytest.raises(ValueError, match="frozen generation"):
        await prepare_execution_attempt(
            db_session, job.id, 1, token, second.id, origins=origins, logical_rows=(invalid, logicals[1])
        )
    assert logicals[0].generation_id == first.id
    assert await db_session.scalar(sa.select(sa.func.count()).select_from(ApplyExecutionAttempt)) == 0


@pytest.mark.anyio
async def test_coalesced_settlement_requires_the_complete_carried_authority(db_session):
    from nso_adapter.store.execution_attempt_store import (
        AttemptOwnershipLost,
        prepare_execution_attempt,
        record_classified_response,
    )

    job, generations, _rows, token, origins, logicals = await _seed_coalesced(db_session)
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, generations[-1].id, origins=origins, logical_rows=logicals
    )
    await db_session.commit()
    with pytest.raises(AttemptOwnershipLost):
        async with db_session.begin_nested():
            await db_session.execute(
                sa.update(DeploymentGeneration).where(DeploymentGeneration.id == generations[0].id).values(job_id=None)
            )
            await record_classified_response(
                db_session, attempt.id, job.id, 1, token, ProtocolFault(), sender_phase="commit"
            )
    await db_session.refresh(attempt)
    assert attempt.phase == "prepared" and attempt.response_classification == {}


@pytest.mark.anyio
async def test_execution_attempt_identity_is_unique(db_session):
    seeded = await _seed(db_session)
    attempt = await _prepare(db_session, seeded)
    attempt_id, job_id = attempt.id, seeded[0].id
    await db_session.commit()
    with pytest.raises(sa.exc.IntegrityError):
        async with db_session.begin_nested():
            await _prepare(db_session, seeded)
    assert await db_session.scalar(sa.select(sa.func.count()).select_from(ApplyExecutionAttempt)) == 1
    await db_session.execute(sa.delete(DeviceClaim))
    await db_session.execute(sa.delete(Job).where(Job.id == job_id))
    await db_session.commit()
    db_session.expire_all()
    saved = await db_session.get(ApplyExecutionAttempt, attempt_id)
    assert saved.job_id is None and saved.job_correlation_id == job_id


@pytest.mark.anyio
async def test_row_outcome_constraints(db_session):
    from nso_adapter.store.execution_attempt_store import stage_row_outcome

    seeded = await _seed(db_session)
    attempt = await _prepare(db_session, seeded)
    attempt_id, job_id = attempt.id, seeded[0].id
    await db_session.commit()
    for disposition in ("in_sync", "refused", "not_applied", "unavailable"):
        async with db_session.begin_nested():
            causes = (refusal("vlan", seeded[-1].row, "write-blocked"),) if disposition == "refused" else ()
            outcome = await stage_row_outcome(
                db_session, attempt.id, seeded[0].id, 1, seeded[4], seeded[-1], disposition, refusals=causes
            )
            assert outcome.disposition == disposition
            await db_session.execute(sa.delete(RowOutcome).where(RowOutcome.id == outcome.id))
    with pytest.raises(sa.exc.IntegrityError):
        async with db_session.begin_nested():
            await db_session.execute(
                sa.insert(RowOutcome).values(
                    execution_attempt_id=attempt.id,
                    stream="vlan",
                    row_subject="vlan.vlan",
                    row_key=seeded[-1].row.key,
                    settlement_cohort=None,
                    source_identity=[],
                    logical_identity={},
                    carrier_identity={},
                    disposition="pending",
                    refusals=[],
                )
            )
    forged = replace(refusal("vlan", seeded[-1].row, "write-blocked"), message="placeholder-secret-canary")
    with pytest.raises(ValueError, match="catalog"):
        async with db_session.begin_nested():
            await stage_row_outcome(
                db_session, attempt.id, seeded[0].id, 1, seeded[4], seeded[-1], "refused", refusals=(forged,)
            )
    with pytest.raises(ValueError, match="catalog"):
        async with db_session.begin_nested():
            db_session.add(
                RowOutcome(
                    execution_attempt_id=attempt.id,
                    stream="vlan",
                    row_subject="vlan.vlan",
                    row_key=seeded[-1].row.key,
                    settlement_cohort=None,
                    source_identity=[],
                    logical_identity={},
                    carrier_identity={},
                    disposition="refused",
                    refusals=[{"message": "placeholder-secret-canary"}],
                )
            )
            await db_session.flush()
    await stage_row_outcome(db_session, attempt_id, job_id, 1, seeded[4], seeded[-1], "not_applied")
    with pytest.raises(sa.exc.IntegrityError):
        async with db_session.begin_nested():
            await stage_row_outcome(db_session, attempt_id, job_id, 1, seeded[4], seeded[-1], "not_applied")
    await db_session.commit()


@pytest.mark.anyio
async def test_child_outcomes_share_one_carrier(db_session):
    from nso_adapter.store.execution_attempt_store import prepare_execution_attempt, stage_row_outcome
    from nso_adapter.store.models import BgpPeerIntent, BgpRouterIntent, BgpScopeIntent

    fixture = json.loads((Path(__file__).parents[1] / "fixtures/refusal_settlement.json").read_text())["bgp"]
    seeded = await _seed(db_session)
    job, generation, _row, _state, token, _origins, _logical = seeded
    router = BgpRouterIntent(
        device_id=job.device_id,
        asn="64512",
        router_id="192.0.2.254",
        accepted_at=ACCEPTED,
        last_apply_at=APPLIED,
        last_apply_error={"code": "prior"},
    )
    db_session.add(router)
    await db_session.flush()
    scope = BgpScopeIntent(router_id=router.id, vrf="")
    db_session.add(scope)
    await db_session.flush()
    peers = [BgpPeerIntent(scope_id=scope.id, peer_address=f"192.0.2.{i}", remote_as=str(64512 + i)) for i in (1, 2)]
    db_session.add_all(peers)
    db_session.add(
        DeviceProjectionStream(device_id=job.device_id, stream="bgp", desired_revision=7, authorized_revision=7)
    )
    await db_session.flush()
    # This new generation owns the frozen BGP document; the original remains immutable.
    generation = DeploymentGeneration(
        id=fixture["fence"]["generation_id"],
        device_id=job.device_id,
        job_id=job.id,
        seq=2,
        mode=GenerationMode.networked,
        document={
            "bgp": {
                r.__tablename__: [_row_dict(v) for v in rows]
                for r, rows in ((router, [router]), (scope, [scope]), (peers[0], peers))
            }
            | {"_execution": {"context": {"ned_id": None, "dialect": community_dialect_for(None).name}}}
        },
        digest="b" * 64,
        stream_revisions={"bgp": 7},
        source_push_seq={"bgp": 41},
        settlement_cohort=fixture["fence"]["settlement_cohort"],
    )
    db_session.add(generation)
    await db_session.flush()
    execution = SectionExecution(
        None, community_dialect_for(None), origin_context=OriginContext.from_generation(generation)
    )
    encoded = section_registry()["bgp"].encode(section_rows_by_table(generation.document, "bgp"), execution)
    logicals = {
        logical.row.key: logical
        for binding in encoded.origins.bindings.values()
        for logical in binding.logical_rows
        if logical.row.subject == "bgp.router.scope.peer"
    }
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, generation.id, origins=encoded.origins, logical_rows=tuple(logicals.values())
    )
    assert len(attempt.attempted_carriers) == 1
    for expected in fixture["expected"]:
        logical = logicals[expected["row"]["key"]]
        causes = (refusal("bgp", logical.row, "write-blocked"),) if expected["outcome"] == "refused" else ()
        await stage_row_outcome(db_session, attempt.id, job.id, 1, token, logical, expected["outcome"], refusals=causes)
    await db_session.commit()
    outcomes = list(await db_session.scalars(sa.select(RowOutcome).order_by(RowOutcome.id)))
    assert [outcome.row_key for outcome in outcomes] == [item["row"]["key"] for item in fixture["expected"]]
    assert [outcome.disposition for outcome in outcomes] == ["refused", "not_applied"]
    assert all(outcome.carrier_identity["row"] == fixture["expected"][0]["carrier"]["row"] for outcome in outcomes)
    assert [
        {
            **outcome.logical_identity,
            "outcome": outcome.disposition,
            "cause": outcome.refusals[0]["message_id"] if outcome.refusals else None,
        }
        for outcome in outcomes
    ] == fixture["expected"]


@pytest.mark.anyio
@pytest.mark.parametrize("prior_state", [SyncState.in_sync, SyncState.accepted, SyncState.apply_failed])
async def test_prior_bookkeeping_snapshot_is_durable(db_session, rival_engine, prior_state):
    seeded = await _seed(db_session, attribute=True, prior_state=prior_state)
    attempt = await _prepare(db_session, seeded)
    assert attempt.prior_bookkeeping, "prepared evidence must contain the actual prior bookkeeping"
    from nso_adapter.store.execution_attempt_store import (
        record_classified_response,
        restore_prior_bookkeeping,
        stage_row_outcome,
    )

    await db_session.commit()
    factory = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with factory() as restarted:
        saved = await restarted.get(ApplyExecutionAttempt, attempt.id)
        snapshot = saved.prior_bookkeeping[0]
        assert snapshot["prior"] == {"last_apply_at": APPLIED.isoformat(), "last_apply_error": {"code": "prior"}}
        assert snapshot["attribute"]["prior_sync_state"] == prior_state.value
        await record_classified_response(
            restarted, attempt.id, seeded[0].id, 1, seeded[4], ProtocolFault(), sender_phase="commit"
        )
        await stage_row_outcome(restarted, attempt.id, seeded[0].id, 1, seeded[4], seeded[-1], "unavailable")
        restored = await restore_prior_bookkeeping(restarted, attempt.id, seeded[0].id, 1, seeded[4])
        assert restored == {(seeded[2].__tablename__, seeded[2].id)}
        await restarted.commit()
    await db_session.refresh(seeded[3])
    await db_session.refresh(seeded[2])
    assert seeded[3].sync_state == prior_state
    assert (seeded[2].last_apply_at, seeded[2].last_apply_error) == (APPLIED, {"code": "prior"})


@pytest.mark.anyio
async def test_prior_bookkeeping_snapshot_hydrates_the_interface_proof_once(db_session, monkeypatch):
    from nso_adapter.store import execution_attempt_store, execution_bookkeeping

    hydrations = []
    hydrate = execution_bookkeeping.hydrate_interface_execution
    monkeypatch.setattr(
        execution_bookkeeping,
        "hydrate_interface_execution",
        lambda document: hydrations.append(document) or hydrate(document),
    )
    job, generation, _row, _state, token, origins, _logical = await _seed(
        db_session, attribute=True, attributes=("description", "enabled")
    )
    logicals = tuple(
        dict.fromkeys(logical for binding in origins.bindings.values() for logical in binding.logical_rows)
    )
    attempt = await execution_attempt_store.prepare_execution_attempt(
        db_session, job.id, job.run_attempt, token, generation.id, origins=origins, logical_rows=logicals
    )
    assert len(attempt.prior_bookkeeping) == 2
    assert len(hydrations) == 1


async def _wait_for_block(db, pid):
    async with asyncio.timeout(4):
        while not await db.scalar(sa.text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"), {"pid": pid}):  # noqa: ASYNC110 - Poll the real PostgreSQL lock wait.
            await asyncio.sleep(0.01)


@pytest.mark.anyio
@pytest.mark.parametrize("writer_first", [True, False])
@pytest.mark.parametrize("changed_bookkeeping", [False, True])
async def test_restore_cas_preserves_successor_intent(db_session, rival_engine, writer_first, changed_bookkeeping):
    await _restore_race(db_session, rival_engine, writer_first, False, changed_bookkeeping)


@pytest.mark.anyio
@pytest.mark.parametrize("writer_first", [True, False])
@pytest.mark.parametrize("observed_state", [SyncState.changed, SyncState.deploying])
async def test_restore_cas_preserves_newer_attribute_observation(
    db_session, rival_engine, writer_first, observed_state
):
    await _restore_race(db_session, rival_engine, writer_first, True, observed_state=observed_state)


async def _restore_race(
    db, rival_engine, writer_first, observation, changed_bookkeeping=True, observed_state=SyncState.changed
):
    from nso_adapter.store.execution_attempt_store import restore_prior_bookkeeping, stage_row_outcome

    seeded = await _seed(db, attribute=True)
    job, _generation, row, state, token, _origins, logical = seeded
    attempt = await _prepare(db, seeded)
    await db.commit()
    factory = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with factory() as rival:
        rival_pid = await rival.scalar(sa.text("SELECT pg_backend_pid()"))
        await db.get(InterfaceAttrState, state.id)
        changed = (
            (
                sa.update(InterfaceAttrState)
                .where(InterfaceAttrState.id == state.id)
                .values(sync_state=observed_state, last_checked_at=ACCEPTED, nso_value="placeholder newer observation")
            )
            if observation
            else (
                sa.update(InterfaceIntent)
                .where(InterfaceIntent.id == row.id)
                .values(
                    intent_value="placeholder successor",
                    accepted_at=ACCEPTED + timedelta(days=1),
                    **(
                        {"last_apply_at": ACCEPTED, "last_apply_error": {"code": "successor"}}
                        if changed_bookkeeping
                        else {}
                    ),
                )
            )
        )

        async def restore():
            # Preload the stale object to prove that the helper refreshes it under its lock.
            cached_rows = [await rival.get(InterfaceIntent, row.id), await rival.get(InterfaceAttrState, state.id)]
            await stage_row_outcome(rival, attempt.id, job.id, 1, token, logical, "not_applied")
            result = await restore_prior_bookkeeping(rival, attempt.id, job.id, 1, token)
            assert all(cached is not None for cached in cached_rows)
            await rival.commit()
            return result

        if writer_first:
            await db.execute(changed)
            task = asyncio.create_task(restore())
            try:
                await _wait_for_block(db, rival_pid)
                await db.commit()
                assert await task == set(), "a stale restoration must miss its compare-and-set"
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        else:
            await stage_row_outcome(db, attempt.id, job.id, 1, token, logical, "not_applied")
            assert await restore_prior_bookkeeping(db, attempt.id, job.id, 1, token) == {(row.__tablename__, row.id)}

            async def write():
                await rival.execute(changed)
                await rival.commit()

            task = asyncio.create_task(write())
            try:
                await _wait_for_block(db, rival_pid)
                await db.commit()
                await task
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
    await db.refresh(row)
    await db.refresh(state)
    if observation:
        assert (state.sync_state, state.last_checked_at, state.nso_value) == (
            observed_state,
            ACCEPTED,
            "placeholder newer observation",
        )
    else:
        assert (row.intent_value, row.accepted_at, row.last_apply_at, row.last_apply_error) == (
            "placeholder successor",
            ACCEPTED + timedelta(days=1),
            ACCEPTED if changed_bookkeeping else APPLIED,
            {"code": "successor"} if changed_bookkeeping else {"code": "prior"},
        )
    outcomes = list(await db.scalars(sa.select(RowOutcome)))
    assert len(outcomes) == 1 and outcomes[0].disposition == "not_applied"
