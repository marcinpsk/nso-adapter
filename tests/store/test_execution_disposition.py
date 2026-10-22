# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Historical attribution and interrupted sends on the migrated PostgreSQL store."""

import asyncio
import json
from datetime import timedelta

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker
from structlog.testing import capture_logs

from nso_adapter.core.apply import encode_device_document, overlay_retained_routes
from nso_adapter.core.projection import _row_dict
from nso_adapter.core.static_route_reader import certified_static_route_section
from nso_adapter.nso.apply import apply_device_intent
from nso_adapter.nso.origins import (
    AttributionFailure,
    EncodedSection,
    OriginContext,
    OriginIndex,
    PriorContribution,
    RemovalAuthority,
    resolve_refusals,
)
from nso_adapter.nso.refusal_protocol import refusal, render_refusals
from nso_adapter.nso.refusals import DefiniteRefusal, ProtocolFault, classify_error
from nso_adapter.store.device_settle import create_counter
from nso_adapter.store.execution_attempt_store import (
    AttemptOwnershipLost,
    automatic_removal_reissue_blocked,
    bind_historical_origins,
    dispose_execution_attempt,
    prepare_execution_attempt,
    record_acknowledged_origins,
    record_classified_response,
)
from nso_adapter.store.models import (
    ApplyExecutionAttempt,
    DeploymentGeneration,
    Device,
    DeviceClaim,
    DeviceProjectionStream,
    GenerationMode,
    GenerationStatus,
    InterfaceAttrState,
    InterfaceIntent,
    Job,
    JobStatus,
    JobType,
    LoggingLevelsIntent,
    RowOutcome,
    StaticRouteIntent,
    StaticRouteTombstone,
    SyncState,
)
from tests.core.test_static_route_put import _Recorder
from tests.nso.test_apply_send import _client_with, _RecordingTransport
from tests.store.test_execution_attempts import ACCEPTED, APPLIED, _prepare, _seed, _seed_coalesced, _wait_for_block

pytestmark = pytest.mark.anyio


class RouteTransport(_RecordingTransport):
    def __init__(self, device_name, entry, *, refusal_record=None):
        super().__init__()
        self.entry = entry
        self.refusal_record = refusal_record
        self.recorder = _Recorder(device_name)

    async def handle_async_request(self, request):
        if request.method == "GET":
            self.requests.append(request)
            return httpx.Response(200, json={"device-intent:device-intent": [self.entry]}, request=request)
        response = await self.recorder._handle(request.method.lower(), str(request.url), request.content)
        self.requests.append(request)
        if self.refusal_record is not None and "dry-run=" not in str(request.url):
            return httpx.Response(
                500,
                json={
                    "ietf-restconf:errors": {
                        "error": [
                            {
                                "error-type": "application",
                                "error-tag": "operation-failed",
                                "error-message": render_refusals((self.refusal_record,)),
                            }
                        ]
                    }
                },
                request=request,
            )
        return response


async def _route_baseline(db, *, acknowledge=True, logging=False):
    device = Device(nso_instance="test", nso_device_name="placeholder-device")
    db.add(device)
    await db.flush()
    await create_counter(db, device.id)
    job = Job(device_id=device.id, job_type=JobType.apply, status=JobStatus.running, run_attempt=1, coalescible=False)
    db.add(job)
    row = StaticRouteIntent(
        device_id=device.id, route_id=41, vrf="", prefix="192.0.2.0/24", next_hop="192.0.2.1", accepted_at=ACCEPTED
    )
    db.add(row)
    await db.flush()
    token = "placeholder-claim"
    db.add(DeviceClaim(device_id=device.id, job_id=job.id, claim_token=token, purpose="job"))
    db.add(
        DeviceProjectionStream(device_id=device.id, stream="static_route", desired_revision=7, authorized_revision=7)
    )
    generation = DeploymentGeneration(
        device_id=device.id,
        job_id=job.id,
        seq=1,
        mode=GenerationMode.networked,
        status=GenerationStatus.running,
        digest="a" * 64,
        stream_revisions={"static_route": 7},
        source_push_seq={"static_route": 41},
        document={
            "static_route": {
                "static_route_intent": [_row_dict(row)],
                "_execution": {"context": {"ned_id": None, "dialect": "identity"}},
            }
        },
    )
    if logging:
        levels = LoggingLevelsIntent(device_id=device.id, console_severity="warnings", accepted_at=ACCEPTED)
        db.add(levels)
        await db.flush()
        generation.document = {
            **generation.document,
            "logging": {
                "logging_levels_intent": [_row_dict(levels)],
                "_execution": {"context": {"ned_id": None, "dialect": "identity"}},
            },
        }
        generation.stream_revisions = {**generation.stream_revisions, "logging": 7}
        generation.source_push_seq = {**generation.source_push_seq, "logging": 41}
        db.add(DeviceProjectionStream(device_id=device.id, stream="logging", desired_revision=7, authorized_revision=7))
    db.add(generation)
    await db.flush()
    encoded = encode_device_document(generation.document, origin_context=OriginContext.from_generation(generation))
    logical = next(
        binding.logical_rows[0]
        for key, binding in encoded.origins.bindings.items()
        if key.row.subject == "static-route.route"
    )
    attempt = await prepare_execution_attempt(
        db, job.id, 1, token, generation.id, origins=encoded.origins, logical_rows=(logical,)
    )
    await db.commit()
    instance = {"device": device.nso_device_name, **encoded.body}
    transport = RouteTransport(device.nso_device_name, instance)
    client = _client_with(transport)
    if acknowledge:
        await apply_device_intent(client, device.nso_device_name, encoded.body, device_id=device.id)
        await record_classified_response(
            db, attempt.id, job.id, 1, token, None, sender_phase="commit", commit_acknowledged=True
        )
        certified = await certified_static_route_section(client, device)
        await record_acknowledged_origins(db, attempt.id, job.id, 1, token, certified)
        await db.commit()
    return device, job, generation, token, encoded, logical, attempt, client, transport


async def test_acknowledged_history_survives_closed_send_gate(db_session, monkeypatch):
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "1")
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(
        db_session, acknowledge=False, logging=True
    )
    assert encoded.body["logging"]["local-levels"] == {"console-severity": "warnings"}
    await apply_device_intent(client, device.nso_device_name, encoded.body, device_id=device.id)
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, None, sender_phase="commit", commit_acknowledged=True
    )
    await db_session.commit()
    assert len(transport.recorder.commits) == 1 and attempt.commit_acknowledged
    assert "origin_history" not in attempt.response_classification
    certified = await certified_static_route_section(client, device)
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "0")
    await record_acknowledged_origins(db_session, attempt.id, job.id, 1, token, certified)
    await db_session.commit()
    await db_session.refresh(attempt)
    assert attempt.response_classification["origin_history"]
    assert all(
        record["entry_fingerprint"]
        for record in attempt.response_classification["origin_history"]
        if record["family"] == "static-route"
    )
    retained = EncodedSection({"route": []}, OriginIndex())
    overlay_retained_routes(retained, certified.routes)
    await bind_historical_origins(db_session, device.id, retained.origins, gen)
    result = resolve_refusals((refusal("static-route", logical.row, "write-blocked"),), retained.origins)
    assert not isinstance(result, AttributionFailure)
    assert result[0].logical_rows == (logical,)


async def test_retained_route_uses_certified_baseline_origin(db_session):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(db_session)
    certified = await certified_static_route_section(client, device)
    retained = EncodedSection({"route": []}, OriginIndex())
    overlay_retained_routes(retained, certified.routes)
    await bind_historical_origins(db_session, device.id, retained.origins, gen)
    result = resolve_refusals((refusal("static-route", logical.row, "write-blocked"),), retained.origins)
    assert not isinstance(result, AttributionFailure), "a certified acknowledged baseline must bind its retained owner"
    assert result[0].logical_rows == (logical,)
    assert retained.body["route"] == certified.routes
    assert len(transport.recorder.commits) == 1


async def test_retained_route_without_history_is_not_guessed(db_session):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(
        db_session, acknowledge=False
    )
    certified = await certified_static_route_section(client, device)
    retained = EncodedSection({"route": []}, OriginIndex())
    overlay_retained_routes(retained, certified.routes)
    await bind_historical_origins(db_session, device.id, retained.origins, gen)
    result = resolve_refusals((refusal("static-route", logical.row, "write-blocked"),), retained.origins)
    assert isinstance(result, AttributionFailure) and result.reason == "missing_baseline_provenance"
    assert retained.body["route"] == certified.routes
    assert not transport.recorder.commits


async def test_retained_route_does_not_use_history_before_latest_acknowledgement(db_session):
    device, job, gen, token, encoded, logical, older, client, transport = await _route_baseline(db_session)
    assert older.commit_acknowledged and older.response_classification["origin_history"]
    await dispose_execution_attempt(db_session, job.id, 1, token, "disposed")
    await db_session.commit()
    newer_job = Job(
        device_id=device.id, job_type=JobType.apply, status=JobStatus.running, run_attempt=1, coalescible=False
    )
    db_session.add(newer_job)
    await db_session.flush()
    newer_gen = DeploymentGeneration(
        device_id=device.id,
        job_id=newer_job.id,
        seq=gen.seq + 1,
        mode=gen.mode,
        status=GenerationStatus.running,
        digest=gen.digest,
        stream_revisions=gen.stream_revisions,
        source_push_seq=gen.source_push_seq,
        document=gen.document,
    )
    db_session.add(newer_gen)
    claim = await db_session.get(DeviceClaim, device.id)
    claim.job_id = newer_job.id
    await db_session.flush()
    newer_encoded = encode_device_document(newer_gen.document, origin_context=OriginContext.from_generation(newer_gen))
    newer_logical = next(
        binding.logical_rows[0]
        for key, binding in newer_encoded.origins.bindings.items()
        if key.row.subject == "static-route.route"
    )
    newer = await prepare_execution_attempt(
        db_session,
        newer_job.id,
        1,
        token,
        newer_gen.id,
        origins=newer_encoded.origins,
        logical_rows=(newer_logical,),
    )
    await apply_device_intent(client, device.nso_device_name, newer_encoded.body, device_id=device.id)
    await record_classified_response(
        db_session, newer.id, newer_job.id, 1, token, None, sender_phase="commit", commit_acknowledged=True
    )
    await db_session.commit()
    assert newer.id > older.id and newer.commit_acknowledged
    assert "origin_history" not in newer.response_classification
    certified = await certified_static_route_section(client, device)
    retained = EncodedSection({"route": []}, OriginIndex())
    overlay_retained_routes(retained, certified.routes)
    await bind_historical_origins(db_session, device.id, retained.origins, newer_gen)
    result = resolve_refusals((refusal("static-route", logical.row, "write-blocked"),), retained.origins)
    assert isinstance(result, AttributionFailure) and result.reason == "missing_baseline_provenance"
    assert retained.body["route"] == certified.routes
    assert len(transport.recorder.commits) == 2


async def test_changed_retained_entry_is_not_misattributed(db_session):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(db_session)
    assert "origin_history" in attempt.response_classification, (
        "acknowledgement must retain certified entry fingerprints"
    )
    transport.entry["static-route"]["route"][0]["metric"] = 73
    certified = await certified_static_route_section(client, device)
    retained = EncodedSection({"route": []}, OriginIndex())
    overlay_retained_routes(retained, certified.routes)
    await bind_historical_origins(db_session, device.id, retained.origins, gen)
    result = resolve_refusals((refusal("static-route", logical.row, "write-blocked"),), retained.origins)
    assert isinstance(result, AttributionFailure) and result.reason == "missing_baseline_provenance"
    assert retained.body["route"][0]["metric"] == 73


@pytest.mark.parametrize("history,authority", [(True, True), (False, True), (True, False)])
async def test_omission_requires_prior_origin_and_authority(db_session, history, authority):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(
        db_session, acknowledge=history
    )
    operation = DeploymentGeneration(
        device_id=device.id,
        seq=2,
        mode=GenerationMode.networked,
        digest="b" * 64,
        stream_revisions={"static_route": 8},
        document={
            "static_route": {
                "_execution": {
                    "context": {"ned_id": None, "dialect": "identity"},
                    "operation": {"removal": {"authorized_removal_keys": [["", "192.0.2.0/24", "192.0.2.1"]]}},
                },
            }
        },
    )
    db_session.add(operation)
    await db_session.flush()
    fence = RemovalAuthority(operation.id, "static_route", 8, "removal", logical.row) if authority else None
    origins = OriginIndex()
    origins.record_omission(PriorContribution("static-route", logical.row, fence))
    await bind_historical_origins(db_session, device.id, origins, operation)
    result = resolve_refusals((refusal("static-route", logical.row, "write-blocked"),), origins)
    assert isinstance(result, AttributionFailure) is not (history and authority), (
        "historical removal requires both proofs"
    )


async def test_recorded_refusal_recovers_without_resend(db_session, rival_engine):
    seeded = await _seed(db_session, attribute=True)
    job, generation, row, state, token, origins, logical = seeded
    await create_counter(db_session, job.device_id)
    attempt = await _prepare(db_session, seeded)
    await db_session.commit()
    cause = refusal("interface", logical.row, "write-blocked")
    transport = RouteTransport("placeholder-device", {}, refusal_record=cause)
    client = _client_with(transport)
    async with client._client() as http:
        response = await http.put("http://nso/restconf/data/device-intent:device-intent", content=json.dumps({}))
    classification = classify_error(response.status_code, response.content)
    assert isinstance(classification, DefiniteRefusal)
    await record_classified_response(db_session, attempt.id, job.id, 1, token, classification, sender_phase="commit")
    await db_session.commit()
    async with async_sessionmaker(rival_engine, expire_on_commit=False)() as restarted:
        assert await dispose_execution_attempt(restarted, job.id, 1, token, "cancelled") == "settled", (
            "a recorded refusal must settle after restart"
        )
        await restarted.commit()
    for record in (job, generation, row, state, attempt):
        await db_session.refresh(record)
    outcomes = list(await db_session.scalars(sa.select(RowOutcome)))
    assert [outcome.disposition for outcome in outcomes] == ["refused"]
    assert state.sync_state == SyncState.apply_failed and row.last_apply_at == APPLIED
    assert row.last_apply_error["refusals"][0]["message"] == cause.message
    assert job.status == JobStatus.failed and generation.status == GenerationStatus.failed
    assert job.result["row_outcomes"][0]["disposition"] == "refused"
    assert job.settle_seq == 1 and attempt.phase == "terminal"
    assert await dispose_execution_attempt(db_session, job.id, 1, token, "cancelled") == "terminal"
    assert len(transport.recorder.commits) == 1


@pytest.mark.parametrize("prior_state", [SyncState.in_sync, SyncState.accepted, SyncState.apply_failed])
async def test_prepared_attempt_disposition_restores_owned_state(db_session, prior_state):
    seeded = await _seed(db_session, attribute=True, prior_state=prior_state)
    job, generation, row, state, token, origins, logical = seeded
    await create_counter(db_session, job.device_id)
    attempt = await _prepare(db_session, seeded)
    await db_session.commit()
    assert state.sync_state == SyncState.deploying
    await dispose_execution_attempt(db_session, job.id, 1, token, "timeout")
    await db_session.commit()
    await db_session.refresh(state)
    assert state.sync_state == prior_state, "an interrupted prepared send must restore attempt-owned state"
    assert (row.last_apply_at, row.last_apply_error) == (APPLIED, {"code": "prior"})
    outcomes = list(await db_session.scalars(sa.select(RowOutcome)))
    assert [outcome.disposition for outcome in outcomes] == ["unavailable"]
    await db_session.refresh(attempt)
    assert attempt.response_classification["delivery"]["kind"] == "unavailable"
    assert attempt.phase == "terminal"


@pytest.mark.parametrize("response", ["refused", "prepared"])
async def test_outcome_evidence_survives_job_pruning(db_session, response):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(
        db_session, acknowledge=False
    )
    # A second immutable generation carries the removal authority.
    await db_session.execute(sa.delete(ApplyExecutionAttempt))
    job.job_type = JobType.removal
    tomb = StaticRouteTombstone(
        device_id=device.id,
        route_id=41,
        vrf="",
        prefix="192.0.2.0/24",
        next_hop="192.0.2.1",
        marking="delete_origin",
        job_id=job.id,
    )
    db_session.add(tomb)
    await db_session.flush()
    removal = DeploymentGeneration(
        device_id=device.id,
        job_id=job.id,
        seq=2,
        mode=GenerationMode.networked,
        status=GenerationStatus.running,
        digest="b" * 64,
        stream_revisions={"static_route": 7},
        source_push_seq={"static_route": 41},
        document={
            "static_route": {
                "static_route_intent": gen.document["static_route"]["static_route_intent"],
                "_execution": {
                    "context": {"ned_id": None, "dialect": "identity"},
                    "operation": {"tombstone_ids": [tomb.id]},
                },
            }
        },
    )
    db_session.add(removal)
    await db_session.flush()
    encoded = encode_device_document(removal.document, origin_context=OriginContext.from_generation(removal))
    logical = next(
        binding.logical_rows[0]
        for key, binding in encoded.origins.bindings.items()
        if key.row.subject == "static-route.route"
    )
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, removal.id, origins=encoded.origins, logical_rows=(logical,)
    )
    if response == "refused":
        await record_classified_response(
            db_session,
            attempt.id,
            job.id,
            1,
            token,
            DefiniteRefusal((refusal("static-route", logical.row, "write-blocked"),)),
            sender_phase="commit",
        )
    await db_session.commit()
    assert await automatic_removal_reissue_blocked(db_session, device.id, tomb.id), (
        "durable removal authority must block automatic resend"
    )
    await dispose_execution_attempt(db_session, job.id, 1, token, "cancelled")
    await db_session.commit()
    ids = attempt.id, job.id, tomb.id, device.id, removal.id
    await db_session.execute(sa.delete(DeviceClaim))
    await db_session.execute(sa.delete(Job).where(Job.id == job.id))
    await db_session.commit()
    db_session.expire_all()
    saved = await db_session.get(ApplyExecutionAttempt, ids[0])
    generation = await db_session.get(DeploymentGeneration, ids[4])
    assert saved.job_id is None and saved.job_correlation_id == ids[1] and saved.phase == "terminal"
    outcomes = list(await db_session.scalars(sa.select(RowOutcome).where(RowOutcome.execution_attempt_id == saved.id)))
    assert [outcome.disposition for outcome in outcomes] == ["refused" if response == "refused" else "unavailable"]
    assert generation.carrier_job_result["row_outcomes"][0]["row"] == logical.public_record()["row"]
    assert await automatic_removal_reissue_blocked(db_session, ids[3], ids[2])
    assert not await automatic_removal_reissue_blocked(db_session, ids[3], ids[2] + 100)


async def test_certified_retention_accepts_absent_optional_key_leaves(db_session):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(
        db_session, acknowledge=False
    )
    transport.entry["static-route"]["route"] = [{"prefix": "192.0.2.0/24", "metric": 73}]
    certified = await certified_static_route_section(client, device)
    retained = EncodedSection({"route": []}, OriginIndex())
    overlay_retained_routes(retained, certified.routes)
    assert retained.body["route"] == [{"prefix": "192.0.2.0/24", "metric": 73}]


@pytest.mark.parametrize("verification", [False, True])
async def test_disposition_keeps_post_commit_evidence(db_session, verification):
    seeded = await _seed(db_session, attribute=True)
    job, generation, row, state, token, origins, logical = seeded
    await create_counter(db_session, job.device_id)
    attempt = await _prepare(db_session, seeded)
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, None, sender_phase="commit", commit_acknowledged=True
    )
    if verification:
        await record_classified_response(
            db_session,
            attempt.id,
            job.id,
            1,
            token,
            DefiniteRefusal((refusal("interface", logical.row, "write-blocked"),)),
            sender_phase="post_commit_verification",
        )
    await db_session.commit()
    await dispose_execution_attempt(db_session, job.id, 1, token, "runner_failed")
    await db_session.commit()
    await db_session.refresh(job)
    await db_session.refresh(generation)
    await db_session.refresh(attempt)
    outcomes = list(await db_session.scalars(sa.select(RowOutcome)))
    assert [outcome.disposition for outcome in outcomes] == ["unavailable"]
    assert attempt.commit_acknowledged and job.result["commit_acknowledged"]
    assert bool(job.result["refusals"]) == bool(verification)
    assert job.error["code"] == "post_commit_unavailable"
    assert all(record["sender_phase"] == "post_commit_verification" for record in job.result["refusals"])
    assert generation.status == GenerationStatus.outcome_unknown


@pytest.mark.parametrize("claimed", [False, True])
async def test_disposition_declines_only_an_unprepared_run(db_session, claimed):
    seeded = await _seed(db_session)
    job, generation, row, state, token, origins, logical = seeded
    if not claimed:
        await db_session.execute(sa.delete(DeviceClaim))
    assert (
        await dispose_execution_attempt(db_session, job.id, 1, token if claimed else None, "orphaned") == "no_attempt"
    )
    attempt = await _prepare(db_session, seeded) if claimed else None
    if attempt is not None:
        await db_session.commit()
        assert await dispose_execution_attempt(db_session, job.id, 2, token, "disposed") == "no_attempt"
        with pytest.raises(AttemptOwnershipLost):
            await dispose_execution_attempt(db_session, job.id, 1, "placeholder-successor", "disposed")


@pytest.mark.parametrize("response", ["prepared", "refused", "protocol_fault"])
async def test_claimless_disposition_uses_the_same_terminal_transaction(db_session, response):
    seeded = await _seed(db_session, attribute=True)
    job, generation, row, state, token, origins, logical = seeded
    await create_counter(db_session, job.device_id)
    attempt = await _prepare(db_session, seeded)
    if response != "prepared":
        classification = (
            ProtocolFault()
            if response == "protocol_fault"
            else DefiniteRefusal((refusal("interface", logical.row, "write-blocked"),))
        )
        await record_classified_response(
            db_session, attempt.id, job.id, 1, token, classification, sender_phase="commit"
        )
    await db_session.execute(sa.delete(DeviceClaim))
    await db_session.commit()
    assert await dispose_execution_attempt(db_session, job.id, 1, None, "orphaned") == "settled"
    await db_session.commit()
    await db_session.refresh(job)
    await db_session.refresh(state)
    assert job.status == JobStatus.failed
    assert state.sync_state == (SyncState.apply_failed if response == "refused" else SyncState.in_sync)


async def test_omission_rejects_an_authority_for_another_row(db_session):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(db_session)
    operation = DeploymentGeneration(
        device_id=device.id,
        seq=2,
        mode=GenerationMode.networked,
        digest="b" * 64,
        stream_revisions={"static_route": 8},
        document={
            "static_route": {
                "_execution": {
                    "context": {"ned_id": None, "dialect": "identity"},
                    "operation": {"removal": {"authorized_removal_keys": [["", "192.0.2.8/32", "192.0.2.1"]]}},
                }
            }
        },
    )
    db_session.add(operation)
    await db_session.flush()
    from nso_adapter.nso.refusal_protocol import row_ref

    other = row_ref("static-route", "static-route.route", (("route", ("", "192.0.2.8/32", "192.0.2.1")),))
    origins = OriginIndex()
    origins.record_omission(
        PriorContribution(
            "static-route", logical.row, RemovalAuthority(operation.id, "static_route", 8, "removal", other)
        )
    )
    await bind_historical_origins(db_session, device.id, origins, operation)
    result = resolve_refusals((refusal("static-route", logical.row, "write-blocked"),), origins)
    assert isinstance(result, AttributionFailure), "a different removed key cannot authorize this historical origin"


@pytest.mark.parametrize("operation_kind", ["removal", "clear"])
async def test_historical_omission_uses_the_stream_operation_authority(db_session, operation_kind):
    seeded = await _seed(db_session)
    job, generation, row, state, token, origins, logical = seeded
    attempt = await _prepare(db_session, seeded)
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, None, sender_phase="commit", commit_acknowledged=True
    )
    await record_acknowledged_origins(db_session, attempt.id, job.id, 1, token)
    await db_session.commit()
    operation = DeploymentGeneration(
        device_id=job.device_id,
        seq=2,
        mode=GenerationMode.networked,
        digest="b" * 64,
        stream_revisions={"vlan": 8},
        removal_context={"scope": "vlan"},
        allowed_removal_keys={"vlan": {"vlan": [[10]]}} if operation_kind == "removal" else {},
        document={
            "vlan": {
                "_execution": {
                    "context": {"ned_id": None, "dialect": "identity"},
                    "operation": {"pending_clear_ids": [41]} if operation_kind == "clear" else {},
                }
            }
        },
    )
    db_session.add(operation)
    await db_session.flush()
    omitted = OriginIndex()
    omitted.record_omission(
        PriorContribution("vlan", logical.row, RemovalAuthority(operation.id, "vlan", 8, operation_kind, logical.row))
    )
    await bind_historical_origins(db_session, job.device_id, omitted, operation)
    result = resolve_refusals((refusal("vlan", logical.row, "write-blocked"),), omitted)
    assert not isinstance(result, AttributionFailure), (
        "a stream omission requires its recorded origin and exact immutable operation"
    )


@pytest.mark.parametrize("stream", ["ip", "isis_flex_algo"])
@pytest.mark.parametrize("authorized", [True, False])
async def test_historical_split_stream_removal_keeps_stream_authorization(db_session, stream, authorized):
    from nso_adapter.core.projection import build_interface_proof, stream_section
    from nso_adapter.nso.origins import OriginKey
    from nso_adapter.store.models import DbInterface, InterfaceIpIntent, IsisFlexAlgoIntent

    job, _, _, _, token, _, _ = await _seed(db_session)
    section = stream_section(stream)
    execution = {"context": {"ned_id": None, "dialect": "identity"}}
    if stream == "ip":
        interface = DbInterface(device_id=job.device_id, name="Ethernet1")
        db_session.add(interface)
        await db_session.flush()
        row = InterfaceIpIntent(interface_id=interface.id, address="192.0.2.1/24", family="ipv4", accepted_at=ACCEPTED)
        subject, family, keys = "interface.ipv4-address", "interface", ["Ethernet1", "192.0.2.1"]
    else:
        row = IsisFlexAlgoIntent(device_id=job.device_id, process_tag="CORE", algo_id=128, accepted_at=ACCEPTED)
        subject, family, keys = "isis.process-config.flex-algo", "isis", ["CORE", 128]
    db_session.add(row)
    db_session.add(
        DeviceProjectionStream(device_id=job.device_id, stream=stream, desired_revision=7, authorized_revision=7)
    )
    await db_session.flush()
    tables = {row.__tablename__: [_row_dict(row)]}
    if stream == "ip":
        execution["proof"] = build_interface_proof(section, tables, [interface], {})
    baseline = DeploymentGeneration(
        device_id=job.device_id,
        job_id=job.id,
        seq=2,
        mode=GenerationMode.networked,
        digest="b" * 64,
        stream_revisions={stream: 7},
        document={section: {**tables, "_execution": execution}},
    )
    db_session.add(baseline)
    await db_session.flush()
    encoded = encode_device_document(baseline.document, origin_context=OriginContext.from_generation(baseline))
    logical = next(
        logical
        for binding in encoded.origins.bindings.values()
        for logical in binding.logical_rows
        if logical.row.subject == subject
    )
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, baseline.id, origins=encoded.origins, logical_rows=(logical,)
    )
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, None, sender_phase="commit", commit_acknowledged=True
    )
    await record_acknowledged_origins(db_session, attempt.id, job.id, 1, token)
    await db_session.commit()
    sibling = "interface_config" if stream == "ip" else "isis"
    operation = DeploymentGeneration(
        device_id=job.device_id,
        seq=3,
        mode=GenerationMode.networked,
        digest="c" * 64,
        stream_revisions={stream if authorized else sibling: 8},
        removal_context={"scope": section},
        allowed_removal_keys={section: {subject.rsplit(".", 1)[-1]: [keys]}},
        document={section: {"_execution": {"context": execution["context"], "operation": {}}}},
    )
    db_session.add(operation)
    await db_session.flush()
    omitted = OriginIndex()
    omitted.record_omission(
        PriorContribution(family, logical.row, RemovalAuthority(operation.id, stream, 8, "removal", logical.row))
    )
    await bind_historical_origins(db_session, job.device_id, omitted, operation)
    result = resolve_refusals((refusal(family, logical.row, "write-blocked"),), omitted)
    assert isinstance(result, AttributionFailure) is not authorized, (
        "section scope must preserve the exact stream fence"
    )
    if authorized:
        assert result[0].logical_rows == (logical,)
        assert omitted.bindings[OriginKey(family, logical.row)].stream == stream


@pytest.mark.parametrize(("stream", "counter"), [("ip", "ip"), ("isis_flex_algo", "isis")])
async def test_disposed_split_stream_carrier_counts_under_its_section_counter(db_session, stream, counter):
    from nso_adapter.core.projection import build_interface_proof, section_registry, stream_section
    from nso_adapter.store.models import DbInterface, InterfaceIpIntent, IsisFlexAlgoIntent

    job, _, _, _, token, _, _ = await _seed(db_session)
    await create_counter(db_session, job.device_id)
    section = stream_section(stream)
    execution = {"context": {"ned_id": None, "dialect": "identity"}}
    if stream == "ip":
        interface = DbInterface(device_id=job.device_id, name="Ethernet1")
        db_session.add(interface)
        await db_session.flush()
        row = InterfaceIpIntent(interface_id=interface.id, address="192.0.2.1/24", family="ipv4", accepted_at=ACCEPTED)
    else:
        row = IsisFlexAlgoIntent(device_id=job.device_id, process_tag="CORE", algo_id=128, accepted_at=ACCEPTED)
    db_session.add(row)
    db_session.add(
        DeviceProjectionStream(device_id=job.device_id, stream=stream, desired_revision=7, authorized_revision=7)
    )
    await db_session.flush()
    tables = {row.__tablename__: [_row_dict(row)]}
    if stream == "ip":
        execution["proof"] = build_interface_proof(section, tables, [interface], {})
    generation = DeploymentGeneration(
        device_id=job.device_id,
        job_id=job.id,
        seq=2,
        mode=GenerationMode.networked,
        digest="b" * 64,
        stream_revisions={stream: 7},
        source_push_seq={stream: 41},
        document={section: {**tables, "_execution": execution}},
    )
    db_session.add(generation)
    await db_session.flush()
    encoded = encode_device_document(generation.document, origin_context=OriginContext.from_generation(generation))
    logicals = tuple(
        dict.fromkeys(
            logical
            for binding in encoded.origins.bindings.values()
            for logical in binding.logical_rows
            if logical.stream == stream
        )
    )
    await prepare_execution_attempt(
        db_session, job.id, 1, token, generation.id, origins=encoded.origins, logical_rows=logicals
    )
    await db_session.commit()
    assert await dispose_execution_attempt(db_session, job.id, 1, token, "cancelled") == "settled"
    await db_session.commit()
    await db_session.refresh(job)
    registered = {f"{key}_count_by_outcome" for entry in section_registry().values() for key in entry.result_keys}
    counters = {key for key in job.result if key.endswith("_count_by_outcome")}
    assert counters <= registered, f"{stream} carriers must count under a registry counter, not {counters}"
    assert job.result[f"{counter}_count_by_outcome"]["unavailable"] == 1


@pytest.mark.parametrize(
    "authority_stream,revision_delta,authorized",
    [
        ("route_policy", 0, True),
        ("route_policy", 1, False),
        ("vlan", 0, False),
    ],
)
async def test_historical_immediate_policy_clear_uses_real_admission(
    adapter_client, db_session, authority_stream, revision_delta, authorized
):
    from nso_adapter.core.projection import section_operation
    from nso_adapter.nso.origins import OriginKey
    from nso_adapter.nso.refusal_protocol import row_ref
    from nso_adapter.store.models import DeviceGenerationCounter, RoutePolicyObjectIntent, StreamPendingClear
    from tests.conftest import AUTH, push_seq

    job, _, _, _, token, _, _ = await _seed(db_session)
    entries = [{"sequence": 10, "action": "permit"}, {"sequence": 20, "action": "deny"}]
    row = RoutePolicyObjectIntent(
        device_id=job.device_id, family="route_map", name="RM-TEST", entries=entries, accepted_at=ACCEPTED
    )
    db_session.add(row)
    db_session.add(
        DeviceProjectionStream(
            device_id=job.device_id, stream="route_policy", desired_revision=7, authorized_revision=7
        )
    )
    await db_session.flush()
    baseline = DeploymentGeneration(
        device_id=job.device_id,
        job_id=job.id,
        seq=2,
        mode=GenerationMode.networked,
        digest="b" * 64,
        stream_revisions={"route_policy": 7},
        document={
            "route_policy": {
                row.__tablename__: [_row_dict(row)],
                "_execution": {"context": {"ned_id": None, "dialect": "identity"}},
            }
        },
    )
    db_session.add(baseline)
    db_session.add(DeviceGenerationCounter(device_id=job.device_id, last_seq=2))
    await db_session.flush()
    encoded = encode_device_document(baseline.document, origin_context=OriginContext.from_generation(baseline))
    removed_row = row_ref(
        "route-policy", "route-policy.route-map.entry", (("route-map", ("RM-TEST",)), ("entry", (20,)))
    )
    logical = encoded.origins.bindings[OriginKey("route-policy", removed_row)].logical_rows[0]
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, baseline.id, origins=encoded.origins, logical_rows=(logical,)
    )
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, None, sender_phase="commit", commit_acknowledged=True
    )
    await record_acknowledged_origins(db_session, attempt.id, job.id, 1, token)
    await db_session.commit()

    response = await adapter_client.put(
        f"/api/v1/devices/{job.device_id}/route-policy-intent",
        headers=AUTH | push_seq(),
        json={"objects": [{"family": "route_map", "name": "RM-TEST", "entries": entries[:1], "accepted": True}]},
    )
    assert response.status_code == 200, response.text
    operation = await db_session.scalar(
        sa.select(DeploymentGeneration)
        .where(
            DeploymentGeneration.device_id == job.device_id,
            DeploymentGeneration.seq > baseline.seq,
            DeploymentGeneration.removal_context.is_not(None),
        )
        .order_by(DeploymentGeneration.seq)
    )
    assert operation is not None and operation.mode == GenerationMode.networked
    assert section_operation(operation.document, "route_policy")["pending_clear_ids"] == []
    assert section_operation(operation.document, "route_policy")["immediate_clear_streams"] == ["route_policy"]
    assert not list(
        await db_session.scalars(sa.select(StreamPendingClear).where(StreamPendingClear.device_id == job.device_id))
    )
    omitted = encode_device_document(
        operation.document, origin_context=OriginContext.from_generation(operation)
    ).origins
    assert OriginKey("route-policy", removed_row) not in omitted.bindings
    omitted.record_omission(
        PriorContribution(
            "route-policy",
            removed_row,
            RemovalAuthority(
                operation.id,
                authority_stream,
                operation.stream_revisions["route_policy"] + revision_delta,
                "clear",
                removed_row,
            ),
        )
    )
    await bind_historical_origins(db_session, job.device_id, omitted, operation)
    result = resolve_refusals((refusal("route-policy", removed_row, "write-blocked"),), omitted)
    assert isinstance(result, AttributionFailure) is not authorized, (
        "an immediate clear needs its exact stream and revision"
    )
    if authorized:
        assert result[0].logical_rows == (logical,)
        assert omitted.bindings[OriginKey("route-policy", removed_row)].provenance == "prior"


@pytest.mark.parametrize("attribution_fault", [False, True])
async def test_disposition_requires_complete_attribution(db_session, attribution_fault):
    job, generations, rows, token, origins, logicals = await _seed_coalesced(db_session)
    await create_counter(db_session, job.device_id)
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, generations[-1].id, origins=origins, logical_rows=logicals
    )
    causes = (refusal("vlan", logicals[0].row, "write-blocked"),)
    if attribution_fault:
        from nso_adapter.nso.refusal_protocol import row_ref

        unknown = row_ref("bgp", "bgp.router", (("router", ("64513",)),))
        causes += (refusal("bgp", unknown, "write-blocked"),)
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, DefiniteRefusal(causes), sender_phase="commit"
    )
    await db_session.commit()
    await dispose_execution_attempt(db_session, job.id, 1, token, "cancelled")
    await db_session.commit()
    for record in (*rows, job, *generations):
        await db_session.refresh(record)
    outcomes = list(await db_session.scalars(sa.select(RowOutcome).order_by(RowOutcome.id)))
    assert [outcome.disposition for outcome in outcomes] == (
        ["unavailable", "unavailable"] if attribution_fault else ["refused", "not_applied"]
    )
    assert rows[1].last_apply_error == {"code": "prior BGP"} and rows[1].last_apply_at == APPLIED
    assert [
        {key: value for key, value in record.items() if key not in {"logical_rows", "provenance", "sender_phase"}}
        for record in job.result["refusals"]
    ] == ([] if attribution_fault else outcomes[0].refusals)
    assert rows[0].last_apply_error["code"] == ("prior" if attribution_fault else "refused")
    assert all(generation.carrier_job_result == job.result for generation in generations)


@pytest.mark.parametrize("writer_first", [False, True])
@pytest.mark.parametrize("named,observation", [(False, False), (True, False), (False, True), (True, True)])
async def test_disposition_preserves_successor_writes(db_session, rival_engine, writer_first, named, observation):
    seeded = await _seed(db_session, attribute=True)
    job, generation, row, state, token, origins, logical = seeded
    await create_counter(db_session, job.device_id)
    attempt = await _prepare(db_session, seeded)
    if named:
        await record_classified_response(
            db_session,
            attempt.id,
            job.id,
            1,
            token,
            DefiniteRefusal((refusal("interface", logical.row, "write-blocked"),)),
            sender_phase="commit",
        )
    await db_session.commit()
    changed = (
        sa.update(InterfaceAttrState)
        .where(InterfaceAttrState.id == state.id)
        .values(sync_state=SyncState.changed, last_checked_at=ACCEPTED, nso_value="placeholder newer observation")
        if observation
        else sa.update(InterfaceIntent)
        .where(InterfaceIntent.id == row.id)
        .values(
            intent_value="placeholder successor",
            accepted_at=ACCEPTED + timedelta(days=1),
            last_apply_at=ACCEPTED,
            last_apply_error={"code": "successor"},
        )
    )
    async with async_sessionmaker(rival_engine, expire_on_commit=False)() as rival:
        pid = await rival.scalar(sa.text("SELECT pg_backend_pid()"))

        async def settle():
            await rival.get(InterfaceIntent, row.id)
            await rival.get(InterfaceAttrState, state.id)
            await dispose_execution_attempt(rival, job.id, 1, token, "cancelled")
            await rival.commit()

        async def write():
            await rival.execute(changed)
            await rival.commit()

        if writer_first:
            await db_session.execute(changed)
            task = asyncio.create_task(settle())
        else:
            await dispose_execution_attempt(db_session, job.id, 1, token, "cancelled")
            task = asyncio.create_task(write())
        try:
            await _wait_for_block(db_session, pid)
            await db_session.commit()
            await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    await db_session.refresh(row)
    await db_session.refresh(state)
    if observation:
        assert (state.sync_state, state.last_checked_at, state.nso_value) == (
            SyncState.changed,
            ACCEPTED,
            "placeholder newer observation",
        )
    else:
        assert (row.intent_value, row.accepted_at, row.last_apply_at, row.last_apply_error) == (
            "placeholder successor",
            ACCEPTED + timedelta(days=1),
            ACCEPTED,
            {"code": "successor"},
        )
    outcomes = list(await db_session.scalars(sa.select(RowOutcome)))
    assert [outcome.disposition for outcome in outcomes] == (["refused"] if named else ["unavailable"])


async def test_disposition_rollback_preserves_recoverable_attempt(db_session):
    seeded = await _seed(db_session, attribute=True)
    job, generation, row, state, token, origins, logical = seeded
    await create_counter(db_session, job.device_id)
    attempt = await _prepare(db_session, seeded)
    await db_session.commit()
    assert await dispose_execution_attempt(db_session, job.id, 1, token, "timeout") == "settled"
    await db_session.rollback()
    for record in (job, generation, row, state, attempt):
        await db_session.refresh(record)
    assert job.status == JobStatus.running and attempt.phase == "prepared"
    assert state.sync_state == SyncState.deploying
    assert await db_session.scalar(sa.select(sa.func.count()).select_from(RowOutcome)) == 0
    assert await db_session.scalar(sa.select(DeviceClaim.claim_token)) == token
    assert await dispose_execution_attempt(db_session, job.id, 1, token, "timeout") == "settled"
    await db_session.commit()


@pytest.mark.parametrize("exit_reason", ["cancelled", "timeout", "claim_expired", "orphaned", "disposed"])
async def test_disposed_not_applied_route_carries_the_job_error(db_session, exit_reason):
    from nso_adapter.nso.refusals import OrdinaryRejection

    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(
        db_session, acknowledge=False
    )
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, OrdinaryRejection(), sender_phase="commit"
    )
    await db_session.commit()
    assert await dispose_execution_attempt(db_session, job.id, 1, token, exit_reason) == "settled"
    await db_session.commit()
    await db_session.refresh(job)
    [route] = job.result["static_route_results"]
    assert route["outcome"] == "apply_failed"
    assert route["error"] == job.error and job.error["code"] == "rejected", "an apply_failed route needs its error"


async def test_acknowledged_origin_history_survives_job_pruning(db_session):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(db_session)
    ids = (job.id, gen.id, device.id)
    await db_session.execute(sa.delete(DeviceClaim))
    await db_session.execute(sa.delete(Job).where(Job.id == job.id))
    await db_session.commit()
    db_session.expire_all()
    gen = await db_session.get(DeploymentGeneration, ids[1])
    device = await db_session.get(Device, ids[2])
    certified = await certified_static_route_section(client, device)
    retained = EncodedSection({"route": []}, OriginIndex())
    overlay_retained_routes(retained, certified.routes)
    await bind_historical_origins(db_session, device.id, retained.origins, gen)
    assert not isinstance(
        resolve_refusals((refusal("static-route", logical.row, "write-blocked"),), retained.origins), AttributionFailure
    )


async def test_retained_history_contains_only_safe_identity(db_session):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(db_session)
    payload = json.dumps(attempt.response_classification["origin_history"])
    from tests._secret_discipline import assert_text_free_of

    assert_text_free_of(payload, ["192.0.2.0/24", "192.0.2.1", "placeholder-device"])
    assert all(record["entry_fingerprint"] for record in attempt.response_classification["origin_history"])


async def test_current_field_binding_survives_historical_clear(db_session):
    seeded = await _seed(db_session)
    job, generation, row, state, token, origins, logical = seeded
    origins.record_omission(
        PriorContribution(
            "vlan", logical.row, RemovalAuthority(generation.id, "vlan", 7, "clear", logical.row, ("vlan.name",))
        )
    )
    result = resolve_refusals((refusal("vlan", logical.row, "write-blocked", field="vlan.name"),), origins)
    assert not isinstance(result, AttributionFailure), (
        "an emitted field keeps its current producer without prior history"
    )
    assert result[0].logical_rows == (logical,)


async def test_child_omission_uses_its_recorded_parent_removal_authority(db_session):
    from nso_adapter.store.models import BgpPeerIntent, BgpRouterIntent, BgpScopeIntent

    seeded = await _seed(db_session)
    job, generation, row, state, token, origins, logical = seeded
    router = BgpRouterIntent(device_id=job.device_id, asn="64512", router_id="192.0.2.254", accepted_at=ACCEPTED)
    db_session.add(router)
    await db_session.flush()
    scope = BgpScopeIntent(router_id=router.id, vrf="")
    db_session.add(scope)
    await db_session.flush()
    peer = BgpPeerIntent(scope_id=scope.id, peer_address="192.0.2.1", remote_as="64513")
    db_session.add(peer)
    db_session.add(
        DeviceProjectionStream(device_id=job.device_id, stream="bgp", desired_revision=7, authorized_revision=7)
    )
    await db_session.flush()
    baseline = DeploymentGeneration(
        device_id=job.device_id,
        job_id=job.id,
        seq=2,
        mode=GenerationMode.networked,
        digest="b" * 64,
        stream_revisions={"bgp": 7},
        source_push_seq={"bgp": 41},
        document={
            "bgp": {
                "bgp_router_intent": [_row_dict(router)],
                "bgp_scope_intent": [_row_dict(scope)],
                "bgp_peer_intent": [_row_dict(peer)],
                "_execution": {"context": {"ned_id": None, "dialect": "identity"}},
            }
        },
    )
    db_session.add(baseline)
    await db_session.flush()
    encoded = encode_device_document(baseline.document, origin_context=OriginContext.from_generation(baseline))
    child = next(
        logical
        for binding in encoded.origins.bindings.values()
        for logical in binding.logical_rows
        if logical.row.subject == "bgp.router.scope.peer"
    )
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, baseline.id, origins=encoded.origins, logical_rows=(child,)
    )
    await record_classified_response(
        db_session, attempt.id, job.id, 1, token, None, sender_phase="commit", commit_acknowledged=True
    )
    await record_acknowledged_origins(db_session, attempt.id, job.id, 1, token)
    await db_session.commit()
    operation = DeploymentGeneration(
        device_id=job.device_id,
        seq=3,
        mode=GenerationMode.networked,
        digest="c" * 64,
        stream_revisions={"bgp": 8},
        removal_context={"scope": "bgp"},
        allowed_removal_keys={"bgp": {"router": [["64512"]]}},
        document={"bgp": {"_execution": {"context": {"ned_id": None, "dialect": "identity"}, "operation": {}}}},
    )
    db_session.add(operation)
    await db_session.flush()
    omitted = OriginIndex()
    omitted.record_omission(
        PriorContribution("bgp", child.row, RemovalAuthority(operation.id, "bgp", 8, "removal", child.carrier.root_row))
    )
    await bind_historical_origins(db_session, job.device_id, omitted, operation)
    result = resolve_refusals((refusal("bgp", child.row, "write-blocked"),), omitted)
    assert not isinstance(result, AttributionFailure), "a prior child must join its own immutable parent removal"
    assert result[0].logical_rows == (child,)


@pytest.mark.parametrize("field,allowed", [("static-route.metric", True), ("static-route.tag", False)])
async def test_historical_route_clear_requires_exact_field_authority(db_session, field, allowed):
    device, job, gen, token, encoded, logical, attempt, client, transport = await _route_baseline(db_session)
    operation = DeploymentGeneration(
        device_id=device.id,
        seq=2,
        mode=GenerationMode.networked,
        digest="b" * 64,
        stream_revisions={"static_route": 8},
        document={
            "static_route": {
                "_execution": {
                    "context": {"ned_id": None, "dialect": "identity"},
                    "operation": {
                        "removal": {
                            "candidate_clears": [
                                {"row_id": 41, "key": ["", "192.0.2.0/24", "192.0.2.1"], "fields": ["metric"]}
                            ]
                        }
                    },
                }
            }
        },
    )
    db_session.add(operation)
    await db_session.flush()
    omitted = OriginIndex()
    omitted.record_omission(
        PriorContribution(
            "static-route",
            logical.row,
            RemovalAuthority(operation.id, "static_route", 8, "clear", logical.row, (field,)),
        )
    )
    await bind_historical_origins(db_session, device.id, omitted, operation)
    result = resolve_refusals((refusal("static-route", logical.row, "write-blocked", field=field),), omitted)
    assert isinstance(result, AttributionFailure) is not allowed, (
        "a historical field clear needs that field's immutable authority"
    )


# ── #1825: unprepared-run evidence is the attempt path's evidence, derived without a session ──


async def _seed_coalesced_apply(device_name: str, netbox_device_id: int, *, repeated: bool):
    """Seed one started apply carrying two adjacent generations through the production producers.

    The first promotes ``static_route``. The second promotes ``vlan`` (disjoint streams) or
    ``static_route`` again after one more route (a repeated stream). Returns
    ``(device_id, job_id, first_id, second_id)``.
    """
    from datetime import UTC, datetime

    from nso_adapter.core.generation import attach_to_job, create_generation, mark_job_generations_running, note_write
    from nso_adapter.store.models import VlanIntent
    from tests.conftest import seed_device, session
    from tests.core.test_static_route_put import B, C, seed_rows

    device_id = await seed_device(nso_device_name=device_name, netbox_device_id=netbox_device_id)
    await seed_rows(device_id, [{"triple": B, "route_id": 7}])
    async with session() as db:
        job = Job(
            device_id=device_id, job_type=JobType.apply, status=JobStatus.running, run_attempt=1, coalescible=True
        )
        db.add(job)
        await db.flush()
        await note_write(db, device_id, "static_route")
        first = await create_generation(db, device_id, streams=("static_route",), mode=GenerationMode.networked)
        assert await attach_to_job(db, first, job)
        await db.commit()
        job_id, first_id = job.id, first.id
    if repeated:
        await seed_rows(device_id, [{"triple": C, "route_id": 8}])
    else:
        async with session() as db:
            db.add(VlanIntent(device_id=device_id, vlan_id=10, name="placeholder VLAN", accepted_at=datetime.now(UTC)))
            await db.commit()
    stream = "static_route" if repeated else "vlan"
    async with session() as db:
        await note_write(db, device_id, stream)
        second = await create_generation(db, device_id, streams=(stream,), mode=GenerationMode.networked)
        assert await attach_to_job(db, second, await db.get(Job, job_id))
        await mark_job_generations_running(db, job_id)
        await db.commit()
        return device_id, job_id, first_id, second.id


@pytest.mark.parametrize("repeated", [False, True], ids=["disjoint_streams", "repeated_stream"])
async def test_1825_unprepared_evidence_is_the_attempt_paths_evidence_for_a_coalesced_carrier(
    adapter_client, monkeypatch, repeated
):
    """DERIVE, run with no session, returns exactly what the attempt path wrote for the same generations.

    The attempt path here is ``settle_unsent`` (an inconclusive retention read after
    preparation), so the result shapes must be equal byte for byte, ``static_route_results``
    included. A repeated stream keeps only its latest authority, as the attempt path does:
    nothing is invented for the earlier generation (the plugin keeps that carrier blocked).
    """
    from sqlalchemy import inspect as sa_inspect
    from sqlalchemy.ext.asyncio import AsyncSession

    from nso_adapter.core.execution import derive_unprepared_evidence
    from tests.conftest import session
    from tests.core.test_static_route_put import inconclusive, run_started_apply, sr_client

    name = f"coalesced-{'repeated' if repeated else 'disjoint'}"
    device_id, job_id, first_id, second_id = await _seed_coalesced_apply(name, 9881 + repeated, repeated=repeated)
    job = await run_started_apply(job_id, device_id, sr_client(name, state=inconclusive())[0])
    assert job.error["code"] == "static_route_snapshot_inconclusive"
    assert "static_route_results" in job.result
    async with session() as db:
        generations = list(
            await db.scalars(
                sa.select(DeploymentGeneration)
                .where(DeploymentGeneration.job_id == job_id)
                .order_by(DeploymentGeneration.seq)
            )
        )
    assert [generation.id for generation in generations] == [first_id, second_id]
    assert all(sa_inspect(generation).detached for generation in generations)

    def _no_session(*_args, **_kwargs):
        raise AssertionError("DERIVE must not touch a session")

    for method in ("execute", "scalar", "scalars", "get", "flush"):
        monkeypatch.setattr(AsyncSession, method, _no_session)
    derived = derive_unprepared_evidence(JobType.apply, generations, job.error, attempt_exists=False)

    assert json.loads(json.dumps(derived)) == job.result
    named = {row["generation_id"] for row in job.result["row_outcomes"]}
    assert named == ({second_id} if repeated else {first_id, second_id})
    assert {row["disposition"] for row in job.result["row_outcomes"]} == {"not_applied"}


async def test_1825_unprepared_evidence_skips_a_section_the_attempt_path_cannot_encode(adapter_client, monkeypatch):
    """A section whose encode fails contributes nothing on BOTH paths; its siblings still report.

    Two IP rows on one interface in different VRFs make the interface section unencodable
    (competing field producers). The attempt path settles the VLAN row ``not_applied``; DERIVE
    must return the same result instead of failing the whole derivation.
    """
    from datetime import UTC, datetime

    from sqlalchemy.ext.asyncio import AsyncSession

    from nso_adapter.core.execution import derive_unprepared_evidence
    from nso_adapter.core.generation import attach_to_job, create_generation, mark_job_generations_running, note_write
    from nso_adapter.store.models import DbInterface, InterfaceIpIntent, VlanIntent
    from tests.conftest import seed_device, session
    from tests.core.test_static_route_put import absent, run_started_apply, sr_client

    name = "mixed-vrf-ip"
    device_id = await seed_device(nso_device_name=name, netbox_device_id=9884)
    now = datetime.now(UTC)
    async with session() as db:
        interface = DbInterface(device_id=device_id, name="Ethernet1")
        db.add(interface)
        await db.flush()
        for address, vrf in (("192.0.2.1/24", "VRF-A"), ("198.51.100.1/24", "VRF-B")):
            db.add(
                InterfaceIpIntent(interface_id=interface.id, address=address, vrf=vrf, family="ipv4", accepted_at=now)
            )
        db.add(VlanIntent(device_id=device_id, vlan_id=10, name="placeholder VLAN", accepted_at=now))
        job = Job(
            device_id=device_id, job_type=JobType.apply, status=JobStatus.running, run_attempt=1, coalescible=True
        )
        db.add(job)
        await db.flush()
        for stream in ("ip", "vlan"):
            await note_write(db, device_id, stream)
        generation = await create_generation(db, device_id, streams=("ip", "vlan"), mode=GenerationMode.networked)
        assert await attach_to_job(db, generation, job)
        await mark_job_generations_running(db, job.id)
        await db.commit()
        job_id = job.id

    job = await run_started_apply(job_id, device_id, sr_client(name, state=absent())[0])
    assert job.error["code"] == "origin_construction"
    async with session() as db:
        generations = list(
            await db.scalars(sa.select(DeploymentGeneration).where(DeploymentGeneration.job_id == job_id))
        )

    def _no_session(*_args, **_kwargs):
        raise AssertionError("DERIVE must not touch a session")

    for method in ("execute", "scalar", "scalars", "get", "flush"):
        monkeypatch.setattr(AsyncSession, method, _no_session)
    with capture_logs() as logs:
        derived = derive_unprepared_evidence(JobType.apply, generations, job.error, attempt_exists=False)

    assert json.loads(json.dumps(derived)) == job.result
    skipped = [log for log in logs if log["event"] == "apply.document_section_skipped"]
    assert [(log["section"], log["code"]) for log in skipped] == [("interface_config", "origin_construction")]
    assert job.result["row_outcomes"]
    assert {row["disposition"] for row in job.result["row_outcomes"]} == {"not_applied"}
