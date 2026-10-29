# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Coherent read-only settlement observations through the real HTTP boundary."""

import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy import event, select
from sqlalchemy.engine import make_url

from nso_adapter.store.db import get_engine
from nso_adapter.store.models import (
    Base,
    DeploymentApplyAttempt,
    DeploymentGeneration,
    GenerationStatus,
    IntentPushReceipt,
    Job,
    JobStatus,
    JobType,
)
from tests.api.test_deployment_evidence import _generation
from tests.conftest import AUTH, empty_execution_result, seed_device, session

pytestmark = pytest.mark.anyio


async def _seed_observation():
    device_id = await seed_device(nso_device_name="observe-device", netbox_device_id=18514)
    attempt_id = uuid.uuid4()
    async with session() as db:
        job = Job(
            device_id=device_id,
            job_type=JobType.apply,
            status=JobStatus.failed,
            coalescible=True,
            result=empty_execution_result(),
            settle_seq=7,
        )
        attempt = DeploymentApplyAttempt(
            id=attempt_id,
            device_id=device_id,
            selected={"vlan": 1},
            admission_state="admitted",
            http_status=202,
            response={},
        )
        db.add_all([job, attempt])
        await db.flush()
        generation = _generation(device_id, 1, GenerationStatus.failed, job_id=job.id, apply_attempt_id=attempt_id)
        generation.carrier_job_id = job.id
        generation.carrier_job_status = "failed"
        generation.carrier_job_result = empty_execution_result()
        db.add(generation)
        await db.flush()
        attempt.response = {"generations": [{"generation_id": generation.id}]}
        db.add(
            IntentPushReceipt(
                device_id=device_id,
                section="vlan",
                push_seq=1,
                request_digest="a" * 64,
                response={"generation_id": generation.id},
                generation_id=generation.id,
            )
        )
        await db.commit()
        return device_id, job.id, attempt_id, generation.id


async def _store_state():
    async with session() as db:
        return {table.name: (await db.execute(select(table))).mappings().all() for table in Base.metadata.sorted_tables}


async def test_observation_reuses_existing_projections_and_performs_no_writes(adapter_client):
    device_id, job_id, attempt_id, generation_id = await _seed_observation()
    missing = uuid.uuid4()
    before = await _store_state()
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    engine = get_engine()
    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/settlement-observation",
            headers=AUTH,
            json={"job_id": job_id, "after_settle_seq": 2, "apply_attempt_ids": [str(attempt_id), str(missing)]},
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["device"]["device_id"] == device_id
    assert body["next_feed"] == {"id": job_id, "device_id": device_id, "settle_seq": 7}
    assert body["absent"] == {"job_id": None, "apply_attempt_ids": [str(missing)], "receipt_sections": []}
    assert [item["identity"]["generation_id"] for item in body["generations"]] == [generation_id]
    job = await adapter_client.get(f"/api/v1/jobs/{job_id}", headers=AUTH)
    listing = await adapter_client.get(f"/api/v1/devices/{device_id}/generations", headers=AUTH)
    evidence = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id), str(missing)]},
    )
    receipts = await adapter_client.get("/api/v1/intent-receipts", headers=AUTH, params={"device_id": device_id})
    assert body["job"] == job.json()
    assert body["generations"] == listing.json()
    assert body["deployment"] == evidence.json()
    assert [item["projection"] for item in body["receipts"]] == receipts.json()["receipts"]
    from nso_adapter.store.meta import get_store_incarnation

    assert body["store_incarnation"] == get_store_incarnation()[0]
    assert len(body["digest"]) == 64
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in statements)
    assert await _store_state() == before


async def test_observation_hides_a_commit_between_its_queries(adapter_client):
    device_id, job_id, attempt_id, generation_id = await _seed_observation()
    engine = get_engine()
    writer = sa.create_engine(
        make_url(engine.url).set(drivername="postgresql+psycopg2"),
        isolation_level="AUTOCOMMIT",
        poolclass=sa.pool.NullPool,
    )
    fired = []

    def commit_between_queries(conn, cursor, statement, parameters, context, executemany):
        if "deployment_generation" in statement and statement.lstrip().upper().startswith("SELECT") and not fired:
            fired.append(statement)
            with writer.connect() as other:
                other.execute(
                    sa.update(DeploymentGeneration)
                    .where(DeploymentGeneration.id == generation_id)
                    .values(status=GenerationStatus.abandoned)
                )
                other.execute(
                    sa.update(IntentPushReceipt)
                    .where(IntentPushReceipt.device_id == device_id)
                    .values(response={"changed": True})
                )

    event.listen(engine.sync_engine, "before_cursor_execute", commit_between_queries)
    try:
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/settlement-observation",
            headers=AUTH,
            json={"job_id": job_id, "after_settle_seq": 2, "apply_attempt_ids": [str(attempt_id)]},
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", commit_between_queries)
        writer.dispose()
    assert response.status_code == 200, response.text
    assert fired
    body = response.json()
    assert body["deployment"]["head"]["projection"]["status"] == "failed"
    assert body["receipts"][0]["projection"]["response"] == {"generation_id": generation_id}
    later = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 2, "apply_attempt_ids": [str(attempt_id)]},
    )
    assert later.json()["deployment"]["head"] is None
    assert later.json()["digest"] != body["digest"]


async def test_observation_reports_missing_and_foreign_records_without_skipping_the_next_job(adapter_client):
    device_id, job_id, attempt_id, _ = await _seed_observation()
    other_id = await seed_device(nso_device_name="observe-other-device", netbox_device_id=18515)
    response = await adapter_client.post(
        f"/api/v1/devices/{other_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": [str(attempt_id)]},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["job"] is body["next_feed"] is body["deployment"]["head"] is None
    assert body["generations"] == body["receipts"] == body["deployment"]["attempts"] == []
    assert body["absent"] == {"job_id": job_id, "apply_attempt_ids": [str(attempt_id)], "receipt_sections": []}
    historical = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 7, "apply_attempt_ids": []},
    )
    assert historical.json()["job"]["identity"]["settle_seq"] == 7
    assert historical.json()["next_feed"] is None


async def test_observation_digest_orders_requested_identities_and_marks_missing_receipts(adapter_client):
    device_id, job_id, attempt_id, _ = await _seed_observation()
    missing = uuid.uuid4()
    async with session() as db:
        from sqlalchemy import update

        await db.execute(
            update(DeploymentApplyAttempt)
            .where(DeploymentApplyAttempt.id == attempt_id)
            .values(selected={"vlan": 1, "snmp": 2})
        )
        await db.commit()
    first = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": [str(attempt_id), str(missing)]},
    )
    second = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": [str(missing), str(attempt_id)]},
    )
    assert first.status_code == second.status_code == 200
    assert first.json()["absent"]["receipt_sections"] == ["snmp"]
    assert first.json()["digest"] == second.json()["digest"]


@pytest.mark.parametrize("missing_target", [False, True])
async def test_observation_reports_the_actual_next_job_across_sequence_gaps(adapter_client, missing_target):
    device_id, job_id, _, _ = await _seed_observation()
    async with session() as db:
        earlier = Job(
            device_id=device_id, job_type=JobType.sync, status=JobStatus.succeeded, coalescible=True, settle_seq=3
        )
        db.add(earlier)
        await db.commit()
        earlier_id = earlier.id
    requested_id = 999999 if missing_target else job_id
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": requested_id, "after_settle_seq": 1, "apply_attempt_ids": []},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["next_feed"] == {"id": earlier_id, "device_id": device_id, "settle_seq": 3}
    assert body["absent"]["job_id"] == (requested_id if missing_target else None)


@pytest.mark.parametrize(
    "body",
    [
        {"job_id": 1, "after_settle_seq": -1, "apply_attempt_ids": []},
        {"job_id": 1, "after_settle_seq": 2**63, "apply_attempt_ids": []},
        {"job_id": True, "after_settle_seq": 0, "apply_attempt_ids": []},
        {"job_id": 1, "after_settle_seq": 0, "apply_attempt_ids": [], "apply": True},
    ],
)
async def test_observation_rejects_invalid_requests(adapter_client, body):
    response = await adapter_client.post("/api/v1/devices/999999/settlement-observation", headers=AUTH, json=body)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_observation_requires_auth_and_a_device(adapter_client):
    body = {"job_id": 1, "after_settle_seq": 0, "apply_attempt_ids": []}
    unauthenticated = await adapter_client.post("/api/v1/devices/999999/settlement-observation", json=body)
    missing = await adapter_client.post("/api/v1/devices/999999/settlement-observation", headers=AUTH, json=body)
    assert unauthenticated.status_code == 401
    assert missing.status_code == 404


async def test_observation_preserves_an_unmapped_device_identity(adapter_client):
    from nso_adapter.store.models import Device

    device_id, job_id, _, _ = await _seed_observation()
    async with session() as db:
        device = await db.get(Device, device_id)
        device.netbox_device_id = None
        await db.commit()
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": []},
    )
    assert response.status_code == 200, response.text
    assert response.json()["device"]["netbox_device_id"] is None


async def test_invalid_admission_does_not_hide_missing_receipt_evidence(adapter_client):
    device_id, job_id, attempt_id, _ = await _seed_observation()
    async with session() as db:
        from sqlalchemy import update

        await db.execute(
            update(DeploymentApplyAttempt)
            .where(DeploymentApplyAttempt.id == attempt_id)
            .values(selected={"vlan": 1, "snmp": 2}, response={"generations": []})
        )
        await db.commit()
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": [str(attempt_id)]},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["deployment"]["attempts"][0]["evidence_error"]["code"] == "attempt_generation_membership"
    assert body["absent"]["receipt_sections"] == ["snmp"]
    assert len(body["receipts"]) == 1


@pytest.mark.parametrize("generation_count", [1, 2])
async def test_observation_retains_generation_membership_after_a_retry_replaces_the_carrier(
    adapter_client, generation_count
):
    from nso_adapter.store.models import ApplyExecutionAttempt

    device_id = await seed_device(nso_device_name="observe-historical-carrier", netbox_device_id=18517)
    async with session() as db:
        old = Job(
            device_id=device_id,
            job_type=JobType.apply,
            status=JobStatus.failed,
            coalescible=True,
            result=empty_execution_result(),
            settle_seq=4,
        )
        current = Job(
            device_id=device_id,
            job_type=JobType.apply,
            status=JobStatus.succeeded,
            coalescible=True,
            result=empty_execution_result(),
            settle_seq=8,
        )
        db.add_all([old, current])
        await db.flush()
        generations = [
            _generation(device_id, seq, GenerationStatus.settled, job_id=current.id)
            for seq in range(1, generation_count + 1)
        ]
        for generation in generations:
            generation.carrier_job_id = current.id
            generation.carrier_job_status = "succeeded"
            generation.carrier_job_result = empty_execution_result()
        db.add_all(generations)
        await db.flush()
        db.add(
            ApplyExecutionAttempt(
                device_id=device_id,
                generation_id=generation.id,
                job_id=old.id,
                job_correlation_id=old.id,
                run_attempt=1,
                operation_kind="apply",
                document_digest="b" * 64,
                claim_token="c" * 64,
                stream_revisions={"vlan": 1},
                generation_authorities=[
                    {
                        "generation_id": row.id,
                        "document_digest": row.digest,
                        "stream_revisions": row.stream_revisions,
                        "source_push_seq": row.source_push_seq,
                        "settlement_cohort": row.settlement_cohort,
                    }
                    for row in generations
                ],
                removal_tombstone_ids=[],
                logical_membership=[],
                attempted_carriers=[],
                safe_origins=[],
                prior_bookkeeping=[],
                phase="terminal",
            )
        )
        await db.commit()
        job_id, generation_ids = old.id, [row.id for row in generations]
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 4, "apply_attempt_ids": []},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["deployment"]["head"] is None
    assert body["job"]["identity"]["settle_seq"] == 4
    assert [item["identity"]["generation_id"] for item in body["generations"]] == generation_ids
    assert all(item["projection"]["carrier_job_id"] == current.id for item in body["generations"])
