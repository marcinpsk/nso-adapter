# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Attempt-addressable deployment evidence through the authenticated API."""

from __future__ import annotations

import uuid

import pytest
import sqlalchemy as sa

from nso_adapter.api.timestamps import iso_z
from tests.conftest import VALID_TOKEN, seed_device, session

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


def _generation(
    device_id: int,
    seq: int,
    status,
    *,
    job_id: int | None = None,
    apply_attempt_id: uuid.UUID | None = None,
    settlement_cohort: int | None = None,
    source_push_seq: dict | None = None,
    stream_revisions: dict | None = None,
):
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode

    return DeploymentGeneration(
        device_id=device_id,
        seq=seq,
        mode=GenerationMode.networked,
        status=status,
        document={"vlan": {"vlan_intent": []}},
        digest=f"{seq:064x}",
        allowed_removal_keys={},
        source_push_seq=source_push_seq if source_push_seq is not None else {"vlan": seq},
        stream_revisions=stream_revisions if stream_revisions is not None else {"vlan": seq},
        settlement_cohort=settlement_cohort,
        apply_attempt_id=apply_attempt_id,
        job_id=job_id,
    )


async def test_evidence_serves_terminal_attempt_after_carrier_deletion(adapter_client):
    from nso_adapter.core.claim import terminalize
    from nso_adapter.store.models import (
        DeploymentApplyAttempt,
        DeploymentGeneration,
        GenerationStatus,
        Job,
        JobStatus,
        JobType,
    )

    device_id = await seed_device(nso_device_name="evidence-retained-carrier", netbox_device_id=16230)
    attempt_id = uuid.uuid4()
    unknown_id = uuid.uuid4()
    result = {"sections": {"vlan": {"outcome": "failed"}}}
    error = {"code": "nso_commit_failed", "message": "device refused the deployment", "detail": {}}
    async with session() as db:
        attempt = DeploymentApplyAttempt(
            id=attempt_id,
            device_id=device_id,
            selected={"vlan": 71},
            admission_state="admitted",
            http_status=202,
            response={},
        )
        job = Job(
            job_type=JobType.apply,
            status=JobStatus.running,
            coalescible=True,
            device_id=device_id,
            run_attempt=1,
        )
        db.add_all([attempt, job])
        await db.flush()
        generation = _generation(
            device_id,
            1,
            GenerationStatus.running,
            job_id=job.id,
            apply_attempt_id=attempt_id,
            source_push_seq={"vlan": 71},
            stream_revisions={"vlan": 4},
        )
        db.add(generation)
        await db.flush()
        replay = {
            "device_id": device_id,
            "outcome": "promoted",
            "job_id": job.id,
            "selected": {"vlan": 71},
            "skipped": {},
            "skipped_detail": None,
            "generations": [{"generation_id": generation.id, "seq": 1, "job_id": job.id}],
        }
        attempt.response = replay
        write = await terminalize(
            db,
            job.id,
            status=JobStatus.failed,
            expect=JobStatus.running,
            run_attempt=1,
            result=result,
            error=error,
        )
        assert write is not None
        await db.commit()
        generation_id = generation.id
        job_id = job.id

    async with session() as db:
        await db.execute(sa.delete(Job).where(Job.id == job_id))
        await db.commit()
        retained = await db.get(DeploymentGeneration, generation_id)
        assert retained.job_id is None
        created_at = iso_z(retained.created_at)
        updated_at = iso_z(retained.updated_at)

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id), str(unknown_id)]},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body == {
        "device_id": device_id,
        "head": {
            "generation_id": generation_id,
            "seq": 1,
            "status": "failed",
            "mode": "networked",
            "settlement_cohort": None,
            "sections": ["vlan"],
            "source_push_seq": {"vlan": 71},
            "apply_attempt_id": str(attempt_id),
            "carrier_job_id": job_id,
            "carrier_job_status": "failed",
            "carrier_job_result": result,
            "carrier_job_error": error,
            "created_at": created_at,
            "updated_at": updated_at,
        },
        "blocked": True,
        "write_work_pending": False,
        "held_jobs": [],
        "pending_generations": 0,
        "attempts": [
            {
                "apply_attempt_id": str(attempt_id),
                "admission_state": "admitted",
                "http_status": 202,
                "response": replay,
                "generations": [
                    {
                        "generation_id": generation_id,
                        "seq": 1,
                        "status": "failed",
                        "sections": ["vlan"],
                        "source_push_seq": {"vlan": 71},
                        "carrier_job_id": job_id,
                        "carrier_job_status": "failed",
                        "carrier_job_result": result,
                        "carrier_job_error": error,
                        "updated_at": updated_at,
                    }
                ],
            }
        ],
        "unknown_apply_attempt_ids": [str(unknown_id)],
    }


async def test_evidence_preserves_split_stream_provenance_without_duplicate_sections(adapter_client):
    from nso_adapter.store.models import DeploymentApplyAttempt, GenerationStatus

    device_id = await seed_device(nso_device_name="evidence-split-streams", netbox_device_id=16240)
    attempt_id = uuid.uuid4()
    async with session() as db:
        attempt = DeploymentApplyAttempt(
            id=attempt_id,
            device_id=device_id,
            selected={"interface_config": 201, "ip": 202},
            admission_state="admitted",
            http_status=202,
            response={},
        )
        db.add(attempt)
        generation = _generation(
            device_id,
            1,
            GenerationStatus.failed,
            apply_attempt_id=attempt_id,
            source_push_seq={"interface_config": 201, "ip": 202},
            stream_revisions={"interface_config": 7, "ip": 8},
        )
        db.add(generation)
        await db.flush()
        attempt.response = {"generations": [{"generation_id": generation.id}]}
        await db.commit()

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id)]},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    for served_generation in (body["head"], body["attempts"][0]["generations"][0]):
        assert served_generation["sections"] == ["interface_config"]
        assert served_generation["source_push_seq"] == {"interface_config": 201, "ip": 202}


@pytest.mark.parametrize(
    ("job_status", "generation_status"),
    [
        pytest.param("succeeded", "settled", id="worker-success"),
        pytest.param("failed", "failed", id="worker-failure"),
        pytest.param("failed", "outcome_unknown", id="outcome-unknown"),
        pytest.param("failed", "abandoned", id="abandon"),
    ],
)
async def test_terminal_snapshots_survive_carrier_deletion_in_evidence(
    adapter_client,
    job_status: str,
    generation_status: str,
):
    from nso_adapter.core.claim import terminalize
    from nso_adapter.store.models import (
        DeploymentApplyAttempt,
        GenerationStatus,
        Job,
        JobStatus,
        JobType,
    )

    device_id = await seed_device(
        nso_device_name=f"evidence-retained-{generation_status}",
        netbox_device_id={
            "settled": 16236,
            "failed": 16237,
            "outcome_unknown": 16238,
            "abandoned": 16239,
        }[generation_status],
    )
    attempt_id = uuid.uuid4()
    result = {"terminal_path": generation_status}
    error = (
        None
        if job_status == "succeeded"
        else {
            "code": f"{generation_status}_carrier",
            "message": f"{generation_status} carrier evidence",
            "detail": {},
        }
    )
    async with session() as db:
        attempt = DeploymentApplyAttempt(
            id=attempt_id,
            device_id=device_id,
            selected={"vlan": 73},
            admission_state="admitted",
            http_status=202,
            response={},
        )
        job = Job(
            job_type=JobType.apply,
            status=JobStatus.running,
            coalescible=True,
            device_id=device_id,
            run_attempt=1,
        )
        db.add_all([attempt, job])
        await db.flush()
        generation = _generation(
            device_id,
            1,
            GenerationStatus.running,
            job_id=job.id,
            apply_attempt_id=attempt_id,
            source_push_seq={"vlan": 73},
            stream_revisions={"vlan": 5},
        )
        db.add(generation)
        await db.flush()
        attempt.response = {
            "device_id": device_id,
            "outcome": "promoted",
            "selected": {"vlan": 73},
            "skipped": {},
            "skipped_detail": None,
            "generations": [{"generation_id": generation.id}],
        }
        write = await terminalize(
            db,
            job.id,
            status=JobStatus(job_status),
            expect=JobStatus.running,
            run_attempt=1,
            result=result,
            error=error,
            generation_outcome=GenerationStatus(generation_status),
        )
        assert write is not None
        await db.commit()
        generation_id = generation.id
        job_id = job.id

    async with session() as db:
        await db.execute(sa.delete(Job).where(Job.id == job_id))
        await db.commit()

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id)]},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    if generation_status in {"settled", "abandoned"}:
        assert body["head"] is None
    else:
        assert body["head"]["generation_id"] == generation_id
    assert body["attempts"][0]["generations"] == [
        {
            "generation_id": generation_id,
            "seq": 1,
            "status": generation_status,
            "sections": ["vlan"],
            "source_push_seq": {"vlan": 73},
            "carrier_job_id": job_id,
            "carrier_job_status": job_status,
            "carrier_job_result": result,
            "carrier_job_error": error,
            "updated_at": body["attempts"][0]["generations"][0]["updated_at"],
        }
    ]


async def test_evidence_reports_a_blocked_removal_and_its_held_apply(adapter_client):
    from nso_adapter.store.models import GenerationStatus, Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="evidence-held-apply", netbox_device_id=16231)
    async with session() as db:
        failed_carrier = Job(
            job_type=JobType.removal,
            status=JobStatus.failed,
            coalescible=False,
            device_id=device_id,
            error={"code": "removal_failed"},
        )
        held_carrier = Job(
            job_type=JobType.apply,
            status=JobStatus.queued,
            coalescible=True,
            device_id=device_id,
        )
        db.add_all([failed_carrier, held_carrier])
        await db.flush()
        head = _generation(device_id, 1, GenerationStatus.failed, job_id=failed_carrier.id)
        successor = _generation(device_id, 2, GenerationStatus.pending, job_id=held_carrier.id)
        db.add_all([head, successor])
        await db.commit()
        head_id = head.id
        held_job_id = held_carrier.id

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": []},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["head"]["generation_id"] == head_id
    assert body["head"]["status"] == "failed"
    assert body["blocked"] is True
    assert body["write_work_pending"] is False
    assert body["held_jobs"] == [held_job_id]
    assert body["pending_generations"] == 1


async def test_evidence_keeps_the_device_head_before_cohortless_and_reissue_successors(adapter_client):
    from nso_adapter.store.models import GenerationStatus, Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="evidence-device-head", netbox_device_id=16232)
    async with session() as db:
        jobs = [
            Job(
                job_type=JobType.apply,
                status=JobStatus.failed if index == 0 else JobStatus.queued,
                coalescible=False,
                device_id=device_id,
            )
            for index in range(3)
        ]
        db.add_all(jobs)
        await db.flush()
        failed = _generation(device_id, 1, GenerationStatus.failed, job_id=jobs[0].id, settlement_cohort=83)
        single = _generation(device_id, 2, GenerationStatus.pending, job_id=jobs[1].id, settlement_cohort=None)
        reissue = _generation(
            device_id,
            3,
            GenerationStatus.pending,
            job_id=jobs[2].id,
            settlement_cohort=None,
            source_push_seq={"vlan": None},
            stream_revisions={},
        )
        db.add_all([failed, single, reissue])
        await db.commit()
        failed_id = failed.id

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": []},
    )

    assert response.status_code == 200, response.text
    assert response.json()["head"]["generation_id"] == failed_id
    assert response.json()["pending_generations"] == 2


async def test_evidence_finds_the_head_after_120_settled_predecessors(adapter_client):
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="evidence-unpaged-head", netbox_device_id=16233)
    async with session() as db:
        db.add_all(_generation(device_id, seq, GenerationStatus.settled) for seq in range(1, 121))
        head = _generation(device_id, 121, GenerationStatus.pending)
        db.add(head)
        await db.commit()
        head_id = head.id

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": []},
    )

    assert response.status_code == 200, response.text
    assert (response.json()["head"]["generation_id"], response.json()["head"]["seq"]) == (head_id, 121)


async def test_unknown_attempt_ids_are_reported_as_non_actionable(adapter_client):
    device_id = await seed_device(nso_device_name="evidence-unknown-attempts", netbox_device_id=16234)
    unknown_ids = [uuid.uuid4(), uuid.uuid4()]

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(item) for item in unknown_ids]},
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "device_id": device_id,
        "head": None,
        "blocked": False,
        "write_work_pending": False,
        "held_jobs": [],
        "pending_generations": 0,
        "attempts": [],
        "unknown_apply_attempt_ids": [str(item) for item in unknown_ids],
    }


@pytest.mark.parametrize(
    "generation_field",
    [
        pytest.param({}, id="absent"),
        pytest.param({"generations": None}, id="null"),
    ],
)
async def test_unstamped_rejection_envelope_is_valid_attempt_evidence(adapter_client, generation_field):
    from nso_adapter.store.models import DeploymentApplyAttempt

    device_id = await seed_device(nso_device_name="evidence-rejected-attempt", netbox_device_id=16241)
    attempt_id = uuid.uuid4()
    replay = {
        "error": {
            "code": "conflict",
            "message": "A job is already queued or running for this device",
            "detail": {"job_id": 91},
        },
        **generation_field,
    }
    async with session() as db:
        db.add(
            DeploymentApplyAttempt(
                id=attempt_id,
                device_id=device_id,
                selected={"vlan": 74},
                admission_state="rejected",
                http_status=409,
                response=replay,
            )
        )
        await db.commit()

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id)]},
    )

    assert response.status_code == 200, response.text
    assert response.json()["attempts"] == [
        {
            "apply_attempt_id": str(attempt_id),
            "admission_state": "rejected",
            "http_status": 409,
            "response": replay,
            "generations": [],
        }
    ]


async def test_rejection_envelope_without_generation_list_is_corrupt_when_a_generation_is_stamped(adapter_client):
    from nso_adapter.store.models import DeploymentApplyAttempt, GenerationStatus

    device_id = await seed_device(nso_device_name="evidence-rejected-stamped", netbox_device_id=16242)
    attempt_id = uuid.uuid4()
    async with session() as db:
        db.add(
            DeploymentApplyAttempt(
                id=attempt_id,
                device_id=device_id,
                selected={"vlan": 75},
                admission_state="rejected",
                http_status=409,
                response={
                    "error": {
                        "code": "conflict",
                        "message": "A job is already queued or running for this device",
                        "detail": {"job_id": 92},
                    }
                },
            )
        )
        db.add(
            _generation(
                device_id,
                1,
                GenerationStatus.failed,
                apply_attempt_id=attempt_id,
            )
        )
        await db.commit()

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id)]},
    )

    assert response.status_code == 500
    assert response.json() == {"error": {"code": "internal", "message": "Internal server error", "detail": {}}}


async def test_corrupt_attempt_generation_evidence_returns_the_internal_error_envelope(
    adapter_client,
):
    from structlog.testing import capture_logs

    from nso_adapter.store.models import DeploymentApplyAttempt

    device_id = await seed_device(nso_device_name="evidence-corrupt-attempt", netbox_device_id=16235)
    attempt_id = uuid.uuid4()
    async with session() as db:
        db.add(
            DeploymentApplyAttempt(
                id=attempt_id,
                device_id=device_id,
                selected={"vlan": 72},
                admission_state="admitted",
                http_status=202,
                response={
                    "device_id": device_id,
                    "outcome": "promoted",
                    "selected": {"vlan": 72},
                    "skipped": {},
                    "skipped_detail": None,
                    "generations": [{"generation_id": 999999}],
                },
            )
        )
        await db.commit()

    with capture_logs() as logs:
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/deployment-evidence",
            headers=AUTH,
            json={"apply_attempt_ids": [str(attempt_id)]},
        )

    assert response.status_code == 500
    assert response.json() == {"error": {"code": "internal", "message": "Internal server error", "detail": {}}}
    invariant = next(log for log in logs if log["event"] == "deployment_evidence.invariant_violation")
    assert invariant["apply_attempt_id"] == attempt_id
    assert invariant["response_generation_ids"] == [999999]
    assert invariant["stamped_generation_ids"] == []
    assert invariant["log_level"] == "error"


async def test_evidence_deduplicates_attempt_ids_before_applying_the_request_bound(adapter_client):
    from nso_adapter.store.apply_attempt_store import begin_apply_attempt, complete_apply_attempt

    device_id = await seed_device(nso_device_name="evidence-duplicate-attempts", netbox_device_id=16236)
    attempt_id = uuid.uuid4()
    unknown_id = uuid.uuid4()
    replay = {"generations": None}
    async with session() as db:
        assert await begin_apply_attempt(db, attempt_id, device_id, {}) is None
        await complete_apply_attempt(
            db,
            attempt_id,
            admission_state="rejected",
            http_status=409,
            response=replay,
        )
        await db.commit()

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id), str(unknown_id)] * 51},
    )

    assert response.status_code == 200, response.text
    assert [attempt["apply_attempt_id"] for attempt in response.json()["attempts"]] == [str(attempt_id)]
    assert response.json()["unknown_apply_attempt_ids"] == [str(unknown_id)]


async def test_evidence_rejects_more_than_100_attempt_ids(adapter_client):
    response = await adapter_client.post(
        "/api/v1/devices/999999/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(uuid.uuid4()) for _ in range(101)]},
    )

    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_error"
    assert error["message"] == "Request validation failed"
    assert any(item["loc"] == ["body", "apply_attempt_ids"] for item in error["detail"]["errors"])


async def test_evidence_returns_the_house_not_found_envelope(adapter_client):
    response = await adapter_client.post(
        "/api/v1/devices/999999/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": []},
    )

    assert response.status_code == 404
    assert response.json() == {"error": {"code": "not_found", "message": "Device not found", "detail": {}}}


async def test_evidence_requires_authentication(adapter_client):
    response = await adapter_client.post(
        "/api/v1/devices/999999/deployment-evidence",
        json={"apply_attempt_ids": []},
    )

    assert response.status_code == 401
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing or invalid bearer token",
            "detail": {},
        }
    }


def test_evidence_barrier_state_names_are_public():
    from nso_adapter.core.generation import CROSSABLE_STATUSES, DEVICE_WRITING_JOB_TYPES, LIVE_JOB_STATUSES
    from nso_adapter.store.models import GenerationStatus, JobStatus, JobType

    assert CROSSABLE_STATUSES == (GenerationStatus.settled, GenerationStatus.abandoned)
    assert DEVICE_WRITING_JOB_TYPES == (JobType.apply, JobType.removal)
    assert LIVE_JOB_STATUSES == (JobStatus.queued, JobStatus.running)


def test_evidence_openapi_pins_the_bound_and_non_actionable_contract():
    from nso_adapter.main import create_app

    schema = create_app().openapi()
    operation = schema["paths"]["/api/v1/devices/{device_id}/deployment-evidence"]["post"]
    request_ref = operation["requestBody"]["content"]["application/json"]["schema"]["$ref"]
    request_name = request_ref.rsplit("/", maxsplit=1)[1]

    assert schema["components"]["schemas"][request_name]["properties"]["apply_attempt_ids"]["maxItems"] == 100
    assert set(operation["responses"]) >= {"200", "401", "404", "422", "500"}
    assert operation["responses"]["500"] == {
        "description": "Internal adapter invariant failed",
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/ErrorEnvelope"},
            }
        },
    }
    assert "NON-ACTIONABLE" in operation["description"]
    assert "corrupt" in operation["description"].lower()
    assert "deterministic rejection" in operation["description"].lower()
    assert "null" in operation["description"].lower()
    for component in ("DeploymentEvidenceGenerationOut", "DeploymentEvidenceHeadOut"):
        assert schema["components"]["schemas"][component]["properties"]["source_push_seq"]["description"] == (
            "Plugin X-Push-Seq keyed by intent stream."
        )


def test_api_contract_documents_unknown_attempts_and_retention():
    from tests.api.gen_openapi import SNAPSHOT_PATH

    contract = (SNAPSHOT_PATH.parents[2] / "docs" / "api-contract.md").read_text()
    section = contract.split(
        "### `POST /api/v1/devices/{id}/deployment-evidence`",
        maxsplit=1,
    )[1].split("\n### ", maxsplit=1)[0]

    assert "non-actionable" in section.lower()
    assert "corrupt" in section.lower()
    assert "500 internal" in section.lower()
    assert "referenced by a generation is never deleted" in section
    assert "unique document section names" in section
    assert "keyed by intent stream" in section
    assert "deterministic rejection" in section.lower()
    assert "omits `generations` or stores it as `null`" in section
