# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Malformed stored evidence stays attributable without hiding valid siblings."""

import hashlib
import json

import pytest

from nso_adapter.store.models import Job, JobStatus, JobType
from tests.conftest import AUTH, empty_execution_result, seed_device, session

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def terminal_execution_result_contract():
    """These fixtures deliberately retain corrupt evidence to test the read boundary."""
    yield


@pytest.mark.parametrize(
    "bad_result,code",
    [
        ({**empty_execution_result(), "row_outcomes": [{}]}, "schema_validation"),
        ([], "projection_construction"),
        ({**empty_execution_result(), "commit_acknowledged": "false"}, "execution_contract"),
    ],
)
async def test_feed_isolates_invalid_evidence_between_valid_siblings(adapter_client, bad_result, code):
    device_id = await seed_device(nso_device_name="envelope-feed", netbox_device_id=18510)
    async with session() as db:
        rows = [
            Job(
                device_id=device_id,
                job_type=JobType.apply,
                status=JobStatus.failed,
                coalescible=True,
                settle_seq=seq,
                result=result,
            )
            for seq, result in enumerate([empty_execution_result(), bad_result, empty_execution_result()], 1)
        ]
        db.add_all(rows)
        await db.commit()
        ids = [row.id for row in rows]
    response = await adapter_client.get("/api/v1/jobs", headers=AUTH, params={"device_id": device_id, "order": "asc"})
    assert response.status_code == 200, response.text
    a, b, c = response.json()
    assert [item["identity"]["id"] for item in (a, b, c)] == ids
    assert a["evidence_error"] is c["evidence_error"] is None
    assert a["projection"]["result"] == c["projection"]["result"] == empty_execution_result()
    assert b["identity"] == {"id": ids[1], "device_id": device_id, "settle_seq": 2}
    assert b["projection"] is None
    assert b["evidence_error"] == {"code": code, "digest": b["digest"]}
    assert len(b["digest"]) == 64
    assert (
        a["digest"]
        == hashlib.sha256(
            json.dumps(
                a["projection"], sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            ).encode()
        ).hexdigest()
    )

    observation = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": ids[1], "after_settle_seq": 0, "apply_attempt_ids": []},
    )
    assert observation.status_code == 200, observation.text
    assert observation.json()["job"] == b
    assert observation.json()["next_feed"] == a["identity"]


@pytest.mark.parametrize(
    "failure,code",
    [
        ("row", "schema_validation"),
        ("non_object", "projection_construction"),
        ("stream", "projection_construction"),
        ("schema", "schema_validation"),
        ("contract", "execution_contract"),
    ],
)
async def test_nested_generation_and_head_errors_leave_sibling_generations_visible(adapter_client, failure, code):
    import uuid

    from nso_adapter.store.models import DeploymentApplyAttempt, GenerationStatus
    from tests.api.test_deployment_evidence import _generation

    device_id = await seed_device(nso_device_name="envelope-generations", netbox_device_id=18511)
    attempt_id = uuid.uuid4()
    async with session() as db:
        attempt = DeploymentApplyAttempt(
            id=attempt_id,
            device_id=device_id,
            selected={"vlan": 1},
            admission_state="admitted",
            http_status=202,
            response={},
        )
        db.add(attempt)
        await db.flush()
        rows = [
            _generation(
                device_id,
                seq,
                GenerationStatus.settled if seq == 1 else GenerationStatus.failed,
                apply_attempt_id=attempt_id,
            )
            for seq in range(1, 4)
        ]
        for row in rows:
            row.carrier_job_result = empty_execution_result()
        bad = rows[1]
        if failure == "row":
            bad.carrier_job_result = {**empty_execution_result(), "row_outcomes": [{}]}
        elif failure == "non_object":
            bad.carrier_job_result = []
        elif failure == "stream":
            bad.stream_revisions = {"unknown_stream": 1}
        elif failure == "schema":
            bad.source_push_seq = {"vlan": "bad"}
        else:
            bad.carrier_job_result = {**empty_execution_result(), "commit_acknowledged": "false"}
        db.add_all(rows)
        await db.flush()
        ids = [row.id for row in rows]
        attempt.response = {"generations": [{"generation_id": value} for value in ids]}
        await db.commit()
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence", headers=AUTH, json={"apply_attempt_ids": [str(attempt_id)]}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    a, b, c = body["attempts"][0]["generations"]
    assert [item["identity"]["generation_id"] for item in (a, b, c)] == ids
    assert a["evidence_error"] is c["evidence_error"] is None
    assert a["projection"] is not None and c["projection"] is not None
    for item in (b, body["head"]):
        assert item["identity"]["generation_id"] == ids[1]
        assert item["projection"] is None
        assert item["evidence_error"] == {"code": code, "digest": item["digest"]}


@pytest.mark.parametrize(
    "bad_response,code",
    [
        ({"generations": [{"generation_id": 999999}]}, "attempt_generation_membership"),
        ([], "projection_construction"),
    ],
)
async def test_attempt_admission_error_keeps_siblings_and_its_generations(adapter_client, bad_response, code):
    import uuid

    from nso_adapter.store.models import DeploymentApplyAttempt, GenerationStatus
    from tests.api.test_deployment_evidence import _generation

    device_id = await seed_device(nso_device_name="envelope-attempts", netbox_device_id=18512)
    ids = [uuid.uuid4() for _ in range(3)]
    async with session() as db:
        db.add_all(
            DeploymentApplyAttempt(
                id=value,
                device_id=device_id,
                selected={},
                admission_state="admitted",
                http_status=202,
                response=bad_response if index == 1 else {},
            )
            for index, value in enumerate(ids)
        )
        await db.flush()
        generation = _generation(device_id, 1, GenerationStatus.pending, apply_attempt_id=ids[1])
        db.add(generation)
        await db.commit()
        generation_id = generation.id
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(value) for value in ids]},
    )
    assert response.status_code == 200, response.text
    a, b, c = response.json()["attempts"]
    assert [item["identity"]["apply_attempt_id"] for item in (a, b, c)] == list(map(str, ids))
    assert a["evidence_error"] is c["evidence_error"] is None
    assert b["projection"] is None
    assert b["evidence_error"] == {"code": code, "digest": b["digest"]}
    assert b["generations"][0]["identity"]["generation_id"] == generation_id
    assert b["generations"][0]["evidence_error"] is None


async def test_generation_listing_isolates_invalid_metadata(adapter_client):
    from nso_adapter.store.models import GenerationStatus
    from tests.api.test_deployment_evidence import _generation

    device_id = await seed_device(nso_device_name="envelope-listing", netbox_device_id=18513)
    async with session() as db:
        rows = [_generation(device_id, seq, GenerationStatus.pending) for seq in range(1, 4)]
        rows[1].source_push_seq = {"vlan": "bad"}
        db.add_all(rows)
        await db.commit()
        ids = [row.id for row in rows]
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/generations", headers=AUTH)
    assert response.status_code == 200, response.text
    a, b, c = response.json()
    assert [item["identity"]["generation_id"] for item in (a, b, c)] == ids
    assert a["evidence_error"] is c["evidence_error"] is None
    assert b["projection"] is None
    assert b["evidence_error"] == {"code": "schema_validation", "digest": b["digest"]}


@pytest.mark.parametrize(
    "before,after,equal",
    [
        ({"accepted_at": "2026-10-01T12:00:00+02:00"}, {"accepted_at": "2026-10-01T10:00:00Z"}, True),
        ({"acceptance_identity": "2026-10-01T12:00:00+02:00"}, {"acceptance_identity": "2026-10-01T10:00:00Z"}, True),
        ({"generation_ids": [2, 1]}, {"generation_ids": [1, 2]}, True),
        ({"ordered": [2, 1]}, {"ordered": [1, 2]}, False),
        ({"nested": {}}, {"nested": {"missing": None}}, False),
        ({"typed": True}, {"typed": 1}, False),
    ],
)
async def test_evidence_digest_preserves_types_absence_and_meaningful_order(adapter_client, before, after, equal):
    from datetime import UTC, datetime

    stamp = datetime(2026, 10, 1, tzinfo=UTC)
    device_id = await seed_device(nso_device_name="digest-device", netbox_device_id=18516)
    async with session() as db:
        row = Job(
            device_id=device_id,
            job_type=JobType.sync,
            status=JobStatus.queued,
            coalescible=True,
            result=before,
            updated_at=stamp,
        )
        db.add(row)
        await db.commit()
        job_id = row.id
    first = await adapter_client.get(f"/api/v1/jobs/{job_id}", headers=AUTH)
    async with session() as db:
        from sqlalchemy import update

        await db.execute(update(Job).where(Job.id == job_id).values(result=after, updated_at=stamp))
        await db.commit()
    second = await adapter_client.get(f"/api/v1/jobs/{job_id}", headers=AUTH)
    assert first.status_code == second.status_code == 200
    assert first.json()["evidence_error"] is second.json()["evidence_error"] is None
    assert first.json()["projection"]["result"] == before
    assert second.json()["projection"]["result"] == after
    assert (first.json()["digest"] == second.json()["digest"]) is equal
