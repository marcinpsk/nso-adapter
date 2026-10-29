# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Malformed stored evidence stays attributable without hiding valid siblings."""

import pytest

from nso_adapter.store.models import Job, JobStatus, JobType
from tests.api.evidence import empty_vlan_execution_result
from tests.conftest import AUTH, empty_execution_result, seed_device, session

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def terminal_execution_result_contract():
    """These fixtures deliberately retain corrupt evidence to test the read boundary."""
    yield


@pytest.mark.parametrize(
    "field,bad_key",
    [
        ("source_push_seq", "\ud800"),
        ("stream_revisions", "\ud800"),
        ("source_push_seq", "unknown_section"),
        ("stream_revisions", "unknown_section"),
        ("selected", "unknown_section"),
    ],
)
async def test_observation_contains_invalid_section_keys_and_retains_sibling_evidence(adapter_client, field, bad_key):
    import uuid

    from nso_adapter.store.models import DeploymentApplyAttempt, GenerationStatus, IntentPushReceipt
    from tests.api.test_deployment_evidence import _generation

    device_id = await seed_device(nso_device_name="invalid-section-evidence", netbox_device_id=18523)
    attempt_id = uuid.uuid4()
    mapping = {"vlan": 1, "snmp": 2, bad_key: 1}
    async with session() as db:
        job = Job(device_id=device_id, job_type=JobType.sync, status=JobStatus.queued, coalescible=True)
        attempt = DeploymentApplyAttempt(
            id=attempt_id,
            device_id=device_id,
            selected=mapping if field == "selected" else {"vlan": 1, "snmp": 2},
            admission_state="admitted",
            http_status=202,
            response={},
        )
        db.add_all([job, attempt])
        await db.flush()
        rows = [
            _generation(device_id, seq, GenerationStatus.pending, job_id=job.id, apply_attempt_id=attempt_id)
            for seq in (1, 2)
        ]
        if field != "selected":
            setattr(rows[0], field, mapping)
        db.add_all(rows)
        await db.flush()
        generation_id, sibling_id = [row.id for row in rows]
        attempt.response = {"generations": [{"generation_id": row.id} for row in rows]}
        db.add(
            IntentPushReceipt(
                device_id=device_id,
                section="vlan",
                push_seq=1,
                request_digest="a" * 64,
                response={"generation_id": generation_id},
                generation_id=generation_id,
            )
        )
        await db.commit()
        job_id = job.id
    listing = await adapter_client.get(f"/api/v1/devices/{device_id}/generations", headers=AUTH)
    assert listing.status_code == 200, listing.text
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": [str(attempt_id)]},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["generations"] == listing.json()
    assert [item["identity"]["generation_id"] for item in body["generations"]] == [generation_id, sibling_id]
    assert body["generations"][1]["evidence_error"] is None
    assert body["receipts"][0]["projection"]["response"] == {"generation_id": generation_id}
    assert body["absent"]["receipt_sections"] == ["snmp"]
    if bad_key == "\ud800":
        item = body["deployment"]["attempts"][0] if field == "selected" else body["generations"][0]
        assert item["projection"] is None
        assert item["evidence_error"]["code"] in {"schema_validation", "projection_construction"}


@pytest.mark.parametrize("endpoint", ["feed", "listing", "deployment", "observation"])
@pytest.mark.parametrize("stamp", ["0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00"])
async def test_unnormalizable_evidence_stays_inside_its_item(adapter_client, endpoint, stamp):
    import uuid

    from nso_adapter.store.models import DeploymentApplyAttempt, GenerationStatus
    from tests.api.test_deployment_evidence import _generation

    device_id = await seed_device(nso_device_name="unnormalizable-evidence", netbox_device_id=18518)
    attempt_id = uuid.uuid4()
    bad_result = {**empty_execution_result(), "row_outcomes": [{}], "accepted_at": stamp}
    async with session() as db:
        jobs = [
            Job(
                device_id=device_id,
                job_type=JobType.apply,
                status=JobStatus.failed,
                coalescible=True,
                settle_seq=seq,
                result=bad_result if seq == 2 else empty_execution_result(),
            )
            for seq in range(1, 4)
        ]
        attempt = DeploymentApplyAttempt(
            id=attempt_id, device_id=device_id, selected={}, admission_state="admitted", http_status=202, response={}
        )
        db.add_all([*jobs, attempt])
        await db.flush()
        generations = [
            _generation(device_id, seq, GenerationStatus.pending, apply_attempt_id=attempt_id, job_id=jobs[seq - 1].id)
            for seq in range(1, 4)
        ]
        for generation in generations:
            generation.carrier_job_result = bad_result if generation.seq == 2 else empty_vlan_execution_result()
        if endpoint == "listing":
            generations[1].source_push_seq = {"accepted_at": stamp}
        db.add_all(generations)
        await db.flush()
        attempt.response = {"generations": [{"generation_id": row.id} for row in generations]}
        await db.commit()
        job_id = jobs[1].id
        generation_ids = [row.id for row in generations]

    async def read():
        if endpoint == "feed":
            return await adapter_client.get(
                "/api/v1/jobs", headers=AUTH, params={"device_id": device_id, "order": "asc"}
            )
        if endpoint == "listing":
            return await adapter_client.get(f"/api/v1/devices/{device_id}/generations", headers=AUTH)
        if endpoint == "deployment":
            return await adapter_client.post(
                f"/api/v1/devices/{device_id}/deployment-evidence",
                headers=AUTH,
                json={"apply_attempt_ids": [str(attempt_id)]},
            )
        return await adapter_client.post(
            f"/api/v1/devices/{device_id}/settlement-observation",
            headers=AUTH,
            json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": [str(attempt_id)]},
        )

    first, second = await read(), await read()
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    body = first.json()
    if endpoint in ("feed", "listing"):
        a, b, c = body
    else:
        deployment = body if endpoint == "deployment" else body["deployment"]
        a, b, c = deployment["attempts"][0]["generations"]
        assert [item["identity"]["generation_id"] for item in (a, b, c)] == generation_ids
        if endpoint == "observation":
            assert body["job"]["identity"]["id"] == job_id
            assert body["job"]["projection"] is None
            assert body["job"]["evidence_error"]["code"] == "schema_validation"
    assert a["evidence_error"] is c["evidence_error"] is None
    assert a["projection"] is not None and c["projection"] is not None
    assert b["projection"] is None
    assert b["evidence_error"] == {"code": "schema_validation", "digest": b["digest"]}


@pytest.mark.parametrize("endpoint", ["listing", "deployment", "observation"])
@pytest.mark.parametrize("failure", ["push_seq", "revision", "carrier", "sections"])
async def test_generation_readers_apply_the_complete_execution_contract(adapter_client, endpoint, failure):
    import uuid

    from nso_adapter.store.models import DeploymentApplyAttempt, GenerationStatus
    from tests.api.test_deployment_evidence import _generation

    device_id = await seed_device(nso_device_name="generation-contract", netbox_device_id=18519)
    attempt_id = uuid.uuid4()
    async with session() as db:
        attempt = DeploymentApplyAttempt(
            id=attempt_id, device_id=device_id, selected={}, admission_state="admitted", http_status=202, response={}
        )
        db.add(attempt)
        await db.flush()
        rows = [
            _generation(device_id, seq, GenerationStatus.pending, apply_attempt_id=attempt_id) for seq in range(1, 4)
        ]
        bad = rows[1]
        if failure == "push_seq":
            bad.source_push_seq = {"vlan": -1}
        elif failure == "revision":
            bad.stream_revisions = {"vlan": 0}
        elif failure == "carrier":
            bad.carrier_job_status = "succeeded"
        else:
            bad.carrier_job_result = empty_execution_result()
        db.add_all(rows)
        await db.flush()
        ids = [row.id for row in rows]
        attempt.response = {"generations": [{"generation_id": value} for value in ids]}
        await db.commit()

    if endpoint == "listing":
        response = await adapter_client.get(f"/api/v1/devices/{device_id}/generations", headers=AUTH)
    else:
        body = {"apply_attempt_ids": [str(attempt_id)]}
        if endpoint == "observation":
            body.update(job_id=1, after_settle_seq=0)
        route = "deployment-evidence" if endpoint == "deployment" else "settlement-observation"
        response = await adapter_client.post(f"/api/v1/devices/{device_id}/{route}", headers=AUTH, json=body)
    assert response.status_code == 200, response.text
    body = response.json()
    if endpoint == "listing":
        items = body
    else:
        deployment = body if endpoint == "deployment" else body["deployment"]
        items = deployment["attempts"][0]["generations"]
        if endpoint == "observation":
            assert body["generations"][1]["evidence_error"] is not None
            assert body["generations"][1]["evidence_error"]["code"] == "execution_contract"
    a, b, c = items
    assert [item["identity"]["generation_id"] for item in items] == ids
    assert a["evidence_error"] is c["evidence_error"] is None
    assert b["projection"] is None
    assert b["evidence_error"] == {"code": "execution_contract", "digest": b["digest"]}


@pytest.mark.parametrize("failure", ["push_seq", "revision", "carrier", "sections"])
async def test_generation_head_applies_the_complete_execution_contract(adapter_client, failure):
    from nso_adapter.store.models import GenerationStatus
    from tests.api.test_deployment_evidence import _generation

    device_id = await seed_device(nso_device_name="head-contract", netbox_device_id=18520)
    async with session() as db:
        row = _generation(device_id, 1, GenerationStatus.pending)
        if failure == "push_seq":
            row.source_push_seq = {"vlan": -1}
        elif failure == "revision":
            row.stream_revisions = {"vlan": 0}
        elif failure == "carrier":
            row.carrier_job_status = "succeeded"
        else:
            row.carrier_job_result = empty_execution_result()
        db.add(row)
        await db.commit()
        generation_id = row.id
    for route, extra in [("deployment-evidence", {}), ("settlement-observation", {"job_id": 1, "after_settle_seq": 0})]:
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/{route}", headers=AUTH, json={"apply_attempt_ids": [], **extra}
        )
        assert response.status_code == 200, response.text
        body = response.json()
        head = body["head"] if route == "deployment-evidence" else body["deployment"]["head"]
        assert head["identity"]["generation_id"] == generation_id
        assert head["projection"] is None
        assert head["evidence_error"] == {"code": "execution_contract", "digest": head["digest"]}


async def test_large_stored_json_numbers_keep_distinct_digests(adapter_client):
    from sqlalchemy import text

    device_id = await seed_device(nso_device_name="large-number-evidence", netbox_device_id=18521)
    async with session() as db:
        row = Job(
            device_id=device_id,
            job_type=JobType.apply,
            status=JobStatus.queued,
            coalescible=True,
            settle_seq=1,
            result={**empty_execution_result(), "row_outcomes": [{}]},
        )
        db.add(row)
        await db.commit()
        job_id = row.id
    digests = []
    for leading in ("1", "2"):
        number = leading + "0" * 400 + ".1"
        async with session() as db:
            await db.execute(
                text(
                    "UPDATE jobs SET result = jsonb_set(result::jsonb, '{unbounded}', CAST(:number AS jsonb)) WHERE id = :id"
                ),
                {"number": number, "id": job_id},
            )
            await db.commit()
        response = await adapter_client.get("/api/v1/jobs", headers=AUTH, params={"device_id": device_id})
        assert response.status_code == 200, response.text
        item = response.json()[0]
        assert item["projection"] is None
        assert item["evidence_error"]["code"] == "projection_construction"
        observation = await adapter_client.post(
            f"/api/v1/devices/{device_id}/settlement-observation",
            headers=AUTH,
            json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": []},
        )
        assert observation.status_code == 200, observation.text
        assert observation.json()["job"] == item
        digests.append(item["digest"])
    assert digests[0] != digests[1]


@pytest.mark.parametrize("surface", ["receipt", "attempt"])
async def test_distinct_positive_json_numbers_keep_item_and_observation_digests(adapter_client, surface):
    from sqlalchemy import text

    from tests.api.test_settlement_observation import _seed_observation

    device_id, job_id, attempt_id, generation_id = await _seed_observation()
    table, column, identity_column, identity = {
        "receipt": ("intent_push_receipt", "response", "device_id", device_id),
        "attempt": ("deployment_apply_attempt", "response", "id", attempt_id),
    }[surface]
    item_digests = []
    observation_digests = []
    for leading in ("1", "2"):
        number = leading + "0" * 400 + ".1"
        async with session() as db:
            await db.execute(
                text(
                    f"UPDATE {table} SET {column} = jsonb_set(CAST({column} AS jsonb), "
                    f"'{{unbounded}}', CAST(:number AS jsonb)) WHERE {identity_column} = :id"
                ),
                {"number": number, "id": identity},
            )
            await db.commit()
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/settlement-observation",
            headers=AUTH,
            json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": [str(attempt_id)]},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        item = body["receipts"][0] if surface == "receipt" else body["deployment"]["attempts"][0]
        assert item["projection"] is None
        assert item["evidence_error"]["code"] in {"schema_validation", "projection_construction"}
        assert body["generations"][0]["identity"]["generation_id"] == generation_id
        assert body["generations"][0]["evidence_error"] is None
        item_digests.append(item["digest"])
        observation_digests.append(body["digest"])
    assert item_digests[0] != item_digests[1]
    assert observation_digests[0] != observation_digests[1]


@pytest.mark.parametrize("value", ["\ud800", "\udfff"])
async def test_invalid_unicode_evidence_stays_attributable(adapter_client, value):
    device_id = await seed_device(nso_device_name="unicode-evidence", netbox_device_id=18522)
    async with session() as db:
        row = Job(
            device_id=device_id,
            job_type=JobType.apply,
            status=JobStatus.queued,
            coalescible=True,
            result={**empty_execution_result(), "row_outcomes": [{}], "unpaired": value},
        )
        db.add(row)
        await db.commit()
        job_id = row.id
    response = await adapter_client.get("/api/v1/jobs", headers=AUTH, params={"device_id": device_id})
    assert response.status_code == 200, response.text
    item = response.json()[0]
    assert item["identity"]["id"] == job_id
    assert item["projection"] is None
    assert item["evidence_error"] == {"code": "schema_validation", "digest": item["digest"]}
    observation = await adapter_client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": []},
    )
    assert observation.status_code == 200, observation.text
    assert observation.json()["job"] == item


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
    from nso_adapter.api.execution import evidence_digest

    assert a["digest"] == evidence_digest(a["projection"])

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
            row.carrier_job_id = row.seq
            row.carrier_job_status = "succeeded" if row.seq == 1 else "failed"
            row.carrier_job_result = empty_vlan_execution_result()
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
        ({"accepted_at": "2026-10-01T12:00:00+02:00"}, {"accepted_at": "2026-10-01T10:00:00Z"}, False),
        ({"acceptance_identity": "2026-10-01T12:00:00+02:00"}, {"acceptance_identity": "2026-10-01T10:00:00Z"}, False),
        ({"generation_ids": [2, 1]}, {"generation_ids": [1, 2]}, False),
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
