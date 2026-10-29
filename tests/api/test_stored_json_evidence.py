# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Stored JSON retains exact value identity through PostgreSQL and HTTP."""

import json
from contextlib import asynccontextmanager

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from nso_adapter.main import create_app
from nso_adapter.store.db import get_engine, get_session
from nso_adapter.store.models import Job, JobStatus, JobType
from tests.api.test_settlement_observation import _seed_observation
from tests.conftest import AUTH, VALID_TOKEN, empty_execution_result, seed_device, session

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def terminal_execution_result_contract():
    """These fixtures store invalid evidence to exercise item containment."""
    yield


@asynccontextmanager
async def _reader_client(*, default_decoder=False):
    app = create_app()
    app.state.adapter_token = VALID_TOKEN
    engine = None
    if default_decoder:
        # The pre-branch store used SQLAlchemy's default JSON decoder.
        engine = create_async_engine(get_engine().url, json_deserializer=json.loads)

        async def read_session():
            async with AsyncSession(engine) as db:
                yield db

        app.dependency_overrides[get_session] = read_session
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test"
        ) as client:
            yield client
    finally:
        if engine is not None:
            await engine.dispose()


async def _observe(client, device_id, job_id, attempt_id):
    response = await client.post(
        f"/api/v1/devices/{device_id}/settlement-observation",
        headers=AUTH,
        json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": [str(attempt_id)]},
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_integer_beyond_python_digit_limit_preserves_feed_siblings_and_observation(adapter_client):
    device_id = await seed_device(nso_device_name="integer-evidence", netbox_device_id=18525)
    async with session() as db:
        jobs = [
            Job(
                device_id=device_id,
                job_type=JobType.apply,
                status=JobStatus.queued,
                coalescible=False,
                settle_seq=seq,
                result=empty_execution_result(),
            )
            for seq in range(1, 4)
        ]
        db.add_all(jobs)
        await db.commit()
        ids = [job.id for job in jobs]
        await db.execute(
            text(
                "UPDATE jobs SET status = 'failed', result = jsonb_set(result::jsonb, '{unbounded}', CAST(:number AS jsonb)) WHERE id = :id"
            ),
            {"number": "1" + "0" * 4300, "id": ids[1]},
        )
        await db.execute(text("UPDATE jobs SET status = 'failed' WHERE device_id = :id"), {"id": device_id})
        await db.commit()
    async with _reader_client() as client:
        response = await client.get("/api/v1/jobs", headers=AUTH, params={"device_id": device_id, "order": "asc"})
        assert response.status_code == 200, response.text
        a, b, c = response.json()
        assert [item["identity"]["id"] for item in (a, b, c)] == ids
        assert a["evidence_error"] is c["evidence_error"] is None
        assert b["projection"] is None
        assert b["evidence_error"] == {"code": "projection_construction", "digest": b["digest"]}
        observation = await client.post(
            f"/api/v1/devices/{device_id}/settlement-observation",
            headers=AUTH,
            json={"job_id": ids[1], "after_settle_seq": 0, "apply_attempt_ids": []},
        )
        assert observation.status_code == 200, observation.text
        assert observation.json()["job"] == b
        assert observation.json()["next_feed"] == a["identity"]


@pytest.mark.parametrize(
    "before,after,equal",
    [
        ("1e-400", "2e-400", False),
        ("1.00000000000000001", "1.00000000000000002", False),
        ("0.1", "0.10000000000000001", False),
        ("1.0", "1.00", True),
        ("1", "1.00", True),
        ("0", "-0.0", True),
        ("1e-400", "10e-401", True),
        ("1.00000000000000001", "1.000000000000000010", True),
        ("1e400", "10e399", True),
        ("true", "1", False),
        ('{"$json_number":"1"}', '{"$$json_number":"1"}', False),
        ('{"accepted_at":"2026-10-01T12:00:00+02:00"}', '{"accepted_at":"2026-10-01T10:00:00Z"}', False),
        ('{"generation_ids":[2,1]}', '{"generation_ids":[1,2]}', False),
    ],
)
async def test_receipt_and_observation_digests_compare_exact_json_values(adapter_client, before, after, equal):
    device_id, job_id, attempt_id, _ = await _seed_observation()
    observations = []
    for literal in (before, after):
        async with session() as db:
            await db.execute(
                text(
                    "UPDATE intent_push_receipt SET response = jsonb_set(response::jsonb, '{value}', "
                    "CAST(:literal AS jsonb)) WHERE device_id = :id"
                ),
                {"literal": literal, "id": device_id},
            )
            await db.commit()
        observations.append(await _observe(adapter_client, device_id, job_id, attempt_id))
    for extract in (lambda body: body["receipts"][0]["digest"], lambda body: body["digest"]):
        assert (extract(observations[0]) == extract(observations[1])) is equal


async def test_marker_shaped_stored_object_cannot_imitate_number_in_invalid_attempt(adapter_client):
    device_id, job_id, attempt_id, _ = await _seed_observation()
    number = "1" + "0" * 400 + ".1"
    observations = []
    for literal in (number, json.dumps({"$json_number": number})):
        async with session() as db:
            await db.execute(
                text(
                    "UPDATE deployment_apply_attempt SET response = jsonb_build_object("
                    "'generations', jsonb_build_array(jsonb_build_object('generation_id', 0)), "
                    "'value', CAST(:literal AS jsonb)) WHERE id = :id"
                ),
                {"literal": literal, "id": attempt_id},
            )
            await db.commit()
        observations.append(await _observe(adapter_client, device_id, job_id, attempt_id))
    for body in observations:
        attempt = body["deployment"]["attempts"][0]
        assert attempt["projection"] is None
        assert attempt["evidence_error"] is not None
    assert (
        observations[0]["deployment"]["attempts"][0]["digest"] != observations[1]["deployment"]["attempts"][0]["digest"]
    )
    assert observations[0]["digest"] != observations[1]["digest"]


@pytest.mark.parametrize("default_decoder", [True, False], ids=["pre_branch_decoder", "exact_decoder"])
@pytest.mark.parametrize("literal", ["1" + "0" * 4300, "1" + "0" * 400 + ".1"], ids=["integer", "decimal"])
async def test_receipt_reader_rejects_unrepresentable_stored_number(adapter_client, default_decoder, literal):
    device_id, job_id, attempt_id, _ = await _seed_observation()
    async with session() as db:
        await db.execute(
            text(
                "UPDATE intent_push_receipt SET response = jsonb_set(response::jsonb, '{value}', "
                "CAST(:literal AS jsonb)) WHERE device_id = :id"
            ),
            {"literal": literal, "id": device_id},
        )
        await db.commit()
    async with _reader_client(default_decoder=default_decoder) as client:
        response = await client.get("/api/v1/intent-receipts", headers=AUTH, params={"device_id": device_id})
    if default_decoder and "." in literal:
        assert response.status_code == 200, response.text
        assert response.json()["receipts"][0]["response"]["value"] is None
    else:
        assert response.status_code == 500, response.text
        assert response.json()["error"]["code"] == "internal"
    if not default_decoder:
        body = await _observe(adapter_client, device_id, job_id, attempt_id)
        receipt = body["receipts"][0]
        assert receipt["projection"] is None
        assert receipt["evidence_error"] == {"code": "projection_construction", "digest": receipt["digest"]}


@pytest.mark.parametrize(
    "before,after",
    [
        ("0e9999999999999999999", "0e8888888888888888888"),
        ("1e9999999999999999999", "2e9999999999999999999"),
        ("1e-9999999999999999999", "2e-9999999999999999999"),
        ("1e9999999999999999999", "10e9999999999999999998"),
        ("1e-9999999999999999999", "10e-10000000000000000000"),
        ("1e" + "1" + "0" * 4300, "10e" + "9" * 4300),
    ],
    ids=["zero", "huge_distinct", "tiny_distinct", "huge_equal", "tiny_equal", "long_exponent_equal"],
)
async def test_postgresql_json_exponents_preserve_exact_item_and_observation_digests(adapter_client, before, after):
    device_id, job_id, attempt_id, generation_id = await _seed_observation()
    observations = []
    for literal in (before, after):
        async with session() as db:
            await db.execute(
                text("UPDATE intent_push_receipt SET response = CAST(:payload AS json) WHERE device_id = :id"),
                {"payload": '{"generation_id":' + str(generation_id) + ',"value":' + literal + "}", "id": device_id},
            )
            await db.commit()
        body = await _observe(adapter_client, device_id, job_id, attempt_id)
        receipt = body["receipts"][0]
        assert receipt["projection"] is None
        assert receipt["evidence_error"] == {"code": "projection_construction", "digest": receipt["digest"]}
        assert body["generations"][0]["evidence_error"] is None
        observations.append(body)
    equal = not before.startswith("1e") or after.startswith("10e")
    assert (observations[0]["receipts"][0]["digest"] == observations[1]["receipts"][0]["digest"]) is equal
    assert (observations[0]["digest"] == observations[1]["digest"]) is equal
