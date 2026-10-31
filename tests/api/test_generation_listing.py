# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba
"""Generation listings validate and digest only their public fields."""

import pytest
from sqlalchemy import event, select, text, update

from nso_adapter.store.db import get_engine
from nso_adapter.store.json_codec import UnrepresentableJSONDocument, UnrepresentableJSONNumber
from nso_adapter.store.models import DeploymentGeneration, GenerationStatus
from tests.api.test_deployment_evidence import _generation
from tests.api.test_settlement_observation import _seed_observation
from tests.api.test_stored_json_evidence import _observe
from tests.conftest import AUTH, session

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
async def terminal_execution_result_contract(adapter_client):
    """Allow deliberate corruption in this test's private database."""
    async with session() as db:
        await db.execute(text("ALTER TABLE deployment_generation DISABLE TRIGGER deployment_generation_immutable"))
        await db.execute(
            text("ALTER TABLE deployment_generation DISABLE TRIGGER deployment_generation_carrier_immutable")
        )
        await db.commit()
    try:
        yield
    finally:
        async with session() as db:
            await db.execute(text("ALTER TABLE deployment_generation ENABLE TRIGGER deployment_generation_immutable"))
            await db.execute(
                text("ALTER TABLE deployment_generation ENABLE TRIGGER deployment_generation_carrier_immutable")
            )
            await db.commit()


async def _listing(client, device_id):
    response = await client.get(f"/api/v1/devices/{device_id}/generations", headers=AUTH)
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.parametrize("kind", ["document", "number"])
async def test_listing_reads_undecodable_result_while_bounded_routes_reject_it(adapter_client, kind):
    device_id, job_id, attempt_id, generation_id = await _seed_observation()
    literal = (
        '{"nested":' + "[" * 257 + "0" + "]" * 257 + "}"
        if kind == "document"
        else '{"value":' + "1" + "0" * 400 + ".1}"
    )
    async with session() as db:
        await db.execute(
            text("UPDATE deployment_generation SET carrier_job_result = CAST(:literal AS json) WHERE id = :id"),
            {"literal": literal, "id": generation_id},
        )
        await db.commit()
        stored = await db.scalar(
            select(DeploymentGeneration.carrier_job_result).where(DeploymentGeneration.id == generation_id)
        )
        if kind == "document":
            assert isinstance(stored, UnrepresentableJSONDocument)
        else:
            assert isinstance(stored["value"], UnrepresentableJSONNumber)
    listing = await _listing(adapter_client, device_id)
    deployment = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id)]},
    )
    assert deployment.status_code == 200, deployment.text
    observation = await _observe(adapter_client, device_id, job_id, attempt_id)
    for item in (deployment.json()["attempts"][0]["generations"][0], observation["generations"][0]):
        assert item["projection"] is None
        assert item["evidence_error"] == {"code": "projection_construction", "digest": item["digest"]}
    feed = await adapter_client.get(f"/api/v1/jobs/{job_id}", headers=AUTH)
    assert feed.status_code == 200, feed.text
    assert feed.json()["evidence_error"] is None
    assert feed.json()["projection"] is not None
    assert listing[0]["evidence_error"] is None
    assert listing[0]["projection"]["generation_id"] == generation_id


async def test_non_head_result_change_affects_observation_but_not_listing(adapter_client):
    device_id, job_id, _, generation_id = await _seed_observation()
    async with session() as db:
        await db.execute(
            update(DeploymentGeneration)
            .where(DeploymentGeneration.id == generation_id)
            .values(
                status=GenerationStatus.settled,
                carrier_job_status="succeeded",
            )
        )
        head = _generation(device_id, 2, GenerationStatus.pending)
        db.add(head)
        await db.commit()
        head_id = head.id
    listings, observations = [], []
    for value in (0, 1):
        async with session() as db:
            await db.execute(
                text(
                    "UPDATE deployment_generation SET carrier_job_result = jsonb_set(carrier_job_result::jsonb, "
                    "'{value}', CAST(:value AS jsonb)) WHERE id = :id"
                ),
                {"value": str(value), "id": generation_id},
            )
            await db.commit()
        listings.append(await _listing(adapter_client, device_id))
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/settlement-observation",
            headers=AUTH,
            json={"job_id": job_id, "after_settle_seq": 0, "apply_attempt_ids": []},
        )
        assert response.status_code == 200, response.text
        observations.append(response.json())
    for body in observations:
        assert body["deployment"]["head"]["identity"]["generation_id"] == head_id
        assert body["deployment"]["attempts"] == []
        assert body["generations"][0]["identity"]["generation_id"] == generation_id
        assert body["generations"][0]["evidence_error"] is None
    assert observations[0]["job"] == observations[1]["job"]
    assert observations[0]["deployment"] == observations[1]["deployment"]
    assert observations[0]["generations"][0]["digest"] != observations[1]["generations"][0]["digest"]
    assert observations[0]["digest"] != observations[1]["digest"]
    assert listings[0] == listings[1]
    assert all(item["projection"] is not None for item in listings[1])


@pytest.mark.parametrize(
    "field,value,changes",
    [
        ("carrier_job_result", None, False),
        ("carrier_job_error", {"code": "stored_error"}, False),
        ("status", GenerationStatus.abandoned, True),
        ("job_id", None, True),
        ("carrier_job_id", None, True),
        ("carrier_job_status", "running", True),
        ("settlement_cohort", 12, True),
        ("digest", "b" * 64, True),
        ("stream_revisions", {"vlan": 2}, True),
        ("source_push_seq", {"vlan": 2}, True),
    ],
)
async def test_listing_digest_tracks_only_listing_fields(adapter_client, field, value, changes):
    device_id, _, _, generation_id = await _seed_observation()
    before = (await _listing(adapter_client, device_id))[0]
    async with session() as db:
        await db.execute(
            update(DeploymentGeneration)
            .where(DeploymentGeneration.id == generation_id)
            .values({field: value, "updated_at": DeploymentGeneration.updated_at})
        )
        await db.commit()
    after = (await _listing(adapter_client, device_id))[0]
    assert after["evidence_error"] is None
    assert (before["digest"] != after["digest"]) is changes
    if not changes:
        assert before["projection"] == after["projection"]


async def test_listing_rejects_settled_generation_without_succeeded_carrier(adapter_client):
    device_id, _, _, generation_id = await _seed_observation()
    async with session() as db:
        await db.execute(
            update(DeploymentGeneration)
            .where(DeploymentGeneration.id == generation_id)
            .values(status=GenerationStatus.settled)
        )
        await db.commit()
    item = (await _listing(adapter_client, device_id))[0]
    assert item["projection"] is None
    assert item["evidence_error"] == {"code": "execution_contract", "digest": item["digest"]}


async def test_listing_query_omits_carrier_result_and_error(adapter_client):
    device_id, _, _, _ = await _seed_observation()
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    engine = get_engine().sync_engine
    event.listen(engine, "before_cursor_execute", record)
    try:
        await _listing(adapter_client, device_id)
    finally:
        event.remove(engine, "before_cursor_execute", record)
    generation_queries = [statement for statement in statements if "FROM deployment_generation" in statement]
    assert generation_queries
    for statement in generation_queries:
        assert "carrier_job_result" not in statement
        assert "carrier_job_error" not in statement
