# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Device generation-chain reads through the authenticated FastAPI boundary."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

EARLY_CREATED = datetime(2026, 8, 12, 9, 15, tzinfo=UTC)
EARLY_UPDATED = datetime(2026, 8, 12, 9, 30, tzinfo=UTC)
LATE_CREATED = datetime(2026, 8, 13, 10, 45, tzinfo=UTC)
LATE_UPDATED = datetime(2026, 8, 13, 11, 0, tzinfo=UTC)


async def _seed_generation_chain(device_id: int) -> tuple[int, int, int]:
    from nso_adapter.store.models import (
        DeploymentGeneration,
        GenerationMode,
        GenerationStatus,
        Job,
        JobType,
    )

    async with session() as db:
        job = Job(job_type=JobType.apply, device_id=device_id, coalescible=True)
        db.add(job)
        await db.flush()

        late = DeploymentGeneration(
            device_id=device_id,
            seq=9,
            mode=GenerationMode.networked,
            status=GenerationStatus.pending,
            document={"vlan": {"vlan_intent": []}},
            digest="b" * 64,
            allowed_removal_keys={},
            source_push_seq={"vlan": 502},
            stream_revisions={"vlan": 12},
            settlement_cohort=None,
            job_id=None,
            created_at=LATE_CREATED,
            updated_at=LATE_UPDATED,
        )
        early = DeploymentGeneration(
            device_id=device_id,
            seq=4,
            mode=GenerationMode.detach,
            status=GenerationStatus.outcome_unknown,
            document={"vlan": {"vlan_intent": [{"vlan_id": 100}]}},
            digest="a" * 64,
            allowed_removal_keys={},
            source_push_seq={"vlan": 501},
            stream_revisions={"vlan": 11},
            settlement_cohort=73,
            job_id=job.id,
            created_at=EARLY_CREATED,
            updated_at=EARLY_UPDATED,
        )
        db.add_all([late, early])
        await db.commit()
        return early.id, late.id, job.id


async def test_generations_list_has_the_complete_emit_null_shape_in_sequence_order(adapter_client):
    device_id = await seed_device(nso_device_name="generation-list", netbox_device_id=1558)
    early_id, late_id, job_id = await _seed_generation_chain(device_id)

    response = await adapter_client.get(f"/api/v1/devices/{device_id}/generations", headers=AUTH)

    assert response.status_code == 200, response.text
    assert response.json() == [
        {
            "generation_id": early_id,
            "seq": 4,
            "status": "outcome_unknown",
            "job_id": job_id,
            "mode": "detach",
            "settlement_cohort": 73,
            "digest": "a" * 64,
            "stream_revisions": {"vlan": 11},
            "source_push_seq": {"vlan": 501},
            "created_at": "2026-08-12T09:15:00Z",
            "updated_at": "2026-08-12T09:30:00Z",
        },
        {
            "generation_id": late_id,
            "seq": 9,
            "status": "pending",
            "job_id": None,
            "mode": "networked",
            "settlement_cohort": None,
            "digest": "b" * 64,
            "stream_revisions": {"vlan": 12},
            "source_push_seq": {"vlan": 502},
            "created_at": "2026-08-13T10:45:00Z",
            "updated_at": "2026-08-13T11:00:00Z",
        },
    ]


async def test_generations_list_serves_a_null_source_push_seq_value(adapter_client):
    """A reissue-shaped map value is None by design; the listing must not 500 on it."""
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode, GenerationStatus

    device_id = await seed_device(nso_device_name="gen-list-null-seq", netbox_device_id=9931)
    async with session() as db:
        db.add(
            DeploymentGeneration(
                device_id=device_id,
                seq=1,
                mode=GenerationMode.networked,
                status=GenerationStatus.pending,
                document={"static_route": {"static_route_intent": []}},
                digest="c" * 64,
                allowed_removal_keys={},
                source_push_seq={"static_route": None},
                stream_revisions={"static_route": 3},
                settlement_cohort=None,
                job_id=None,
                created_at=EARLY_CREATED,
                updated_at=EARLY_UPDATED,
            )
        )
        await db.commit()

    response = await adapter_client.get(f"/api/v1/devices/{device_id}/generations", headers=AUTH)

    assert response.status_code == 200, response.text
    assert response.json()[0]["source_push_seq"] == {"static_route": None}


async def test_generations_list_filters_strictly_after_since_seq(adapter_client):
    device_id = await seed_device(nso_device_name="generation-increments", netbox_device_id=1559)
    _early_id, late_id, _job_id = await _seed_generation_chain(device_id)

    response = await adapter_client.get(
        f"/api/v1/devices/{device_id}/generations",
        params={"since_seq": 4},
        headers=AUTH,
    )

    assert response.status_code == 200, response.text
    assert [(row["generation_id"], row["seq"]) for row in response.json()] == [(late_id, 9)]


@pytest.mark.parametrize(
    ("since_seq", "expected_status"),
    [
        (-(2**63) - 1, 422),
        (-(2**63), 200),
        (2**63 - 1, 200),
        (2**63, 422),
    ],
)
async def test_generations_list_bounds_since_seq_to_signed_bigint(adapter_client, since_seq, expected_status):
    device_id = await seed_device(nso_device_name="generation-cursor-bounds", netbox_device_id=1561)

    response = await adapter_client.get(
        f"/api/v1/devices/{device_id}/generations",
        params={"since_seq": since_seq},
        headers=AUTH,
    )

    assert response.status_code == expected_status, response.text
    if expected_status == 422:
        error = response.json()["error"]
        assert error["code"] == "validation_error"
        assert any(item["loc"] == ["query", "since_seq"] for item in error["detail"]["errors"])


async def test_generations_list_applies_a_fail_fast_page_limit(adapter_client):
    device_id = await seed_device(nso_device_name="generation-page", netbox_device_id=1560)
    early_id, _late_id, _job_id = await _seed_generation_chain(device_id)

    page = await adapter_client.get(
        f"/api/v1/devices/{device_id}/generations",
        params={"limit": 1},
        headers=AUTH,
    )
    invalid = await adapter_client.get(
        "/api/v1/devices/999999/generations",
        params={"limit": 0},
        headers=AUTH,
    )

    assert page.status_code == 200, page.text
    assert [(row["generation_id"], row["seq"]) for row in page.json()] == [(early_id, 4)]
    assert invalid.status_code == 422
    error = invalid.json()["error"]
    assert error["code"] == "validation_error"
    assert error["message"] == "Request validation failed"
    assert any(
        item["loc"] == ["query", "limit"] and item["type"] == "greater_than_equal" for item in error["detail"]["errors"]
    )


async def test_generations_list_returns_not_found_for_an_unknown_device(adapter_client):
    response = await adapter_client.get("/api/v1/devices/999999/generations", headers=AUTH)

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "not_found",
            "message": "Device not found",
            "detail": {},
        }
    }


async def test_generations_list_requires_authentication(adapter_client):
    response = await adapter_client.get("/api/v1/devices/999999/generations")

    assert response.status_code == 401
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing or invalid bearer token",
            "detail": {},
        }
    }
