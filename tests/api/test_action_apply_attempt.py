# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Durable identity and replay for manual Apply admissions."""

from __future__ import annotations

import uuid

import pytest
import sqlalchemy as sa

from tests.conftest import VALID_TOKEN, seed_device, session
from tests.core.test_generation_protocol import seed_settings

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _put_vlans(client, device_id: int, vids: list[int], *, push_seq: int):
    return await client.put(
        f"/api/v1/devices/{device_id}/vlan-intent?store_only=true",
        json={"vlans": [{"vlan_id": vid, "name": f"vlan-{vid}"} for vid in vids]},
        headers=AUTH | {"X-Push-Seq": str(push_seq)},
    )


async def _put_svis(client, device_id: int, vlan_ids: list[int], *, push_seq: int):
    return await client.put(
        f"/api/v1/devices/{device_id}/svi-intent?store_only=true",
        json={
            "interfaces": [
                {"interface_name": f"Vlan{vlan_id}", "vlan_id": vlan_id, "type": "svi"} for vlan_id in vlan_ids
            ]
        },
        headers=AUTH | {"X-Push-Seq": str(push_seq)},
    )


async def _apply(client, device_id: int, attempt_id: uuid.UUID, selected: dict[str, int]):
    return await client.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={"apply_attempt_id": str(attempt_id), "selected": selected},
        headers=AUTH,
    )


async def _settle(job_id: int) -> None:
    from nso_adapter.core.generation import settle_job_generations
    from nso_adapter.store.models import GenerationStatus, Job, JobStatus

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job is not None
        job.status = JobStatus.succeeded
        await settle_job_generations(db, job_id, outcome=GenerationStatus.settled)
        await db.commit()


async def _fail(job_id: int) -> None:
    from nso_adapter.core.generation import settle_job_generations
    from nso_adapter.store.models import GenerationStatus, Job, JobStatus

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job is not None
        job.status = JobStatus.failed
        await settle_job_generations(db, job_id, outcome=GenerationStatus.failed)
        await db.commit()


async def test_identical_apply_attempt_replays_without_duplicate_work(adapter_client):
    from nso_adapter.store.models import DeploymentApplyAttempt, DeploymentGeneration, Job

    device_id = await seed_device(nso_device_name="apply-attempt-replay", netbox_device_id=16232)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], push_seq=7101)).status_code == 200
    attempt_id = uuid.uuid4()

    first = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7101})
    replay = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7101})

    assert first.status_code == replay.status_code == 202
    assert first.content == replay.content
    async with session() as db:
        attempt = await db.get(DeploymentApplyAttempt, attempt_id)
        assert attempt is not None
        assert attempt.device_id == device_id
        assert attempt.selected == {"vlan": 7101}
        assert attempt.admission_state == "admitted"
        assert attempt.http_status == 202
        assert attempt.response == first.json()
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(DeploymentApplyAttempt)
                .where(DeploymentApplyAttempt.id == attempt_id)
            )
            == 1
        )
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(DeploymentGeneration)
                .where(DeploymentGeneration.device_id == device_id)
            )
            == 1
        )
        assert await db.scalar(sa.select(sa.func.count()).select_from(Job).where(Job.device_id == device_id)) == 1


async def test_apply_stamps_generations_but_reissues_remain_unstamped(adapter_client):
    from nso_adapter.core.generation import create_reissue_generation
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode

    device_id = await seed_device(nso_device_name="apply-attempt-stamp", netbox_device_id=16233)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [20], push_seq=7201)).status_code == 200
    attempt_id = uuid.uuid4()

    response = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7201})
    assert response.status_code == 202, response.text

    async with session() as db:
        applied = await db.scalar(sa.select(DeploymentGeneration).where(DeploymentGeneration.device_id == device_id))
        assert applied is not None
        assert applied.apply_attempt_id == attempt_id

        reissue = await create_reissue_generation(db, device_id, mode=GenerationMode.networked)
        await db.commit()
        assert reissue.apply_attempt_id is None


async def test_deterministic_rejection_is_persisted_replayed_and_served_as_evidence(adapter_client):
    from nso_adapter.store.models import DeploymentApplyAttempt

    device_id = await seed_device(nso_device_name="apply-attempt-rejection", netbox_device_id=16234)
    await seed_settings(device_id, auto_apply=False)
    baseline = await _put_vlans(adapter_client, device_id, [10, 20], push_seq=7301)
    assert baseline.status_code == 200, baseline.text
    admitted = await _apply(adapter_client, device_id, uuid.uuid4(), {"vlan": 7301})
    assert admitted.status_code == 202, admitted.text
    await _settle(admitted.json()["job_id"])
    changed = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent?store_only=true",
        json={"vlans": [{"vlan_id": 10, "name": ""}]},
        headers=AUTH | {"X-Push-Seq": "7302"},
    )
    assert changed.status_code == 200, changed.text
    attempt_id = uuid.uuid4()

    rejected = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7302})
    replay = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7302})

    assert rejected.status_code == replay.status_code == 409
    assert rejected.content == replay.content
    assert rejected.json() == {
        "error": {
            "code": "apply_unexecutable",
            "message": "Selected stream(s) cannot be applied faithfully: vlan",
            "detail": {"streams": {"vlan": "mixed_detach_replacement"}},
        }
    }
    async with session() as db:
        attempt = await db.get(DeploymentApplyAttempt, attempt_id)
        assert attempt is not None
        assert attempt.admission_state == "rejected"
        assert attempt.http_status == 409
        assert attempt.response == rejected.json()

    evidence = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id)]},
    )
    assert evidence.status_code == 200, evidence.text
    assert evidence.json()["unknown_apply_attempt_ids"] == []
    assert evidence.json()["attempts"] == [
        {
            "apply_attempt_id": str(attempt_id),
            "admission_state": "rejected",
            "http_status": 409,
            "response": rejected.json(),
            "generations": [],
        }
    ]

    # Discriminating replay: supersede the rejected receipt so a FRESH evaluation of the
    # same selection would now answer 200 no_op (superseded), then re-POST the same
    # attempt. A replay must serve the stored 409 bytes, never re-evaluate.
    superseding = await _put_vlans(adapter_client, device_id, [10, 20], push_seq=7303)
    assert superseding.status_code == 200, superseding.text
    still_replayed = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7302})
    assert still_replayed.status_code == 409
    assert still_replayed.content == rejected.content


async def test_complete_no_op_replays_and_serves_its_stored_response_as_evidence(adapter_client):
    from nso_adapter.store.models import DeploymentApplyAttempt

    device_id = await seed_device(nso_device_name="apply-attempt-no-op", netbox_device_id=16235)
    attempt_id = uuid.uuid4()

    first = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7401})
    replay = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7401})

    expected = {
        "device_id": device_id,
        "outcome": "no_op",
        "selected": {"vlan": 7401},
        "skipped": {"vlan": "no_receipt"},
        "skipped_detail": None,
        "generations": [],
    }
    assert first.status_code == replay.status_code == 200
    assert first.json() == expected
    assert first.content == replay.content
    async with session() as db:
        attempt = await db.get(DeploymentApplyAttempt, attempt_id)
        assert attempt is not None
        assert attempt.response == expected

    evidence = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id)]},
    )
    assert evidence.status_code == 200, evidence.text
    assert evidence.json()["unknown_apply_attempt_ids"] == []
    assert evidence.json()["attempts"] == [
        {
            "apply_attempt_id": str(attempt_id),
            "admission_state": "admitted",
            "http_status": 200,
            "response": expected,
            "generations": [],
        }
    ]


async def test_same_uuid_with_a_different_selection_is_a_non_mutating_conflict(adapter_client):
    from nso_adapter.store.models import DeploymentApplyAttempt, DeploymentGeneration, Job

    device_id = await seed_device(nso_device_name="apply-attempt-mismatch", netbox_device_id=16236)
    attempt_id = uuid.uuid4()
    admitted = await _apply(adapter_client, device_id, attempt_id, {})
    assert admitted.status_code == 200

    conflict = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7501})

    assert conflict.status_code == 409
    assert conflict.json() == {
        "error": {
            "code": "conflict",
            "message": "Apply attempt UUID belongs to a different request identity",
            "detail": {"mismatch": "selected"},
        }
    }
    async with session() as db:
        attempt = await db.get(DeploymentApplyAttempt, attempt_id)
        assert attempt is not None
        assert attempt.selected == {}
        assert attempt.response == admitted.json()
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(DeploymentGeneration)
                .where(DeploymentGeneration.device_id == device_id)
            )
            == 0
        )
        assert await db.scalar(sa.select(sa.func.count()).select_from(Job).where(Job.device_id == device_id)) == 0


async def test_begin_apply_attempt_loser_reads_the_committed_winner(adapter_client):
    """The post-lock ON CONFLICT path: a loser session must serve the winner's stored row."""
    from nso_adapter.store.apply_attempt_store import (
        ApplyAttemptIdentityMismatch,
        begin_apply_attempt,
        complete_apply_attempt,
    )

    device_id = await seed_device(nso_device_name="apply-attempt-loser", netbox_device_id=16238)
    attempt_id = uuid.uuid4()
    async with session() as db:
        assert await begin_apply_attempt(db, attempt_id, device_id, {}) is None
        await complete_apply_attempt(
            db, attempt_id, admission_state="admitted", http_status=200, response={"outcome": "no_op"}
        )
        await db.commit()

    async with session() as db:
        stored = await begin_apply_attempt(db, attempt_id, device_id, {})
        assert stored is not None
        assert (stored.http_status, stored.response) == (200, {"outcome": "no_op"})
        with pytest.raises(ApplyAttemptIdentityMismatch) as excinfo:
            await begin_apply_attempt(db, attempt_id, device_id, {"vlan": 9001})
        assert excinfo.value.mismatch == "selected"


async def test_concurrent_identical_applies_produce_one_attempt_and_identical_bytes(adapter_client):
    import asyncio

    from nso_adapter.store.models import DeploymentApplyAttempt

    device_id = await seed_device(nso_device_name="apply-attempt-race", netbox_device_id=16239)
    attempt_id = uuid.uuid4()

    first, second = await asyncio.gather(
        _apply(adapter_client, device_id, attempt_id, {}),
        _apply(adapter_client, device_id, attempt_id, {}),
    )

    assert first.status_code == second.status_code == 200
    assert first.content == second.content
    async with session() as db:
        count = await db.scalar(
            sa.select(sa.func.count())
            .select_from(DeploymentApplyAttempt)
            .where(DeploymentApplyAttempt.id == attempt_id)
        )
        assert count == 1


async def test_same_uuid_on_a_nonexistent_device_is_an_identity_conflict(adapter_client):
    device_id = await seed_device(nso_device_name="apply-attempt-foreign", netbox_device_id=16237)
    attempt_id = uuid.uuid4()
    admitted = await _apply(adapter_client, device_id, attempt_id, {})
    assert admitted.status_code == 200

    conflict = await _apply(adapter_client, 999999, attempt_id, {})

    assert conflict.status_code == 409
    assert conflict.json()["error"]["detail"] == {"mismatch": "device_id"}


async def test_skipped_stream_detail_is_stored_and_replayed_with_promoted_work(adapter_client):
    from nso_adapter.store.models import DeploymentApplyAttempt

    device_id = await seed_device(nso_device_name="apply-attempt-partial", netbox_device_id=16237)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [30], push_seq=7601)).status_code == 200
    vlan_apply = await _apply(adapter_client, device_id, uuid.uuid4(), {"vlan": 7601})
    assert vlan_apply.status_code == 202, vlan_apply.text
    vlan_generation = vlan_apply.json()["generations"][0]
    await _fail(vlan_apply.json()["job_id"])
    assert (await _put_svis(adapter_client, device_id, [30], push_seq=7602)).status_code == 200
    attempt_id = uuid.uuid4()

    first = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7601, "svi": 7602})
    replay = await _apply(adapter_client, device_id, attempt_id, {"svi": 7602, "vlan": 7601})

    assert first.status_code == replay.status_code == 202
    assert first.content == replay.content
    assert first.json()["skipped"] == {"vlan": "already_authorized"}
    assert first.json()["skipped_detail"] == {
        "vlan": {
            "generation_id": vlan_generation["generation_id"],
            "seq": vlan_generation["seq"],
            "status": "failed",
        }
    }
    async with session() as db:
        attempt = await db.get(DeploymentApplyAttempt, attempt_id)
        assert attempt is not None
        assert attempt.selected == {"svi": 7602, "vlan": 7601}
        assert attempt.response["skipped_detail"] == first.json()["skipped_detail"]


async def test_store_failure_after_admission_rolls_back_attempt_generation_and_projection(
    adapter_client,
    monkeypatch,
):
    from nso_adapter.store import apply_attempt_store
    from nso_adapter.store.models import (
        DeploymentApplyAttempt,
        DeploymentGeneration,
        DeviceProjectionStream,
        Job,
    )

    device_id = await seed_device(nso_device_name="apply-attempt-rollback", netbox_device_id=16238)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [40], push_seq=7701)).status_code == 200
    attempt_id = uuid.uuid4()
    complete = apply_attempt_store.complete_apply_attempt

    async def fail_after_recording(*args, **kwargs):
        await complete(*args, **kwargs)
        raise RuntimeError("forced failure after Apply admission")

    monkeypatch.setattr(apply_attempt_store, "complete_apply_attempt", fail_after_recording)

    response = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7701})

    assert response.status_code == 500
    assert response.json() == {"error": {"code": "internal", "message": "Internal server error", "detail": {}}}
    async with session() as db:
        assert await db.get(DeploymentApplyAttempt, attempt_id) is None
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(DeploymentGeneration)
                .where(DeploymentGeneration.device_id == device_id)
            )
            == 0
        )
        assert await db.scalar(sa.select(sa.func.count()).select_from(Job).where(Job.device_id == device_id)) == 0
        stream = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "vlan",
            )
        )
        assert stream is not None
        assert stream.authorized_revision == 0


async def test_settled_post_created_attempt_remains_available_after_job_pruning(adapter_client):
    from nso_adapter.store.models import DeploymentGeneration, Job

    device_id = await seed_device(nso_device_name="apply-attempt-aged", netbox_device_id=16239)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [50], push_seq=7801)).status_code == 200
    attempt_id = uuid.uuid4()
    admitted = await _apply(adapter_client, device_id, attempt_id, {"vlan": 7801})
    assert admitted.status_code == 202, admitted.text
    generation_id = admitted.json()["generations"][0]["generation_id"]
    job_id = admitted.json()["job_id"]
    await _settle(job_id)

    async with session() as db:
        await db.execute(sa.delete(Job).where(Job.id == job_id))
        await db.commit()
        generation = await db.get(DeploymentGeneration, generation_id)
        assert generation is not None
        assert generation.job_id is None

    evidence = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence",
        headers=AUTH,
        json={"apply_attempt_ids": [str(attempt_id)]},
    )

    assert evidence.status_code == 200, evidence.text
    body = evidence.json()
    assert body["head"] is None
    assert body["unknown_apply_attempt_ids"] == []
    assert body["attempts"][0]["apply_attempt_id"] == str(attempt_id)
    assert body["attempts"][0]["response"] == admitted.json()
    assert body["attempts"][0]["generations"][0]["generation_id"] == generation_id
    assert body["attempts"][0]["generations"][0]["status"] == "settled"
