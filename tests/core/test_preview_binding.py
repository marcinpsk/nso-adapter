# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Maintenance Apply holds jobs until an inspected generation is released."""

import pytest

from tests.conftest import AUTH, seed_device
from tests.core.test_generation_protocol import generations, put_vlans, seed_settings

pytestmark = pytest.mark.anyio


async def test_preview_identity_matches_sent_document(maintenance_client):
    http, recorder = maintenance_client
    device_id = await seed_device(nso_device_name="cutover-vlan", netbox_device_id=18101)
    await seed_settings(device_id, auto_apply=True)
    assert (await put_vlans(http, device_id, [10])).status_code == 200
    assert (await put_vlans(http, device_id, [10, 20])).status_code == 200
    chain = await generations(device_id)
    assert len(chain) == 2 and chain[0].job_id == chain[1].job_id
    response = await http.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert response.status_code == 200
    sent = recorder.instance(recorder.calls[-1])["vlan"]["vlan"]
    assert [row["vlan-id"] for row in sent] == [10, 20]
    assert response.json()["generation_id"] == chain[-1].id, response.json()
    assert response.json()["document_digest"] == chain[-1].digest


async def test_preview_invalidated_when_job_document_changes_during_dry_run(maintenance_client):
    http, recorder = maintenance_client
    device_id = await seed_device(nso_device_name="cutover-vlan", netbox_device_id=18102)
    await seed_settings(device_id, auto_apply=True)
    assert (await put_vlans(http, device_id, [10])).status_code == 200
    original = recorder._handle
    injected = False

    async def attach_during_network(method, url, content=None, headers=None):
        nonlocal injected
        if "dry-run=" in url and not injected:
            injected = True
            assert (await put_vlans(http, device_id, [10, 20])).status_code == 200
        return await original(method, url, content, headers)

    recorder._handle = attach_during_network
    response = await http.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert response.status_code == 200
    chain = await generations(device_id)
    assert len(chain) == 2 and chain[0].job_id == chain[1].job_id
    sent = recorder.instance(recorder.calls[-1])["vlan"]["vlan"]
    assert [row["vlan-id"] for row in sent] == [10]
    assert response.json()["generation_id"] is None, response.json()
    assert "preview unavailable" in response.json()["diffs"]["device_intent"]


async def test_preview_unavailable_when_document_tampered_during_dry_run(maintenance_client):
    import json

    from sqlalchemy import text

    from tests.conftest import session

    http, recorder = maintenance_client
    device_id = await seed_device(nso_device_name="cutover-vlan", netbox_device_id=18105)
    await seed_settings(device_id, auto_apply=True)
    assert (await put_vlans(http, device_id, [10])).status_code == 200
    generation_id = (await generations(device_id))[-1].id
    original = recorder._handle
    tampered = False

    async def tamper_during_network(method, url, content=None, headers=None):
        nonlocal tampered
        if "dry-run=" in url and not tampered:
            tampered = True
            async with session() as db:
                await db.execute(
                    text("ALTER TABLE deployment_generation DISABLE TRIGGER deployment_generation_immutable")
                )
                await db.execute(
                    text("UPDATE deployment_generation SET document = CAST(:doc AS json) WHERE id = :gid"),
                    {"doc": json.dumps({"vlan": {"vlan_intent": []}}), "gid": generation_id},
                )
                await db.execute(
                    text("ALTER TABLE deployment_generation ENABLE TRIGGER deployment_generation_immutable")
                )
                await db.commit()
        return await original(method, url, content, headers)

    recorder._handle = tamper_during_network
    response = await http.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert tampered and response.status_code == 200, response.text
    assert response.json()["generation_id"] is None, response.json()
    assert response.json()["document_digest"] is None
    assert "preview unavailable" in response.json()["diffs"]["device_intent"]


async def test_preview_invalidated_when_retry_rebinds_head_during_dry_run(maintenance_client):
    from nso_adapter.store.models import DeploymentGeneration, GenerationStatus, Job, JobStatus
    from tests.conftest import session

    http, recorder = maintenance_client
    device_id = await seed_device(nso_device_name="cutover-vlan", netbox_device_id=18104)
    await seed_settings(device_id, auto_apply=True)
    assert (await put_vlans(http, device_id, [10])).status_code == 200
    assert (await put_vlans(http, device_id, [10, 20])).status_code == 200
    chain = await generations(device_id)
    assert len(chain) == 2 and chain[0].job_id == chain[1].job_id
    async with session() as db:
        job = await db.get(Job, chain[0].job_id)
        job.status = JobStatus.failed
        for generation in chain:
            stored = await db.get(DeploymentGeneration, generation.id)
            stored.status = GenerationStatus.failed
        await db.commit()
    original = recorder._handle
    retried = False

    async def retry_during_network(method, url, content=None, headers=None):
        nonlocal retried
        if "dry-run=" in url and not retried:
            retried = True
            response = await http.post(
                f"/api/v1/devices/{device_id}/actions/retry-generation",
                json={"generation_id": chain[0].id},
                headers=AUTH,
            )
            assert response.status_code == 202
        return await original(method, url, content, headers)

    recorder._handle = retry_during_network
    response = await http.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert response.status_code == 200
    current = await generations(device_id)
    assert retried and current[0].job_id != current[1].job_id
    assert response.json()["generation_id"] is None, response.json()
    assert response.json()["document_digest"] is None
    assert "preview unavailable" in response.json()["diffs"]["device_intent"]


async def test_release_accepts_ordinary_apply_replacement_carrier(maintenance_client):
    from nso_adapter.core.generation import executable_head
    from nso_adapter.core.worker import run_inspected_generation
    from nso_adapter.store.models import Job, JobStatus, JobType
    from tests.conftest import session
    from tests.core.test_action_apply_promotion import _apply

    http, recorder = maintenance_client
    device_id = await seed_device(nso_device_name="cutover-vlan", netbox_device_id=18103)
    await seed_settings(device_id, auto_apply=False)
    assert (await put_vlans(http, device_id, [10], seq=18103, names={10: "managed"})).status_code == 200
    admitted = await _apply(http, device_id, {"vlan": 18103})
    assert admitted.status_code == 202
    initial = (await generations(device_id))[0]
    original = recorder._handle

    async def applied_state(method, url, content=None, headers=None):
        response = await original(method, url, content, headers)
        if "dry-run=" not in url:
            recorder.dry_run_delta = ""
        return response

    recorder._handle = applied_state
    assert await run_inspected_generation(device_id, initial.id, initial.digest) is JobStatus.succeeded
    assert (
        await put_vlans(http, device_id, [10], seq=18104, query="?store_only=true", names={10: ""})
    ).status_code == 200
    admitted = await _apply(http, device_id, {"vlan": 18104})
    assert admitted.status_code == 202
    async with session() as db:
        head = await executable_head(db, device_id)
        job = await db.get(Job, head.job_id)
        assert job.job_type is JobType.removal
    preview = await http.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert preview.status_code == 200
    assert preview.json()["generation_id"] == head.id
    assert preview.json()["document_digest"] == head.digest
    assert not any("preview unavailable" in delta for delta in preview.json()["diffs"].values())
    await run_inspected_generation(device_id, head.id, head.digest)
