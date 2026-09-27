# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Maintenance Apply holds jobs until an inspected generation is released."""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError

from nso_adapter.config import reset_config
from nso_adapter.core.generation import executable_head
from nso_adapter.core.worker import FollowupSyncFailed, run_inspected_generation
from nso_adapter.store.models import DeploymentGeneration, DeviceClaim, Job, JobStatus, JobType
from tests.api.test_action_apply_attempt import _put_vlans
from tests.conftest import AUTH, _write_config, seed_device, session
from tests.core.test_action_apply_promotion import _apply
from tests.core.test_generation_protocol import put_vlans, seed_settings

pytestmark = pytest.mark.anyio


async def test_serve_keeps_ordinary_apply_queued_and_preview_identifies_it(maintenance_client):
    http, recorder = maintenance_client
    device_id = await seed_device(nso_device_name="cutover-vlan", netbox_device_id=16130)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(http, device_id, [10], push_seq=7101)).status_code == 200
    response = await http.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={"apply_attempt_id": str(uuid.uuid4()), "selected": {"vlan": 7101}},
        headers=AUTH,
    )
    assert response.status_code == 202
    generation_id = response.json()["generations"][0]["generation_id"]
    preview = await http.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert preview.status_code == 200
    async with session() as db:
        generation = await db.get(DeploymentGeneration, generation_id)
        job = await db.get(Job, generation.job_id)
        assert preview.json()["generation_id"] == generation_id
        assert preview.json()["document_digest"] == generation.digest
        assert job.status is JobStatus.queued
    await asyncio.sleep(2.2)
    assert recorder.commits == []
    async with session() as db:
        assert (await db.get(Job, job.id)).status is JobStatus.queued


async def _admit(http, name: str, sequence: int) -> tuple[int, int, int, str]:
    device_id = await seed_device(nso_device_name=name, netbox_device_id=sequence)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(http, device_id, [10], push_seq=sequence)).status_code == 200
    admitted = await http.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={"apply_attempt_id": str(uuid.uuid4()), "selected": {"vlan": sequence}},
        headers=AUTH,
    )
    assert admitted.status_code == 202
    generation = admitted.json()["generations"][0]
    return device_id, generation["generation_id"], generation["job_id"], generation["digest"]


async def test_release_refuses_stale_digest_and_unavailable_preview(maintenance_client):
    from nso_adapter.core.cutover_runner import ReleaseRefused, run_inspected_generation

    http, recorder = maintenance_client
    device_id, generation_id, job_id, digest = await _admit(http, "cutover-vlan", 7102)
    with pytest.raises(ReleaseRefused, match="not the execution document"):
        await run_inspected_generation(device_id, generation_id + 1, digest)
    with pytest.raises(ReleaseRefused, match="document digest differs"):
        await run_inspected_generation(device_id, generation_id, "0" * 64)
    original = recorder._handle

    async def unavailable_preview(method, url, content=None, headers=None):
        if "dry-run=" in url:
            return httpx.Response(200, request=httpx.Request(method.upper(), url), json={})
        return await original(method, url, content, headers)

    recorder._handle = unavailable_preview
    with pytest.raises(ReleaseRefused, match="preview unavailable"):
        await run_inspected_generation(device_id, generation_id, digest)
    assert recorder.commits == []
    async with session() as db:
        assert (await db.get(Job, job_id)).status is JobStatus.queued


async def test_release_preview_does_not_hold_job_or_claim_locks(maintenance_client):
    http, recorder = maintenance_client
    device_id, generation_id, job_id, digest = await _admit(http, "cutover-vlan", 7105)
    original = recorder._handle
    observations = []

    async def probe_dry_run(method, url, content=None, headers=None):
        if "dry-run=" in url and not observations:
            async with session() as db:
                claim = await db.scalar(select(DeviceClaim).where(DeviceClaim.device_id == device_id))
                try:
                    locked = await db.scalar(select(Job).where(Job.id == job_id).with_for_update(nowait=True))
                except DBAPIError as exc:
                    if getattr(exc.orig, "sqlstate", None) != "55P03":
                        raise
                    observations.append((False, claim is None))
                else:
                    observations.append((locked is not None, claim is None))
                finally:
                    await db.rollback()
        response = await original(method, url, content, headers)
        if "dry-run=" not in url:
            recorder.dry_run_delta = ""
        return response

    recorder._handle = probe_dry_run
    assert await run_inspected_generation(device_id, generation_id, digest) is JobStatus.succeeded
    assert observations == [(True, True)]


async def test_release_executes_only_inspected_generation(maintenance_client):
    from nso_adapter.core.worker import run_inspected_generation

    http, recorder = maintenance_client
    device_id, generation_id, job_id, digest = await _admit(http, "cutover-vlan", 7103)
    other_device = await seed_device(nso_device_name="cutover-other", netbox_device_id=7104)
    async with session() as db:
        other = Job(device_id=other_device, job_type="sync", status=JobStatus.queued, coalescible=False)
        db.add(other)
        await db.commit()
        other_id = other.id
    original = recorder._handle

    async def applied_state(method, url, content=None, headers=None):
        response = await original(method, url, content, headers)
        if "dry-run=" not in url:
            recorder.dry_run_delta = ""
        return response

    recorder._handle = applied_state
    status = await run_inspected_generation(device_id, generation_id, digest)
    assert status is JobStatus.succeeded
    assert len(recorder.commits) == 1
    async with session() as db:
        generation = await db.get(DeploymentGeneration, generation_id)
        assert generation.status.value == "settled"
        assert generation.job_id == job_id
        assert (await db.get(Job, other_id)).status is JobStatus.queued


async def test_release_progresses_after_removal_followup(maintenance_client):
    device_id, removal = await _admit_vlan_removal(maintenance_client, 18103)
    assert await run_inspected_generation(device_id, removal.id, removal.digest) is JobStatus.succeeded
    http, _recorder = maintenance_client
    for _ in range(3):
        async with session() as db:
            head = await executable_head(db, device_id)
        if head is None:
            break
        assert await run_inspected_generation(device_id, head.id, head.digest) is JobStatus.succeeded
    assert (await put_vlans(http, device_id, [10, 20], seq=18105, query="?store_only=true")).status_code == 200
    admitted = await _apply(http, device_id, {"vlan": 18105})
    assert admitted.status_code == 202, admitted.text
    preview = await http.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert preview.status_code == 200
    inspected = preview.json()
    assert inspected["generation_id"] is not None
    assert not any("preview unavailable" in delta for delta in inspected["diffs"].values())
    assert (
        await run_inspected_generation(device_id, inspected["generation_id"], inspected["document_digest"])
        is JobStatus.succeeded
    )


async def _admit_vlan_removal(maintenance_client, netbox_device_id: int):
    http, recorder = maintenance_client
    device_id = await seed_device(nso_device_name="cutover-vlan", netbox_device_id=netbox_device_id)
    await seed_settings(device_id, auto_apply=False)
    assert (await put_vlans(http, device_id, [10], seq=netbox_device_id, names={10: "managed"})).status_code == 200
    assert (await _apply(http, device_id, {"vlan": netbox_device_id})).status_code == 202
    async with session() as db:
        initial = await executable_head(db, device_id)
        assert initial is not None
    original = recorder._handle

    async def applied_state(method, url, content=None, headers=None):
        response = await original(method, url, content, headers)
        if "dry-run=" not in url:
            recorder.dry_run_delta = ""
        return response

    recorder._handle = applied_state
    assert await run_inspected_generation(device_id, initial.id, initial.digest) is JobStatus.succeeded
    assert (
        await put_vlans(http, device_id, [10], seq=netbox_device_id + 1, query="?store_only=true", names={10: ""})
    ).status_code == 200
    assert (await _apply(http, device_id, {"vlan": netbox_device_id + 1})).status_code == 202
    async with session() as db:
        removal = await executable_head(db, device_id)
        assert removal is not None
        carrier = await db.get(Job, removal.job_id)
        assert carrier is not None and carrier.job_type is JobType.removal
    return device_id, removal


async def test_release_runs_only_its_removal_followup(maintenance_client):
    device_id, removal = await _admit_vlan_removal(maintenance_client, 18106)
    async with session() as db:
        unrelated = Job(device_id=device_id, job_type=JobType.detect_drift, status=JobStatus.queued, coalescible=False)
        db.add(unrelated)
        await db.commit()
        unrelated_id = unrelated.id
    assert await run_inspected_generation(device_id, removal.id, removal.digest) is JobStatus.succeeded
    async with session() as db:
        sync = await db.scalar(
            select(Job).where(
                Job.device_id == device_id, Job.job_type == JobType.sync, Job.status == JobStatus.succeeded
            )
        )
        assert sync is not None
        assert sync.context == {"followup_of_job_id": removal.job_id}
        assert (await db.get(Job, unrelated_id)).status is JobStatus.queued


async def test_release_followup_does_not_coalesce_into_queued_sync(maintenance_client):
    device_id, removal = await _admit_vlan_removal(maintenance_client, 18110)
    async with session() as db:
        ordinary = Job(device_id=device_id, job_type=JobType.sync, status=JobStatus.queued, coalescible=True)
        db.add(ordinary)
        await db.commit()
        ordinary_id = ordinary.id
    assert await run_inspected_generation(device_id, removal.id, removal.digest) is JobStatus.succeeded
    async with session() as db:
        syncs = (await db.scalars(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.sync))).all()
        followups = [sync for sync in syncs if sync.context == {"followup_of_job_id": removal.job_id}]
        assert len(followups) == 1
        assert followups[0].status is JobStatus.succeeded
        assert (await db.get(Job, ordinary_id)).status is JobStatus.queued


async def test_release_reports_failed_followup_without_changing_removal(maintenance_client):
    device_id, removal = await _admit_vlan_removal(maintenance_client, 18108)
    from nso_adapter.core.importer import get_nso_client

    get_nso_client("nso-dev").get_device_ned_id = AsyncMock(return_value="")
    with pytest.raises(FollowupSyncFailed, match="follow-up sync .* finished with failed"):
        await run_inspected_generation(device_id, removal.id, removal.digest)
    async with session() as db:
        generation = await db.get(DeploymentGeneration, removal.id)
        carrier = await db.get(Job, removal.job_id)
        sync = await db.scalar(
            select(Job).where(Job.device_id == device_id, Job.job_type == JobType.sync, Job.status == JobStatus.failed)
        )
        assert generation is not None and generation.status.value == "settled"
        assert carrier is not None and carrier.status is JobStatus.succeeded
        assert sync is not None and sync.context == {"followup_of_job_id": carrier.id}


async def test_normal_lifespan_starts_background_components(store_engine, pg_url, tmp_path, monkeypatch):
    from nso_adapter.main import create_app

    _write_config(tmp_path, monkeypatch, database_url=pg_url)
    reset_config()
    calls: list[str] = []

    async def start_workers(_concurrency):
        calls.append("workers")

    async def stop_workers():
        calls.append("stop_workers")

    def start_streams(*_args):
        calls.append("sse")
        return []

    app = create_app()
    with (
        patch("nso_adapter.main.init_db"),
        patch("nso_adapter.main._dispose_engine", new=AsyncMock()),
        patch("nso_adapter.main.set_netbox_client"),
        patch("nso_adapter.main._start_sse_streams", side_effect=start_streams),
        patch("nso_adapter.main.start_workers", side_effect=start_workers),
        patch("nso_adapter.main.stop_workers", side_effect=stop_workers),
        patch("nso_adapter.main.start_scheduler", side_effect=lambda: calls.append("scheduler")),
        patch("nso_adapter.main.stop_scheduler", side_effect=lambda: calls.append("stop_scheduler")),
    ):
        async with app.router.lifespan_context(app):
            assert calls == ["sse", "workers", "scheduler"]
    assert calls[-2:] == ["stop_scheduler", "stop_workers"]
