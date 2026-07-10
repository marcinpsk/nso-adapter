# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for actions.py endpoint functions."""

from __future__ import annotations

import pytest

from nso_adapter.api.actions import (
    _trigger,
    action_connect,
    action_detect_drift,
    action_sync,
    sync_notify,
)
from nso_adapter.api.errors import ApiError
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, Job, JobStatus, JobType


async def _seed_device(nso_device_name: str, netbox_id: int) -> int:
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id
    raise RuntimeError("no DB session")


# ── _trigger ──────────────────────────────────────────────────────────────────


async def test_trigger_device_not_found(adapter_client):
    """_trigger() raises 404 when device_id is unknown."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await _trigger(99989, JobType.sync, db)
        assert exc_info.value.status_code == 404
        break


async def test_trigger_job_conflict_returns_409(adapter_client):
    """_trigger() raises 409 (surfacing the active job's id) when one already runs."""
    device_id = await _seed_device("actions-conflict-01", 1300)
    async for db in get_session():
        # A real active job already exists for the device.
        existing = Job(job_type=JobType.sync, device_id=device_id, status=JobStatus.running)
        db.add(existing)
        await db.commit()
        await db.refresh(existing)

        with pytest.raises(ApiError) as exc_info:
            await _trigger(device_id, JobType.sync, db)
        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["error"]["detail"] == {"job_id": existing.id}
        break


async def test_trigger_success_enqueues_real_job(adapter_client):
    """_trigger() enqueues a real queued job and returns its id."""
    device_id = await _seed_device("actions-ok-01", 1310)
    async for db in get_session():
        result = await _trigger(device_id, JobType.sync, db)
        job = await db.get(Job, result["job_id"])
        assert job is not None
        assert job.device_id == device_id
        assert job.job_type == JobType.sync
        assert job.status == JobStatus.queued
        break


# ── individual action endpoints ───────────────────────────────────────────────


async def test_action_sync_enqueues_sync_job(adapter_client):
    """action_sync enqueues a real job of type sync."""
    device_id = await _seed_device("actions-sync-01", 1320)
    async for db in get_session():
        result = await action_sync(device_id=device_id, db=db)
        job = await db.get(Job, result["job_id"])
        assert job.job_type == JobType.sync
        assert job.status == JobStatus.queued
        break


async def test_action_force_removal_enqueues_forced_removal_job(adapter_client):
    """The operator override for a removal_blocked_collateral failure: enqueues a
    removal job with force=True so the guard is skipped ON PURPOSE (reviewed flush)."""
    from nso_adapter.api.actions import ForceRemovalBody, action_force_removal

    device_id = await _seed_device("actions-frm-01", 1340)
    async for db in get_session():
        result = await action_force_removal(device_id=device_id, body=ForceRemovalBody(scope="isis"), db=db)
        job = await db.get(Job, result["job_id"])
        assert job.job_type == JobType.removal
        assert job.context == {"scope": "isis", "force": True}
        break


async def test_action_force_removal_rejects_unknown_scope(adapter_client):
    from nso_adapter.api.actions import ForceRemovalBody, action_force_removal

    device_id = await _seed_device("actions-frm-02", 1341)
    async for db in get_session():
        try:
            await action_force_removal(device_id=device_id, body=ForceRemovalBody(scope="nonsense"), db=db)
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 400
        else:
            raise AssertionError("unknown scope must be rejected")
        break


async def test_action_apply_diff_forwards_outformat(adapter_client):
    """?outformat=cli reaches collect_apply_diff and is echoed in the response."""
    from unittest.mock import AsyncMock, patch

    from nso_adapter.api.actions import action_apply_diff

    device_id = await _seed_device("actions-adiff-01", 1350)
    coll = AsyncMock(return_value={"isis": "+ interface x"})
    with patch("nso_adapter.core.apply.collect_apply_diff", coll):
        async for db in get_session():
            result = await action_apply_diff(device_id=device_id, outformat="cli", db=db)
            break
    assert result["diffs"] == {"isis": "+ interface x"}
    assert result["outformat"] == "cli"
    assert coll.await_args.kwargs.get("outformat") == "cli"


async def test_action_apply_diff_rejects_unknown_outformat(adapter_client):
    from nso_adapter.api.actions import action_apply_diff

    device_id = await _seed_device("actions-adiff-02", 1351)
    async for db in get_session():
        try:
            await action_apply_diff(device_id=device_id, outformat="nonsense", db=db)
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 400
        else:
            raise AssertionError("invalid outformat must 400")
        break


async def test_action_detect_drift_enqueues_detect_drift_job(adapter_client):
    """action_detect_drift enqueues a real job of type detect_drift (verifies the TYPE)."""
    device_id = await _seed_device("actions-cc-01", 1330)
    async for db in get_session():
        result = await action_detect_drift(device_id=device_id, db=db)
        job = await db.get(Job, result["job_id"])
        assert job.device_id == device_id
        assert job.job_type == JobType.detect_drift
        break


async def test_action_connect_enqueues_connect_job(adapter_client):
    """action_connect enqueues a real job of type connect (verifies the TYPE)."""
    device_id = await _seed_device("actions-conn-01", 1340)
    async for db in get_session():
        result = await action_connect(device_id=device_id, db=db)
        job = await db.get(Job, result["job_id"])
        assert job.device_id == device_id
        assert job.job_type == JobType.connect
        break


async def test_sync_notify_enqueues_sync_job(adapter_client):
    """sync_notify enqueues a real job of type sync."""
    device_id = await _seed_device("actions-notify-01", 1350)
    async for db in get_session():
        result = await sync_notify(device_id=device_id, db=db)
        job = await db.get(Job, result["job_id"])
        assert job.job_type == JobType.sync
        break


# ── verify_token (deps.py line 44: wrong token branch) ───────────────────────


async def test_wrong_token_raises_401(adapter_client):
    """Sending wrong bearer token → 401."""
    resp = await adapter_client.get(
        "/api/v1/devices",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 401
