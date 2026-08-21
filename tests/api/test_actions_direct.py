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
from nso_adapter.store.models import Device, Job, JobStatus, JobType
from tests.conftest import session


async def _seed_device(nso_device_name: str, netbox_id: int) -> int:
    async with session() as db:
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id


# ── _trigger ──────────────────────────────────────────────────────────────────


async def test_trigger_device_not_found(adapter_client):
    """_trigger() raises 404 when device_id is unknown."""
    async with session() as db:
        with pytest.raises(ApiError) as exc_info:
            await _trigger(99989, JobType.sync, db)
        assert exc_info.value.status_code == 404


async def test_trigger_job_conflict_returns_409(adapter_client):
    """_trigger() raises 409 naming the QUEUED job of the requested type.

    Queued, not running: a running job no longer refuses its successor, because the
    successor carries the newer intent and execution is serialized by the device claim.
    """
    device_id = await _seed_device("actions-conflict-01", 1300)
    async with session() as db:
        existing = Job(job_type=JobType.sync, device_id=device_id, status=JobStatus.queued, coalescible=True)
        db.add(existing)
        await db.commit()
        await db.refresh(existing)

        with pytest.raises(ApiError) as exc_info:
            await _trigger(device_id, JobType.sync, db)
        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["error"]["detail"] == {"job_id": existing.id}


async def test_trigger_is_admitted_while_a_different_type_runs(adapter_client):
    """The narrowing: an unrelated running job must not 409 an operator action."""
    device_id = await _seed_device("actions-conflict-02", 1301)
    async with session() as db:
        db.add(
            Job(
                job_type=JobType.removal,
                device_id=device_id,
                status=JobStatus.running,
                coalescible=False,
                context={"scope": "bgp"},
            )
        )
        await db.commit()

        result = await _trigger(device_id, JobType.sync, db)
        assert result["job_id"]


async def test_trigger_success_enqueues_real_job(adapter_client):
    """_trigger() enqueues a real queued job and returns its id."""
    device_id = await _seed_device("actions-ok-01", 1310)
    async with session() as db:
        result = await _trigger(device_id, JobType.sync, db)
        job = await db.get(Job, result["job_id"])
        assert job is not None
        assert job.device_id == device_id
        assert job.job_type == JobType.sync
        assert job.status == JobStatus.queued


# ── individual action endpoints ───────────────────────────────────────────────


async def test_action_sync_enqueues_sync_now_job(adapter_client):
    """Operator Sync-Now enqueues the grain-c ATOMIC job type (READSEM S3 B7)."""
    device_id = await _seed_device("actions-sync-01", 1320)
    async with session() as db:
        result = await action_sync(device_id=device_id, db=db)
        job = await db.get(Job, result["job_id"])
        assert job.job_type == JobType.sync_now
        assert job.status == JobStatus.queued


async def test_action_sync_from_nso_enqueues_its_job(adapter_client):
    """S5a B: the comprehensive CDB-only read gets its own job type + endpoint."""
    from nso_adapter.api.actions import action_sync_from_nso

    device_id = await _seed_device("actions-sfn-01", 1321)
    async with session() as db:
        result = await action_sync_from_nso(device_id=device_id, db=db)
        job = await db.get(Job, result["job_id"])
        assert job.job_type == JobType.sync_from_nso
        assert job.status == JobStatus.queued


async def test_action_sync_from_nso_404_unknown_device(adapter_client):
    """Unknown device -> the shared _trigger 404."""
    from fastapi import HTTPException

    from nso_adapter.api.actions import action_sync_from_nso

    async with session() as db:
        try:
            await action_sync_from_nso(device_id=999999, db=db)
            raise AssertionError("expected HTTPException")
        except HTTPException as exc:
            assert exc.status_code == 404


async def test_action_sync_from_nso_409_on_active_job(adapter_client):
    """An active job for the device 409s with the incumbent id (shared _trigger)."""
    from fastapi import HTTPException

    from nso_adapter.api.actions import action_sync_from_nso

    device_id = await _seed_device("actions-sfn-02", 1322)
    async with session() as db:
        first = await action_sync_from_nso(device_id=device_id, db=db)
        try:
            await action_sync_from_nso(device_id=device_id, db=db)
            raise AssertionError("expected HTTPException")
        except HTTPException as exc:
            assert exc.status_code == 409
            assert exc.detail["error"]["detail"]["job_id"] == first["job_id"]


async def test_action_force_removal_enqueues_forced_removal_job(adapter_client):
    """The operator override for a removal_blocked_collateral failure: enqueues a
    removal job with force=True so the guard is skipped ON PURPOSE (reviewed flush)."""
    from nso_adapter.api.actions import ForceRemovalBody, action_force_removal

    device_id = await _seed_device("actions-frm-01", 1340)
    async with session() as db:
        result = await action_force_removal(device_id=device_id, body=ForceRemovalBody(scope="isis"), db=db)
        job = await db.get(Job, result["job_id"])
        assert job.job_type == JobType.removal
        assert job.context == {"scope": "isis", "force": True}


async def test_action_force_removal_rejects_unknown_scope(adapter_client):
    from nso_adapter.api.actions import ForceRemovalBody, action_force_removal

    device_id = await _seed_device("actions-frm-02", 1341)
    async with session() as db:
        try:
            await action_force_removal(device_id=device_id, body=ForceRemovalBody(scope="nonsense"), db=db)
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 400
        else:
            raise AssertionError("unknown scope must be rejected")


async def test_action_force_removal_interface_config_requires_interfaces(adapter_client):
    """interface_config is per-instance: the job needs the interface names or it flushes
    NOTHING.

    _replace_interface_config iterates context["interfaces"], and only put_ip_intent ever
    supplied that key — so a force-removal of this scope pushed no PUT-replace and no
    DELETE, yet reported a green job. The operator believed the orphaned addresses and
    descriptions had been flushed while the config was still live on the device. Reject
    the ambiguous call rather than succeed at nothing.
    """
    from nso_adapter.api.actions import ForceRemovalBody, action_force_removal

    device_id = await _seed_device("actions-frm-03", 1342)
    async with session() as db:
        try:
            await action_force_removal(device_id=device_id, body=ForceRemovalBody(scope="interface_config"), db=db)
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 400
        else:
            raise AssertionError("interface_config force-removal without interfaces must be rejected")


async def test_action_force_removal_interface_config_carries_the_interfaces(adapter_client):
    """Given the names, the job carries them so _replace_interface_config actually pushes."""
    from nso_adapter.api.actions import ForceRemovalBody, action_force_removal

    device_id = await _seed_device("actions-frm-04", 1343)
    async with session() as db:
        result = await action_force_removal(
            device_id=device_id,
            body=ForceRemovalBody(scope="interface_config", interfaces=["GigabitEthernet0/1"]),
            db=db,
        )
        job = await db.get(Job, result["job_id"])
        assert job.context == {
            "scope": "interface_config",
            "interfaces": ["GigabitEthernet0/1"],
            "force": True,
        }


async def test_action_apply_diff_forwards_outformat(adapter_client):
    """?outformat=cli reaches collect_apply_diff and is echoed in the response."""
    from unittest.mock import AsyncMock, patch

    from nso_adapter.api.actions import action_apply_diff

    device_id = await _seed_device("actions-adiff-01", 1350)
    coll = AsyncMock(return_value={"isis": "+ interface x"})
    with patch("nso_adapter.core.apply.collect_apply_diff", coll):
        async with session() as db:
            result = await action_apply_diff(device_id=device_id, outformat="cli", db=db)
    assert result["diffs"] == {"isis": "+ interface x"}
    assert result["outformat"] == "cli"
    assert coll.await_args.kwargs.get("outformat") == "cli"


async def test_action_apply_diff_rejects_unknown_outformat(adapter_client):
    from nso_adapter.api.actions import action_apply_diff

    device_id = await _seed_device("actions-adiff-02", 1351)
    async with session() as db:
        try:
            await action_apply_diff(device_id=device_id, outformat="nonsense", db=db)
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 400
        else:
            raise AssertionError("invalid outformat must 400")


async def test_action_detect_drift_enqueues_detect_drift_job(adapter_client):
    """action_detect_drift enqueues a real job of type detect_drift (verifies the TYPE)."""
    device_id = await _seed_device("actions-cc-01", 1330)
    async with session() as db:
        result = await action_detect_drift(device_id=device_id, db=db)
        job = await db.get(Job, result["job_id"])
        assert job.device_id == device_id
        assert job.job_type == JobType.detect_drift


async def test_action_connect_enqueues_connect_job(adapter_client):
    """action_connect enqueues a real job of type connect (verifies the TYPE)."""
    device_id = await _seed_device("actions-conn-01", 1340)
    async with session() as db:
        result = await action_connect(device_id=device_id, db=db)
        job = await db.get(Job, result["job_id"])
        assert job.device_id == device_id
        assert job.job_type == JobType.connect


async def test_sync_notify_enqueues_sync_job(adapter_client):
    """sync_notify enqueues a real job of type sync."""
    device_id = await _seed_device("actions-notify-01", 1350)
    async with session() as db:
        result = await sync_notify(device_id=device_id, db=db)
        job = await db.get(Job, result["job_id"])
        assert job.job_type == JobType.sync


# ── verify_token (deps.py line 44: wrong token branch) ───────────────────────


async def test_wrong_token_raises_401(adapter_client):
    """Sending wrong bearer token → 401."""
    resp = await adapter_client.get(
        "/api/v1/devices",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 401
