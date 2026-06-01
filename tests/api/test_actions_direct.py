# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for actions.py endpoint functions."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import BackgroundTasks

from nso_adapter.api.actions import (
    _trigger,
    action_check_compliance,
    action_connect,
    action_sync,
    sync_notify,
)
from nso_adapter.api.errors import ApiError
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, JobStatus, JobType


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
        bt = BackgroundTasks()
        with pytest.raises(ApiError) as exc_info:
            await _trigger(99989, JobType.sync, db, bt)
        assert exc_info.value.status_code == 404
        break


async def test_trigger_job_conflict_returns_409(adapter_client):
    """_trigger() raises 409 when enqueue_job returns created=False."""
    device_id = await _seed_device("actions-conflict-01", 1300)
    async for db in get_session():
        bt = BackgroundTasks()
        fake_job = MagicMock()
        fake_job.id = 999
        fake_job.status = JobStatus.running

        with patch("nso_adapter.api.actions.enqueue_job", new_callable=AsyncMock) as mock_enq:
            mock_enq.return_value = (fake_job, False)
            with pytest.raises(ApiError) as exc_info:
                await _trigger(device_id, JobType.sync, db, bt)
        assert exc_info.value.status_code == 409
        break


async def test_trigger_success_returns_job_id(adapter_client):
    """_trigger() returns {job_id: ...} on success."""
    device_id = await _seed_device("actions-ok-01", 1310)
    async for db in get_session():
        bt = BackgroundTasks()
        fake_job = MagicMock()
        fake_job.id = 42

        with patch("nso_adapter.api.actions.enqueue_job", new_callable=AsyncMock) as mock_enq:
            mock_enq.return_value = (fake_job, True)
            result = await _trigger(device_id, JobType.sync, db, bt)
        assert result == {"job_id": 42}
        break


# ── individual action endpoints ───────────────────────────────────────────────


async def test_action_sync_returns_job_id(adapter_client):
    """action_sync() calls _trigger with JobType.sync."""
    device_id = await _seed_device("actions-sync-01", 1320)
    async for db in get_session():
        bt = BackgroundTasks()
        fake_job = MagicMock()
        fake_job.id = 10

        with patch("nso_adapter.api.actions.enqueue_job", new_callable=AsyncMock) as mock_enq:
            mock_enq.return_value = (fake_job, True)
            result = await action_sync(device_id=device_id, background_tasks=bt, db=db)
        assert result == {"job_id": 10}
        break


async def test_action_check_compliance_returns_job_id(adapter_client):
    """action_check_compliance() calls _trigger with JobType.check_compliance."""
    device_id = await _seed_device("actions-cc-01", 1330)
    async for db in get_session():
        bt = BackgroundTasks()
        fake_job = MagicMock()
        fake_job.id = 11

        with patch("nso_adapter.api.actions.enqueue_job", new_callable=AsyncMock) as mock_enq:
            mock_enq.return_value = (fake_job, True)
            result = await action_check_compliance(device_id=device_id, background_tasks=bt, db=db)
        assert result == {"job_id": 11}
        break


async def test_action_connect_returns_job_id(adapter_client):
    """action_connect() calls _trigger with JobType.connect."""
    device_id = await _seed_device("actions-conn-01", 1340)
    async for db in get_session():
        bt = BackgroundTasks()
        fake_job = MagicMock()
        fake_job.id = 12

        with patch("nso_adapter.api.actions.enqueue_job", new_callable=AsyncMock) as mock_enq:
            mock_enq.return_value = (fake_job, True)
            result = await action_connect(device_id=device_id, background_tasks=bt, db=db)
        assert result == {"job_id": 12}
        break


async def test_sync_notify_returns_job_id(adapter_client):
    """sync_notify() calls _trigger with JobType.sync."""
    device_id = await _seed_device("actions-notify-01", 1350)
    async for db in get_session():
        bt = BackgroundTasks()
        fake_job = MagicMock()
        fake_job.id = 13

        with patch("nso_adapter.api.actions.enqueue_job", new_callable=AsyncMock) as mock_enq:
            mock_enq.return_value = (fake_job, True)
            result = await sync_notify(device_id=device_id, background_tasks=bt, db=db)
        assert result == {"job_id": 13}
        break


# ── verify_token (deps.py line 44: wrong token branch) ───────────────────────


async def test_wrong_token_raises_401(adapter_client):
    """Sending wrong bearer token → 401."""
    resp = await adapter_client.get(
        "/api/v1/devices",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 401
