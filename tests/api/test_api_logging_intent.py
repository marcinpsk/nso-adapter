# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for PUT /api/v1/devices/{id}/logging-intent (remote-syslog write path)."""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _count_intent(device_id: int) -> int:
    from sqlalchemy import select

    from nso_adapter.store.models import LoggingHostIntent

    async with session() as db:
        rows = (
            (await db.execute(select(LoggingHostIntent).where(LoggingHostIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return len(rows)
    return 0


@pytest.mark.anyio
async def test_put_logging_intent_stores_rows(adapter_client):
    device_id = await seed_device()
    body = {
        "hosts": [
            {"address": "10.0.0.1", "severity": "informational", "source": "Loopback0"},
            {"address": "10.0.0.2", "port": 6514, "vrf": "MGMT"},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/logging-intent", json=body, headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["count"] == 2
    assert await _count_intent(device_id) == 2


@pytest.mark.anyio
async def test_put_logging_intent_full_replace(adapter_client):
    device_id = await seed_device()
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.0.0.1"}, {"address": "10.0.0.2"}]},
        headers=AUTH,
    )
    # Second PUT with only one host → the other is deleted (full-replace).
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.0.0.2", "severity": "debugging"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["count"] == 1
    assert await _count_intent(device_id) == 1


@pytest.mark.anyio
async def test_put_logging_intent_clears_on_empty(adapter_client):
    device_id = await seed_device()
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.0.0.1"}]},
        headers=AUTH,
    )
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/logging-intent", json={"hosts": []}, headers=AUTH)
    assert resp.status_code == 200
    assert await _count_intent(device_id) == 0


@pytest.mark.anyio
async def test_put_logging_intent_unknown_device_404(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/9999/logging-intent", json={"hosts": []}, headers=AUTH)
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_put_logging_intent_requires_auth(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/1/logging-intent", json={"hosts": []})
    assert resp.status_code in (401, 403)


# ── local_levels (NX-P4a) — presence-sensitive singleton intent ──────────────


async def _levels_intent(device_id: int):
    from sqlalchemy import select

    from nso_adapter.store.models import LoggingLevelsIntent

    async with session() as db:
        return (
            await db.execute(select(LoggingLevelsIntent).where(LoggingLevelsIntent.device_id == device_id))
        ).scalar_one_or_none()
    return None


async def _logging_removal_jobs(device_id: int):
    from sqlalchemy import select

    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        return [j for j in jobs if (j.context or {}).get("scope") == "logging"]
    return []


@pytest.mark.anyio
async def test_put_local_levels_upserts_singleton(adapter_client):
    device_id = await seed_device()
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [], "local_levels": {"console_severity": "CRITICAL", "monitor_severity": "NOTICE"}},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, "count": 1, "removed": 0, "replaced": False}
    row = await _levels_intent(device_id)
    assert row is not None
    assert row.console_severity == "CRITICAL"
    assert row.monitor_severity == "NOTICE"
    assert row.module_severity is None
    assert row.accepted_at is not None


@pytest.mark.anyio
async def test_put_omitted_local_levels_preserves_intent(adapter_client):
    """R2/F10: an old hosts-only client that OMITS local_levels must not clear the
    levels intent — presence-sensitivity via model_fields_set, not a bare default."""
    device_id = await seed_device()
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [], "local_levels": {"console_severity": "ERROR"}},
        headers=AUTH,
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.0.0.8"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200
    row = await _levels_intent(device_id)
    assert row is not None and row.console_severity == "ERROR"
    assert await _logging_removal_jobs(device_id) == []  # nothing was cleared


@pytest.mark.anyio
async def test_put_local_levels_null_deletes_and_retracts(adapter_client):
    """Explicit null un-manages every destination: the row is deleted and a real
    (networking, non-detach) PUT-replace retract is enqueued."""
    device_id = await seed_device()
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [], "local_levels": {"console_severity": "CRITICAL"}},
        headers=AUTH,
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [], "local_levels": None},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, "count": 0, "removed": 1, "replaced": True}
    assert await _levels_intent(device_id) is None
    jobs = await _logging_removal_jobs(device_id)
    assert len(jobs) == 1
    assert jobs[0].context.get("detach") is None  # a real, networking replace
    assert not jobs[0].context.get("removed")  # no host key was un-owned


@pytest.mark.anyio
async def test_put_local_levels_cleared_severity_retracts(adapter_client):
    """Dropping ONE previously-set severity is the #83 cleared-owned-scalar shape:
    a merge-PATCH can never revert it, so the PUT must enqueue the retract."""
    device_id = await seed_device()
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [], "local_levels": {"console_severity": "CRITICAL", "monitor_severity": "NOTICE"}},
        headers=AUTH,
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [], "local_levels": {"console_severity": "CRITICAL"}},
        headers=AUTH,
    )
    assert resp.status_code == 200
    row = await _levels_intent(device_id)
    assert row is not None and row.monitor_severity is None
    assert len(await _logging_removal_jobs(device_id)) == 1


@pytest.mark.anyio
async def test_put_local_levels_same_values_no_retract(adapter_client):
    device_id = await seed_device()
    body = {"hosts": [], "local_levels": {"console_severity": "CRITICAL"}}
    await adapter_client.put(f"/api/v1/devices/{device_id}/logging-intent", json=body, headers=AUTH)
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/logging-intent", json=body, headers=AUTH)
    assert resp.status_code == 200
    assert await _logging_removal_jobs(device_id) == []


@pytest.mark.anyio
async def test_put_local_levels_invalid_severity_422(adapter_client):
    device_id = await seed_device()
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [], "local_levels": {"console_severity": "verbose"}},
        headers=AUTH,
    )
    assert resp.status_code == 422
    assert await _levels_intent(device_id) is None
