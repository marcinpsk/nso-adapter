# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for onboarding endpoints: POST, PATCH, DELETE /api/v1/devices."""

from __future__ import annotations

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


# ── POST /api/v1/devices (onboard) ──────────────────────────────────────────


async def test_onboard_happy_path(adapter_client_with_nso):
    """POST with valid payload → 201 with device fields."""
    resp = await adapter_client_with_nso.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev", "nso_device_name": "core-rtr-01", "netbox_device_id": 42},
        headers=AUTH,
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["nso_instance"] == "nso-dev"
    assert body["nso_device_name"] == "core-rtr-01"
    assert body["netbox_device_id"] == 42
    assert body["mapping_status"] == "mapped"
    assert "id" in body


async def test_onboard_duplicate_netbox_id_returns_409(adapter_client_with_nso):
    """POST with a netbox_device_id that is already onboarded → 409 conflict."""
    await adapter_client_with_nso.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev", "nso_device_name": "core-rtr-01", "netbox_device_id": 100},
        headers=AUTH,
    )
    resp = await adapter_client_with_nso.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev", "nso_device_name": "different-device", "netbox_device_id": 100},
        headers=AUTH,
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "conflict"


async def test_onboard_duplicate_nso_device_returns_409(adapter_client_with_nso):
    """POST with a (nso_instance, nso_device_name) pair already registered → 409 conflict."""
    await adapter_client_with_nso.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev", "nso_device_name": "edge-rtr-01", "netbox_device_id": 200},
        headers=AUTH,
    )
    resp = await adapter_client_with_nso.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev", "nso_device_name": "edge-rtr-01", "netbox_device_id": 201},
        headers=AUTH,
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "conflict"


async def test_onboard_unknown_instance_returns_422(adapter_client):
    """POST with an NSO instance not in config → 422 validation_error.

    Uses adapter_client (no nso_instances) so any instance name is unknown.
    """
    resp = await adapter_client.post(
        "/api/v1/devices",
        json={"nso_instance": "nonexistent-nso", "nso_device_name": "router-01", "netbox_device_id": 42},
        headers=AUTH,
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


async def test_onboard_requires_auth(adapter_client):
    """POST without bearer token → 401."""
    resp = await adapter_client.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev", "nso_device_name": "router-01", "netbox_device_id": 42},
    )
    assert resp.status_code == 401


async def test_onboard_invalid_body_returns_422(adapter_client):
    """POST with missing required fields → 422 (FastAPI validation)."""
    resp = await adapter_client.post(
        "/api/v1/devices",
        json={"nso_instance": "nso-dev"},
        headers=AUTH,
    )
    assert resp.status_code == 422


# ── PATCH /api/v1/devices/{id} (re-key) ─────────────────────────────────────


async def test_rekey_device_happy_path(adapter_client_with_nso):
    """PATCH with new device name → 200 and device updated."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="old-name", netbox_device_id=300)
    resp = await adapter_client_with_nso.patch(
        f"/api/v1/devices/{device_id}",
        json={"nso_device_name": "new-name"},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["nso_device_name"] == "new-name"


async def test_rekey_device_noop_empty_body(adapter_client_with_nso):
    """PATCH with no fields changed → 200, same device returned (no-op)."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="stable-device", netbox_device_id=301)
    resp = await adapter_client_with_nso.patch(
        f"/api/v1/devices/{device_id}",
        json={},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["nso_device_name"] == "stable-device"


async def test_rekey_device_unknown_device_returns_404(adapter_client):
    """PATCH on a device that doesn't exist → 404."""
    resp = await adapter_client.patch(
        "/api/v1/devices/9999",
        json={"nso_device_name": "new-name"},
        headers=AUTH,
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_rekey_device_unknown_instance_returns_422(adapter_client):
    """PATCH to an NSO instance not in config → 422.

    Uses adapter_client (no nso_instances) — seeds directly via DB helper,
    then tries to rekey to an unknown instance.
    """
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="rekey-device", netbox_device_id=302)
    resp = await adapter_client.patch(
        f"/api/v1/devices/{device_id}",
        json={"nso_instance": "nonexistent-nso"},
        headers=AUTH,
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


async def test_rekey_requires_auth(adapter_client):
    """PATCH without auth → 401."""
    resp = await adapter_client.patch("/api/v1/devices/1", json={"nso_device_name": "x"})
    assert resp.status_code == 401


async def test_rekey_device_to_claimed_name_returns_409(adapter_client_with_nso):
    """PATCH that would claim a nso_device_name already used by another device → 409."""
    await seed_device(nso_instance="nso-dev", nso_device_name="already-claimed", netbox_device_id=303)
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="free-device", netbox_device_id=304)
    resp = await adapter_client_with_nso.patch(
        f"/api/v1/devices/{device_id}",
        json={"nso_device_name": "already-claimed"},
        headers=AUTH,
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "conflict"


async def test_rekey_device_clears_interface_state_retains_jobs(adapter_client_with_nso):
    """PATCH clears DbInterface rows but keeps Job rows (job history preserved)."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DbInterface, Job, JobStatus, JobType

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="rk-device", netbox_device_id=305)

    # Seed an interface and a completed job for this device
    async for db in get_session():
        db.add(DbInterface(device_id=device_id, name="Gi0/0"))
        db.add(Job(device_id=device_id, job_type=JobType.sync, status=JobStatus.succeeded))
        await db.commit()

    resp = await adapter_client_with_nso.patch(
        f"/api/v1/devices/{device_id}",
        json={"nso_device_name": "rk-device-new"},
        headers=AUTH,
    )
    assert resp.status_code == 200

    # Interface rows must be gone; job must still exist
    async for db in get_session():
        ifaces = (await db.execute(DbInterface.__table__.select().where(DbInterface.device_id == device_id))).all()
        jobs = (await db.execute(Job.__table__.select().where(Job.device_id == device_id))).all()
        assert len(ifaces) == 0, "interface state must be cleared on rekey"
        assert len(jobs) == 1, "job history must be retained on rekey"


# ── DELETE /api/v1/devices/{id} (offboard) ──────────────────────────────────


async def test_offboard_device_happy_path(adapter_client_with_nso):
    """DELETE existing device → 204 and device gone."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="to-delete", netbox_device_id=400)
    resp = await adapter_client_with_nso.delete(
        f"/api/v1/devices/{device_id}",
        headers=AUTH,
    )
    assert resp.status_code == 204

    # Confirm gone
    check = await adapter_client_with_nso.get(f"/api/v1/devices/{device_id}", headers=AUTH)
    assert check.status_code == 404


async def test_offboard_device_with_active_job_succeeds_and_nullifies_job(adapter_client_with_nso):
    """DELETE a device that has a running job → 204; job row survives with device_id=None."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="active-job-dev", netbox_device_id=401)

    job_id = None
    async for db in get_session():
        job = Job(device_id=device_id, job_type=JobType.sync, status=JobStatus.running)
        db.add(job)
        await db.commit()
        await db.refresh(job)
        job_id = job.id

    resp = await adapter_client_with_nso.delete(f"/api/v1/devices/{device_id}", headers=AUTH)
    assert resp.status_code == 204

    # Job must still exist but device_id must be NULL
    async for db in get_session():
        surviving_job = await db.get(Job, job_id)
        assert surviving_job is not None, "job row must survive offboard"
        assert surviving_job.device_id is None, "job device_id must be nullified on offboard"


async def test_offboard_device_unknown_returns_404(adapter_client):
    """DELETE on non-existent device → 404."""
    resp = await adapter_client.delete("/api/v1/devices/9999", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_offboard_requires_auth(adapter_client):
    """DELETE without auth → 401."""
    resp = await adapter_client.delete("/api/v1/devices/1")
    assert resp.status_code == 401
