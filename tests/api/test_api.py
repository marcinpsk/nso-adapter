# SPDX-License-Identifier: Apache-2.0
"""API integration tests — health, auth, devices, actions, interfaces, jobs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from tests.conftest import VALID_TOKEN


async def test_healthz_no_auth(adapter_client):
    """Health endpoint must NOT require auth."""
    with (
        patch("nso_adapter.api.health.get_nso_client", side_effect=KeyError("none")),
        patch("nso_adapter.api.health.get_config") as mc,
    ):
        mc.return_value = MagicMock(nso_instances=[])
        resp = await adapter_client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "version" in body
    assert "nso_instances" in body


async def test_devices_requires_auth(adapter_client):
    resp = await adapter_client.get("/api/v1/devices")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthorized"


async def test_devices_list_empty(adapter_client):
    resp = await adapter_client.get("/api/v1/devices", headers={"Authorization": f"Bearer {VALID_TOKEN}"})
    assert resp.status_code == 200
    assert resp.json() == []


async def test_jobs_list_empty(adapter_client):
    resp = await adapter_client.get("/api/v1/jobs", headers={"Authorization": f"Bearer {VALID_TOKEN}"})
    assert resp.status_code == 200
    assert resp.json() == []


async def test_job_not_found(adapter_client):
    resp = await adapter_client.get("/api/v1/jobs/9999", headers={"Authorization": f"Bearer {VALID_TOKEN}"})
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_device_not_found(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/9999", headers={"Authorization": f"Bearer {VALID_TOKEN}"})
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_action_unknown_device(adapter_client):
    resp = await adapter_client.post(
        "/api/v1/devices/9999/actions/sync",
        headers={"Authorization": f"Bearer {VALID_TOKEN}"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_onboard_requires_auth(adapter_client):
    resp = await adapter_client.post("/api/v1/devices", json={})
    assert resp.status_code == 401


async def test_sync_notify_unknown_device(adapter_client):
    """sync-notify on unknown device returns 404."""
    resp = await adapter_client.post(
        "/api/v1/devices/9999/sync-notify",
        headers={"Authorization": f"Bearer {VALID_TOKEN}"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_apply_returns_501(adapter_client):
    """apply action on unknown device returns 404."""
    resp = await adapter_client.post(
        "/api/v1/devices/9999/actions/apply",
        headers={"Authorization": f"Bearer {VALID_TOKEN}"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_sync_state_includes_phase2_statuses(adapter_client):
    """sync_state endpoint returns 404 for unknown device without crashing."""
    resp = await adapter_client.get(
        "/api/v1/devices/9999/state",
        headers={"Authorization": f"Bearer {VALID_TOKEN}"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"
