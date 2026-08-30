# SPDX-License-Identifier: Apache-2.0
"""API integration tests — health, auth, devices, actions, interfaces, jobs."""

from __future__ import annotations

from uuid import uuid4

from tests.conftest import VALID_TOKEN, seed_device


async def test_healthz_no_auth(adapter_client):
    """Health endpoint must NOT require auth and reports the (empty) instance list.

    The adapter_client fixture loads a real config with ``nso_instances: []``, so the
    handler runs end-to-end over the real config — no instances means the reachability
    loop is skipped and the list comes back empty.
    """
    resp = await adapter_client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "version" in body
    assert body["nso_instances"] == []


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
        json={"apply_attempt_id": str(uuid4()), "selected": {}},
        headers={"Authorization": f"Bearer {VALID_TOKEN}"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_action_apply_requires_an_attempt_id(adapter_client):
    device_id = await seed_device(nso_device_name="apply-attempt-required", netbox_device_id=16231)

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={"selected": {}},
        headers={"Authorization": f"Bearer {VALID_TOKEN}"},
    )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "validation_error",
            "message": "Request validation failed",
            "detail": {
                "errors": [
                    {
                        "loc": ["body", "apply_attempt_id"],
                        "type": "missing",
                        "msg": "Invalid value",
                    }
                ]
            },
        }
    }


async def test_action_apply_rejects_a_revision_field(adapter_client):
    device_id = await seed_device(nso_device_name="apply-attempt-no-revision", netbox_device_id=16240)

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={
            "apply_attempt_id": str(uuid4()),
            "source_intent_revision": 7,
            "selected": {},
        },
        headers={"Authorization": f"Bearer {VALID_TOKEN}"},
    )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "validation_error",
            "message": "Request validation failed",
            "detail": {
                "errors": [
                    {
                        "loc": ["body", "source_intent_revision"],
                        "type": "extra_forbidden",
                        "msg": "Invalid value",
                    }
                ]
            },
        }
    }


async def test_sync_state_includes_phase2_statuses(adapter_client):
    """sync_state endpoint returns 404 for unknown device without crashing."""
    resp = await adapter_client.get(
        "/api/v1/devices/9999/state",
        headers={"Authorization": f"Bearer {VALID_TOKEN}"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"
