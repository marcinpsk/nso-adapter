# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The plugin's provision-attempt contract, end to end through the HTTP routes.

The plugin (netbox-nso-plugin ``provision_lifecycle``) names every provision with a
``provision_attempt_id``, polls ``GET /api/v1/provision-attempts/{id}`` and accepts the same
evidence document on its ``provision-complete`` callback. The plugin validator rules are
restated in ``_assert_plugin_evidence`` so a drift on this side fails here.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import httpx
import respx
from sqlalchemy import func, select

from nso_adapter.bindings.netbox.client import NetboxClient
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Job, JobType
from tests.conftest import VALID_TOKEN, session, start_job

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
_NETBOX = "http://netbox.local"
_CALLBACK = f"{_NETBOX}/api/plugins/nso/provision-complete/"

_BODY = {
    "nso_instance": "nso-dev",
    "device_name": "attempt-rtr",
    "address": "10.0.0.5",
    "ned_id": "cisco-ios-cli-6.114:cisco-ios-cli-6.114",
    "authgroup": "network",
    "admin_state": "unlocked",
    "sync": True,
}


def _provision_body(attempt_id: uuid.UUID, **overrides) -> dict:
    """The body the plugin's ``adapter_client.provision_device`` sends: no ``netbox_device_id``."""
    return {**_BODY, "provision_attempt_id": str(attempt_id), **overrides}


def _assert_plugin_evidence(evidence: dict, attempt_id: uuid.UUID) -> None:
    """The plugin's ``validate_provision_evidence`` rules, plus its attempt-id match."""
    assert set(evidence) == {"provision_attempt_id", "status", "job_id", "result", "error"}
    assert uuid.UUID(evidence["provision_attempt_id"]) == attempt_id
    assert evidence["status"] in {"queued", "running", "succeeded", "failed"}
    assert type(evidence["job_id"]) is int and evidence["job_id"] > 0
    for member in ("result", "error"):
        assert evidence[member] is None or isinstance(evidence[member], dict)
    result = evidence["result"]
    if evidence["status"] == "succeeded":
        assert isinstance(result, dict) and type(result["ok"]) is bool
    device_id = result.get("device_id") if isinstance(result, dict) else None
    assert device_id is None or (type(device_id) is int and device_id > 0)


def _ok_nso_client():
    client = AsyncMock(spec=NsoClient)
    client.device_exists.return_value = False
    client.sync_from.return_value = True
    return client


async def _provision_job_count() -> int:
    async with session() as db:
        return await db.scalar(select(func.count()).select_from(Job).where(Job.job_type == JobType.provision))


async def _run_job(job_id: int, nso_client) -> list[dict]:
    """Run the real provision runner; return the callback bodies the plugin endpoint received."""
    from nso_adapter.core.jobs import _JOB_RUNNERS

    netbox = NetboxClient(url=_NETBOX, token="nb-token", timeout=5.0)
    with respx.mock(assert_all_called=False) as router:
        route = router.post(_CALLBACK).mock(return_value=httpx.Response(202, json={"queued": True}))
        with (
            patch("nso_adapter.core.importer.get_nso_client", return_value=nso_client),
            patch("nso_adapter.core.importer.get_netbox_client", lambda: netbox),
        ):
            await start_job(job_id)
            await _JOB_RUNNERS[JobType.provision](job_id, None)
        await netbox.aclose()
        return [json.loads(call.request.content) for call in route.calls]


async def test_admitted_attempt_is_served_as_queued_evidence(adapter_client_with_nso):
    attempt_id = uuid.uuid4()
    admitted = await adapter_client_with_nso.post(
        "/api/v1/devices/provision", json=_provision_body(attempt_id), headers=AUTH
    )
    assert admitted.status_code == 202, admitted.text

    response = await adapter_client_with_nso.get(f"/api/v1/provision-attempts/{attempt_id}", headers=AUTH)

    assert response.status_code == 200, response.text
    evidence = response.json()
    _assert_plugin_evidence(evidence, attempt_id)
    assert evidence == {
        "provision_attempt_id": str(attempt_id),
        "status": "queued",
        "job_id": int(admitted.json()["job_id"]),
        "result": None,
        "error": None,
    }


async def test_a_finished_attempt_serves_and_posts_the_same_success_evidence(adapter_client_with_nso):
    attempt_id = uuid.uuid4()
    admitted = await adapter_client_with_nso.post(
        "/api/v1/devices/provision", json=_provision_body(attempt_id), headers=AUTH
    )
    job_id = int(admitted.json()["job_id"])

    callbacks = await _run_job(job_id, _ok_nso_client())
    response = await adapter_client_with_nso.get(f"/api/v1/provision-attempts/{attempt_id}", headers=AUTH)

    assert response.status_code == 200, response.text
    evidence = response.json()
    _assert_plugin_evidence(evidence, attempt_id)
    assert evidence["status"] == "succeeded"
    assert evidence["job_id"] == job_id
    assert evidence["error"] is None
    assert evidence["result"]["ok"] is True
    # The plugin omits netbox_device_id, so the adapter creates no mapping row and reports none.
    assert evidence["result"]["device_id"] is None
    assert {step["step"] for step in evidence["result"]["steps"]} >= {"create", "fetch_host_keys", "sync_from"}
    assert callbacks == [evidence], "the callback must carry the document the poll serves"


async def test_a_crashed_attempt_serves_and_posts_failed_evidence(adapter_client_with_nso):
    attempt_id = uuid.uuid4()
    admitted = await adapter_client_with_nso.post(
        "/api/v1/devices/provision", json=_provision_body(attempt_id), headers=AUTH
    )
    job_id = int(admitted.json()["job_id"])
    with patch("nso_adapter.core.onboarding.provision_nso_device", side_effect=RuntimeError("crash")):
        callbacks = await _run_job(job_id, _ok_nso_client())
    response = await adapter_client_with_nso.get(f"/api/v1/provision-attempts/{attempt_id}", headers=AUTH)

    evidence = response.json()
    _assert_plugin_evidence(evidence, attempt_id)
    assert evidence["status"] == "failed"
    assert evidence["result"] is None
    assert evidence["error"]["code"]
    assert callbacks == [evidence]


async def test_a_retried_attempt_returns_the_same_job(adapter_client_with_nso):
    attempt_id = uuid.uuid4()
    body = _provision_body(attempt_id)
    first = await adapter_client_with_nso.post("/api/v1/devices/provision", json=body, headers=AUTH)
    second = await adapter_client_with_nso.post("/api/v1/devices/provision", json=body, headers=AUTH)

    assert first.status_code == second.status_code == 202
    assert first.json()["job_id"] == second.json()["job_id"]
    assert await _provision_job_count() == 1


async def test_a_retry_after_the_attempt_finished_does_not_provision_again(adapter_client_with_nso):
    attempt_id = uuid.uuid4()
    body = _provision_body(attempt_id)
    first = await adapter_client_with_nso.post("/api/v1/devices/provision", json=body, headers=AUTH)
    job_id = int(first.json()["job_id"])
    await _run_job(job_id, _ok_nso_client())

    retry = await adapter_client_with_nso.post("/api/v1/devices/provision", json=body, headers=AUTH)

    assert retry.status_code == 202, retry.text
    assert retry.json()["job_id"] == str(job_id)
    assert retry.json()["status"] == "succeeded"
    assert await _provision_job_count() == 1


async def test_an_attempt_id_reused_with_another_body_is_refused(adapter_client_with_nso):
    attempt_id = uuid.uuid4()
    first = await adapter_client_with_nso.post(
        "/api/v1/devices/provision", json=_provision_body(attempt_id), headers=AUTH
    )

    reused = await adapter_client_with_nso.post(
        "/api/v1/devices/provision", json=_provision_body(attempt_id, address="10.0.0.6"), headers=AUTH
    )

    assert reused.status_code == 409, reused.text
    error = reused.json()["error"]
    assert error["code"] == "conflict"
    assert error["detail"] == {
        "reason": "provision_attempt_mismatch",
        "provision_attempt_id": str(attempt_id),
        "job_id": int(first.json()["job_id"]),
    }
    assert await _provision_job_count() == 1


async def test_another_attempt_for_an_active_device_is_refused_with_the_active_attempt(adapter_client_with_nso):
    active_id = uuid.uuid4()
    first = await adapter_client_with_nso.post(
        "/api/v1/devices/provision", json=_provision_body(active_id), headers=AUTH
    )

    rival_id = uuid.uuid4()
    rival = await adapter_client_with_nso.post(
        "/api/v1/devices/provision", json=_provision_body(rival_id), headers=AUTH
    )

    assert rival.status_code == 409, rival.text
    error = rival.json()["error"]
    assert error["code"] == "conflict"
    assert error["detail"] == {
        "reason": "provision_active",
        "provision_attempt_id": str(active_id),
        "job_id": int(first.json()["job_id"]),
    }
    missing = await adapter_client_with_nso.get(f"/api/v1/provision-attempts/{rival_id}", headers=AUTH)
    assert missing.status_code == 404, "a refused attempt must stay unknown so the plugin closes its claim"


async def test_an_unknown_attempt_is_not_found(adapter_client):
    response = await adapter_client.get(f"/api/v1/provision-attempts/{uuid.uuid4()}", headers=AUTH)

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


async def test_a_provision_without_an_attempt_id_is_rejected(adapter_client_with_nso):
    response = await adapter_client_with_nso.post("/api/v1/devices/provision", json=_BODY, headers=AUTH)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert await _provision_job_count() == 0


async def test_the_attempt_route_requires_auth(adapter_client):
    response = await adapter_client.get(f"/api/v1/provision-attempts/{uuid.uuid4()}")

    assert response.status_code == 401
