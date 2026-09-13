# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""POST /api/v1/devices/provision is now async: it enqueues a ``provision`` job and
returns 202 + job_id instead of running the (slow) onboarding inline. These tests pin
the new contract and prove the durable worker drains the job end-to-end against the
real store (the only mock is the NSO HTTP boundary, spec-bound to NsoClient)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
from sqlalchemy import select

from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, Job, JobStatus, JobType
from tests.conftest import VALID_TOKEN, session, start_job

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

_PROVISION_BODY = {
    "nso_instance": "nso-dev",
    "device_name": "new-rtr",
    "address": "10.0.0.5",
    "ned_id": "cisco-ios-cli-6.114:cisco-ios-cli-6.114",
    "authgroup": "network",
}


def _ok_nso_client():
    """A spec=NsoClient AsyncMock with the onboarding methods stubbed to succeed."""
    c = AsyncMock(spec=NsoClient)
    c.device_exists.return_value = False
    c.sync_from.return_value = True
    return c


async def _active_provision_jobs():
    async with session() as db:
        rows = await db.execute(select(Job).where(Job.job_type == JobType.provision))
        return list(rows.scalars().all())
    return []


# ── 202 contract + enqueue ──────────────────────────────────────────────────


async def test_provision_returns_202_and_enqueues_job(adapter_client_with_nso):
    """POST /provision → 202 with a job_id; a queued provision Job carries the params."""
    resp = await adapter_client_with_nso.post("/api/v1/devices/provision", json=_PROVISION_BODY, headers=AUTH)
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "queued"
    assert body["nso_device_name"] == "new-rtr"
    assert body["job_id"]

    jobs = await _active_provision_jobs()
    assert len(jobs) == 1
    job = jobs[0]
    assert str(job.id) == body["job_id"]
    assert job.job_type == JobType.provision
    assert job.status == JobStatus.queued
    assert job.device_id is None  # no adapter Device row exists yet at enqueue time
    assert job.context["nso_instance"] == "nso-dev"
    assert job.context["device_name"] == "new-rtr"
    assert job.context["ned_id"] == _PROVISION_BODY["ned_id"]


async def test_provision_dedup_same_device_returns_same_job(adapter_client_with_nso):
    """A double-submit for the same (instance, device_name) returns the in-flight job."""
    first = await adapter_client_with_nso.post("/api/v1/devices/provision", json=_PROVISION_BODY, headers=AUTH)
    second = await adapter_client_with_nso.post("/api/v1/devices/provision", json=_PROVISION_BODY, headers=AUTH)
    assert first.status_code == 202 and second.status_code == 202
    assert first.json()["job_id"] == second.json()["job_id"]
    assert len(await _active_provision_jobs()) == 1, "must not enqueue a second provision for the same device"


async def test_provision_unknown_instance_returns_422(adapter_client):
    """An NSO instance not in config → 422 (no job enqueued)."""
    resp = await adapter_client.post(
        "/api/v1/devices/provision",
        json={**_PROVISION_BODY, "nso_instance": "ghost-nso"},
        headers=AUTH,
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


async def test_provision_requires_auth(adapter_client):
    resp = await adapter_client.post("/api/v1/devices/provision", json=_PROVISION_BODY)
    assert resp.status_code == 401


# ── worker drains the provision job end-to-end ──────────────────────────────


async def test_worker_runs_provision_job_creates_device(adapter_client_with_nso):
    """Enqueue → run the provision runner → job succeeds and the adapter Device row exists.

    Drives the real ``_run_provision`` runner (the worker's registered runner) against the
    real store; only the NSO HTTP client is faked (spec=NsoClient). Proves the async path
    actually provisions + maps, not just that a row was inserted.
    """
    from nso_adapter.core.jobs import _JOB_RUNNERS

    body = {**_PROVISION_BODY, "device_name": "drained-rtr", "netbox_device_id": 555}
    resp = await adapter_client_with_nso.post("/api/v1/devices/provision", json=body, headers=AUTH)
    job_id = int(resp.json()["job_id"])

    client = _ok_nso_client()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        await start_job(job_id)  # the worker head's transition, which this test stands in for
        await _JOB_RUNNERS[JobType.provision](job_id, None)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded, job.error
        assert job.result["ok"] is True
        steps = {s["step"]: s["status"] for s in job.result["steps"]}
        assert steps["create"] == "ok" and steps["adapter_mapping"] == "ok"
        # The provision created the adapter Device row, and the job is now linked to it.
        assert job.result["device_id"] is not None
        assert job.device_id == job.result["device_id"]
        dev = (await db.execute(select(Device).where(Device.nso_device_name == "drained-rtr"))).scalar_one_or_none()
        assert dev is not None and dev.netbox_device_id == 555

    # The NSO node was actually created + synced over the faked boundary.
    client.create_device.assert_awaited_once()
    client.sync_from.assert_awaited()


async def test_worker_provision_job_records_blocking_step_failure(adapter_client_with_nso):
    """A blocking step (create fails) → job SUCCEEDS (it ran) with result.ok False + steps.

    The provision core never raises for a blocking step; it returns ok=False so the plugin
    can show the steps. The job itself only fails on a crash/timeout.
    """
    from nso_adapter.core.jobs import _JOB_RUNNERS

    body = {**_PROVISION_BODY, "device_name": "boom-rtr"}
    resp = await adapter_client_with_nso.post("/api/v1/devices/provision", json=body, headers=AUTH)
    job_id = int(resp.json()["job_id"])

    client = _ok_nso_client()
    client.create_device.side_effect = RuntimeError("unreachable")
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        await start_job(job_id)  # the worker head's transition, which this test stands in for
        await _JOB_RUNNERS[JobType.provision](job_id, None)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["ok"] is False
        assert {s["step"]: s["status"] for s in job.result["steps"]}["create"] == "failed"


# ── the step detail carries no server bytes (#1698) ─────────────────────────

#: What a proxy or a NED can put in a host-key answer. Not UTF-8, so the parse raises.
_SERVER_BYTES = b"\xffplaceholder-server-secret"


def _provision_transport(host_key_answer: httpx.Response) -> httpx.MockTransport:
    """Real NSO wire: the node is created and unlocked, then the host-key action answers
    *host_key_answer*. Every call is one factory, because httpx reads the body once."""

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ssh/fetch-host-keys"):
            return httpx.Response(host_key_answer.status_code, content=host_key_answer.content)
        if request.method == "GET":
            return httpx.Response(404)  # device_exists → the create branch
        return httpx.Response(204)  # create (PUT) and admin-state (PATCH)

    return httpx.MockTransport(handle)


async def _drain_provision(client_http, device_name: str, transport: httpx.MockTransport) -> Job:
    """Enqueue a provision over the real API and run the real runner against *transport*."""
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.core.jobs import _JOB_RUNNERS

    resp = await client_http.post(
        "/api/v1/devices/provision", json={**_PROVISION_BODY, "device_name": device_name}, headers=AUTH
    )
    job_id = int(resp.json()["job_id"])
    nso = get_nso_client("nso-dev")  # the REAL client the lifespan registered
    nso._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso-dev:8080")
    with patch("nso_adapter.core.onboarding.asyncio.sleep", new=AsyncMock()):
        await start_job(job_id)
        await _JOB_RUNNERS[JobType.provision](job_id, None)
    async with session() as db:
        return await db.get(Job, job_id)


async def test_provision_step_detail_carries_no_bytes_from_the_host_key_answer(adapter_client_with_nso):
    """A malformed host-key body raises UnicodeDecodeError, whose repr COPIES the server's
    bytes. That repr reached the step detail, so it landed in the persisted job result and
    in every answer that serves it. The step must carry the classification alone."""
    transport = _provision_transport(httpx.Response(200, content=_SERVER_BYTES))
    job = await _drain_provision(adapter_client_with_nso, "badkey-rtr", transport)

    assert job.status == JobStatus.succeeded
    assert job.result["ok"] is False
    step = next(s for s in job.result["steps"] if s["step"] == "fetch_host_keys")
    assert step["status"] == "failed"
    assert "placeholder-server-secret" not in repr(job.result), "the job result repeats the server's bytes"
    assert step["detail"] == "UnicodeDecodeError", "the step must still name WHAT failed"


async def test_provision_step_detail_keeps_the_ADAPTER_AUTHORED_host_key_refusal(adapter_client_with_nso):
    """The other half of the inversion: our own refusal IS the diagnostic and must survive.

    NSO answers 200 with ``result: failed``; the client refuses it with a message the adapter
    wrote, which names the action and the failure kind and repeats nothing the action said."""
    answer = httpx.Response(200, json={"tailf-ncs:output": {"result": "failed", "info": "placeholder-server-secret"}})
    job = await _drain_provision(adapter_client_with_nso, "nokey-rtr", _provision_transport(answer))

    assert job.result["ok"] is False
    step = next(s for s in job.result["steps"] if s["step"] == "fetch_host_keys")
    assert "did not report a stored key" in step["detail"], "the authored refusal is the diagnostic"
    assert "placeholder-server-secret" not in repr(job.result), "the action's own text must not travel"
