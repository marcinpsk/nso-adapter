# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Durable refusal settlement through HTTP, the worker, and real PostgreSQL."""

import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from sqlalchemy import delete, event, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from nso_adapter.core import worker
from nso_adapter.core.claim import BookkeepingOutcomeUnknown, ClaimLostError, revoke_stale_claims
from nso_adapter.core.jobs import _JOB_RUNNERS
from nso_adapter.nso.client import DEVICE_INTENT_ROOT
from nso_adapter.nso.refusal_protocol import RowRef, refusal, row_ref
from nso_adapter.store.models import (
    ApplyExecutionAttempt,
    BgpRouterIntent,
    DbInterface,
    DeviceClaim,
    InterfaceAttrState,
    InterfaceIntent,
    Job,
    JobStatus,
    JobType,
    RowOutcome,
    SnmpV3UserIntent,
    StaticRouteTombstone,
    SyncState,
    VlanIntent,
)
from tests._secret_discipline import assert_records_free_of, assert_text_free_of
from tests.conftest import push_seq, seed_device, session
from tests.core.test_action_apply_promotion import _apply, _put_routes
from tests.core.test_generation_protocol import AUTH, generations, job_row, put_vlans, seed_settings
from tests.core.test_static_route_removal import SrFake
from tests.nso.test_apply_send import _client_with
from tests.nso.test_refusal_sender_cutover import CANARY, SITE, envelope

pytestmark = pytest.mark.anyio
PRIOR_AT = datetime(2025, 12, 31, 1, 2, 3, tzinfo=UTC)
PRIOR_ERROR = {"code": "prior", "message": "placeholder prior error"}


class RuntimeTransport(httpx.AsyncBaseTransport):
    def __init__(self, name):
        self.fake = SrFake(name, service=None)
        self.records = (SITE,)
        self.status = 500
        self.raw = None
        self.refuse = True
        self.verification_error = None
        self.preview_error = None
        self.device_sections = None
        self.before_send = asyncio.Event()
        self.release_send = asyncio.Event()
        self.release_send.set()
        self.documents = []

    async def handle_async_request(self, request):
        url = str(request.url)
        if request.method == "PUT" and "dry-run=" in url:
            error = self.verification_error if self.documents else self.preview_error
            if error is not None:
                return httpx.Response(error[0], content=error[1], request=request)
        if request.method == "PUT" and "dry-run=" not in url and "device-intent" in url:
            self.documents.append(json.loads(request.content)[DEVICE_INTENT_ROOT][0])
            self.before_send.set()
            await self.release_send.wait()
            if self.refuse:
                return httpx.Response(
                    self.status, content=self.raw if self.raw is not None else envelope(self.records), request=request
                )
        if request.method == "POST":
            if "device-state-read" in url and self.device_sections is not None:
                request_body = json.loads(request.content)["network-state-export:input"]
                sections = {
                    wire: self.device_sections.get(wire, {"status": "unsupported"}) for wire in request_body["family"]
                }
                return httpx.Response(
                    200,
                    json={
                        "network-state-export:output": {
                            "atomic": True,
                            "device-name": self.fake.device_name,
                            **sections,
                        }
                    },
                    request=request,
                )
            if "device-state-read" in url:
                return httpx.Response(200, json={"network-state-export:output": {"device-state": {}}}, request=request)
            return httpx.Response(200, json={"tailf-ncs:output": {"result": True}}, request=request)
        return await self.fake.handle(request.method.lower(), url, request.content)


async def setup(api, monkeypatch, *, operation="apply", family="vlan", admit=True, prior_state=SyncState.in_sync):
    name = "placeholder-refusal-device"
    device_id = await seed_device(nso_device_name=name, netbox_device_id=None)
    await seed_settings(device_id, auto_apply=False)
    transport = RuntimeTransport(name)
    client = _client_with(transport)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    if family == "bgp":
        fixture = json.loads((Path(__file__).parents[1] / "fixtures/refusal_settlement.json").read_text())["bgp"]
        response = await api.put(
            f"/api/v1/devices/{device_id}/bgp-intent",
            json={"routers": [{**fixture["accepted_payload"], "accepted_at": fixture["fence"]["acceptance_identity"]}]},
            headers=AUTH | push_seq(41),
        )
        assert_text_free_of(response.text, [CANARY])
        assert response.status_code == 200
        response = await _apply(api, device_id, {"bgp": 41})
        transport.records = (
            refusal(
                "bgp",
                row_ref(
                    "bgp", "bgp.router.scope.peer", (("router", ("64512",)), ("scope", ("",)), ("peer", ("192.0.2.1",)))
                ),
                "write-blocked",
            ),
        )
        model = BgpRouterIntent
    else:
        response = await put_vlans(api, device_id, [101, 102], seq=41)
        assert_text_free_of(response.text, [CANARY])
        assert response.status_code == 200
        response = await api.put(
            f"/api/v1/devices/{device_id}/intent",
            json={
                "attributes": [
                    {"interface": name, "attribute": "description", "intent_value": "placeholder description"}
                    for name in ("Ethernet1", "Ethernet2")
                ]
            },
            headers=AUTH | push_seq(41),
        )
        assert_text_free_of(response.text, [CANARY])
        assert response.status_code == 200
        if admit:
            response = await _apply(api, device_id, {"vlan": 41, "interface_config": 41})
        model = VlanIntent
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == (202 if admit else 200)
    async with session() as db:
        for row in await db.scalars(select(model).where(model.device_id == device_id)):
            row.last_apply_at = PRIOR_AT
            row.last_apply_error = deepcopy(PRIOR_ERROR)
        if family == "vlan":
            for row in await db.scalars(
                select(InterfaceIntent).join(DbInterface).where(DbInterface.device_id == device_id)
            ):
                row.last_apply_at = PRIOR_AT
                row.last_apply_error = deepcopy(PRIOR_ERROR)
            for state in await db.scalars(
                select(InterfaceAttrState).join(DbInterface).where(DbInterface.device_id == device_id)
            ):
                state.sync_state = prior_state
                state.last_checked_at = PRIOR_AT
                state.nso_value = "placeholder observed value"
        await db.commit()
    if operation == "removal":
        transport.refuse = False
        await run_next(device_id)
        response = await put_vlans(api, device_id, [101], seq=42, query="?delete_origin=true")
        assert_text_free_of(response.text, [CANARY])
        assert response.status_code == 200
        transport.refuse = True
        transport.documents.clear()
        transport.before_send.clear()
    return device_id, transport


async def run_next(device_id, *, runner=None):
    claimed = await worker._claim_next_job()
    if claimed is None:
        return None
    job_id, device, kind, reg = claimed
    assert device == device_id
    await worker._run_one_job(1, job_id, device, kind, runner or _JOB_RUNNERS[kind], reg)
    return job_id


async def evidence(device_id):
    async with session() as db:
        attempts = list(
            await db.scalars(
                select(ApplyExecutionAttempt)
                .where(ApplyExecutionAttempt.device_id == device_id)
                .order_by(ApplyExecutionAttempt.id)
            )
        )
        outcomes = list(
            await db.scalars(
                select(RowOutcome).where(RowOutcome.execution_attempt_id.in_([attempt.id for attempt in attempts]))
            )
        )
        return attempts, outcomes


async def terminal(api, device_id, *, refused=True):
    attempts, outcomes = await evidence(device_id)
    assert attempts, "worker must durably prepare before the first mutating RESTCONF request"
    attempt = attempts[-1]
    assert attempt.phase == "terminal"
    job = await job_row(attempt.job_correlation_id)
    assert job.status == JobStatus.failed
    outcomes = [outcome for outcome in outcomes if outcome.execution_attempt_id == attempt.id]
    assert outcomes or not attempt.logical_membership
    assert all(outcome.disposition != "in_sync" for outcome in outcomes)
    response = await api.get(f"/api/v1/jobs/{job.id}", headers=AUTH)
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    assert response.json()["row_outcomes"] == job.result["row_outcomes"]
    for key, counts in job.result.items():
        if key.endswith("_count_by_outcome"):
            assert set(counts) == {"in_sync", "apply_failed", "not_applied", "unavailable"}
    if refused:
        assert job.result["refusals"]
        assert any(outcome.disposition == "refused" for outcome in outcomes) or not attempt.logical_membership
    else:
        assert not job.result["refusals"]
        assert all(outcome.disposition == "unavailable" for outcome in outcomes)
    response = await api.post(
        f"/api/v1/devices/{device_id}/deployment-evidence", json={"apply_attempt_ids": []}, headers=AUTH
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    assert response.json()["head"]["row_outcomes"] == job.result["row_outcomes"]
    return job


class CommitGate:
    """Pause around a real checkpoint commit without replacing settlement behavior."""

    def __init__(self, monkeypatch, phase, *, before=False, failure=None, sender_phase=None):
        self.reached = asyncio.Event()
        self.release = asyncio.Event()
        self.phase = phase
        self.hit = False
        original = AsyncSession.commit

        def observe(db, _context, _instances=None):
            for row in (*db.new, *db.dirty):
                if isinstance(row, ApplyExecutionAttempt):
                    db.info["observed_attempt_phase"] = row.phase
                    db.info["observed_response_phase"] = row.response_phase

        self.observe = observe
        event.listen(Session, "before_flush", observe)
        event.listen(Session, "after_flush", observe)

        def observed(db):
            matches = db.info.get("observed_attempt_phase") == phase
            if sender_phase is not None:
                matches = matches and db.info.get("observed_response_phase") == sender_phase
            return matches

        async def commit(db):
            observe(db, None)
            matches = observed(db)
            if matches and not self.hit and before:
                self.hit = True
                self.reached.set()
                await self.release.wait()
                if failure:
                    raise failure
            await original(db)
            # The commit's own flush can set the phase; clear it so a later commit cannot match stale info.
            matches = matches or observed(db)
            db.info.pop("observed_attempt_phase", None)
            db.info.pop("observed_response_phase", None)
            if matches and not self.hit:
                self.hit = True
                self.reached.set()
                await self.release.wait()
                if failure:
                    raise failure

        monkeypatch.setattr(AsyncSession, "commit", commit)

    def close(self):
        event.remove(Session, "before_flush", self.observe)
        event.remove(Session, "after_flush", self.observe)


async def interrupt(
    api, monkeypatch, operation, checkpoint, *, failure=None, recovery=None, prior_state=SyncState.in_sync
):
    device_id, transport = await setup(api, monkeypatch, operation=operation, prior_state=prior_state)
    gate = CommitGate(monkeypatch, "response_recorded" if checkpoint == "refusal" else "prepared", failure=failure)
    task = asyncio.create_task(run_next(device_id))
    try:
        await asyncio.wait_for(gate.reached.wait(), 5)
        attempts, _outcomes = await evidence(device_id)
        assert attempts, "preparation must be durable before delivery"
        if checkpoint == "refusal":
            assert attempts[-1].response_classification["commit"]["kind"] == "refused"
        else:
            assert attempts[-1].phase == "prepared"
            assert transport.documents == [], "no mutating NSO request may precede preparation"
        if recovery:
            # A lost holder performs no cleanup; recovery owns the durable checkpoint.
            gate.release.set()
            with pytest.raises(ClaimLostError):
                await task
            async with session() as db:
                if recovery == "claimless":
                    await db.execute(delete(DeviceClaim).where(DeviceClaim.device_id == device_id))
                    await db.execute(
                        update(Job)
                        .where(Job.device_id == device_id, Job.status == JobStatus.running)
                        .values(heartbeat_at=None)
                    )
                else:
                    await db.execute(
                        update(DeviceClaim).where(DeviceClaim.device_id == device_id).values(heartbeat_at=PRIOR_AT)
                    )
                await db.commit()
            if recovery == "claimless":
                await worker.requeue_orphaned_jobs()
            else:
                await revoke_stale_claims(cutoff_seconds=0)
        else:
            if failure:
                gate.release.set()
                await task
            else:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
        job = await terminal(api, device_id, refused=checkpoint == "refusal")
        async with session() as db:
            states = list(
                await db.scalars(select(InterfaceAttrState).join(DbInterface).where(DbInterface.device_id == device_id))
            )
            expected = SyncState.in_sync if operation == "removal" else prior_state
            assert all(state.sync_state == expected for state in states), (
                "unnamed attributes must restore their exact prior state"
            )
            for row in await db.scalars(
                select(InterfaceIntent).join(DbInterface).where(DbInterface.device_id == device_id)
            ):
                if operation == "apply":
                    assert row.last_apply_at == PRIOR_AT and row.last_apply_error == PRIOR_ERROR
        assert len(transport.documents) == (1 if checkpoint == "refusal" else 0)
        await worker.recover_interrupted_work()
        assert await worker._claim_next_job() is None
        assert len(transport.documents) == (1 if checkpoint == "refusal" else 0)
        return job
    finally:
        if gate:
            gate.release.set()
            gate.close()
        transport.release_send.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_apply_refusal_changes_only_named_rows(adapter_client, monkeypatch):
    device_id, transport = await setup(adapter_client, monkeypatch)
    await run_next(device_id)
    job = await terminal(adapter_client, device_id)
    assert job.result["vlan_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1, "not_applied": 1, "unavailable": 0}
    async with session() as db:
        rows = {
            row.vlan_id: row for row in await db.scalars(select(VlanIntent).where(VlanIntent.device_id == device_id))
        }
        assert rows[101].last_apply_error["code"] == "refused"
        assert rows[102].last_apply_error == PRIOR_ERROR
        assert rows[101].last_apply_at == rows[102].last_apply_at == PRIOR_AT
    assert len(transport.documents) == 1
    assert not any(call["dry_run"] for call in transport.fake.calls)


@pytest.mark.parametrize("prior_state", [SyncState.in_sync, SyncState.accepted, SyncState.apply_failed])
async def test_apply_cancel_after_prepare_restores_attempt_state(adapter_client, monkeypatch, prior_state):
    await interrupt(adapter_client, monkeypatch, "apply", "prepare", prior_state=prior_state)


async def test_removal_cancel_after_prepare_restores_attempt_state(adapter_client, monkeypatch):
    await interrupt(adapter_client, monkeypatch, "removal", "prepare")


@pytest.mark.parametrize("prior_state", [SyncState.in_sync, SyncState.accepted, SyncState.apply_failed])
async def test_apply_cancel_after_refusal_checkpoint_settles_without_resend(adapter_client, monkeypatch, prior_state):
    await interrupt(adapter_client, monkeypatch, "apply", "refusal", prior_state=prior_state)


async def test_removal_cancel_after_refusal_checkpoint_settles_without_resend(adapter_client, monkeypatch):
    await interrupt(adapter_client, monkeypatch, "removal", "refusal")


@pytest.mark.parametrize("operation", ["apply", "removal"])
async def test_execution_budget_uses_attempt_disposition(adapter_client, monkeypatch, operation):
    device_id, transport = await setup(adapter_client, monkeypatch, operation=operation)
    transport.release_send.clear()
    monkeypatch.setattr(worker, "JOB_EXECUTION_BUDGET", 1.0)
    await run_next(device_id)
    await terminal(adapter_client, device_id, refused=False)
    assert await worker._claim_next_job() is None


@pytest.mark.parametrize("operation", ["apply", "removal"])
async def test_runner_failure_uses_attempt_disposition(adapter_client, monkeypatch, operation):
    await interrupt(adapter_client, monkeypatch, operation, "refusal", failure=RuntimeError(CANARY))


@pytest.mark.parametrize("operation", ["apply", "removal"])
@pytest.mark.parametrize("lane", ["claimless", "expired"])
async def test_recorded_refusal_recovery_lanes_do_not_resend(adapter_client, monkeypatch, operation, lane):
    await interrupt(
        adapter_client,
        monkeypatch,
        operation,
        "refusal",
        failure=ClaimLostError("placeholder claim lost"),
        recovery=lane,
    )


@pytest.mark.parametrize("lane", ["claimless", "expired"])
async def test_recovery_lane_isolates_an_attempt_that_lost_ownership(adapter_client, monkeypatch, lane):
    device_id, _transport = await setup(adapter_client, monkeypatch)
    gate = CommitGate(monkeypatch, "response_recorded", failure=ClaimLostError("placeholder lost holder"))
    task = asyncio.create_task(run_next(device_id))
    try:
        await asyncio.wait_for(gate.reached.wait(), 5)
        gate.release.set()
        with pytest.raises(ClaimLostError):
            await task
    finally:
        gate.release.set()
        gate.close()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    healthy = await seed_device(nso_device_name="placeholder-healthy-device", netbox_device_id=None)
    async with session() as db:
        # A job that no longer matches its attempt makes disposal raise AttemptOwnershipLost.
        await db.execute(update(Job).where(Job.device_id == device_id).values(job_type=JobType.sync))
        if lane == "claimless":
            await db.execute(delete(DeviceClaim).where(DeviceClaim.device_id == device_id))
            await db.execute(update(Job).where(Job.device_id == device_id).values(heartbeat_at=None))
            healthy_job = Job(
                job_type=JobType.sync, device_id=healthy, status=JobStatus.running, coalescible=True, heartbeat_at=None
            )
            db.add(healthy_job)
            await db.flush()
            healthy_job_id = healthy_job.id
        else:
            await db.execute(
                update(DeviceClaim).where(DeviceClaim.device_id == device_id).values(heartbeat_at=PRIOR_AT)
            )
            db.add(
                DeviceClaim(
                    device_id=healthy,
                    claim_token="placeholder-healthy-token",
                    purpose="intent_put",
                    job_id=None,
                    heartbeat_at=PRIOR_AT,
                )
            )
        await db.commit()
    if lane == "claimless":
        await worker.requeue_orphaned_jobs()
        assert (await job_row(healthy_job_id)).status == JobStatus.queued
    else:
        revoked = await revoke_stale_claims(cutoff_seconds=0)
        assert [entry.device_id for entry in revoked] == [healthy]
        async with session() as db:
            assert set(await db.scalars(select(DeviceClaim.device_id))) == {device_id}
    attempts, outcomes = await evidence(device_id)
    assert attempts[-1].phase != "terminal" and not outcomes
    assert (await job_row(attempts[-1].job_correlation_id)).status == JobStatus.running


async def test_attempt_membership_excludes_a_carried_stream_the_job_did_not_promote(adapter_client, monkeypatch):
    device_id, transport = await setup(adapter_client, monkeypatch)
    transport.refuse = False
    await run_next(device_id)
    response = await put_vlans(adapter_client, device_id, [101, 102, 103], seq=42)
    assert response.status_code == 200
    response = await _apply(adapter_client, device_id, {"vlan": 42})
    assert response.status_code == 202, response.text
    transport.refuse = True
    transport.documents.clear()
    await run_next(device_id)
    assert "interface" in json.dumps(transport.documents[-1]), "the document must still carry interface_config"
    job = await terminal(adapter_client, device_id)
    attempts, _outcomes = await evidence(device_id)
    assert {logical["stream"] for logical in attempts[-1].logical_membership} == {"vlan"}
    assert all(row["stream"] == "vlan" for row in job.result["row_outcomes"])


@pytest.mark.parametrize("field", ["auth-credential-ref", "priv-credential-ref"])
async def test_first_apply_missing_snmp_reference_refuses_only_user(adapter_client, monkeypatch, field):
    device_id, transport = await setup(adapter_client, monkeypatch, admit=False)
    user = {"username": CANARY, "auth_protocol": "sha"}
    if field == "priv-credential-ref":
        user.update(auth_vault_ref="placeholder/auth#value", priv_protocol="aes-128")
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/snmp-intent", json={"v3_users": [user]}, headers=AUTH | push_seq(41)
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    response = await _apply(adapter_client, device_id, {"vlan": 41, "interface_config": 41, "snmp": 41})
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    site = row_ref("snmp", "snmp.v3-user", (("v3-user", (CANARY,)),))
    transport.records = (
        refusal(
            "snmp",
            site,
            "snmp.auth-reference-missing" if field == "auth-credential-ref" else "snmp.priv-reference-missing",
            field="snmp.v3_user.auth_secret" if field == "auth-credential-ref" else "snmp.v3_user.priv_secret",
        ),
    )
    await run_next(device_id)
    job = await terminal(adapter_client, device_id)
    users = transport.documents[-1]["snmp"]["v3-user"]
    assert len(users) == 1 and users[0]["username"] == CANARY and field not in users[0]
    assert [
        (row["row"]["subject"], row["disposition"]) for row in job.result["row_outcomes"] if row["stream"] == "snmp"
    ] == [("snmp.v3-user", "refused")]
    attempts, outcomes = await evidence(device_id)
    assert_text_free_of(
        json.dumps([attempt.safe_origins for attempt in attempts]) + json.dumps(job.result) + repr(outcomes), [CANARY]
    )
    async with session() as db:
        user_row = await db.scalar(select(SnmpV3UserIntent).where(SnmpV3UserIntent.device_id == device_id))
        assert user_row.last_apply_at is None and user_row.last_apply_error["code"] == "refused"


async def test_child_refusal_restores_unnamed_peer(adapter_client, monkeypatch):
    device_id, transport = await setup(adapter_client, monkeypatch, family="bgp")
    await run_next(device_id)
    job = await terminal(adapter_client, device_id)
    peers = [row for row in job.result["row_outcomes"] if row["row"]["subject"] == "bgp.router.scope.peer"]
    assert sorted(row["disposition"] for row in peers) == ["not_applied", "refused"]
    assert peers[0]["carrier"] == peers[1]["carrier"]
    fixture = json.loads((Path(__file__).parents[1] / "fixtures/refusal_settlement.json").read_text())["bgp"]
    assert {row["row"]["key"] for row in peers} == {row["row"]["key"] for row in fixture["expected"]}
    assert all(
        row["source_push_seq"] == 41 and row["acceptance_identity"] == fixture["fence"]["acceptance_identity"]
        for row in peers
    )
    assert job.result["bgp_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1, "not_applied": 0, "unavailable": 0}
    assert len(transport.documents) == 1


async def test_child_refusal_restores_unnamed_peer_after_restart(adapter_client, monkeypatch):
    device_id, transport = await setup(adapter_client, monkeypatch, family="bgp")
    gate = CommitGate(monkeypatch, "response_recorded", failure=ClaimLostError("placeholder lost holder"))
    task = asyncio.create_task(run_next(device_id))
    try:
        await asyncio.wait_for(transport.before_send.wait(), 5)
        assert (await evidence(device_id))[0], "child attempt membership must be durable"
        await asyncio.wait_for(gate.reached.wait(), 5)
        gate.release.set()
        with pytest.raises(ClaimLostError):
            await task
        async with session() as db:
            await db.execute(
                update(DeviceClaim).where(DeviceClaim.device_id == device_id).values(heartbeat_at=PRIOR_AT)
            )
            await db.commit()
        await revoke_stale_claims(cutoff_seconds=0)
        job = await terminal(adapter_client, device_id)
        assert sorted(
            row["disposition"] for row in job.result["row_outcomes"] if row["row"]["subject"] == "bgp.router.scope.peer"
        ) == ["not_applied", "refused"]
        assert len(transport.documents) == 1
    finally:
        gate.release.set()
        gate.close()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("writer_first", [True, False])
async def test_child_refusal_preserves_successor_peer(adapter_client, monkeypatch, rival_engine, writer_first):
    device_id, transport = await setup(adapter_client, monkeypatch, family="bgp")
    transport.release_send.clear()
    task = asyncio.create_task(run_next(device_id))
    try:
        await asyncio.wait_for(transport.before_send.wait(), 5)
        attempts, _ = await evidence(device_id)
        assert attempts, "successor fencing requires the frozen logical membership"
        fixture = json.loads((Path(__file__).parents[1] / "fixtures/refusal_settlement.json").read_text())["bgp"][
            "accepted_payload"
        ]
        fixture["scopes"][0]["peers"][1]["remote_as"] = "64515"
        if not writer_first:
            transport.release_send.set()
            await task
        response = await adapter_client.put(
            f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [fixture]}, headers=AUTH | push_seq(42)
        )
        assert_text_free_of(response.text, [CANARY])
        assert response.status_code == 200
        if writer_first:
            transport.release_send.set()
            await task
        old_job = await job_row(attempts[-1].job_correlation_id)
        assert any(
            row["disposition"] == "not_applied"
            for row in old_job.result["row_outcomes"]
            if row["row"]["subject"] == "bgp.router.scope.peer"
        )
        async with session() as db:
            router = await db.scalar(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))
            assert router.last_apply_error is None
    finally:
        transport.release_send.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("site", ["peer", "peer-af"])
async def test_refusal_origin_handles_children_retention_and_omission(adapter_client, monkeypatch, site):
    device_id, transport = await setup(adapter_client, monkeypatch, family="bgp")
    if site == "peer-af":
        path = (
            ("router", ("64512",)),
            ("scope", ("",)),
            ("peer", ("192.0.2.1",)),
            ("peer-address-family", ("ipv4-unicast",)),
        )
        transport.records = (
            refusal("bgp", row_ref("bgp", "bgp.router.scope.peer.peer-address-family", path), "write-blocked"),
        )
    await run_next(device_id)
    job = await terminal(adapter_client, device_id)
    named = [row for row in job.result["row_outcomes"] if row["disposition"] == "refused"]
    assert len(named) == 1 and named[0]["row"]["subject"] == "bgp.router.scope.peer"
    assert named[0]["refusals"][0]["row"]["subject"].endswith(site.replace("peer-af", "peer-address-family"))


@pytest.mark.parametrize("mode", ["valid", "malformed", "duplicate", "escaped", "transport"])
async def test_refusal_secret_canary_across_the_apply_path(adapter_client, monkeypatch, debug_logs, mode):
    device_id, transport = await setup(adapter_client, monkeypatch)
    if mode == "malformed":
        transport.raw = b"device-intent: refused " + CANARY.encode()
    elif mode == "duplicate":
        transport.raw = envelope()[:-1] + b',"secret":"' + CANARY.encode() + b'","secret":"device-intent: refused"}'
    elif mode == "escaped":
        transport.raw = b'{"message":"device-intent: refu\\u0073ed ' + CANARY.encode()
    elif mode == "transport":
        transport.release_send.clear()
        monkeypatch.setattr(worker, "JOB_EXECUTION_BUDGET", 1.0)
    await run_next(device_id)
    job = await terminal(adapter_client, device_id, refused=mode == "valid")
    attempts, outcomes = await evidence(device_id)
    assert_text_free_of(
        json.dumps(job.result)
        + json.dumps(job.error)
        + json.dumps([attempt.response_classification for attempt in attempts])
        + repr(outcomes),
        [CANARY],
    )
    assert_records_free_of(debug_logs, [CANARY])


async def test_refusal_api_and_counts_use_public_owner_identity(adapter_client, monkeypatch):
    device_id, transport = await setup(adapter_client, monkeypatch)
    preview = await adapter_client.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert_text_free_of(preview.text, [CANARY])
    assert preview.status_code == 200, preview.text
    # Dry-run success and commit refusal must use distinct evidence.
    assert preview.json().get("status") == "ready"
    await run_next(device_id)
    job = await terminal(adapter_client, device_id)
    assert job.result["row_outcomes"][0]["carrier"]["row"]["subject"] == "vlan.vlan"
    assert "sources" not in job.result["row_outcomes"][0]


async def removal_with_tombstone(api, monkeypatch, *, refusal_kind="refused"):
    device_id, transport = await setup(api, monkeypatch)
    transport.refuse = False
    await run_next(device_id)
    route = {"route_id": 1, "vrf": "", "prefix": "198.18.0.0/24", "next_hop": "192.0.2.1"}
    response = await _put_routes(api, device_id, [route], seq=41)
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    response = await _apply(api, device_id, {"static_route": 41})
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    await run_next(device_id)
    from tests.api.test_static_route_deleted_routes import deleted

    response = await _put_routes(api, device_id, [], seq=42, deleted=[deleted(1, [("", "198.18.0.0/24", "192.0.2.1")])])
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    transport.refuse = True
    if refusal_kind == "unavailable":
        transport.status = 503
        transport.raw = b"opaque " + CANARY.encode()
    transport.documents.clear()
    await run_next(device_id)
    assert (await evidence(device_id))[0], "removal must retain durable execution evidence"
    assert (await evidence(device_id))[0][-1].phase == "terminal"
    return device_id, transport


@pytest.mark.parametrize("prune", [False, True])
@pytest.mark.parametrize("lane", ["sweeper", "reclaimer"])
@pytest.mark.parametrize("refusal_kind", ["refused", "unavailable"])
async def test_refused_removal_is_not_reissued_by_sweeper(adapter_client, monkeypatch, prune, lane, refusal_kind):
    device_id, transport = await removal_with_tombstone(adapter_client, monkeypatch, refusal_kind=refusal_kind)
    attempts, _ = await evidence(device_id)
    refused = attempts[-1]
    assert refused.response_classification["commit"]["kind"] == refusal_kind
    frozen_result = (await job_row(refused.job_correlation_id)).result
    if prune:
        async with session() as db:
            await db.execute(delete(Job).where(Job.id == refused.job_correlation_id))
            await db.commit()
    from nso_adapter.core.tombstone_sweep import sweep_one_device

    before = len(await generations(device_id))
    if lane == "sweeper":
        assert await sweep_one_device(device_id) == 0
    else:
        from nso_adapter.core.static_route_reclaim import reclaim_one_device
        from nso_adapter.store.models import JobType

        async with session() as db:
            owner = Job(device_id=device_id, job_type=JobType.removal, status=JobStatus.succeeded, coalescible=False)
            db.add(owner)
            await db.flush()
            tombstone = await db.scalar(select(StaticRouteTombstone).where(StaticRouteTombstone.device_id == device_id))
            tombstone.job_id = owner.id
            await db.commit()
        assert await reclaim_one_device(device_id) == (0, 0)
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence", json={"apply_attempt_ids": []}, headers=AUTH
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    assert response.json()["head"]["row_outcomes"] == frozen_result["row_outcomes"]
    assert response.json()["head"]["refusals"] == frozen_result["refusals"]
    head = (await generations(device_id))[-1]
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/abandon-generation", json={"generation_id": head.id}, headers=AUTH
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    await run_next(device_id)
    assert len(await generations(device_id)) == before
    assert len(transport.documents) == 1


async def test_explicit_retry_reissues_refused_removal(adapter_client, monkeypatch):
    device_id, transport = await removal_with_tombstone(adapter_client, monkeypatch)
    head = (await generations(device_id))[-1]
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/retry-generation", json={"generation_id": head.id}, headers=AUTH
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    await run_next(device_id)
    attempts, _ = await evidence(device_id)
    assert len([attempt for attempt in attempts if attempt.operation_kind == "removal"]) == 2
    assert len(transport.documents) == 2


async def test_unattempted_tombstone_still_reissued(adapter_client, monkeypatch):
    from nso_adapter.core.tombstone_sweep import sweep_one_device

    device_id, transport = await setup(adapter_client, monkeypatch)
    transport.refuse = False
    await run_next(device_id)
    response = await _put_routes(adapter_client, device_id, [], seq=41)
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    response = await _apply(adapter_client, device_id, {"static_route": 41})
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    await run_next(device_id)
    async with session() as db:
        db.add(
            StaticRouteTombstone(
                device_id=device_id,
                route_id=1,
                vrf="",
                prefix="198.18.0.0/24",
                next_hop="192.0.2.1",
                marking="delete_origin",
            )
        )
        await db.commit()
    assert await sweep_one_device(device_id) == 1
    await run_next(device_id)
    await run_next(device_id)
    attempts, _ = await evidence(device_id)
    assert attempts and attempts[-1].operation_kind == "removal", (
        "reissued removals must use the same durable execution path"
    )


@pytest.mark.parametrize("before", [False, True])
async def test_terminal_commit_interruption_has_no_torn_evidence(adapter_client, monkeypatch, before):
    device_id, transport = await setup(adapter_client, monkeypatch)
    gate = CommitGate(
        monkeypatch,
        "terminal",
        before=before,
        failure=BookkeepingOutcomeUnknown("placeholder terminal acknowledgement lost"),
    )
    task = asyncio.create_task(run_next(device_id))
    try:
        await asyncio.wait_for(transport.before_send.wait(), 5)
        assert (await evidence(device_id))[0], "terminal settlement requires prepared execution evidence"
        await asyncio.wait_for(gate.reached.wait(), 5)
        gate.release.set()
        await task
        async with session() as db:
            await db.execute(
                update(DeviceClaim).where(DeviceClaim.device_id == device_id).values(heartbeat_at=PRIOR_AT)
            )
            await db.commit()
        await revoke_stale_claims(cutoff_seconds=0)
        await terminal(adapter_client, device_id)
        assert len(transport.documents) == 1
    finally:
        gate.release.set()
        gate.close()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_post_commit_refusal_exposes_verification_without_rollback(adapter_client, monkeypatch):
    device_id, transport = await setup(adapter_client, monkeypatch)
    transport.refuse = False
    transport.verification_error = (500, envelope())
    await run_next(device_id)
    attempts, outcomes = await evidence(device_id)
    assert attempts[-1].commit_acknowledged
    assert all(row.disposition == "unavailable" for row in outcomes)
    job = await job_row(attempts[-1].job_correlation_id)
    assert job.result["refusals"], "a verification refusal must remain public evidence"
    assert job.result["refusals"][0]["sender_phase"] == "post_commit_verification"
    assert all(not row["refusals"] for row in job.result["row_outcomes"])
    async with session() as db:
        for row in await db.scalars(select(VlanIntent).where(VlanIntent.device_id == device_id)):
            assert row.last_apply_at == PRIOR_AT and row.last_apply_error == PRIOR_ERROR
    assert len(transport.documents) == 1


@pytest.mark.parametrize("outformat", ["native", "cli"])
@pytest.mark.parametrize(
    "kind,expected", [("refused", "refused"), ("fault", "error"), ("unknown", "error"), ("unavailable", "unavailable")]
)
async def test_preview_exposes_attribution_and_phase(adapter_client, monkeypatch, outformat, kind, expected):
    device_id, transport = await setup(adapter_client, monkeypatch)
    if kind == "unknown":
        foreign = refusal("logging", row_ref("logging", "logging.host", (("host", ("192.0.2.99",)),)), "write-blocked")
        transport.preview_error = (500, envelope((SITE, foreign)))
    elif kind == "fault":
        transport.preview_error = (500, b"device-intent: refused " + CANARY.encode())
    elif kind == "unavailable":
        transport.preview_error = (503, b"opaque " + CANARY.encode())
    else:
        transport.preview_error = (500, envelope())
    response = await adapter_client.get(
        f"/api/v1/devices/{device_id}/actions/apply-diff", params={"outformat": outformat}, headers=AUTH
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == expected
    if kind == "refused":
        assert body["refusals"][0].get("sender_phase") == "pre_commit_dry_run"
        assert body["refusals"][0]["logical_rows"][0]["row"]["key"] == SITE.row.key
    else:
        assert body["refusals"] == []
    assert "!! preview unavailable" in body["diffs"]["device_intent"], "a non-ready preview never reads as no delta"
    if kind == "unknown":
        assert "refusal attribution failed" in body["diffs"]["device_intent"]
    assert not transport.documents
    assert (await evidence(device_id)) == ([], [])


async def test_reader_proof_identity_includes_source_table(adapter_client, monkeypatch):
    from nso_adapter.store.models import IsisInterfaceIntent, IsisProcessIntent

    device_id, transport = await setup(adapter_client, monkeypatch, admit=False)
    body = {
        "interfaces": [{"interface_name": "Ethernet1", "af": "ipv4", "process_tag": "placeholder-process"}],
        "processes": [{"process_tag": "placeholder-process"}],
    }
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent", json=body, headers=AUTH | push_seq(41)
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    response = await _apply(adapter_client, device_id, {"isis": 41})
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    transport.refuse = False
    transport.device_sections = {
        "isis-interface": {"status": "ok", "interface": [{"interface-name": "Ethernet1", "af": "ipv4"}], "process": []}
    }
    await run_next(device_id)
    attempts, outcomes = await evidence(device_id)
    by_subject = {row.row_subject: row.disposition for row in outcomes}
    assert by_subject["isis.interface-config"] == "in_sync", (
        "a missing process must not replace an interface's proof with the same ORM id"
    )
    assert by_subject["isis.process-config"] == "unavailable"
    async with session() as db:
        interface = await db.scalar(select(IsisInterfaceIntent).where(IsisInterfaceIntent.device_id == device_id))
        process = await db.scalar(select(IsisProcessIntent).where(IsisProcessIntent.device_id == device_id))
        assert interface.id == process.id
        assert interface.last_apply_at is not None and process.last_apply_at is None
    job = await job_row(attempts[-1].job_correlation_id)
    assert job.result["isis_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0, "not_applied": 0, "unavailable": 1}


async def test_missing_peer_proof_withholds_carrier_success(adapter_client, monkeypatch):
    device_id, transport = await setup(adapter_client, monkeypatch, family="bgp")
    transport.refuse = False
    transport.device_sections = {
        "bgp-config": {
            "status": "ok",
            "router": [{"asn": 64512, "scope": [{"vrf": "", "peer": [{"peer-address": "192.0.2.2"}]}]}],
        }
    }
    await run_next(device_id)
    attempts, outcomes = await evidence(device_id)
    assert all(row.disposition == "unavailable" for row in outcomes), (
        "conflicting peer proof must not certify a carrier's unproven children"
    )
    job = await job_row(attempts[-1].job_correlation_id)
    assert job.result["bgp_count_by_outcome"] == {"in_sync": 0, "apply_failed": 0, "not_applied": 0, "unavailable": 1}


async def lock_wait_seen(db, query, pid):
    from sqlalchemy import text

    # PostgreSQL snapshots pg_stat_activity per transaction; a backend that connects later is invisible without a clear.
    await db.execute(text("SELECT pg_stat_clear_snapshot()"))
    return await db.scalar(text(query), {"pid": pid})


async def blocked_writer(db, writer_pid):
    async with asyncio.timeout(5):
        while not await lock_wait_seen(
            db,
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = :pid AND cardinality(pg_blocking_pids(pid)) > 0)",
            writer_pid,
        ):
            await asyncio.wait([asyncio.create_task(asyncio.sleep(0.01))])


@pytest.mark.parametrize("writer_first", [True, False])
@pytest.mark.parametrize("named", [True, False])
@pytest.mark.parametrize("observation", [True, False])
async def test_worker_refusal_preserves_both_successor_schedules(
    adapter_client, monkeypatch, rival_engine, writer_first, named, observation
):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker

    device_id, transport = await setup(adapter_client, monkeypatch, prior_state=SyncState.accepted)
    cause = refusal(
        "interface", row_ref("interface", "interface.description", (("interface", ("Ethernet1",)),)), "write-blocked"
    )
    transport.records = (cause,)
    gate = CommitGate(monkeypatch, "response_recorded" if writer_first else "terminal", before=not writer_first)
    task = asyncio.create_task(run_next(device_id))
    writer = None
    try:
        await asyncio.wait_for(gate.reached.wait(), 5)
        async with session() as db:
            interface = await db.scalar(
                select(DbInterface).where(
                    DbInterface.device_id == device_id, DbInterface.name == ("Ethernet1" if named else "Ethernet2")
                )
            )
            intent_id = await db.scalar(select(InterfaceIntent.id).where(InterfaceIntent.interface_id == interface.id))
            state_id = await db.scalar(
                select(InterfaceAttrState.id).where(InterfaceAttrState.interface_id == interface.id)
            )
        next_at = datetime(2027, 1, 1, tzinfo=UTC)
        change = (
            update(InterfaceAttrState)
            .where(InterfaceAttrState.id == state_id)
            .values(sync_state=SyncState.changed, last_checked_at=next_at, nso_value="placeholder new observation")
            if observation
            else update(InterfaceIntent)
            .where(InterfaceIntent.id == intent_id)
            .values(
                intent_value="placeholder successor",
                accepted_at=next_at,
                last_apply_at=next_at,
                last_apply_error={"code": "successor"},
            )
        )
        async with async_sessionmaker(rival_engine, expire_on_commit=False)() as rival:
            pid = await rival.scalar(text("SELECT pg_backend_pid()"))
            if writer_first:
                await rival.execute(change)
                gate.release.set()
                async with session() as observer:
                    async with asyncio.timeout(5):
                        while not await lock_wait_seen(
                            observer,
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE :pid = ANY(pg_blocking_pids(pid)))",
                            pid,
                        ):
                            await asyncio.wait([asyncio.create_task(asyncio.sleep(0.01))])
                await rival.commit()
            else:

                async def write():
                    await rival.execute(change)
                    await rival.commit()

                writer = asyncio.create_task(write())
                async with session() as observer:
                    await blocked_writer(observer, pid)
                gate.release.set()
                await writer
        await task
        async with session() as db:
            intent = await db.get(InterfaceIntent, intent_id)
            state = await db.get(InterfaceAttrState, state_id)
            if observation:
                assert (state.sync_state, state.last_checked_at, state.nso_value) == (
                    SyncState.changed,
                    next_at,
                    "placeholder new observation",
                )
            else:
                assert (intent.intent_value, intent.accepted_at, intent.last_apply_at, intent.last_apply_error) == (
                    "placeholder successor",
                    next_at,
                    next_at,
                    {"code": "successor"},
                )
        attempts, outcomes = await evidence(device_id)
        own = [row for row in outcomes if row.row_subject == "interface.description"]
        assert sorted(row.disposition for row in own) == ["not_applied", "refused"]
        assert attempts[-1].phase == "terminal" and len(transport.documents) == 1
    finally:
        gate.release.set()
        gate.close()
        for pending in (writer, task):
            if pending is not None and not pending.done():
                pending.cancel()
        await asyncio.gather(*(pending for pending in (writer, task) if pending is not None), return_exceptions=True)


@pytest.mark.parametrize("case_index", [0, 2, 3, 4, 5, 6])
async def test_shared_child_mappings_through_worker(adapter_client, monkeypatch, case_index):
    fixture = json.loads((Path(__file__).parents[1] / "fixtures/refusal_settlement.json").read_text())
    case = fixture["mapping_cases"][case_index]
    device_id = await seed_device(nso_device_name="placeholder-mapping-device")
    await seed_settings(device_id, auto_apply=False)
    transport = RuntimeTransport("placeholder-mapping-device")
    client = _client_with(transport)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    family = case["family"]
    stream = family.replace("-", "_")
    if family == "lag":
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/lag-config/apply",
            json={
                "bundles": [
                    {
                        "name": "Port-channel1",
                        "lag_id": 1,
                        "members": [
                            {"interface_name": "GigabitEthernet0/1", "mode": "active"},
                            {"interface_name": "GigabitEthernet0/2", "mode": "active"},
                        ],
                    }
                ],
                "source_revision": 41,
                "deleted_roots": [],
            },
            headers=AUTH,
        )
    elif family == "switchport":
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/switchport/apply",
            json={
                "interfaces": [{"interface_name": "GigabitEthernet0/3", "mode": "trunk", "tagged_vlans": [101, 102]}],
                "source_revision": 41,
                "deleted_roots": [],
            },
            headers=AUTH,
        )
    elif family == "route-policy":
        response = await adapter_client.put(
            f"/api/v1/devices/{device_id}/route-policy-intent",
            json={
                "objects": [
                    {
                        "family": "prefix_list",
                        "name": "PL-OUT",
                        "entries": [{"sequence": 10, "action": "permit", "prefix": "198.18.0.0/24"}],
                        "accepted": True,
                    }
                ]
            },
            headers=AUTH | push_seq(41),
        )
    elif family == "bgp":
        payload = deepcopy(fixture["bgp"]["accepted_payload"])
        payload["scopes"][0]["address_families"] = [
            {"af": "ipv4-unicast", "redistribution": [{"source_protocol": "static"}]}
        ]
        response = await adapter_client.put(
            f"/api/v1/devices/{device_id}/bgp-intent", json={"routers": [payload]}, headers=AUTH | push_seq(41)
        )
    else:
        payload = {"process_id": "1", "redistribution": [{"source_protocol": "connected"}]}
        if family == "isis":
            payload = {
                "process_tag": "CORE",
                "net": "49.0001.0000.0000.0001.00",
                "redistribution": [{"source_protocol": "connected"}],
            }
        response = await adapter_client.put(
            f"/api/v1/devices/{device_id}/" + ("isis-interface-intent" if family == "isis" else "ospf-intent"),
            json={"processes": [payload], "interfaces": []} if family == "isis" else {"instances": [payload]},
            headers=AUTH | push_seq(41),
        )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    response = await _apply(
        adapter_client,
        device_id,
        {stream: response.json()["selection_revision"] if family in {"lag", "switchport"} else 41},
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    from nso_adapter.nso.refusal_protocol import RowRef

    transport.records = (
        refusal(
            family,
            RowRef(**case["cause"]),
            "write-blocked",
            field="switchport.tagged_vlans" if family == "switchport" else None,
        ),
    )
    await run_next(device_id)
    job = await terminal(adapter_client, device_id)
    named = [row for row in job.result["row_outcomes"] if row["disposition"] == "refused"]
    assert len(named) == 1
    assert named[0]["row"] == case["logical"]
    assert named[0]["carrier"]["row"] == case["carrier"]
    assert sum(counts["apply_failed"] for key, counts in job.result.items() if key.endswith("_count_by_outcome")) == 1


@pytest.mark.parametrize("baseline", ["acknowledged", "missing", "changed"])
async def test_refusal_origin_handles_retained_route_and_prior_omission(adapter_client, monkeypatch, baseline):
    from nso_adapter.store.models import DeploymentGeneration
    from tests.api.test_static_route_deleted_routes import deleted

    device_id, transport = await setup(adapter_client, monkeypatch)
    transport.refuse = False
    await run_next(device_id)
    route = {"route_id": 1, "vrf": "", "prefix": "198.18.0.0/24", "next_hop": "192.0.2.1"}
    response = await _put_routes(adapter_client, device_id, [route], seq=41)
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    response = await _apply(adapter_client, device_id, {"static_route": 41})
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    if baseline == "missing":
        transport.raw = b"{}"
        transport.refuse = True
        await run_next(device_id)
        transport.raw = None
        transport.fake.service = [{"vrf": "", "prefix": route["prefix"], "next-hop": route["next_hop"]}]
        head = (await generations(device_id))[-1]
        response = await adapter_client.post(
            f"/api/v1/devices/{device_id}/actions/abandon-generation", json={"generation_id": head.id}, headers=AUTH
        )
        assert_text_free_of(response.text, [CANARY])
        assert response.status_code == 202
    else:
        await run_next(device_id)
    if baseline == "changed":
        transport.fake.service[0]["tag"] = 77
    cause = refusal(
        "static-route",
        row_ref("static-route", "static-route.route", (("route", ("", route["prefix"], route["next_hop"])),)),
        "write-blocked",
    )
    response = await _put_routes(
        adapter_client, device_id, [], seq=42, deleted=[deleted(1, [("", route["prefix"], route["next_hop"])])]
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    transport.refuse = True
    transport.records = (cause,)
    await run_next(device_id)
    attempts, _ = await evidence(device_id)
    omission = await job_row(attempts[-1].job_correlation_id)
    assert omission.result["commit_acknowledged"] is False
    if baseline != "missing":
        assert omission.result["refusals"][0]["provenance"] == "prior"
    else:
        assert omission.result["refusals"] == []
    head = (await generations(device_id))[-1]
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/abandon-generation", json={"generation_id": head.id}, headers=AUTH
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    response = await put_vlans(adapter_client, device_id, [101, 102], seq=42)
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    response = await _apply(adapter_client, device_id, {"vlan": 42})
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    await run_next(device_id)
    attempts, outcomes = await evidence(device_id)
    job = await job_row(attempts[-1].job_correlation_id)
    own = [row for row in outcomes if row.execution_attempt_id == attempts[-1].id]
    assert own
    if baseline == "acknowledged":
        assert job.result["refusals"][0]["provenance"] == "retained"
        assert all(row.disposition == "not_applied" for row in own)
    else:
        assert job.result["refusals"] == []
        assert job.error["code"] == "attribution_failed"
        assert all(row.disposition == "unavailable" for row in own)
    async with session() as db:
        assert await db.scalar(select(StaticRouteTombstone.id).where(StaticRouteTombstone.device_id == device_id))
        assert (await db.get(DeploymentGeneration, attempts[-1].generation_id)).carrier_job_result == job.result


@pytest.mark.parametrize("reclaimer_first", [True, False])
async def test_worker_claim_loss_serializes_with_terminal_settlement(
    adapter_client, monkeypatch, rival_engine, reclaimer_first
):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from nso_adapter.core.claim import acquire_claim, release_claim

    device_id, transport = await setup(adapter_client, monkeypatch, prior_state=SyncState.apply_failed)
    transport.records = (
        refusal(
            "interface",
            row_ref("interface", "interface.description", (("interface", ("Ethernet1",)),)),
            "write-blocked",
        ),
    )
    gate = CommitGate(monkeypatch, "response_recorded" if reclaimer_first else "terminal", before=not reclaimer_first)
    task = asyncio.create_task(run_next(device_id))
    recovery = None
    successor = None
    try:
        await asyncio.wait_for(gate.reached.wait(), 5)
        async with async_sessionmaker(rival_engine, expire_on_commit=False)() as rival:
            pid = await rival.scalar(text("SELECT pg_backend_pid()"))
            if reclaimer_first:
                assert await revoke_stale_claims(db=rival, cutoff_seconds=0)
                successor = await acquire_claim(device_id, "job")
                assert successor is not None
                async with session() as db:
                    interface = await db.scalar(
                        select(DbInterface).where(DbInterface.device_id == device_id, DbInterface.name == "Ethernet2")
                    )
                    row = await db.scalar(select(InterfaceIntent).where(InterfaceIntent.interface_id == interface.id))
                    row.last_apply_at = datetime(2027, 1, 1, tzinfo=UTC)
                    row.last_apply_error = {"code": "successor"}
                    await db.commit()
                gate.release.set()
            else:
                recovery = asyncio.create_task(revoke_stale_claims(db=rival, cutoff_seconds=0))
                async with session() as observer:
                    await blocked_writer(observer, pid)
                gate.release.set()
                await recovery
        if reclaimer_first:
            with pytest.raises(ClaimLostError):
                await task
        else:
            await task
        attempts, outcomes = await evidence(device_id)
        assert attempts[-1].phase == "terminal"
        job = await job_row(attempts[-1].job_correlation_id)
        assert job.status == JobStatus.failed and job.settle_seq is not None
        assert sorted(row.disposition for row in outcomes if row.row_subject == "interface.description") == [
            "not_applied",
            "refused",
        ]
        async with session() as db:
            interface = await db.scalar(
                select(DbInterface).where(DbInterface.device_id == device_id, DbInterface.name == "Ethernet2")
            )
            row = await db.scalar(select(InterfaceIntent).where(InterfaceIntent.interface_id == interface.id))
            state = await db.scalar(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == interface.id))
            assert state.sync_state == SyncState.apply_failed
            assert row.last_apply_error == ({"code": "successor"} if reclaimer_first else PRIOR_ERROR)
            assert row.last_apply_at == (datetime(2027, 1, 1, tzinfo=UTC) if reclaimer_first else PRIOR_AT)
        assert len(transport.documents) == 1
    finally:
        gate.release.set()
        gate.close()
        for pending in (task, recovery):
            if pending is not None and not pending.done():
                pending.cancel()
        await asyncio.gather(*(pending for pending in (task, recovery) if pending is not None), return_exceptions=True)
        if successor is not None:
            await release_claim(successor)


async def test_certified_success_preserves_a_new_writer_context(adapter_client, monkeypatch):
    from nso_adapter.store.models import DeviceProjectionStream

    device_id, transport = await setup(adapter_client, monkeypatch)
    transport.refuse = False
    gate = CommitGate(monkeypatch, "response_recorded")
    task = asyncio.create_task(run_next(device_id))
    try:
        await asyncio.wait_for(gate.reached.wait(), 5)
        async with session() as db:
            stream = await db.scalar(
                select(DeviceProjectionStream).where(
                    DeviceProjectionStream.device_id == device_id, DeviceProjectionStream.stream == "vlan"
                )
            )
            fragment = deepcopy(stream.authorized_document)
            fragment["_execution"]["context"]["ned_id"] = "placeholder-other-ned"
            stream.authorized_document = fragment
            stream.desired_revision += 1
            stream.authorized_revision = stream.desired_revision
            await db.commit()
        gate.release.set()
        await task
        attempts, _ = await evidence(device_id)
        job = await job_row(attempts[-1].job_correlation_id)
        assert job.status == JobStatus.succeeded
        assert job.result["vlan_count_by_outcome"] == {
            "in_sync": 2,
            "apply_failed": 0,
            "not_applied": 0,
            "unavailable": 0,
        }
        async with session() as db:
            rows = list(await db.scalars(select(VlanIntent).where(VlanIntent.device_id == device_id)))
            assert all(row.last_apply_at == PRIOR_AT and row.last_apply_error == PRIOR_ERROR for row in rows)
    finally:
        gate.release.set()
        gate.close()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_removal_unsent_failure_retains_attempt_evidence(adapter_client, monkeypatch):
    device_id, transport = await setup(adapter_client, monkeypatch)
    transport.refuse = False
    await run_next(device_id)
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "1")
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [], "local_levels": {"console_severity": "CRITICAL"}},
        headers=AUTH | push_seq(41),
    )
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    response = await _apply(adapter_client, device_id, {"logging": 41})
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 202
    await run_next(device_id)
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "0")
    response = await put_vlans(adapter_client, device_id, [101], seq=42, query="?delete_origin=true")
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200
    transport.documents.clear()
    job_id = await run_next(device_id)
    job = await job_row(job_id)
    assert job.status == JobStatus.failed
    attempts, outcomes = await evidence(device_id)
    assert attempts[-1].operation_kind == "removal", "a known unsent failure still requires durable logical evidence"
    assert attempts[-1].phase == "terminal" and not attempts[-1].commit_acknowledged
    assert all(row.disposition == "not_applied" for row in outcomes if row.execution_attempt_id == attempts[-1].id)
    assert job.result["vlan_count_by_outcome"] == {"in_sync": 0, "apply_failed": 0, "not_applied": 1, "unavailable": 0}
    assert job.error["code"] == "local_levels_gated"
    assert job.result["refusals"] == []
    assert transport.documents == []
    async with session() as db:
        row = await db.scalar(select(VlanIntent).where(VlanIntent.device_id == device_id))
        assert row.last_apply_at is not None and row.last_apply_error is None


@pytest.mark.parametrize("failure", ["logging_gate", "retention_read"])
async def test_apply_build_failure_preserves_selected_unsent_rows(adapter_client, monkeypatch, failure):
    device_id, transport = await setup(adapter_client, monkeypatch, admit=False)
    selected = {"vlan": 41, "interface_config": 41}
    if failure == "logging_gate":
        monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "1")
        response = await adapter_client.put(
            f"/api/v1/devices/{device_id}/logging-intent",
            json={"hosts": [{"address": "192.0.2.99"}], "local_levels": {"console_severity": "CRITICAL"}},
            headers=AUTH | push_seq(41),
        )
        stream, error_code = "logging", "local_levels_gated"
        expected_rows = {
            row_ref("logging", "logging.host", (("host", ("192.0.2.99",)),)),
            row_ref("logging", "logging.local-levels", ()),
        }
    else:
        routes = [
            {"route_id": index, "vrf": "", "prefix": f"198.18.{index}.0/24", "next_hop": "192.0.2.1"}
            for index in (1, 2)
        ]
        response = await _put_routes(adapter_client, device_id, routes, seq=41)
        stream, error_code = "static_route", "static_route_snapshot_inconclusive"
        expected_rows = {
            row_ref("static-route", "static-route.route", (("route", ("", route["prefix"], route["next_hop"])),))
            for route in routes
        }
    assert response.status_code == 200
    selected[stream] = 41
    response = await _apply(adapter_client, device_id, selected)
    assert response.status_code == 202
    if failure == "logging_gate":
        monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "0")
    else:
        transport.fake.service_status = "inconclusive"
    job_id = await run_next(device_id)
    job = await job_row(job_id)
    assert job.status == JobStatus.failed
    assert job.error["code"] == error_code
    assert transport.documents == []
    attempts, outcomes = await evidence(device_id)
    attempt = attempts[-1]
    assert attempt.phase == "terminal" and not attempt.commit_acknowledged
    own = [row for row in outcomes if row.execution_attempt_id == attempt.id]
    assert expected_rows == {
        RowRef(outcome.row_subject, outcome.row_key) for outcome in own if outcome.stream == stream
    }, "every selected row in the failed section must retain terminal evidence"
    assert len(own) == len(attempt.logical_membership) == 6
    assert all(outcome.disposition == "not_applied" and not outcome.refusals for outcome in own)
    assert job.result["refusals"] == []
    assert not job.result["commit_acknowledged"]
    for key in (stream, "vlan", "attribute"):
        assert job.result[f"{key}_count_by_outcome"] == {
            "in_sync": 0,
            "apply_failed": 0,
            "not_applied": 2,
            "unavailable": 0,
        }
    assert len(job.result["settlement_owners"]) == 6
    response = await adapter_client.get(f"/api/v1/jobs/{job_id}", headers=AUTH)
    assert response.status_code == 200
    assert response.json()["error"] == job.error
    assert response.json()["row_outcomes"] == job.result["row_outcomes"]
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/deployment-evidence", json={"apply_attempt_ids": []}, headers=AUTH
    )
    assert response.status_code == 200
    assert response.json()["head"]["row_outcomes"] == job.result["row_outcomes"]
    async with session() as db:
        for row in await db.scalars(select(VlanIntent).where(VlanIntent.device_id == device_id)):
            assert row.last_apply_at == PRIOR_AT and row.last_apply_error == PRIOR_ERROR


async def missing_retained_route_history(api, monkeypatch):
    from tests.api.test_static_route_deleted_routes import deleted

    device_id, transport = await setup(api, monkeypatch)
    transport.refuse = False
    await run_next(device_id)
    route = {"route_id": 1, "vrf": "", "prefix": "198.18.0.0/24", "next_hop": "192.0.2.1"}
    response = await _put_routes(api, device_id, [route], seq=41)
    assert response.status_code == 200
    response = await _apply(api, device_id, {"static_route": 41})
    assert response.status_code == 202
    transport.raw = b"{}"
    transport.refuse = True
    await run_next(device_id)
    transport.raw = None
    transport.fake.service = [{"vrf": "", "prefix": route["prefix"], "next-hop": route["next_hop"]}]
    response = await api.post(
        f"/api/v1/devices/{device_id}/actions/abandon-generation",
        json={"generation_id": (await generations(device_id))[-1].id},
        headers=AUTH,
    )
    assert response.status_code == 202
    response = await _put_routes(
        api, device_id, [], seq=42, deleted=[deleted(1, [("", route["prefix"], route["next_hop"])])]
    )
    assert response.status_code == 200
    transport.records = (
        refusal(
            "static-route",
            row_ref("static-route", "static-route.route", (("route", ("", route["prefix"], route["next_hop"])),)),
            "write-blocked",
        ),
    )
    await run_next(device_id)
    response = await api.post(
        f"/api/v1/devices/{device_id}/actions/abandon-generation",
        json={"generation_id": (await generations(device_id))[-1].id},
        headers=AUTH,
    )
    assert response.status_code == 202
    response = await put_vlans(api, device_id, [101, 102], seq=42)
    assert response.status_code == 200
    response = await _apply(api, device_id, {"vlan": 42})
    assert response.status_code == 202
    return device_id, transport


@pytest.mark.parametrize("cause", ["unknown_digest", "missing_history"])
@pytest.mark.parametrize("lane", ["normal", "claimless", "expired"])
async def test_post_commit_attribution_failure_remains_public(adapter_client, monkeypatch, cause, lane):
    from nso_adapter.nso.origins import AttributionFailure, resolve_refusals
    from nso_adapter.store.execution_evidence import origins_from_records
    from nso_adapter.store.models import DeploymentGeneration, GenerationStatus

    if cause == "missing_history":
        device_id, transport = await missing_retained_route_history(adapter_client, monkeypatch)
        expected_reason = "missing_baseline_provenance"
    else:
        device_id, transport = await setup(adapter_client, monkeypatch)
        transport.records = (refusal("vlan", row_ref("vlan", "vlan.vlan", (("vlan", (999,)),)), "write-blocked"),)
        expected_reason = "missing_binding"
    transport.refuse = False
    transport.documents.clear()
    transport.verification_error = (500, envelope(transport.records))
    gate = None
    task = None
    try:
        if lane == "normal":
            await run_next(device_id)
        else:
            gate = CommitGate(
                monkeypatch,
                "response_recorded",
                failure=ClaimLostError("placeholder lost holder"),
                sender_phase="post_commit_verification",
            )
            task = asyncio.create_task(run_next(device_id))
            await asyncio.wait_for(gate.reached.wait(), 5)
            attempts, _ = await evidence(device_id)
            assert attempts[-1].commit_acknowledged
            assert attempts[-1].response_classification["post_commit_verification"]["kind"] == "refused"
            gate.release.set()
            with pytest.raises(ClaimLostError):
                await task
            async with session() as db:
                if lane == "claimless":
                    await db.execute(delete(DeviceClaim).where(DeviceClaim.device_id == device_id))
                    await db.execute(
                        update(Job)
                        .where(Job.device_id == device_id, Job.status == JobStatus.running)
                        .values(heartbeat_at=None)
                    )
                else:
                    await db.execute(
                        update(DeviceClaim).where(DeviceClaim.device_id == device_id).values(heartbeat_at=PRIOR_AT)
                    )
                await db.commit()
            if lane == "claimless":
                await worker.requeue_orphaned_jobs()
            else:
                await revoke_stale_claims(cutoff_seconds=0)
        attempts, outcomes = await evidence(device_id)
        attempt = attempts[-1]
        assert attempt.phase == "terminal" and attempt.commit_acknowledged
        assert attempt.response_classification["commit"]["kind"] == "acknowledged"
        assert attempt.response_classification["post_commit_verification"]["kind"] == "refused"
        resolved = resolve_refusals(transport.records, origins_from_records(attempt.safe_origins))
        assert isinstance(resolved, AttributionFailure) and resolved.reason == expected_reason
        own = [outcome for outcome in outcomes if outcome.execution_attempt_id == attempt.id]
        assert len(own) == len(attempt.logical_membership) > 0
        assert all(outcome.disposition == "unavailable" and not outcome.refusals for outcome in own)
        job = await terminal(adapter_client, device_id, refused=False)
        assert job.error["code"] == "attribution_failed"
        assert job.result["commit_acknowledged"]
        assert job.result["refusals"] == []
        assert job.result["vlan_count_by_outcome"] == {
            "in_sync": 0,
            "apply_failed": 0,
            "not_applied": 0,
            "unavailable": 2,
        }
        assert all(owner["disposition"] == "unavailable" for owner in job.result["settlement_owners"])
        response = await adapter_client.get(f"/api/v1/jobs/{job.id}", headers=AUTH)
        assert response.status_code == 200
        assert response.json()["error"]["code"] == "attribution_failed"
        async with session() as db:
            generation = await db.get(DeploymentGeneration, attempt.generation_id)
            assert generation.status == GenerationStatus.outcome_unknown
            assert generation.carrier_job_error["code"] == "attribution_failed"
            assert generation.carrier_job_result == job.result
        assert len(transport.documents) == 1
        assert await run_next(device_id) is None
    finally:
        if gate is not None:
            gate.release.set()
            gate.close()
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
