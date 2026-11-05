# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Manual Apply admission and revalidation through the HTTP boundary."""

from uuid import uuid4

import pytest

from tests._secret_discipline import assert_keys_absent, assert_text_free_of
from tests.conftest import session
from tests.core.test_action_apply_promotion import _put_vlans
from tests.core.test_generation_protocol import AUTH
from tests.core.test_preflight_loop import setup
from tests.core.test_preflight_plan import database_state
from tests.nso.test_refusal_sender_cutover import CANARY

pytestmark = pytest.mark.anyio
ATTEMPTS = {"deployment_apply_attempt"}


async def preview(api, device_id, selected):
    return await api.post(
        f"/api/v1/devices/{device_id}/actions/apply-preflight", json={"selected": selected}, headers=AUTH
    )


async def apply(api, device_id, selected, identity, attempt_id=None):
    return await api.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={
            "apply_attempt_id": str(attempt_id or uuid4()),
            "selected": selected,
            "preflight_identity": identity,
        },
        headers=AUTH,
    )


@pytest.mark.parametrize("case", ["plain", "removal", "detach", "lag"])
async def test_apply_preflight_route_commits_nothing(adapter_client, monkeypatch, case):
    device, selected, _, transport = await setup(adapter_client, monkeypatch, case)
    before = await database_state()
    response = await preview(adapter_client, device.id, selected)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "ready"
    assert body["streams"] == {stream: {"status": "admitted"} for stream in selected}
    assert body["admitted_selection"] == selected
    assert len(body["preflight_identity"]) == 64
    assert body["diffs"] == (
        {"native": "", "cli": ""} if case == "detach" else {"native": "native delta", "cli": "cli delta"}
    )
    assert body["attempts"] == [{"selection": selected, "dropped": []}]
    assert body["baseline_refused"] == body["cross_stream"] == body["context_disturbed"] == []
    assert body["skips"] == {}
    assert transport.dry_requests
    assert len(transport.fake.reads) == 1
    assert not transport.locks
    assert await database_state() == before


@pytest.mark.parametrize(
    "change",
    [
        "selected_push",
        "submitted_selection",
        "baseline",
        "ned",
        "interface_proof",
        "static_route_proof",
        "static_snapshot",
        "service_snapshot",
        "name",
        "instance",
    ],
)
async def test_apply_rejects_stale_preflight_identity(adapter_client, monkeypatch, change):
    from sqlalchemy import select

    from nso_adapter.store.models import (
        DbInterface,
        Device,
        GenerationStatus,
        StaticRouteIntent,
    )
    from tests.api.test_static_route_identity import entry
    from tests.core.test_action_apply_promotion import _A, _B, _generations, _put_routes, _settle
    from tests.core.test_preflight_loop import interface_selection

    device, selected, _, transport = await setup(adapter_client, monkeypatch)
    if change == "interface_proof":
        selected |= await interface_selection(adapter_client, device)
    if change == "static_route_proof":
        pushed = await _put_routes(
            adapter_client, device.id, [entry(_A, route_id=1, generation=1)], seq=41, query="?store_only=true"
        )
        assert_text_free_of(pushed.text, [CANARY])
        assert pushed.status_code == 200
        selected["static_route"] = 41
    response = await preview(adapter_client, device.id, selected)
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200 and response.json()["status"] == "ready"
    identity = response.json()["preflight_identity"]
    if change in {"selected_push", "submitted_selection"}:
        response = await _put_vlans(adapter_client, device.id, [102], seq=42, query="?store_only=true")
        assert_text_free_of(response.text, [CANARY])
        assert response.status_code == 200
        if change == "submitted_selection":
            selected = {"vlan": 42}
    elif change == "baseline":
        sibling = await interface_selection(adapter_client, device)
        baseline = (await preview(adapter_client, device.id, sibling)).json()
        response = await apply(adapter_client, device.id, sibling, baseline["preflight_identity"])
        assert_text_free_of(response.text, [CANARY])
        assert response.status_code == 202
        await _settle((await _generations(device.id))[-1].job_id, GenerationStatus.settled)
    elif change in {"ned", "name", "instance"}:
        async with session() as db:
            stored = await db.get(Device, device.id)
            field, value = {
                "ned": ("ned_id", "juniper-junos-nc-4.11:juniper-junos-nc-4.11"),
                "name": ("nso_device_name", "placeholder-renamed-device"),
                "instance": ("nso_instance", "placeholder-other-instance"),
            }[change]
            setattr(stored, field, value)
            await db.commit()
        if change == "name":
            transport.fake.device_name = value
    elif change == "interface_proof":
        async with session() as db:
            row = await db.scalar(select(DbInterface).where(DbInterface.device_id == device.id))
            row.vrf = "blue"
            await db.commit()
    elif change == "static_route_proof":
        async with session() as db:
            row = await db.scalar(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device.id))
            row.deployed_key = list(_B)
            await db.commit()
    else:
        transport.fake.service = (
            [] if change == "service_snapshot" else [{"vrf": "", "prefix": "198.18.99.0/24", "next-hop": "192.0.2.99"}]
        )
        if change == "service_snapshot":
            transport.fake.instance["vlan"] = {"vlan": [{"vlan-id": 99}]}
    before = await database_state(exclude=ATTEMPTS)
    attempt_id = uuid4()
    response = await apply(adapter_client, device.id, selected, identity, attempt_id)
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "preflight_stale"
    await assert_rejected_without_promotion(attempt_id, before)


async def assert_rejected_without_promotion(attempt_id, before):
    from nso_adapter.store.models import DeploymentApplyAttempt

    assert await database_state(exclude=ATTEMPTS) == before
    async with session() as db:
        assert (await db.get(DeploymentApplyAttempt, attempt_id)).admission_state == "rejected"


@pytest.mark.parametrize("queued_job", [False, True])
async def test_apply_live_input_inconclusive_is_preflight_stale(adapter_client, monkeypatch, queued_job):
    from nso_adapter.store.models import Job, JobStatus, JobType

    device, selected, _, transport = await setup(adapter_client, monkeypatch)
    response = await preview(adapter_client, device.id, selected)
    if queued_job:
        # An inconclusive live input outranks the job conflict that promotion would raise.
        async with session() as db:
            db.add(Job(device_id=device.id, job_type=JobType.sync, status=JobStatus.queued, coalescible=True))
            await db.commit()
    transport.fake.service_status = "inconclusive"
    transport.fake.service = []
    before = await database_state(exclude=ATTEMPTS)
    attempt_id = uuid4()
    response = await apply(adapter_client, device.id, selected, response.json()["preflight_identity"], attempt_id)
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "preflight_stale"
    assert response.json()["error"]["detail"]["reason"] == "live_input_inconclusive"
    await assert_rejected_without_promotion(attempt_id, before)


async def test_apply_identity_replay_is_idempotent(adapter_client, monkeypatch):
    device, selected, _, transport = await setup(adapter_client, monkeypatch)
    identity = (await preview(adapter_client, device.id, selected)).json()["preflight_identity"]
    attempt_id = uuid4()
    first = await apply(adapter_client, device.id, selected, identity, attempt_id)
    assert first.status_code == 202, first.text
    before = await database_state()
    transport.fake.service_status = "inconclusive"
    transport.fake.service = []
    replay = await apply(adapter_client, device.id, selected, identity, attempt_id)
    assert replay.status_code == first.status_code and replay.content == first.content
    other_identity = "0" * 64 if identity != "0" * 64 else "1" * 64
    conflict = await apply(adapter_client, device.id, selected, other_identity, attempt_id)
    assert conflict.status_code == 409, conflict.text
    assert conflict.json()["error"]["detail"] == {"mismatch": "preflight_identity"}
    assert await database_state() == before


async def test_apply_without_preflight_identity_is_422(adapter_client, monkeypatch):
    device, selected, _, _ = await setup(adapter_client, monkeypatch)
    before = await database_state()
    response = await adapter_client.post(
        f"/api/v1/devices/{device.id}/actions/apply",
        json={"apply_attempt_id": str(uuid4()), "selected": selected},
        headers=AUTH,
    )
    assert response.status_code == 422, response.text
    assert await database_state() == before


async def test_empty_selection_preflight_preserves_no_op_apply(adapter_client, monkeypatch):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    result = await preview(adapter_client, device.id, {})
    assert result.status_code == 200 and result.json()["status"] == "ready", result.text
    response = await apply(adapter_client, device.id, {}, result.json()["preflight_identity"])
    assert response.status_code == 200, response.text
    assert response.json()["outcome"] == "no_op"


async def test_capability_apply_preflight_route_is_gone(adapter_client, monkeypatch):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    response = await adapter_client.post(
        f"/api/v1/devices/{device.id}/apply/preflight", json={"scopes": ["vlan"]}, headers=AUTH
    )
    assert response.status_code in {404, 405}, response.text


async def test_apply_promotes_admitted_and_keeps_blocked_authorized_fragment(adapter_client, monkeypatch):
    from nso_adapter.nso.refusal_protocol import refusal, row_ref
    from nso_adapter.store.models import GenerationStatus
    from tests.core.test_action_apply_promotion import _generations, _put_interface_attrs, _put_ip, _settle, _stream
    from tests.core.test_preflight_loop import ADDRESS, INTERFACE, interface_selection

    device, vlan, _, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    initial = (await preview(adapter_client, device.id, selected)).json()
    response = await apply(adapter_client, device.id, selected, initial["preflight_identity"])
    assert response.status_code == 202, response.text
    await _settle(response.json()["job_id"], GenerationStatus.settled)
    blocked_before = await _stream(device.id, "ip")
    addresses = [
        {"interface": INTERFACE, "address": address, "family": "ipv4"} for address in (ADDRESS, "192.0.2.2/24")
    ]
    assert (await _put_ip(adapter_client, device.id, addresses, seq=42)).status_code == 200
    assert (
        await _put_interface_attrs(adapter_client, device.id, "new description", seq=42, query="?store_only=true")
    ).status_code == 200
    record = refusal(
        "interface",
        row_ref("interface", "interface.ipv4-address", (("interface", (INTERFACE,)), ("ipv4-address", ("192.0.2.2",)))),
        "write-blocked",
        field="interface.ip.v4",
    )
    transport.judge = lambda body, _request: (
        (record,)
        if any(row["address"] == "192.0.2.2" for row in body["interface"]["interface"][0]["ipv4-address"])
        else None
    )
    result = (await preview(adapter_client, device.id, {"ip": 42, "interface_config": 42})).json()
    assert result["status"] == "ready", result
    assert result["admitted_selection"] == {"interface_config": 42}
    blocked = result["streams"]["ip"]
    assert blocked["status"] == "blocked"
    assert blocked["refusals"][0]["field"] == "interface.ip.v4"
    assert blocked["refusals"][0]["logical_rows"][0]["stream"] == "ip"
    assert blocked["refusals"][0]["sender_phase"] == "pre_commit_dry_run"
    assert blocked["refusals"][0]["logical_rows"][0]["generation_id"] is None
    response = await apply(adapter_client, device.id, result["admitted_selection"], result["preflight_identity"])
    assert response.status_code == 202, response.text
    blocked_after = await _stream(device.id, "ip")
    assert blocked_after.authorized_document == blocked_before.authorized_document
    assert blocked_after.authorized_revision == blocked_before.authorized_revision
    assert blocked_after.desired_revision > blocked_after.authorized_revision
    generation = (await _generations(device.id))[-1]
    assert generation.stream_revisions == {"interface_config": 2}
    assert [row["address"] for row in generation.document["interface_config"]["interface_ip_intent"]] == [ADDRESS]
    await _settle(response.json()["job_id"], GenerationStatus.settled)
    next_preview = (await preview(adapter_client, device.id, vlan)).json()
    response = await apply(adapter_client, device.id, vlan, next_preview["preflight_identity"])
    assert response.status_code == 202, response.text
    assert [
        row["address"]
        for row in (await _generations(device.id))[-1].document["interface_config"]["interface_ip_intent"]
    ] == [ADDRESS]


@pytest.mark.parametrize("failure", ["transport", "5xx", "empty", "refusal", "mixed", "ordinary", "missing_client"])
async def test_apply_preflight_nso_failures_are_statuses(adapter_client, monkeypatch, failure):
    import httpx

    from nso_adapter.nso.refusal_protocol import refusal, row_ref
    from tests.core.test_preflight_loop import IP_REFUSAL, interface_selection

    device, selected, _, transport = await setup(adapter_client, monkeypatch)
    if failure in {"refusal", "mixed"}:
        selected = await interface_selection(adapter_client, device)
        if failure == "refusal":
            selected = {"ip": selected["ip"]}

    def judge(_body, request):
        if failure == "transport":
            raise httpx.ReadError(CANARY, request=request)
        if failure == "5xx":
            return httpx.Response(503, json={"error": CANARY})
        if failure == "ordinary":
            return httpx.Response(400, json={"error": CANARY})
        if failure == "empty":
            return httpx.Response(200, content=b"null")
        if failure == "mixed":
            return (
                refusal(
                    "interface",
                    row_ref("interface", "interface.interface", (("interface", ("GigabitEthernet0/1",)),)),
                    "write-blocked",
                ),
            )
        return (IP_REFUSAL,)

    transport.judge = judge
    if failure == "missing_client":

        def unavailable(_instance):
            raise RuntimeError(CANARY)

        monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", unavailable)
    response = await preview(adapter_client, device.id, selected)
    assert_text_free_of(response.text, [CANARY])
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == (
        {"mixed": "error", "ordinary": "error", "refusal": "refused"}.get(failure, "unavailable")
    )
    assert result["admitted_selection"] == {}
    assert_keys_absent(result, ["preflight_identity"])
    assert_text_free_of(response.text, [CANARY])


async def test_apply_preflight_live_job_matches_apply_conflict(adapter_client, monkeypatch):
    from nso_adapter.store.models import Job, JobStatus, JobType

    device, selected, _, _ = await setup(adapter_client, monkeypatch)
    identity = (await preview(adapter_client, device.id, selected)).json()["preflight_identity"]
    async with session() as db:
        job = Job(device_id=device.id, job_type=JobType.sync, status=JobStatus.queued, coalescible=True)
        db.add(job)
        await db.commit()
    before = await database_state()
    rejected_preview = await preview(adapter_client, device.id, selected)
    assert await database_state() == before
    rejected_apply = await apply(adapter_client, device.id, selected, identity)
    assert rejected_preview.status_code == rejected_apply.status_code == 409
    assert rejected_preview.json() == rejected_apply.json()
    assert rejected_apply.json()["error"]["detail"] == {"job_id": job.id}


async def refused_immediate_promotion(api, monkeypatch):
    from nso_adapter.nso.refusal_protocol import refusal, row_ref
    from tests.core.test_action_apply_promotion import _put_ip
    from tests.core.test_preflight_loop import INTERFACE
    from tests.core.test_preflight_plan import PlanTransport
    from tests.core.test_refusal_cutover import run_next
    from tests.nso.test_apply_send import _client_with

    device, vlan, _, _ = await setup(api, monkeypatch)
    runtime = PlanTransport(device.nso_device_name)
    runtime.refuse = False
    client = _client_with(runtime)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    addresses = [{"interface": INTERFACE, "address": f"192.0.2.{suffix}/24", "family": "ipv4"} for suffix in (1, 2, 3)]
    assert (await _put_ip(api, device.id, [addresses[0]], seq=41)).status_code == 200
    identity = (await preview(api, device.id, {"ip": 41})).json()["preflight_identity"]
    assert (await apply(api, device.id, {"ip": 41}, identity)).status_code == 202
    await run_next(device.id)
    assert (await _put_ip(api, device.id, addresses, seq=42)).status_code == 200
    runtime.records = (
        refusal(
            "interface",
            row_ref(
                "interface", "interface.ipv4-address", (("interface", (INTERFACE,)), ("ipv4-address", ("192.0.2.2",)))
            ),
            "write-blocked",
            field="interface.ip.v4",
        ),
    )
    runtime.refuse = True
    response = await api.put(
        f"/api/v1/devices/{device.id}/ip-intent?delete_origin=true",
        json={"addresses": addresses[:2]},
        headers=AUTH | {"X-Push-Seq": "43"},
    )
    assert response.status_code == 200, response.text
    job_id = await run_next(device.id)
    return device, vlan, runtime, job_id


async def test_immediate_removal_carries_pending_acquisition_under_commit_time_refusal(adapter_client, monkeypatch):
    from nso_adapter.store.models import GenerationStatus
    from tests.core.test_action_apply_promotion import _generations, _stream
    from tests.core.test_generation_protocol import job_row

    device, _, runtime, job_id = await refused_immediate_promotion(adapter_client, monkeypatch)
    generation = (await _generations(device.id))[-1]
    assert generation.status == GenerationStatus.failed
    assert [row["address"] for row in generation.document["interface_config"]["interface_ip_intent"]] == [
        "192.0.2.1/24",
        "192.0.2.2/24",
    ]
    assert {row["address"] for row in runtime.documents[-1]["interface"]["interface"][0]["ipv4-address"]} == {
        "192.0.2.1",
        "192.0.2.2",
    }
    assert runtime.fake.instance["interface"]["interface"][0]["ipv4-address"] == [
        {"address": "192.0.2.1", "prefix-length": 24, "secondary": False},
    ]
    stream = await _stream(device.id, "ip")
    assert stream.authorized_revision == stream.desired_revision > stream.applied_revision
    job = await job_row(job_id)
    assert job.error["code"] == "removal_failed"
    assert any(record["field"] == "interface.ip.v4" for record in job.result["refusals"])


async def test_repair_push_selected_in_manual_apply_clears_baseline_refusal(adapter_client, monkeypatch):
    from nso_adapter.store.models import GenerationStatus
    from tests.core.test_action_apply_promotion import _generations, _put_ip
    from tests.core.test_preflight_loop import INTERFACE, LoopTransport
    from tests.nso.test_apply_send import _client_with

    device, vlan, runtime, _ = await refused_immediate_promotion(adapter_client, monkeypatch)
    generation = (await _generations(device.id))[-1]
    response = await adapter_client.post(
        f"/api/v1/devices/{device.id}/actions/abandon-generation",
        json={"generation_id": generation.id},
        headers=AUTH,
    )
    assert response.status_code == 202, response.text
    transport = LoopTransport(device.nso_device_name)
    transport.fake = runtime.fake
    client = _client_with(transport)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    transport.judge = lambda body, _request: (
        runtime.records
        if any(row["address"] == "192.0.2.2" for row in body["interface"]["interface"][0].get("ipv4-address", []))
        else None
    )
    result = (await preview(adapter_client, device.id, vlan)).json()
    assert result["status"] == "refused" and result["baseline_refused"] == ["ip"]
    assert_keys_absent(result, ["preflight_identity"])
    assert (
        await _put_ip(
            adapter_client, device.id, [{"interface": INTERFACE, "address": "192.0.2.1/24", "family": "ipv4"}], seq=44
        )
    ).status_code == 200
    selected = {"ip": 44}
    result = (await preview(adapter_client, device.id, selected)).json()
    assert result["status"] == "ready" and not result["baseline_refused"], result
    response = await apply(adapter_client, device.id, selected, result["preflight_identity"])
    assert response.status_code == 202, response.text
    assert (await _generations(device.id))[-1].status == GenerationStatus.pending
    assert [
        row["address"]
        for row in (await _generations(device.id))[-1].document["interface_config"]["interface_ip_intent"]
    ] == ["192.0.2.1/24"]
    from tests.core.test_action_apply_promotion import _settle

    await _settle(response.json()["job_id"], GenerationStatus.settled)
    result = (await preview(adapter_client, device.id, vlan)).json()
    assert result["status"] == "ready" and not result["baseline_refused"], result


async def test_force_removal_reissue_does_not_carry_pending_acquisition(adapter_client, monkeypatch):
    from sqlalchemy import select

    from nso_adapter.store.models import StaticRouteIntent
    from tests.api.test_static_route_identity import entry
    from tests.core.test_action_apply_promotion import _A, _B, _generations, _put_routes, _stream
    from tests.core.test_preflight_plan import PlanTransport
    from tests.core.test_refusal_cutover import run_next
    from tests.nso.test_apply_send import _client_with

    device, _, _, _ = await setup(adapter_client, monkeypatch)
    runtime = PlanTransport(device.nso_device_name)
    runtime.refuse = False
    client = _client_with(runtime)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    assert (
        await _put_routes(
            adapter_client, device.id, [entry(_A, route_id=1, generation=1)], seq=41, query="?store_only=true"
        )
    ).status_code == 200
    result = (await preview(adapter_client, device.id, {"static_route": 41})).json()
    response = await apply(adapter_client, device.id, {"static_route": 41}, result["preflight_identity"])
    assert response.status_code == 202, response.text
    await run_next(device.id)
    baseline = await _stream(device.id, "static_route")
    assert (
        await _put_routes(
            adapter_client,
            device.id,
            [entry(_A, route_id=1, generation=1), entry(_B, route_id=2, generation=1)],
            seq=42,
            query="?store_only=true",
        )
    ).status_code == 200
    response = await adapter_client.post(
        f"/api/v1/devices/{device.id}/actions/force-removal", json={"scope": "static_route"}, headers=AUTH
    )
    assert response.status_code == 202, response.text
    await run_next(device.id)
    reissue = (await _generations(device.id))[-1]
    assert reissue.stream_revisions == {} and reissue.apply_attempt_id is None
    assert [row["route_id"] for row in reissue.document["static_route"]["static_route_intent"]] == [1]
    assert runtime.fake.sent_keys() == {_A}
    stream = await _stream(device.id, "static_route")
    assert stream.authorized_document == baseline.authorized_document
    assert stream.authorized_revision == baseline.authorized_revision < stream.desired_revision
    async with session() as db:
        pending = await db.scalar(
            select(StaticRouteIntent).where(StaticRouteIntent.device_id == device.id, StaticRouteIntent.route_id == 2)
        )
        assert pending.last_apply_at is None


@pytest.mark.parametrize("failure", ["transport", "5xx"])
async def test_live_read_failure_is_unavailable_and_stale_apply(adapter_client, monkeypatch, failure):
    import httpx

    from tests.nso.test_apply_send import _client_with

    device, selected, _, transport = await setup(adapter_client, monkeypatch)
    failing = False

    async def handle(request):
        if failing and request.method == "GET":
            if failure == "transport":
                raise httpx.ReadTimeout(CANARY, request=request)
            return httpx.Response(503, json={"error": CANARY}, request=request)
        return await transport.handle_async_request(request)

    client = _client_with(httpx.MockTransport(handle))
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    identity = (await preview(adapter_client, device.id, selected)).json()["preflight_identity"]
    failing = True
    before = await database_state()
    unavailable = await preview(adapter_client, device.id, selected)
    assert_text_free_of(unavailable.text, [CANARY])
    assert unavailable.status_code == 200, unavailable.text
    assert unavailable.json()["status"] == "unavailable"
    assert_keys_absent(unavailable.json(), ["preflight_identity"])
    assert await database_state() == before
    before = await database_state(exclude=ATTEMPTS)
    attempt_id = uuid4()
    stale = await apply(adapter_client, device.id, selected, identity, attempt_id)
    assert_text_free_of(stale.text, [CANARY])
    assert stale.status_code == 409, stale.text
    assert stale.json()["error"]["code"] == "preflight_stale"
    assert stale.json()["error"]["detail"] == {"reason": "live_input_inconclusive"}
    await assert_rejected_without_promotion(attempt_id, before)
    assert_text_free_of(unavailable.text + stale.text, [CANARY])


@pytest.mark.parametrize("case", ["skipped_only", "mixed", "recomposed"])
async def test_confirm_reproduces_bound_skips(adapter_client, monkeypatch, case):
    from tests.core.test_preflight_loop import IP_REFUSAL, has_ip, interface_selection

    device, vlan, _, transport = await setup(adapter_client, monkeypatch)
    selected = {"snmp": 7}
    if case != "skipped_only":
        selected |= vlan
    if case == "recomposed":
        selected |= await interface_selection(adapter_client, device)
        transport.judge = lambda body, _request: (IP_REFUSAL,) if has_ip(body) else None
    result = (await preview(adapter_client, device.id, selected)).json()
    assert result["status"] == "ready", result
    assert result["skips"] == {"snmp": "no_receipt"}
    assert result["streams"]["snmp"]["status"] == "not_evaluated"
    confirmed = {
        stream: revision for stream, revision in selected.items() if result["streams"][stream]["status"] != "blocked"
    }
    response = await apply(adapter_client, device.id, confirmed, result["preflight_identity"])
    assert response.status_code == (200 if case == "skipped_only" else 202), response.text
    assert response.json()["skipped"] == result["skips"]


async def test_apply_reads_live_inputs_once_before_projection_lock(adapter_client, monkeypatch):
    import httpx
    from sqlalchemy import text

    from tests.nso.test_apply_send import _client_with

    device, selected, _, transport = await setup(adapter_client, monkeypatch)
    identity = (await preview(adapter_client, device.id, selected)).json()["preflight_identity"]
    reads = []

    async def handle(request):
        if request.method == "GET":
            async with session() as db:
                locks = list(
                    await db.execute(
                        text(
                            "SELECT relation FROM pg_locks WHERE pid != pg_backend_pid() "
                            "AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) "
                            "AND mode IN ('RowShareLock', 'RowExclusiveLock') AND granted"
                        )
                    )
                )
                assert not locks
            reads.append(request)
        return await transport.handle_async_request(request)

    client = _client_with(httpx.MockTransport(handle))
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    response = await apply(adapter_client, device.id, selected, identity)
    assert response.status_code == 202, response.text
    assert len(reads) == 1
    assert len(transport.fake.reads) == 2


async def test_apply_preflight_rejects_store_only_mode(adapter_client, monkeypatch):
    device, selected, _, _ = await setup(adapter_client, monkeypatch)
    response = await adapter_client.post(
        f"/api/v1/devices/{device.id}/actions/apply-preflight?store_only=true",
        json={"selected": selected},
        headers=AUTH,
    )
    assert response.status_code == 422, response.text


async def test_stored_mapping_change_during_live_read_needs_new_preflight(adapter_client, monkeypatch):
    import httpx

    from nso_adapter.store.models import Device
    from tests.nso.test_apply_send import _client_with

    device, selected, _, transport = await setup(adapter_client, monkeypatch)

    async def handle(request):
        response = await transport.handle_async_request(request)
        if request.method == "GET":
            async with session() as db:
                (await db.get(Device, device.id)).nso_device_name = "placeholder-remapped-device"
                await db.commit()
        return response

    client = _client_with(httpx.MockTransport(handle))
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    response = await preview(adapter_client, device.id, selected)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "unavailable", body
    assert body["error"] == {"code": "inputs_changed_during_preflight"}
    assert_keys_absent(body, ["preflight_identity"])
    assert not transport.dry_requests


async def test_apply_conflicting_interface_proof_after_preview_is_preflight_stale(adapter_client, monkeypatch):
    from sqlalchemy import select

    from nso_adapter.store.models import DbInterface
    from tests.core.test_preflight_loop import ADDRESS, INTERFACE, _put_interface_attrs, _put_ip, authorize

    device, _, _, _ = await setup(adapter_client, monkeypatch)
    response = await _put_ip(
        adapter_client,
        device.id,
        [{"interface": INTERFACE, "address": ADDRESS, "family": "ipv4", "parent_binding": "lag-1"}],
        seq=41,
    )
    assert response.status_code == 200
    await authorize(adapter_client, device, {"ip": 41})
    response = await _put_interface_attrs(adapter_client, device.id, "new context", seq=41, query="?store_only=true")
    assert response.status_code == 200
    selected = {"interface_config": 41}
    response = await preview(adapter_client, device.id, selected)
    assert response.status_code == 200 and response.json()["status"] == "ready", response.text
    async with session() as db:
        interface = await db.scalar(select(DbInterface).where(DbInterface.device_id == device.id))
        interface.parent_binding = "lag-2"
        await db.commit()
    before = await database_state(exclude=ATTEMPTS)
    attempt_id = uuid4()
    response = await apply(adapter_client, device.id, selected, response.json()["preflight_identity"], attempt_id)
    assert response.status_code == 409, response.text
    assert response.json()["error"] == {
        "code": "preflight_stale",
        "message": "Manual Apply needs a fresh preflight",
        "detail": {"reason": "inputs_changed"},
    }
    await assert_rejected_without_promotion(attempt_id, before)


async def test_apply_ned_change_rejecting_stored_intent_after_preview_is_preflight_stale(adapter_client, monkeypatch):
    from tests.api.test_bgp_asn_validation import _redistribution_request
    from tests.conftest import push_seq
    from tests.core.test_execution_context import _set_ned

    device, _, _, _ = await setup(adapter_client, monkeypatch)
    await _set_ned(device.id, "juniper-junos-nc")
    endpoint, body = _redistribution_request("ospf", "")
    url = f"/api/v1/devices/{device.id}/{endpoint}?store_only=true"
    response = await adapter_client.put(url, json=body, headers=AUTH | push_seq(41))
    assert response.status_code == 200, response.text
    selected = {"ospf": 41}
    response = await preview(adapter_client, device.id, selected)
    assert response.status_code == 200 and response.json()["status"] == "ready", response.text
    identity = response.json()["preflight_identity"]
    await _set_ned(device.id, "cisco-ios-cli-6.95")
    before = await database_state(exclude=ATTEMPTS)
    attempt_id = uuid4()
    response = await apply(adapter_client, device.id, selected, identity, attempt_id)
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "preflight_stale"
    await assert_rejected_without_promotion(attempt_id, before)
    replay = await apply(adapter_client, device.id, selected, identity, attempt_id)
    assert replay.status_code == 409 and replay.json() == response.json()


async def test_recomposition_keeps_skipped_name_only_route_for_confirmation(adapter_client, monkeypatch):
    from nso_adapter.store.models import GenerationStatus
    from tests.core.test_action_apply_promotion import _A, _generations, _put_routes, _settle, route_entry
    from tests.core.test_preflight_loop import IP_REFUSAL, has_ip, interface_selection

    device, selected, _, transport = await setup(adapter_client, monkeypatch)
    pushed = await _put_routes(
        adapter_client,
        device.id,
        [route_entry(_A, route_id=1, generation=1, name="old name")],
        seq=41,
        query="?store_only=true",
    )
    assert pushed.status_code == 200, pushed.text
    routes = (await preview(adapter_client, device.id, {"static_route": 41})).json()
    assert routes["status"] == "ready", routes
    assert (
        await apply(adapter_client, device.id, {"static_route": 41}, routes["preflight_identity"])
    ).status_code == 202
    await _settle((await _generations(device.id))[-1].job_id, GenerationStatus.settled)
    renamed = await _put_routes(
        adapter_client,
        device.id,
        [route_entry(_A, route_id=1, generation=2, name="new name")],
        seq=42,
        query="?store_only=true",
    )
    assert renamed.status_code == 200, renamed.text
    selected = selected | await interface_selection(adapter_client, device) | {"static_route": 42}
    del selected["interface_config"]
    transport.judge = lambda body, _request: (IP_REFUSAL,) if has_ip(body) else None
    response = await preview(adapter_client, device.id, selected)
    body = response.json()
    assert response.status_code == 200 and body["status"] == "ready", response.text
    assert body["streams"]["ip"]["status"] == "blocked"
    assert body["skips"] == {"static_route": "already_applied"}
    confirm = {stream: revision for stream, revision in selected.items() if stream != "ip"}
    response = await apply(adapter_client, device.id, confirm, body["preflight_identity"])
    assert response.status_code == 202, response.text
