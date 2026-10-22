# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Manual admission through PostgreSQL planning and the RESTCONF boundary."""

import json
from dataclasses import FrozenInstanceError

import httpx
import pytest
from sqlalchemy import select, text

from nso_adapter.core import preflight
from nso_adapter.nso.client import DEVICE_INTENT_ROOT
from nso_adapter.nso.refusal_protocol import refusal, row_ref
from nso_adapter.store.db import get_engine, session
from nso_adapter.store.models import DbInterface, Device, GenerationStatus
from tests._secret_discipline import assert_text_contains, assert_text_free_of
from tests.core.test_action_apply_promotion import (
    _B,
    _apply,
    _generations,
    _put_interface_attrs,
    _put_ip,
    _put_vlans,
    _settle,
)
from tests.core.test_generation_protocol import AUTH
from tests.core.test_preflight_plan import PlanTransport, database_state
from tests.core.test_preflight_plan import setup as setup_plan
from tests.nso.test_apply_send import _client_with
from tests.nso.test_refusal_sender_cutover import CANARY, envelope

pytestmark = pytest.mark.anyio
INTERFACE = "GigabitEthernet0/1"
ADDRESS = "192.0.2.1/24"
IP_REFUSAL = refusal(
    "interface",
    row_ref("interface", "interface.ipv4-address", (("interface", (INTERFACE,)), ("ipv4-address", ("192.0.2.1",)))),
    "write-blocked",
    field="interface.ip.v4",
)


class LoopTransport(PlanTransport):
    def __init__(self, name):
        super().__init__(name)
        self.refuse = False
        self.dry_requests = []
        self.judge = lambda _body, _request: None
        self.before_dry_run = None
        self.locks = []

    async def handle_async_request(self, request):
        if request.method == "PUT":
            assert_text_contains(str(request.url), ["dry-run="])
            async with session() as db:
                self.locks.extend(
                    await db.execute(
                        text(
                            "SELECT relation FROM pg_locks WHERE pid != pg_backend_pid() AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) AND mode IN ('RowShareLock', 'RowExclusiveLock') AND granted"
                        )
                    )
                )
            self.dry_requests.append(request)
            if self.before_dry_run is not None:
                await self.before_dry_run()
            body = json.loads(request.content)[DEVICE_INTENT_ROOT][0]
            verdict = self.judge(body, request)
            if isinstance(verdict, httpx.Response):
                return verdict
            if verdict is not None:
                return httpx.Response(500, content=envelope(verdict), request=request)
            return httpx.Response(
                200,
                json={
                    "dry-run-result": {
                        "native": {"device": [{"name": self.fake.device_name, "data": "native delta"}]},
                        "cli": {"local-node": {"data": "cli delta"}},
                    }
                },
                request=request,
            )
        return await super().handle_async_request(request)


async def setup(api, monkeypatch, case="plain"):
    device, selected, _, previous = await setup_plan(api, monkeypatch, case)
    transport = LoopTransport(device.nso_device_name)
    transport.fake = previous.fake
    client = _client_with(transport)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    return device, selected, client, transport


async def interface_selection(api, device):
    response = await _put_interface_attrs(api, device.id, "selected description", seq=41, query="?store_only=true")
    assert response.status_code == 200, response.text
    response = await _put_ip(api, device.id, [{"interface": INTERFACE, "address": ADDRESS, "family": "ipv4"}], seq=41)
    assert response.status_code == 200, response.text
    return {"interface_config": 41, "ip": 41}


def has_ip(body):
    return any(row.get("ipv4-address") for row in body.get("interface", {}).get("interface", []))


async def test_preflight_blocks_refused_ip_stream_and_admits_interface_attributes(adapter_client, monkeypatch):
    device, _, client, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    transport.judge = lambda body, _request: (IP_REFUSAL,) if has_ip(body) else None
    before = await database_state()
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "ready", result.error
    assert result.admitted_selection == {"interface_config": 41}
    assert result.streams["ip"]["status"] == "blocked"
    assert result.streams["ip"]["refusals"][0]["field"] == "interface.ip.v4"
    assert result.streams["interface_config"]["status"] == "admitted"
    assert [attempt["dropped"] for attempt in result.attempts] == [["ip"], []]
    assert result.plan is not None and len(result.plan.links) == 1
    assert result.deltas == [{"order": 0, "native": "native delta", "cli": "cli delta"}]
    assert len(transport.fake.reads) == 1
    assert await database_state() == before


def attribute_refusal():
    return refusal(
        "interface",
        row_ref("interface", "interface.description", (("interface", (INTERFACE,)),)),
        "write-blocked",
        field="interface.attrs.description",
    )


async def authorize(api, device, selected, status=GenerationStatus.settled):
    response = await _apply(api, device.id, selected)
    assert response.status_code == 202, response.text
    await _settle((await _generations(device.id))[-1].job_id, status)


async def test_preflight_flex_algo_refusal_admits_isis(adapter_client, monkeypatch):
    device, _, client, transport = await setup(adapter_client, monkeypatch)
    for endpoint, payload in (
        (
            "isis-interface",
            {"processes": [{"process_tag": "CORE", "net": "49.0001.0000.0000.0001.00"}], "interfaces": []},
        ),
        ("isis-flex-algo", {"flex_algos": [{"process_tag": "CORE", "algo_id": 128, "priority": 100}]}),
    ):
        response = await adapter_client.put(
            f"/api/v1/devices/{device.id}/{endpoint}-intent?store_only=true",
            json=payload,
            headers=AUTH | {"X-Push-Seq": "41"},
        )
        assert response.status_code == 200, response.text
    record = refusal(
        "isis",
        row_ref("isis", "isis.process-config.flex-algo", (("process-config", ("CORE",)), ("flex-algo", (128,)))),
        "write-blocked",
        field="isis.flex.priority",
    )
    transport.judge = lambda body, _request: (
        (record,) if any(row.get("flex-algo") for row in body.get("isis", {}).get("process-config", [])) else None
    )
    result = await preflight.run_preflight(get_engine(), client, device, {"isis": 41, "isis_flex_algo": 41})
    assert result.status == "ready", result.error
    assert result.admitted_selection == {"isis": 41}
    assert result.streams["isis_flex_algo"]["status"] == "blocked"


async def test_preflight_drop_reveals_second_refusal_same_family(adapter_client, monkeypatch):
    device, _, client, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    transport.judge = lambda body, _request: (IP_REFUSAL,) if has_ip(body) else (attribute_refusal(),)
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "refused"
    assert result.admitted_selection == {}
    assert [attempt["dropped"] for attempt in result.attempts] == [["ip"], ["interface_config"], []]
    assert all(verdict["status"] == "blocked" for verdict in result.streams.values())


@pytest.mark.parametrize("failure", ["5xx", "transport", "empty", "none", "live", "protocol"])
async def test_preflight_inconclusive_dry_run_is_unavailable(adapter_client, monkeypatch, failure):
    device, selected, client, transport = await setup(adapter_client, monkeypatch)

    def judge(_body, request):
        if failure == "transport":
            raise httpx.ReadError(CANARY, request=request)
        if failure == "5xx":
            return httpx.Response(503, json={"errors": CANARY})
        if failure == "protocol":
            return httpx.Response(500, content=CANARY.encode())
        return httpx.Response(200, content=b"" if failure == "empty" else b"null")

    transport.judge = judge
    if failure == "live":
        transport.fake.service_status = "inconclusive"
        transport.fake.service = []
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert_text_free_of(repr(result), [CANARY])
    assert result.status == "unavailable", result.error
    assert result.admitted_selection == {} and result.plan is None
    assert result.streams["vlan"]["status"] == "not_evaluated"
    if failure == "live":
        assert not transport.dry_requests


async def test_preflight_mixed_stream_refusal_fails_closed(adapter_client, monkeypatch):
    device, _, client, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    transport.judge = lambda _body, _request: (
        refusal(
            "interface", row_ref("interface", "interface.interface", (("interface", (INTERFACE,)),)), "write-blocked"
        ),
    )
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "error"
    assert result.error == {"code": "attribution_failed", "reason": "mixed_stream"}
    assert not result.admitted_selection


async def test_preflight_ordinary_rejection_is_sanitized_error(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch)
    transport.judge = lambda _body, _request: httpx.Response(400, json={"error": CANARY})
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert_text_free_of(repr(result), [CANARY])
    assert result.status == "error"
    assert result.error == {"code": "dry_run_rejected", "message": "NSO rejected the device document"}
    assert not result.admitted_selection


async def test_preflight_prior_omission_drops_selected_stream(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch, "removal")
    assert (await _put_vlans(adapter_client, device.id, [101], seq=41, query="?store_only=true")).status_code == 200
    record = refusal("static-route", row_ref("static-route", "static-route.route", (("route", _B),)), "write-blocked")
    transport.judge = lambda body, _request: (
        (record,) if not any(row["prefix"] == _B[1] for row in body.get("static-route", {}).get("route", [])) else None
    )
    result = await preflight.run_preflight(get_engine(), client, device, selected | {"vlan": 41})
    assert result.status == "ready", result.error
    assert result.admitted_selection == {"vlan": 41}
    assert result.streams["static_route"]["refusals"][0]["provenance"] == "prior"


async def test_preflight_cross_stream_refusal_admits_nothing(adapter_client, monkeypatch):
    device, _, client, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    await authorize(adapter_client, device, {"ip": selected["ip"]})
    transport.dry_requests.clear()
    transport.judge = lambda body, _request: (
        (IP_REFUSAL,) if any(row.get("description") for row in body.get("interface", {}).get("interface", [])) else None
    )
    result = await preflight.run_preflight(get_engine(), client, device, {"interface_config": 41})
    assert result.status == "refused"
    assert result.cross_stream == ["ip"] and not result.baseline_refused
    assert not result.admitted_selection and result.plan is None
    assert [request.url.params["dry-run"] for request in transport.dry_requests] == ["native", "native", "cli"]


async def test_preflight_baseline_refusal_after_withdrawal(adapter_client, monkeypatch):
    device, vlan, client, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    await authorize(adapter_client, device, {"ip": selected["ip"]})
    transport.dry_requests.clear()
    response = await _put_ip(
        adapter_client, device.id, [{"interface": INTERFACE, "address": "192.0.2.2/24", "family": "ipv4"}], seq=42
    )
    assert response.status_code == 200

    def judge(body, _request):
        address = body["interface"]["interface"][0]["ipv4-address"][0]["address"]
        return (
            refusal(
                "interface",
                row_ref(
                    "interface", "interface.ipv4-address", (("interface", (INTERFACE,)), ("ipv4-address", (address,)))
                ),
                "write-blocked",
                field="interface.ip.v4",
            ),
        )

    transport.judge = judge
    result = await preflight.run_preflight(get_engine(), client, device, vlan | {"ip": 42})
    assert result.status == "refused" and result.baseline_refused == ["ip"]
    assert result.streams["ip"]["status"] == "blocked"
    assert not result.admitted_selection


async def test_baseline_diagnosis_replays_cli_refusal(adapter_client, monkeypatch):
    device, vlan, client, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    await authorize(adapter_client, device, {"ip": selected["ip"]})
    transport.dry_requests.clear()
    transport.judge = lambda body, request: (
        (IP_REFUSAL,) if has_ip(body) and request.url.params["dry-run"] == "cli" else None
    )
    before = await database_state()
    result = await preflight.run_preflight(get_engine(), client, device, vlan)
    assert result.status == "refused"
    assert result.baseline_refused == ["ip"]
    assert not result.cross_stream
    assert not result.admitted_selection and result.plan is None
    assert [request.url.params["dry-run"] for request in transport.dry_requests] == [
        "native",
        "cli",
        "native",
        "cli",
    ]
    assert await database_state() == before


async def test_preflight_ip_backfill_disturbing_blocked_stream(adapter_client, monkeypatch):
    device, _, client, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    await authorize(adapter_client, device, {"ip": selected["ip"]})
    transport.dry_requests.clear()
    response = await _put_ip(
        adapter_client,
        device.id,
        [{"interface": INTERFACE, "address": ADDRESS, "family": "ipv4", "parent_binding": "lag-99", "encap_tag": "99"}],
        seq=42,
    )
    assert response.status_code == 200
    transport.judge = lambda _body, _request: (IP_REFUSAL,)
    result = await preflight.run_preflight(get_engine(), client, device, {"interface_config": 41, "ip": 42})
    assert result.status == "refused" and not result.admitted_selection
    assert result.streams["ip"]["status"] == "blocked"
    assert result.streams["interface_config"]["status"] == "blocked" or result.context_disturbed == ["ip"]
    assert len(transport.dry_requests) == 1


@pytest.mark.parametrize("select_skipped_ip", [False, True])
async def test_preflight_conflicting_interface_context_blocks_promotion(adapter_client, monkeypatch, select_skipped_ip):
    device, vlan, client, transport = await setup(adapter_client, monkeypatch)
    response = await _put_ip(
        adapter_client,
        device.id,
        [{"interface": INTERFACE, "address": ADDRESS, "family": "ipv4", "parent_binding": "lag-1"}],
        seq=41,
    )
    assert response.status_code == 200
    await authorize(adapter_client, device, {"ip": 41})
    transport.dry_requests.clear()
    async with session() as db:
        interface = await db.scalar(select(DbInterface).where(DbInterface.device_id == device.id))
        interface.parent_binding = "lag-2"
        await db.commit()
    response = await _put_interface_attrs(adapter_client, device.id, "new context", seq=41, query="?store_only=true")
    assert response.status_code == 200
    before = await database_state()
    selected = vlan | {"interface_config": 41} | ({"ip": 41} if select_skipped_ip else {})
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "ready", result.error
    assert result.admitted_selection == vlan
    assert result.streams["interface_config"] == {"status": "blocked", "context_disturbed": ["ip"]}
    if select_skipped_ip:
        assert result.streams["ip"]["status"] == "not_evaluated" and result.skips["ip"] == "already_applied"
    assert await database_state() == before


async def test_preflight_skipped_refused_baseline_stays_not_evaluated(adapter_client, monkeypatch):
    device, vlan, client, transport = await setup(adapter_client, monkeypatch)
    await interface_selection(adapter_client, device)
    await authorize(adapter_client, device, {"ip": 41})
    transport.dry_requests.clear()
    transport.judge = lambda _body, _request: (IP_REFUSAL,)
    result = await preflight.run_preflight(get_engine(), client, device, vlan | {"ip": 41})
    assert result.status == "refused" and result.baseline_refused == ["ip"]
    assert result.streams["ip"]["status"] == "not_evaluated"
    assert result.skips["ip"] == "already_applied"
    assert len(result.attempts) == 1


async def test_preflight_skip_that_moves_between_passes_is_unavailable(adapter_client, monkeypatch):
    device, vlan, client, transport = await setup(adapter_client, monkeypatch)
    response = await _put_interface_attrs(adapter_client, device.id, "pending", seq=41, query="?store_only=true")
    assert response.status_code == 200

    async def push_ip():
        transport.before_dry_run = None
        response = await _put_ip(
            adapter_client, device.id, [{"interface": INTERFACE, "address": ADDRESS, "family": "ipv4"}], seq=41
        )
        assert response.status_code == 200

    transport.before_dry_run = push_ip
    transport.judge = lambda body, _request: (
        (attribute_refusal(),) if body.get("interface", {}).get("interface") else None
    )
    result = await preflight.run_preflight(get_engine(), client, device, vlan | {"interface_config": 41, "ip": 41})
    # The skipped ip became selectable between passes, so the preflight stops instead of admitting it.
    assert result.status == "unavailable"
    assert result.error == {"code": "inputs_changed_during_preflight"}
    assert not result.admitted_selection and result.plan is None


async def test_preflight_unrelated_selection_over_merged_split_interface_baseline(adapter_client, monkeypatch):
    device, selected, client, _ = await setup(adapter_client, monkeypatch)
    await interface_selection(adapter_client, device)
    await authorize(adapter_client, device, {"interface_config": 41})
    response = await _put_ip(
        adapter_client,
        device.id,
        [{"interface": INTERFACE, "address": ADDRESS, "family": "ipv4", "parent_binding": "lag-99", "encap_tag": "99"}],
        seq=42,
    )
    assert response.status_code == 200
    from nso_adapter.core.generation import create_automatic_apply

    async with session() as db:
        await create_automatic_apply(db, device.id, "ip", 42)
        await db.commit()
    await _settle((await _generations(device.id))[-1].job_id, GenerationStatus.settled)
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "ready", result.error
    assert result.admitted_selection == selected and not result.context_disturbed


async def test_preflight_detach_only_selection_is_dry_run_with_no_networking(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch, "detach")
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "ready", result.error
    assert len(transport.dry_requests) == 1
    assert_text_contains(str(transport.dry_requests[0].url), ["no-networking"])
    assert result.deltas == []


@pytest.mark.parametrize("selected_scope", [True, False])
async def test_preflight_reports_collateral_guard_block(adapter_client, monkeypatch, selected_scope):
    device, selected, client, transport = await setup(adapter_client, monkeypatch)
    transport.fake.service = []
    transport.fake.instance["vlan"] = {"vlan": [{"vlan-id": 999, "name": "unowned"}]}
    if not selected_scope:
        selected = await interface_selection(adapter_client, device)
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "refused" and not result.admitted_selection
    assert not transport.dry_requests
    assert result.error == {"code": "removal_blocked_collateral", "orphans": {"vlan/vlan": [["999"]]}}
    assert all(verdict["status"] == "not_evaluated" for verdict in result.streams.values())
    assert [attempt["dropped"] for attempt in result.attempts] == [[]]


async def test_preflight_guard_unowned_ip_fails_closed_without_dropping_streams(adapter_client, monkeypatch):
    device, _, client, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    transport.fake.service = []
    transport.fake.instance["interface"] = {
        "interface": [{"interface-name": INTERFACE, "ipv4-address": [{"address": "192.0.2.99", "prefix-length": 24}]}]
    }
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "refused" and not result.admitted_selection
    assert result.error == {
        "code": "removal_blocked_collateral",
        "orphans": {"interface_config/ipv4-address": [[INTERFACE, "192.0.2.99"]]},
    }
    assert result.streams["ip"]["status"] == "not_evaluated"
    assert result.streams["interface_config"]["status"] == "not_evaluated"
    assert [attempt["dropped"] for attempt in result.attempts] == [[]]
    assert not transport.dry_requests


@pytest.mark.parametrize("unowned_orphan", [False, True])
async def test_guard_block_drops_only_stream_owning_orphan(adapter_client, monkeypatch, unowned_orphan):
    device, _, client, transport = await setup(adapter_client, monkeypatch)
    response = await _put_ip(
        adapter_client, device.id, [{"interface": INTERFACE, "address": ADDRESS, "family": "ipv4"}], seq=41
    )
    assert response.status_code == 200, response.text
    await authorize(adapter_client, device, {"ip": 41})
    transport.dry_requests.clear()
    async with session() as db:
        interface = await db.scalar(select(DbInterface).where(DbInterface.device_id == device.id))
        interface.name = "GigabitEthernet0/2"
        await db.commit()
    response = await _put_ip(
        adapter_client,
        device.id,
        [{"interface": "GigabitEthernet0/2", "address": ADDRESS, "family": "ipv4"}],
        seq=42,
    )
    assert response.status_code == 200, response.text
    response = await adapter_client.put(
        f"/api/v1/devices/{device.id}/intent?store_only=true",
        json={
            "attributes": [{"interface": "GigabitEthernet0/3", "attribute": "description", "intent_value": "updated B"}]
        },
        headers=AUTH | {"X-Push-Seq": "42"},
    )
    assert response.status_code == 200, response.text
    transport.fake.service = []
    transport.fake.instance["interface"] = {"interface": [{"interface-name": INTERFACE}]}
    if unowned_orphan:
        transport.fake.instance["interface"]["interface"].append({"interface-name": "GigabitEthernet0/99"})
    before = await database_state()
    result = await preflight.run_preflight(get_engine(), client, device, {"ip": 42, "interface_config": 42})
    assert result.streams["ip"] == {"status": "blocked", "guard": {"interface_config/interface": [[INTERFACE]]}}
    assert await database_state() == before
    if unowned_orphan:
        assert result.status == "refused" and not result.admitted_selection and result.plan is None
        assert result.error == {
            "code": "removal_blocked_collateral",
            "orphans": {"interface_config/interface": [["GigabitEthernet0/99"]]},
        }
        assert result.streams["interface_config"] == {"status": "not_evaluated"}
        assert not transport.dry_requests
        return
    assert result.status == "ready", result.error
    assert result.admitted_selection == {"interface_config": 42}
    assert result.streams["interface_config"] == {"status": "admitted"}
    assert [attempt["dropped"] for attempt in result.attempts] == [["ip"], []]
    assert result.plan is not None
    interfaces = {row["interface-name"]: row for row in result.plan.links[-1].containers["interface"]["interface"]}
    assert interfaces.keys() == {INTERFACE, "GigabitEthernet0/3"}
    assert interfaces["GigabitEthernet0/3"]["description"] == "updated B"
    assert interfaces[INTERFACE]["ipv4-address"] == [{"address": "192.0.2.1", "prefix-length": 24, "secondary": False}]
    assert len(transport.dry_requests) == 2
    assert await database_state() == before


async def test_preflight_no_change_delta_is_conclusive(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch)
    transport.judge = lambda _body, _request: httpx.Response(200, json={"dry-run-result": {}})
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "ready"
    assert result.deltas == [{"order": 0, "native": "", "cli": ""}]


@pytest.mark.parametrize("failure", ["refusal", "unavailable"])
async def test_preflight_cli_failure_is_not_admitted(adapter_client, monkeypatch, failure):
    device, selected, client, transport = await setup(adapter_client, monkeypatch)
    record = refusal("vlan", row_ref("vlan", "vlan.vlan", (("vlan", (101,)),)), "write-blocked")

    def judge(_body, request):
        if request.url.params["dry-run"] == "cli":
            return (record,) if failure == "refusal" else httpx.Response(503, json={})
        return None

    transport.judge = judge
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == ("refused" if failure == "refusal" else "unavailable")
    assert not result.admitted_selection and result.plan is None


async def test_preflight_detach_refusal_is_not_admitted(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch, "detach")
    record = refusal("static-route", row_ref("static-route", "static-route.route", (("route", _B),)), "write-blocked")
    transport.judge = lambda _body, _request: (record,)
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "error" and result.error["code"] == "attribution_failed"
    assert not result.admitted_selection
    assert len(transport.dry_requests) == 1
    assert_text_contains(str(transport.dry_requests[0].url), ["no-networking"])


async def test_preflight_sends_only_dry_runs(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch, "removal")
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "ready", result.error
    assert len(transport.dry_requests) == 3
    assert not transport.locks
    assert len(transport.fake.reads) == 1


async def test_preflight_pass_bound(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch)
    selected |= await interface_selection(adapter_client, device)

    def judge(body, _request):
        if has_ip(body):
            return (IP_REFUSAL,)
        if body.get("interface", {}).get("interface"):
            return (attribute_refusal(),)
        return (refusal("vlan", row_ref("vlan", "vlan.vlan", (("vlan", (101,)),)), "write-blocked"),)

    transport.judge = judge
    result = await preflight.run_preflight(get_engine(), client, device, selected)
    assert result.status == "refused"
    assert len(result.attempts) == len(selected) + 1
    assert [attempt["dropped"] for attempt in result.attempts] == [["ip"], ["interface_config"], ["vlan"], []]


async def test_preflight_identity_stable_for_unchanged_inputs(adapter_client, monkeypatch):
    device, selected, client, _ = await setup(adapter_client, monkeypatch)
    first = await preflight.run_preflight(get_engine(), client, device, selected)
    second = await preflight.run_preflight(get_engine(), client, device, selected)
    assert preflight.preflight_identity(first) == preflight.preflight_identity(second)
    assert len(preflight.preflight_identity(first)) == 64
    with pytest.raises(FrozenInstanceError):
        first.status = "error"
    first.admitted_selection.clear()
    first.streams["vlan"]["status"] = "blocked"
    first.attempts.clear()
    first.deltas.clear()
    assert first.admitted_selection == selected
    assert first.streams["vlan"]["status"] == "admitted"
    assert first.attempts and first.deltas


@pytest.mark.parametrize("change", ["selected_push", "other_promotion", "ned", "name", "instance", "snapshot"])
async def test_preflight_identity_changes_with_inputs(adapter_client, monkeypatch, change):
    device, selected, client, transport = await setup(
        adapter_client, monkeypatch, "removal" if change == "snapshot" else "plain"
    )
    first = await preflight.run_preflight(get_engine(), client, device, selected)
    if change == "selected_push":
        assert (await _put_vlans(adapter_client, device.id, [102], seq=42, query="?store_only=true")).status_code == 200
        selected = {"vlan": 42}
    elif change == "other_promotion":
        await interface_selection(adapter_client, device)
        await authorize(adapter_client, device, {"interface_config": 41})
    elif change in {"ned", "name", "instance"}:
        async with session() as db:
            stored = await db.get(Device, device.id)
            setattr(
                stored,
                {"ned": "ned_id", "name": "nso_device_name", "instance": "nso_instance"}[change],
                {
                    "ned": "juniper-junos-nc-4.11:juniper-junos-nc-4.11",
                    "name": "placeholder-renamed-device",
                    "instance": "placeholder-other-instance",
                }[change],
            )
            await db.commit()
        async with session() as db:
            device = await db.get(Device, device.id)
        if change == "name":
            transport.fake.device_name = device.nso_device_name
    else:
        transport.fake.service[0]["metric"] = 99
    second = await preflight.run_preflight(get_engine(), client, device, selected)
    assert first.status == second.status == "ready"
    assert preflight.preflight_identity(first) != preflight.preflight_identity(second)


@pytest.mark.parametrize("status", [GenerationStatus.failed, GenerationStatus.outcome_unknown])
async def test_preflight_behind_blocked_head_reports_baseline_refused(adapter_client, monkeypatch, status):
    device, vlan, client, transport = await setup(adapter_client, monkeypatch)
    selected = await interface_selection(adapter_client, device)
    await authorize(adapter_client, device, {"ip": selected["ip"]}, status)
    transport.judge = lambda _body, _request: (IP_REFUSAL,)
    result = await preflight.run_preflight(get_engine(), client, device, vlan)
    assert result.status == "refused" and result.baseline_refused == ["ip"]
    assert not result.admitted_selection


async def test_abandoned_refused_immediate_promotion_blocks_unrelated_apply_as_baseline_refused(
    adapter_client, monkeypatch
):
    from tests.core.test_refusal_cutover import run_next

    device, vlan, client, transport = await setup(adapter_client, monkeypatch)
    addresses = [
        {"interface": INTERFACE, "address": address, "family": "ipv4"} for address in (ADDRESS, "192.0.2.2/24")
    ]
    assert (await _put_ip(adapter_client, device.id, addresses, seq=41)).status_code == 200
    runtime = PlanTransport(device.nso_device_name)
    runtime_client = _client_with(runtime)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: runtime_client)
    runtime.records = (IP_REFUSAL,)
    response = await adapter_client.put(
        f"/api/v1/devices/{device.id}/ip-intent?delete_origin=true",
        json={"addresses": [addresses[0]]},
        headers=AUTH | {"X-Push-Seq": "42"},
    )
    assert response.status_code == 200, response.text
    await run_next(device.id)
    generation = (await _generations(device.id))[-1]
    assert generation.status == GenerationStatus.failed
    response = await adapter_client.post(
        f"/api/v1/devices/{device.id}/actions/abandon-generation", json={"generation_id": generation.id}, headers=AUTH
    )
    assert response.status_code == 202, response.text
    transport.fake = runtime.fake
    transport.judge = lambda _body, _request: (IP_REFUSAL,)
    result = await preflight.run_preflight(get_engine(), client, device, vlan)
    assert result.status == "refused" and result.baseline_refused == ["ip"]
    assert not result.admitted_selection
