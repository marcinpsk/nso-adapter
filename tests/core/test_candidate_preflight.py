# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Candidate admission through real ingestion, PostgreSQL and RESTCONF."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from fastapi.routing import iter_route_contexts
from sqlalchemy import select

from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS, OUT_OF_PROTOCOL_APPLY_POSTS
from nso_adapter.core.preflight import normalized_plan_digest, plan_action_apply, read_plan_inputs
from nso_adapter.main import create_app
from nso_adapter.store.db import get_engine, session
from nso_adapter.store.models import Base, DbInterface, Device, LagBundleIntent, SwitchportIntent
from tests._secret_discipline import assert_keys_absent, assert_text_free_of
from tests.conftest import AUTH
from tests.core.test_preflight_loop import ADDRESS, INTERFACE, IP_REFUSAL, authorize, has_ip, setup
from tests.core.test_preflight_plan import database_state

pytestmark = pytest.mark.anyio
ACCEPTED_AT = "2026-10-20T12:00:00Z"
BODIES = {
    "ip": {"addresses": [{"interface": INTERFACE, "address": ADDRESS, "family": "ipv4", "accepted_at": ACCEPTED_AT}]},
    "interface_config": {
        "attributes": [
            {
                "interface": INTERFACE,
                "attribute": "description",
                "intent_value": "candidate description",
                "accepted_at": ACCEPTED_AT,
            }
        ]
    },
    "static_route": {
        "routes": [
            {
                "route_id": 17,
                "generation": 1,
                "prefix": "198.18.1.0/24",
                "next_hop": "198.18.0.1",
                "accepted_at": ACCEPTED_AT,
            }
        ],
        "deleted_routes": [],
    },
    "lag": {
        "bundles": [{"name": "Port-channel1", "lag_id": 1, "members": [{"interface_name": INTERFACE}]}],
        "deleted_roots": [],
        "source_revision": 42,
        "accepted_at": ACCEPTED_AT,
    },
    "switchport": {
        "interfaces": [{"interface_name": INTERFACE, "mode": "trunk", "tagged_vlans": [101, 102]}],
        "deleted_roots": [],
        "source_revision": 42,
        "accepted_at": ACCEPTED_AT,
    },
    "ospf": {
        "instances": [
            {"process_id": "1", "accepted_at": ACCEPTED_AT, "redistribution": [{"source_protocol": "connected"}]}
        ],
        "interfaces": [],
    },
    "bgp": {
        "routers": [
            {
                "asn": "64512",
                "accepted_at": ACCEPTED_AT,
                "scopes": [
                    {"address_families": [{"af": "ipv4-unicast", "redistribution": [{"source_protocol": "connected"}]}]}
                ],
            }
        ]
    },
    "isis": {
        "processes": [
            {
                "process_tag": "CORE",
                "net": "49.0001.0000.0000.0001.00",
                "accepted_at": ACCEPTED_AT,
                "redistribution": [{"source_protocol": "connected"}],
            }
        ],
        "interfaces": [],
    },
    "route_policy": {
        "objects": [
            {
                "family": "prefix_list",
                "name": "candidate-prefixes",
                "entries": [{"sequence": 10, "action": "permit", "prefix": "198.18.1.0/24"}],
                "accepted": True,
                "accepted_at": ACCEPTED_AT,
            }
        ]
    },
}


def endpoint(stream):
    if stream in OUT_OF_PROTOCOL_APPLY_POSTS.values():
        return next(path for path, value in OUT_OF_PROTOCOL_APPLY_POSTS.items() if value == stream), "POST"
    return next(path for path, value in INTENT_PUT_ENDPOINTS.items() if value.stream == stream), "PUT"


async def push(api, device_id, stream, body, seq=42):
    path, method = endpoint(stream)
    return await api.request(
        method,
        path.format(device_id=device_id) + ("?store_only=true" if method == "PUT" else ""),
        json=body,
        headers=AUTH | ({"X-Push-Seq": str(seq)} if method == "PUT" else {}),
    )


async def preview(api, device_id, candidates):
    return await api.post(
        f"/api/v1/devices/{device_id}/actions/apply-preflight",
        json={"candidates": candidates},
        headers=AUTH,
    )


async def ready(api, device_id, candidates):
    response = await preview(api, device_id, candidates)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "ready", result
    assert len(result["candidate_identity"]) == len(result["plan_digest"]) == 64
    assert_keys_absent(result, ["preflight_identity"])
    return result


@pytest.mark.parametrize("stream", BODIES)
async def test_candidate_preflight_matches_pushed_apply_document(adapter_client, monkeypatch, stream):
    device, _, client, _ = await setup(adapter_client, monkeypatch)
    result = await ready(adapter_client, device.id, {stream: BODIES[stream]})
    pushed = await push(adapter_client, device.id, stream, BODIES[stream])
    assert pushed.status_code == 200, pushed.text
    revision = pushed.json()["selection_revision"] if stream in {"lag", "switchport"} else 42
    live = await read_plan_inputs(client, device)
    plan = await plan_action_apply(get_engine(), device.id, {stream: revision}, live.section)
    assert result["plan_digest"] == normalized_plan_digest(plan)


async def all_rows():
    async with session() as db:
        return {
            table.name: (await db.execute(select(table).order_by(*table.primary_key))).all()
            for table in Base.metadata.sorted_tables
        }


async def test_candidate_preflight_persists_nothing(adapter_client, monkeypatch):
    device, _, _, transport = await setup(adapter_client, monkeypatch)
    initial = deepcopy(BODIES["switchport"])
    initial["source_revision"] = 41
    initial["interfaces"][0]["tagged_vlans"] = [100]
    assert (await push(adapter_client, device.id, "switchport", initial)).status_code == 200
    counts, rows = await database_state(), await all_rows()
    await ready(
        adapter_client,
        device.id,
        {stream: BODIES[stream] for stream in ("ip", "interface_config", "static_route", "lag", "switchport")},
    )
    assert await database_state() == counts
    assert await all_rows() == rows
    assert len(transport.fake.reads) == 1
    assert not transport.documents
    assert not transport.locks
    assert all("dry-run=" in str(request.url) for request in transport.dry_requests)


@pytest.mark.parametrize("stream", BODIES)
async def test_candidate_preflight_is_stable_across_repeats(adapter_client, monkeypatch, stream):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    first = await ready(adapter_client, device.id, {stream: BODIES[stream]})
    second = await ready(adapter_client, device.id, {stream: BODIES[stream]})
    assert (first["candidate_identity"], first["plan_digest"]) == (second["candidate_identity"], second["plan_digest"])


async def test_candidate_refusal_names_candidate_rows(adapter_client, monkeypatch):
    device, _, _, transport = await setup(adapter_client, monkeypatch)
    async with session() as db:
        (await db.get(Device, device.id)).ned_id = "juniper-junos-nc-4.1"
        await db.commit()
    transport.judge = lambda body, _request: (IP_REFUSAL,) if has_ip(body) else None
    before = await all_rows()
    response = await preview(
        adapter_client, device.id, {"ip": BODIES["ip"], "interface_config": BODIES["interface_config"]}
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "refused", result
    assert result["streams"]["ip"]["status"] == "blocked"
    record = result["refusals"][0]
    assert record["field"] == "interface.ip.v4"
    assert record["logical_rows"][0]["stream"] == "ip"
    expected_row = {"subject": IP_REFUSAL.row.subject, "key": IP_REFUSAL.row.key}
    assert record["row"] == expected_row
    assert record["logical_rows"][0]["row"] == expected_row
    assert record["logical_rows"][0]["carrier"]["accepted_at"] == "2026-10-20T12:00:00+00:00"
    assert_keys_absent(result, ["candidate_identity", "plan_digest", "preflight_identity"])
    assert [attempt["dropped"] for attempt in result["attempts"]] == [[]]
    assert await all_rows() == before


@pytest.mark.parametrize("stream", ["lag", "switchport"])
async def test_switching_push_stamps_an_explicit_time_on_unchanged_rows(adapter_client, monkeypatch, stream):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    first = deepcopy(BODIES[stream])
    first["source_revision"] = 41
    first["accepted_at"] = "2026-10-19T12:00:00Z"
    assert (await push(adapter_client, device.id, stream, first)).status_code == 200
    assert (await push(adapter_client, device.id, stream, BODIES[stream])).status_code == 200
    model = LagBundleIntent if stream == "lag" else SwitchportIntent
    async with session() as db:
        stamps = (await db.execute(select(model.accepted_at).where(model.device_id == device.id))).scalars().all()
    assert stamps == [datetime(2026, 10, 20, 12, tzinfo=UTC)]


@pytest.mark.parametrize("stream", ["lag", "switchport"])
async def test_candidate_switching_uses_fresh_prepared_slot(adapter_client, monkeypatch, stream):
    device, _, client, _ = await setup(adapter_client, monkeypatch)
    old = deepcopy(BODIES[stream])
    old["source_revision"] = 41
    old["bundles" if stream == "lag" else "interfaces"] = []
    assert (await push(adapter_client, device.id, stream, old)).status_code == 200
    result = await ready(adapter_client, device.id, {stream: BODIES[stream]})
    pushed = await push(adapter_client, device.id, stream, BODIES[stream])
    live = await read_plan_inputs(client, device)
    plan = await plan_action_apply(get_engine(), device.id, {stream: pushed.json()["selection_revision"]}, live.section)
    assert result["plan_digest"] == normalized_plan_digest(plan)
    assert plan.links[0].containers


def test_candidate_registry_covers_every_intent_put():
    app = create_app()
    registry = app.state.candidate_registry
    routes = {route.path: route for route in iter_route_contexts(app.routes) if "PUT" in route.methods}
    assert {stream for stream, handler in registry.items() if handler.receipt} == {
        entry.stream for entry in INTENT_PUT_ENDPOINTS.values()
    }
    for path, entry in INTENT_PUT_ENDPOINTS.items():
        assert registry[entry.stream].endpoint is getattr(
            routes[path].endpoint, "candidate_ingester", routes[path].endpoint
        )
        assert registry[entry.stream].body_name == routes[path].dependant.body_params[0].name


def test_app_build_refuses_incomplete_candidate_wiring(monkeypatch):
    from nso_adapter.api import candidate_ingestion

    posts = {**OUT_OF_PROTOCOL_APPLY_POSTS, "/api/v1/devices/{device_id}/unrouted-intent": "unrouted"}
    monkeypatch.setattr(candidate_ingestion, "OUT_OF_PROTOCOL_APPLY_POSTS", posts)
    with pytest.raises(RuntimeError, match="missing an ingestion route"):
        create_app()


def test_candidate_openapi_declares_both_modes():
    from nso_adapter.core.request_flags import MAX_PUSH_SEQ, MIN_PUSH_SEQ

    schemas = create_app().openapi()["components"]["schemas"]
    request = schemas["ApplyPreflightIn"]
    assert {"selected", "candidates"} == request["properties"].keys()
    selected = next(item for item in request["properties"]["selected"]["anyOf"] if item["type"] == "object")
    assert selected["additionalProperties"]["minimum"] == MIN_PUSH_SEQ
    assert selected["additionalProperties"]["maximum"] == MAX_PUSH_SEQ
    assert {"candidate_identity", "plan_digest"} <= schemas["ApplyPreflightOut"]["properties"].keys()


@pytest.mark.parametrize("stream", BODIES)
async def test_candidate_missing_timestamp_is_422(adapter_client, monkeypatch, stream):
    device, _, _, _ = await setup(adapter_client, monkeypatch)

    def remove_timestamps(value):
        if isinstance(value, list):
            return [remove_timestamps(item) for item in value]
        if isinstance(value, dict):
            return {key: remove_timestamps(item) for key, item in value.items() if key != "accepted_at"}
        return value

    before = await all_rows()
    response = await preview(adapter_client, device.id, {stream: remove_timestamps(BODIES[stream])})
    assert response.status_code == 422, response.text
    assert await all_rows() == before


@pytest.mark.parametrize("change", ["authorized", "ned_id", "nso_device_name", "nso_instance", "live"])
async def test_candidate_identity_changes_when(adapter_client, monkeypatch, change):
    device, selected, _, transport = await setup(adapter_client, monkeypatch)
    first = await ready(adapter_client, device.id, {"static_route": BODIES["static_route"]})
    if change == "authorized":
        await authorize(adapter_client, device, selected)
    elif change == "live":
        transport.fake.service = []
        transport.fake.instance["description"] = "changed live input"
    else:
        async with session() as db:
            row = await db.get(Device, device.id)
            setattr(
                row,
                change,
                {
                    "ned_id": "cisco-ios-cli-6.95",
                    "nso_device_name": "placeholder-remapped-device",
                    "nso_instance": "placeholder-other-instance",
                }[change],
            )
            await db.commit()
        if change == "nso_device_name":
            transport.fake.device_name = "placeholder-remapped-device"
    second = await ready(adapter_client, device.id, {"static_route": BODIES["static_route"]})
    assert first["candidate_identity"] != second["candidate_identity"]


async def test_candidate_normalizes_body_before_hashing(adapter_client, monkeypatch):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    first = await ready(adapter_client, device.id, {"static_route": BODIES["static_route"]})
    equivalent = deepcopy(BODIES["static_route"])
    equivalent["routes"][0].update(accepted_at="2026-10-20T14:00:00+02:00", vrf="", metric=None)
    second = await ready(adapter_client, device.id, {"static_route": equivalent})
    assert first["candidate_identity"] == second["candidate_identity"]
    assert first["plan_digest"] == second["plan_digest"]


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"selected": {}, "candidates": {"ip": BODIES["ip"]}},
        {"selected": None},
        {"candidates": None},
        {"candidates": {}},
        {"candidates": {"unknown": {}}},
        {"candidates": {"route_policy": {}}},
        {
            "candidates": {
                "ip": {
                    "addresses": [{"interface": INTERFACE, "address": ADDRESS, "family": "ipv4", "accepted_at": None}]
                }
            }
        },
    ],
)
async def test_candidate_rejects_invalid_modes_and_timestamps(adapter_client, monkeypatch, body):
    device, _, _, transport = await setup(adapter_client, monkeypatch)
    before = await all_rows()
    response = await adapter_client.post(
        f"/api/v1/devices/{device.id}/actions/apply-preflight", json=body, headers=AUTH
    )
    assert response.status_code == 422, response.text
    assert not transport.fake.reads
    assert await all_rows() == before


@pytest.mark.parametrize("diagnosis", ["baseline_refused", "cross_stream"])
async def test_candidate_unselected_refusal_uses_baseline_diagnosis(adapter_client, monkeypatch, diagnosis):
    device, _, _, transport = await setup(adapter_client, monkeypatch)
    assert (await push(adapter_client, device.id, "ip", BODIES["ip"])).status_code == 200
    await authorize(adapter_client, device, {"ip": 42})
    transport.judge = lambda body, _request: (
        (IP_REFUSAL,)
        if diagnosis == "baseline_refused"
        or any(row.get("description") for row in body.get("interface", {}).get("interface", []))
        else None
    )
    before = await all_rows()
    response = await preview(adapter_client, device.id, {"interface_config": BODIES["interface_config"]})
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "refused", result
    assert result[diagnosis] == ["ip"]
    assert not result["admitted_selection"]
    assert_keys_absent(result, ["candidate_identity", "plan_digest"])
    assert await all_rows() == before


async def test_candidate_guard_block_does_not_drop_candidate(adapter_client, monkeypatch):
    device, _, _, transport = await setup(adapter_client, monkeypatch)
    assert (await push(adapter_client, device.id, "ip", BODIES["ip"])).status_code == 200
    await authorize(adapter_client, device, {"ip": 42})
    async with session() as db:
        interface = await db.scalar(select(DbInterface).where(DbInterface.device_id == device.id))
        interface.name = "GigabitEthernet0/2"
        await db.commit()
    candidates = deepcopy({"ip": BODIES["ip"], "interface_config": BODIES["interface_config"]})
    candidates["ip"]["addresses"][0]["interface"] = "GigabitEthernet0/2"
    candidates["interface_config"]["attributes"][0]["interface"] = "GigabitEthernet0/3"
    transport.fake.service = []
    transport.fake.instance["interface"] = {"interface": [{"interface-name": INTERFACE}]}
    transport.dry_requests.clear()
    before = await all_rows()
    response = await preview(adapter_client, device.id, candidates)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "refused", result
    assert result["streams"]["ip"] == {"status": "blocked", "guard": {"interface_config/interface": [[INTERFACE]]}}
    assert not result["admitted_selection"]
    assert [attempt["dropped"] for attempt in result["attempts"]] == [[]]
    assert not transport.dry_requests
    assert_keys_absent(result, ["candidate_identity", "plan_digest"])
    assert await all_rows() == before


async def test_candidate_mapping_change_during_live_read_needs_new_preflight(adapter_client, monkeypatch):
    import httpx

    from tests.nso.test_apply_send import _client_with

    device, _, _, transport = await setup(adapter_client, monkeypatch)

    async def handle(request):
        response = await transport.handle_async_request(request)
        if request.method == "GET":
            async with session() as db:
                (await db.get(Device, device.id)).nso_device_name = "placeholder-remapped-device"
                await db.commit()
        return response

    client = _client_with(httpx.MockTransport(handle))
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    response = await preview(adapter_client, device.id, {"ip": BODIES["ip"]})
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "unavailable", result
    assert result["error"] == {"code": "inputs_changed_during_preflight"}
    assert_keys_absent(result, ["candidate_identity", "plan_digest"])
    assert not transport.dry_requests


async def test_candidate_validation_redacts_caller_stream_keys(adapter_client, monkeypatch):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    response = await preview(adapter_client, device.id, {"placeholder-candidate-key": []})
    assert response.status_code == 422
    assert_text_free_of(response.text, ["placeholder-candidate-key"])
    assert response.json()["error"]["detail"]["errors"][0]["loc"] == ["body", "candidates", "[redacted]"]
