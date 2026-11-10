# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Candidate admission through real ingestion, PostgreSQL and RESTCONF."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from fastapi.routing import iter_route_contexts
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects import postgresql

from nso_adapter.api.errors import api_error
from nso_adapter.core import apply
from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS, OUT_OF_PROTOCOL_APPLY_POSTS
from nso_adapter.core.preflight import _device_context, normalized_plan_digest, plan_action_apply, read_plan_inputs
from nso_adapter.core.receipt import digest_body
from nso_adapter.core.request_flags import STORE_ONLY
from nso_adapter.main import create_app
from nso_adapter.store.db import get_engine, session
from nso_adapter.store.models import (
    DbInterface,
    DeploymentGeneration,
    Device,
    DeviceSettings,
    Job,
    LagBundleIntent,
    SwitchportIntent,
)
from tests._secret_discipline import assert_keys_absent, assert_text_free_of
from tests.conftest import AUTH
from tests.core.test_preflight_loop import (
    ADDRESS,
    INTERFACE,
    IP_REFUSAL,
    assert_only_dry_runs,
    authorize,
    has_ip,
    setup,
)
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


async def set_auto_apply(device_id, value):
    async with session() as db:
        result = await db.execute(
            update(DeviceSettings).where(DeviceSettings.device_id == device_id).values(auto_apply=value)
        )
        assert result.rowcount == 1
        await db.commit()


async def ready(api, device_id, candidates):
    response = await preview(api, device_id, candidates)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "ready", result
    assert len(result["candidate_identity"]) == len(result["plan_digest"]) == 64
    assert isinstance(result["delivery_effect"]["promotes"], bool)
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
    plan = await plan_action_apply(
        get_engine(), device.id, {stream: revision}, live.section, expected_context=_device_context(device)
    )
    assert result["plan_digest"] == normalized_plan_digest(plan)


@pytest.mark.parametrize("auto_apply", [False, True])
async def test_candidate_preflight_persists_nothing(adapter_client, monkeypatch, auto_apply):
    device, _, _, transport = await setup(adapter_client, monkeypatch)
    initial = deepcopy(BODIES["switchport"])
    initial["source_revision"] = 41
    initial["interfaces"][0]["tagged_vlans"] = [100]
    assert (await push(adapter_client, device.id, "switchport", initial)).status_code == 200
    await set_auto_apply(device.id, auto_apply)
    before = await database_state()
    result = await ready(
        adapter_client,
        device.id,
        {stream: BODIES[stream] for stream in ("ip", "interface_config", "static_route", "lag", "switchport")},
    )
    assert result.get("delivery_effect", {}).get("promotes") is auto_apply
    assert await database_state() == before
    assert len(transport.fake.reads) == 1
    assert_only_dry_runs(transport, len(transport.dry_requests))
    assert transport.dry_requests and not transport.locks


@pytest.mark.parametrize("auto_apply", [False, True])
async def test_candidate_reports_normal_delivery_auto_apply(adapter_client, monkeypatch, auto_apply):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    await set_auto_apply(device.id, auto_apply)
    result = await ready(adapter_client, device.id, {"ip": BODIES["ip"]})
    expected = {"promotes": False}
    if auto_apply:
        expected = {"promotes": True, "effects": [{"stream": "ip", "kind": kind} for kind in ("auto_apply", "job")]}
    assert result.get("delivery_effect") == expected


async def test_candidate_probe_reports_only_new_rows_above_a_long_job_history(adapter_client, monkeypatch):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    await set_auto_apply(device.id, True)
    async with session() as db:
        # More completed jobs than asyncpg accepts bind parameters (32767).
        await db.execute(
            text("""
            INSERT INTO jobs (device_id, job_type, status, coalescible, created_at, updated_at, run_attempt)
            SELECT :device_id, 'sync', 'succeeded', false, now(), now(), 0 FROM generate_series(1, 32768)
        """),
            {"device_id": device.id},
        )
        await db.commit()
    result = await ready(adapter_client, device.id, {"ip": BODIES["ip"]})
    assert result["delivery_effect"] == {
        "promotes": True,
        "effects": [{"stream": "ip", "kind": kind} for kind in ("auto_apply", "job")],
    }


def _index_conditions(plan: dict) -> list[str]:
    nested = [condition for child in plan.get("Plans", []) for condition in _index_conditions(child)]
    return [plan["Index Cond"], *nested] if "Index Cond" in plan else nested


@pytest.mark.parametrize("model", [DeploymentGeneration, Job])
async def test_candidate_probe_watermark_queries_reach_rows_by_device_index(adapter_client, monkeypatch, model):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    queries = [
        select(func.coalesce(func.max(model.id), 0)).where(model.device_id == device.id),
        select(model.id).where(model.device_id == device.id, model.id > 0),
    ]
    async with session() as db:
        # The probe runs these under the projection lock: a full scan of retained history would hold it.
        await db.execute(text("SET LOCAL enable_seqscan = off"))
        for query in queries:
            sql = query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
            plan = (await db.execute(text(f"EXPLAIN (FORMAT JSON) {sql}"))).scalar_one()[0]["Plan"]
            assert any("device_id" in condition for condition in _index_conditions(plan)), plan


@pytest.mark.parametrize("kind", ["removal", "detach"])
@pytest.mark.parametrize("auto_apply", [False, True])
async def test_candidate_reports_normal_delivery_removal(adapter_client, monkeypatch, kind, auto_apply):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    initial = {"interfaces": [{"interface_name": INTERFACE, "mtu": 9000, "accepted_at": ACCEPTED_AT}]}
    assert (await push(adapter_client, device.id, "interface_mtu", initial)).status_code == 200
    candidate = deepcopy(initial)
    if kind == "removal":
        candidate["interfaces"][0].pop("mtu")
    else:
        candidate["interfaces"] = []
    await set_auto_apply(device.id, auto_apply)
    before = await database_state()
    result = await ready(adapter_client, device.id, {"interface_mtu": candidate})
    kinds = [kind, "job"]
    if auto_apply and kind == "removal":
        kinds.extend(["auto_apply", "job"])
    assert result.get("delivery_effect") == {
        "promotes": True,
        "effects": [{"stream": "interface_mtu", "kind": value} for value in sorted(kinds)],
    }
    assert await database_state() == before


@pytest.mark.parametrize("stream", ["lag", "switchport"])
async def test_candidate_switching_preparation_does_not_promote(adapter_client, monkeypatch, stream):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    await set_auto_apply(device.id, True)
    result = await ready(adapter_client, device.id, {stream: BODIES[stream]})
    assert result.get("delivery_effect") == {"promotes": False}


async def test_candidate_identity_binds_delivery_effect(adapter_client, monkeypatch):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    first = await ready(adapter_client, device.id, {"ip": BODIES["ip"]})
    await set_auto_apply(device.id, True)
    second = await ready(adapter_client, device.id, {"ip": BODIES["ip"]})
    assert first["plan_digest"] == second["plan_digest"]
    assert first["candidate_identity"] != second["candidate_identity"]


async def test_candidate_probe_uses_next_normal_delivery_identity(adapter_client, monkeypatch):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    assert (await push(adapter_client, device.id, "ip", BODIES["ip"])).status_code == 200
    digest = digest_body(BODIES["ip"])
    async with session() as db:
        await db.execute(
            text(f"""
            CREATE FUNCTION check_normal_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NOT NEW.store_only AND (
                    NEW.push_seq != 43 OR NEW.request_digest != '{digest}'
                    OR NEW.delete_origin OR NEW.backfill_only
                ) THEN
                    RAISE EXCEPTION 'normal delivery identity differs';
                END IF;
                RETURN NEW;
            END $$
        """)
        )
        await db.execute(
            text("""
            CREATE TRIGGER check_normal_receipt BEFORE INSERT OR UPDATE ON intent_push_receipt
            FOR EACH ROW EXECUTE FUNCTION check_normal_receipt()
        """)
        )
        await db.commit()
    result = await ready(adapter_client, device.id, {"ip": BODIES["ip"]})
    assert result["delivery_effect"] == {"promotes": False}


async def test_candidate_delivery_probe_failure_is_not_ready(adapter_client, monkeypatch):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    async with session() as db:
        await db.execute(
            text("""
            CREATE FUNCTION refuse_normal_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NOT NEW.store_only THEN
                    RAISE EXCEPTION 'normal delivery cannot complete';
                END IF;
                RETURN NEW;
            END $$
        """)
        )
        await db.execute(
            text("""
            CREATE TRIGGER refuse_normal_receipt BEFORE INSERT OR UPDATE ON intent_push_receipt
            FOR EACH ROW EXECUTE FUNCTION refuse_normal_receipt()
        """)
        )
        await db.commit()
    before = await database_state()
    response = await preview(adapter_client, device.id, {"ip": BODIES["ip"]})
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "unavailable", result
    assert result["error"] == {"code": "delivery_probe_failed"}
    assert result["admitted_selection"] == {}
    assert_keys_absent(result, ["candidate_identity", "plan_digest", "delivery_effect"])
    assert await database_state() == before


async def test_candidate_delivery_probe_api_error_is_not_ready(adapter_client, monkeypatch):
    device, _, _, _ = await setup(adapter_client, monkeypatch)
    await set_auto_apply(device.id, True)

    enqueue_apply = apply.enqueue_apply

    async def refuse_normal_auto_apply(*args, **kwargs):
        if STORE_ONLY.get():
            return await enqueue_apply(*args, **kwargs)
        raise api_error(409, "conflict", "normal delivery cannot complete")

    # The normal-mode probe fails after the delivery has written its rows.
    monkeypatch.setattr(apply, "enqueue_apply", refuse_normal_auto_apply)
    before = await database_state()
    response = await preview(adapter_client, device.id, {"ip": BODIES["ip"]})
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "unavailable", result
    assert result["error"] == {"code": "delivery_probe_failed"}
    assert result["admitted_selection"] == {}
    assert_keys_absent(result, ["candidate_identity", "plan_digest", "delivery_effect"])
    assert await database_state() == before


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
    before = await database_state()
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
    assert await database_state() == before


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
    plan = await plan_action_apply(
        get_engine(),
        device.id,
        {stream: pushed.json()["selection_revision"]},
        live.section,
        expected_context=_device_context(device),
    )
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

    before = await database_state()
    response = await preview(adapter_client, device.id, {stream: remove_timestamps(BODIES[stream])})
    assert response.status_code == 422, response.text
    assert await database_state() == before


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
    before = await database_state()
    response = await adapter_client.post(
        f"/api/v1/devices/{device.id}/actions/apply-preflight", json=body, headers=AUTH
    )
    assert response.status_code == 422, response.text
    assert not transport.fake.reads
    assert await database_state() == before


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
    before = await database_state()
    response = await preview(adapter_client, device.id, {"interface_config": BODIES["interface_config"]})
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "refused", result
    assert result[diagnosis] == ["ip"]
    assert not result["admitted_selection"]
    assert_keys_absent(result, ["candidate_identity", "plan_digest"])
    assert await database_state() == before


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
    before = await database_state()
    response = await preview(adapter_client, device.id, candidates)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "refused", result
    assert result["streams"]["ip"] == {"status": "blocked", "guard": {"interface_config/interface": [[INTERFACE]]}}
    assert not result["admitted_selection"]
    assert [attempt["dropped"] for attempt in result["attempts"]] == [[]]
    assert not transport.dry_requests
    assert_keys_absent(result, ["candidate_identity", "plan_digest"])
    assert await database_state() == before


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


@pytest.mark.parametrize(
    "objects",
    [
        {},
        "",
        [[[key, value] for key, value in BODIES["route_policy"]["objects"][0].items()]],
    ],
    ids=["empty-mapping", "empty-string", "pair-list"],
)
async def test_candidate_route_policy_rejects_objects_the_live_put_rejects(adapter_client, monkeypatch, objects):
    device, _, _, transport = await setup(adapter_client, monkeypatch)
    before = await database_state()
    live = await push(adapter_client, device.id, "route_policy", {"objects": objects})
    assert live.status_code == 422, live.text
    response = await preview(adapter_client, device.id, {"route_policy": {"objects": objects}})
    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "validation_error"
    assert not transport.fake.reads
    assert await database_state() == before
