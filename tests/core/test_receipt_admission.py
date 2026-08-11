# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Receipt admission across the WHOLE in-protocol intent vocabulary (#1558 items 8-10).

Three things are pinned here, all against the real app over real HTTP:

* the stream vocabulary is derived from the intent-PUT ENDPOINTS, not from the removal
  scopes — so ``ip`` and ``isis_flex_algo``, which have a PUT and no removal scope, are
  admissible, and a future endpoint cannot silently fall out of the protocol;
* every in-protocol endpoint admits, replays and refuses, not just the vlan vertical;
* the receipt's identity includes the request MODE, so one sequence carrying one body
  cannot alias a store-only, a delete-origin and a networked operation.
"""

from __future__ import annotations

import inspect

import pytest
import sqlalchemy as sa
from fastapi.routing import iter_route_contexts

from tests.conftest import VALID_TOKEN, seed_device, session
from tests.core.test_generation_protocol import generations, put_vlans, seed_settings

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

#: One minimal, valid body per in-protocol intent PUT. Empty payloads on purpose: what is
#: under test is admission, and an empty full-replace is a legal delivery for every family.
MINIMAL_BODIES: dict[str, dict] = {
    "/api/v1/devices/{device_id}/bfd-intent": {"interfaces": []},
    "/api/v1/devices/{device_id}/bgp-intent": {"routers": []},
    "/api/v1/devices/{device_id}/intent": {"attributes": []},
    "/api/v1/devices/{device_id}/interface-mtu-intent": {"interfaces": []},
    "/api/v1/devices/{device_id}/ip-intent": {"addresses": []},
    "/api/v1/devices/{device_id}/isis-flex-algo-intent": {"flex_algos": []},
    "/api/v1/devices/{device_id}/isis-interface-intent": {"interfaces": []},
    "/api/v1/devices/{device_id}/l2-sap-intent": {"saps": []},
    "/api/v1/devices/{device_id}/logging-intent": {"hosts": []},
    "/api/v1/devices/{device_id}/ospf-intent": {},
    "/api/v1/devices/{device_id}/route-policy-intent": {"objects": []},
    "/api/v1/devices/{device_id}/snmp-intent": {},
    "/api/v1/devices/{device_id}/static-route-intent": {"routes": []},
    "/api/v1/devices/{device_id}/subinterface-intent": {"interfaces": []},
    "/api/v1/devices/{device_id}/svi-intent": {"interfaces": []},
    "/api/v1/devices/{device_id}/vlan-intent": {"vlans": []},
}


def _put_routes():
    from nso_adapter.main import create_app

    return [r for r in iter_route_contexts(create_app().routes) if "PUT" in (r.methods or ())]


def _walk_dependant(dependant):
    for dep in dependant.dependencies:
        yield dep.call
        yield from _walk_dependant(dep)


async def _receipt(device_id: int, stream: str):
    from nso_adapter.store.models import IntentPushReceipt

    async with session() as db:
        return await db.scalar(
            sa.select(IntentPushReceipt).where(
                IntentPushReceipt.device_id == device_id,
                IntentPushReceipt.section == stream,
            )
        )


def _url(path: str, device_id: int) -> str:
    return path.replace("{device_id}", str(device_id))


# ── item 10: the vocabulary comes from the endpoints ──────────────────────────


def test_every_in_protocol_intent_put_has_an_admissible_stream():
    """A PUT that is neither registered nor explicitly out of protocol fails HERE.

    The vocabulary used to be derived from the removal scopes, which have no entry for the
    interface-IP or the IS-IS flex-algo PUT — both keyed by the plugin, both unadmittable.
    """
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS, OUT_OF_PROTOCOL_PUTS

    live = {route.path for route in _put_routes()}
    assert live - OUT_OF_PROTOCOL_PUTS == set(INTENT_PUT_ENDPOINTS)
    assert OUT_OF_PROTOCOL_PUTS <= live, "an out-of-protocol exemption names a route that no longer exists"


def test_the_stream_vocabulary_is_the_sixteen_endpoint_streams():
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS, INTENT_STREAMS

    assert INTENT_STREAMS == {endpoint.stream for endpoint in INTENT_PUT_ENDPOINTS.values()}
    assert len(INTENT_STREAMS) == len(INTENT_PUT_ENDPOINTS) == 16
    # The two the removal-scope derivation could never reach.
    assert {"ip", "isis_flex_algo"} <= INTENT_STREAMS


def test_protocol_docs_do_not_copy_the_endpoint_count():
    from nso_adapter.core import generation, receipt

    for module in (generation, receipt):
        source = inspect.getsource(module)
        assert "fourteen intent PUT" not in source
        assert "sixteen intent PUT" not in source


def test_every_intent_stream_promotes_a_real_projection_family():
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS
    from nso_adapter.core.projection import projection_sections, projection_streams

    known = projection_sections()
    for path, endpoint in INTENT_PUT_ENDPOINTS.items():
        assert endpoint.promotes in known, f"{path} promotes unknown projection section {endpoint.promotes!r}"
    assert projection_streams() == {endpoint.stream for endpoint in INTENT_PUT_ENDPOINTS.values()}


def test_every_in_protocol_put_injects_the_delivery_dependency():
    """The admission seam is a dependency, so an endpoint cannot join without one."""
    from nso_adapter.api.intent_push import get_intent_delivery
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS

    for route in _put_routes():
        if route.path not in INTENT_PUT_ENDPOINTS:
            continue
        assert get_intent_delivery in set(_walk_dependant(route.dependant)), (
            f"{route.path} does not inject get_intent_delivery"
        )


def test_every_in_protocol_put_uses_the_shared_delivery_seam():
    """Projection locking and receipt admission stay ordered in one module."""
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS

    for route in _put_routes():
        if route.path not in INTENT_PUT_ENDPOINTS:
            continue
        sources = [inspect.getsource(inspect.unwrap(route.endpoint))]
        if route.path == "/api/v1/devices/{device_id}/static-route-intent":
            from nso_adapter.api.static_route import _apply_static_route_intent

            assert "return await _apply_static_route_intent(" in sources[0]
            sources.append(inspect.getsource(_apply_static_route_intent))
        assert "await begin_delivery(" in sources[-1], f"{route.path} bypasses begin_delivery"
        assert "await record_response(" in sources[-1], f"{route.path} bypasses record_response"
        for source in sources:
            assert "note_write" not in source, f"{route.path} owns projection-write ordering again"


def test_the_minimal_body_table_covers_every_in_protocol_endpoint():
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS

    assert set(MINIMAL_BODIES) == set(INTENT_PUT_ENDPOINTS)


# ── item 9: admission is not a single-vertical deferral ───────────────────────


@pytest.mark.parametrize("path", sorted(MINIMAL_BODIES))
async def test_every_in_protocol_endpoint_admits_replays_and_refuses(adapter_client, path):
    """One keyed delivery per endpoint: admitted, replayed verbatim, then refused as stale."""
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS

    stream = INTENT_PUT_ENDPOINTS[path].stream
    device_id = await seed_device(nso_device_name=f"rcp-{stream}", netbox_device_id=None)
    url = _url(path, device_id)
    body = MINIMAL_BODIES[path]

    first = await adapter_client.put(url, json=body, headers={**AUTH, "X-Push-Seq": "7"})
    assert first.status_code == 200, first.text
    receipt = await _receipt(device_id, stream)
    assert receipt is not None, f"{path} accepted a keyed push without writing a receipt"
    assert receipt.push_seq == 7

    replay = await adapter_client.put(url, json=body, headers={**AUTH, "X-Push-Seq": "7"})
    assert replay.status_code == 200
    assert replay.json() == first.json(), f"{path} did not replay the stored response"

    stale = await adapter_client.put(url, json=body, headers={**AUTH, "X-Push-Seq": "6"})
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "stale"


@pytest.mark.parametrize("path", sorted(MINIMAL_BODIES))
async def test_an_unkeyed_delivery_stays_legal_and_writes_no_receipt(adapter_client, path):
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS

    stream = INTENT_PUT_ENDPOINTS[path].stream
    device_id = await seed_device(nso_device_name=f"rcp-unkeyed-{stream}", netbox_device_id=None)

    resp = await adapter_client.put(_url(path, device_id), json=MINIMAL_BODIES[path], headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert await _receipt(device_id, stream) is None


# ── item 8: the mode is part of the receipt identity ──────────────────────────


@pytest.mark.parametrize("query", ["?store_only=true", "?delete_origin=true"])
async def test_the_same_sequence_and_body_in_another_mode_is_refused(adapter_client, query):
    """Same seq, same bytes, different operation — refused, never replayed as the first one."""
    device_id = await seed_device(nso_device_name=f"rcp-mode{query[1:6]}", netbox_device_id=None)
    body = {"vlans": [{"vlan_id": 10, "name": "ten"}]}
    url = f"/api/v1/devices/{device_id}/vlan-intent"

    assert (await adapter_client.put(url, json=body, headers={**AUTH, "X-Push-Seq": "5"})).status_code == 200

    resp = await adapter_client.put(url + query, json=body, headers={**AUTH, "X-Push-Seq": "5"})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "sequence_reuse"


async def test_a_replay_in_the_same_mode_still_returns_the_stored_response(adapter_client):
    device_id = await seed_device(nso_device_name="rcp-mode-replay", netbox_device_id=None)
    body = {"vlans": [{"vlan_id": 10, "name": "ten"}]}
    url = f"/api/v1/devices/{device_id}/vlan-intent?store_only=true"

    first = await adapter_client.put(url, json=body, headers={**AUTH, "X-Push-Seq": "5"})
    assert first.status_code == 200
    replay = await adapter_client.put(url, json=body, headers={**AUTH, "X-Push-Seq": "5"})
    assert replay.status_code == 200
    assert replay.json() == first.json()

    receipt = await _receipt(device_id, "vlan")
    assert receipt.store_only is True
    assert receipt.delete_origin is False


async def test_a_receipt_without_a_recorded_response_reexecutes_the_delivery(adapter_client):
    """A partial receipt cannot replay JSON null as a successful intent response."""
    from nso_adapter.store.models import IntentPushReceipt

    device_id = await seed_device(nso_device_name="rcp-missing-response", netbox_device_id=None)
    body = {"vlans": [{"vlan_id": 10, "name": "ten"}]}
    url = f"/api/v1/devices/{device_id}/vlan-intent"
    headers = {**AUTH, "X-Push-Seq": "5"}

    first = await adapter_client.put(url, json=body, headers=headers)
    assert first.status_code == 200
    async with session() as db:
        receipt = await db.scalar(
            sa.select(IntentPushReceipt).where(
                IntentPushReceipt.device_id == device_id,
                IntentPushReceipt.section == "vlan",
            )
        )
        receipt.response = None
        await db.commit()

    repeated = await adapter_client.put(url, json=body, headers=headers)
    assert repeated.status_code == 200
    assert repeated.json() == first.json()
    assert (await _receipt(device_id, "vlan")).response == first.json()


async def test_the_receipt_records_the_mode_the_delivery_carried(adapter_client):
    device_id = await seed_device(nso_device_name="rcp-mode-columns", netbox_device_id=None)
    url = f"/api/v1/devices/{device_id}/vlan-intent"
    assert (await adapter_client.put(url, json={"vlans": []}, headers={**AUTH, "X-Push-Seq": "1"})).status_code == 200

    receipt = await _receipt(device_id, "vlan")
    assert (receipt.store_only, receipt.delete_origin) == (False, False)

    resp = await adapter_client.put(
        url + "?delete_origin=true", json={"vlans": []}, headers={**AUTH, "X-Push-Seq": "2"}
    )
    assert resp.status_code == 200
    receipt = await _receipt(device_id, "vlan")
    assert (receipt.store_only, receipt.delete_origin) == (False, True)


async def test_two_endpoints_of_one_promotion_family_keep_separate_receipts(adapter_client):
    """``interface`` and ``ip`` promote one family but are two outbox streams.

    Sharing a receipt row would make the second endpoint's own sequence read as stale.
    """
    device_id = await seed_device(nso_device_name="rcp-two-streams", netbox_device_id=None)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent", json={"attributes": []}, headers={**AUTH, "X-Push-Seq": "40"}
    )
    assert resp.status_code == 200
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ip-intent", json={"addresses": []}, headers={**AUTH, "X-Push-Seq": "3"}
    )
    assert resp.status_code == 200, "the ip stream was judged against the interface stream's sequence"

    assert (await _receipt(device_id, "interface_config")).push_seq == 40
    assert (await _receipt(device_id, "ip")).push_seq == 3


# ── the generation a push authorized ──────────────────────────────────────────


async def _stamped_job_type(receipt):
    """The job type of the generation a receipt names. Proves WHICH link was stamped."""
    from nso_adapter.store.models import DeploymentGeneration, Job

    async with session() as db:
        generation = await db.get(DeploymentGeneration, receipt.generation_id)
        return (await db.get(Job, generation.job_id)).job_type


async def test_the_receipt_names_the_apply_generation_an_auto_apply_push_enqueued(adapter_client):
    """The tail of the chain: a push that reaches its apply is stamped with THAT generation.

    An intent PUT enqueues removals before the apply, so "the last generation this push
    enqueued" is the apply whenever auto-apply fired.
    """
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="rcp-gen-apply", netbox_device_id=9840)
    await seed_settings(device_id)

    assert (await put_vlans(adapter_client, device_id, [10], seq=1)).status_code == 200

    chain = await generations(device_id)
    receipt = await _receipt(device_id, "vlan")
    assert receipt.generation_id == chain[-1].id
    assert await _stamped_job_type(receipt) is JobType.apply


async def test_the_receipt_names_the_removal_generation_when_no_apply_follows(adapter_client):
    """With auto-apply off, the shrink's removal IS the last generation the push enqueued."""
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="rcp-gen-removal", netbox_device_id=9841)
    await seed_settings(device_id, auto_apply=False)

    assert (await put_vlans(adapter_client, device_id, [10, 20], seq=1)).status_code == 200
    assert (await _receipt(device_id, "vlan")).generation_id is None, "a push that enqueued nothing stamped a link"

    assert (await put_vlans(adapter_client, device_id, [10], seq=2)).status_code == 200

    chain = await generations(device_id)
    receipt = await _receipt(device_id, "vlan")
    assert receipt.generation_id == chain[-1].id
    assert await _stamped_job_type(receipt) is JobType.removal


async def test_a_store_only_push_names_no_generation(adapter_client):
    """Store-only authorizes no deployment, so there is no link to name."""
    device_id = await seed_device(nso_device_name="rcp-gen-store-only", netbox_device_id=9842)
    await seed_settings(device_id)

    resp = await put_vlans(adapter_client, device_id, [10], seq=1, query="?store_only=true")
    assert resp.status_code == 200

    assert await generations(device_id) == []
    assert (await _receipt(device_id, "vlan")).generation_id is None


async def test_a_later_push_that_enqueues_nothing_does_not_inherit_the_earlier_stamp(adapter_client):
    """The stamp is request-scoped: the second delivery must not read the first one's link."""
    device_id = await seed_device(nso_device_name="rcp-gen-no-leak", netbox_device_id=9843)
    await seed_settings(device_id)

    assert (await put_vlans(adapter_client, device_id, [10], seq=1)).status_code == 200
    assert (await _receipt(device_id, "vlan")).generation_id is not None

    resp = await put_vlans(adapter_client, device_id, [10], seq=2, query="?store_only=true")
    assert resp.status_code == 200

    assert (await _receipt(device_id, "vlan")).generation_id is None


# ── the two boundary refusals ─────────────────────────────────────────────────


def test_an_unregistered_route_path_has_no_stream():
    from nso_adapter.core.intent_protocol import intent_endpoint

    with pytest.raises(RuntimeError, match="not a registered in-protocol intent PUT"):
        intent_endpoint("/api/v1/devices/{device_id}/not-an-intent")


async def test_admission_refuses_a_stream_outside_the_vocabulary(adapter_client):
    """Guessing a stream would dedupe one outbox lane against another's sequences."""
    from nso_adapter.core.receipt import IntentDelivery, PushIdentity, admit_push

    device_id = await seed_device(nso_device_name="rcp-bad-stream", netbox_device_id=None)
    delivery = IntentDelivery(
        stream="lacp",
        identity=PushIdentity(seq=1, digest="0" * 64, store_only=False, delete_origin=False, backfill_only=False),
    )
    async with session() as db:
        with pytest.raises(RuntimeError, match="not an in-protocol intent stream"):
            await admit_push(db, device_id, delivery)


async def test_a_keyed_empty_json_body_is_a_validation_error(adapter_client):
    """The delivery dependency must not turn an unreadable JSON body into a server error."""
    device_id = await seed_device(nso_device_name="rcp-empty-json", netbox_device_id=None)

    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent",
        content=b"",
        headers={**AUTH, "Content-Type": "application/json", "X-Push-Seq": "9"},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_a_keyed_invalid_utf8_body_is_a_validation_error(adapter_client):
    """Undecodable JSON is a client error, not an internal server failure."""
    device_id = await seed_device(nso_device_name="rcp-invalid-utf8", netbox_device_id=None)

    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent",
        content=b"\xff\xfe{",
        headers={**AUTH, "Content-Type": "application/json", "X-Push-Seq": "10"},
    )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "validation_error",
            "message": "Request body must contain valid JSON",
            "detail": {},
        }
    }


# ── O2b.7: the header is DECLARED, not an undocumented convention ─────────────


def _push_seq_parameters(schema: dict, path: str) -> list[dict]:
    operation = schema["paths"][path.replace("{device_id}", "{device_id}")]["put"]
    return [p for p in operation.get("parameters", []) if p["name"] == "X-Push-Seq"]


def test_o2b_7_every_in_protocol_intent_put_declares_the_push_sequence_header():
    """O2b.7 — OpenAPI truthfulness applies to headers too (§4.4).

    The sequence is what makes a delivery replayable, so leaving it as a convention
    documented only in prose is exactly what the truthfulness program exists to prevent.
    """
    from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS
    from nso_adapter.main import create_app

    schema = create_app().openapi()
    for path in INTENT_PUT_ENDPOINTS:
        declared = _push_seq_parameters(schema, path)
        assert len(declared) == 1, f"{path} does not declare X-Push-Seq"
        assert declared[0]["in"] == "header"


def test_o2b_7_the_out_of_protocol_puts_declare_no_push_sequence_header():
    """O2b.7 control — a claim-less delivery must not appear to be on the sequence path."""
    from nso_adapter.core.intent_protocol import OUT_OF_PROTOCOL_PUTS
    from nso_adapter.main import create_app

    schema = create_app().openapi()
    for path in OUT_OF_PROTOCOL_PUTS:
        assert _push_seq_parameters(schema, path) == [], f"{path} declares X-Push-Seq"


def test_o2b_7_the_declared_domain_is_the_one_the_receipt_column_can_hold():
    """O2b.7 — the bounds are declared, so a client reads them instead of discovering them."""
    from nso_adapter.core.request_flags import MAX_PUSH_SEQ, MIN_PUSH_SEQ
    from nso_adapter.main import create_app

    declared = _push_seq_parameters(create_app().openapi(), "/api/v1/devices/{device_id}/vlan-intent")[0]
    bounds = [sub for sub in declared["schema"].get("anyOf", [declared["schema"]]) if sub.get("type") == "integer"]

    assert bounds, declared["schema"]
    assert (bounds[0]["minimum"], bounds[0]["maximum"]) == (MIN_PUSH_SEQ, MAX_PUSH_SEQ)
