# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""AS number boundaries and stored-content refusals."""

import json
import re
from copy import deepcopy
from pathlib import Path

import pytest
from sqlalchemy import select

from nso_adapter.store.models import BgpRouterIntent, JobStatus, RedistributionIntent
from tests._secret_discipline import assert_text_contains, assert_text_free_of
from tests.api.test_api_bgp_intent import _router_with_redist
from tests.conftest import AUTH, push_seq, seed_device, session


@pytest.mark.parametrize(
    "invalid", ["064520", "+1", "1.2.3", "4294967296", "65536.1x", "", " 64512", "64512 ", "1.02", "٠١"]
)
@pytest.mark.parametrize("field", ["asn", "remote_as", "local_as"])
async def test_bgp_invalid_asn_has_a_field_error(adapter_client, invalid, field):
    device_id = await seed_device(nso_device_name="placeholder-device")
    router = {"asn": "64512", "scopes": [{"peers": [{"peer_address": "198.18.0.1"}]}]}
    if field == "asn":
        router[field] = invalid
        location = ["body", "routers", 0, field]
    else:
        router["scopes"][0]["peers"][0][field] = invalid
        location = ["body", "routers", 0, "scopes", 0, "peers", 0, field]
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent?store_only=true",
        json={"routers": [router]},
        headers=AUTH | push_seq(),
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 422, response.text
    assert any(e["loc"] == location for e in response.json()["error"]["detail"]["errors"])


@pytest.mark.parametrize("replacement", [[], [{"asn": "64512", "scopes": []}]])
async def test_bgp_replacement_refuses_malformed_stored_rows(adapter_client, replacement):
    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        db.add(BgpRouterIntent(device_id=device_id, asn="65536.1x"))
        db.add(
            RedistributionIntent(
                device_id=device_id,
                dest_protocol="bgp",
                dest_ref="65536.1x::ipv4-unicast",
                source_protocol="connected",
                source_ref="",
            )
        )
        await db.commit()
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": replacement},
        headers=AUTH | push_seq(),
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 409, response.text
    error = response.json()["error"]
    assert error["code"] == "asn_rule_violation"
    assert error["detail"]["table"] == "bgp_router_intent"
    assert error["detail"]["field"] == "asn"
    assert "violates RFC 5396" in error["message"]
    async with session() as db:
        stored = (await db.execute(select(BgpRouterIntent))).scalar_one()
        assert stored.asn == "65536.1x"
        assert error["detail"]["row_id"] == stored.id
        assert len(list((await db.execute(select(RedistributionIntent))).scalars())) == 1


@pytest.mark.parametrize("with_redistribution", [False, True])
async def test_bgp_duplicate_as_spellings_are_refused_atomically(adapter_client, with_redistribution):
    device_id = await seed_device(nso_device_name="placeholder-device")
    entries = [{"source_protocol": "connected"}] if with_redistribution else []
    routers = [_router_with_redist(entries, asn=asn) for asn in ["4200000000", "64086.59904"]]
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent?store_only=true",
        json={"routers": routers},
        headers=AUTH | push_seq(),
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 409, response.text
    assert_text_contains(response.json()["error"]["message"], ["unique AS"])
    async with session() as db:
        assert list((await db.execute(select(BgpRouterIntent))).scalars()) == []
        assert list((await db.execute(select(RedistributionIntent))).scalars()) == []


async def test_bgp_duplicate_redistribution_destination_is_refused(adapter_client):
    device_id = await seed_device(nso_device_name="placeholder-device")
    router = _router_with_redist([{"source_protocol": "connected"}], asn="64512")
    router["scopes"].append(deepcopy(router["scopes"][0]))
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent?store_only=true",
        json={"routers": [router]},
        headers=AUTH | push_seq(),
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 409, response.text
    assert_text_contains(response.json()["error"]["message"], ["redistribution"])


async def test_bgp_clear_refuses_stored_duplicate_destinations(adapter_client):
    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        for asn in ["4200000000", "64086.59904"]:
            db.add(
                RedistributionIntent(
                    device_id=device_id,
                    dest_protocol="bgp",
                    dest_ref=f"{asn}::ipv4-unicast",
                    source_protocol="connected",
                    source_ref="",
                )
            )
        await db.commit()
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": []},
        headers=AUTH | push_seq(),
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "asn_rule_violation"
    async with session() as db:
        assert len(list((await db.execute(select(RedistributionIntent))).scalars())) == 2


def _redistribution_request(protocol, source_ref):
    entries = [{"source_protocol": "bgp", "source_ref": source_ref}]
    if protocol == "bgp":
        return "bgp-intent", {"routers": [_router_with_redist(entries, asn="64512")]}
    if protocol == "ospf":
        return "ospf-intent", {
            "instances": [{"process_id": "placeholder-process", "redistribution": entries}],
            "interfaces": [],
        }
    return "isis-interface-intent", {
        "processes": [{"process_tag": "placeholder-process", "redistribution": entries}],
        "interfaces": [],
    }


@pytest.mark.parametrize("protocol", ["bgp", "ospf", "isis"])
@pytest.mark.parametrize("invalid", ["064520", "65536.1x", "", " 64512"])
async def test_redistribution_bgp_source_as_has_a_field_error(adapter_client, protocol, invalid):
    device_id = await seed_device(nso_device_name="placeholder-device")
    endpoint, body = _redistribution_request(protocol, invalid)
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/{endpoint}?store_only=true", json=body, headers=AUTH | push_seq()
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 422, response.text
    assert any(e["loc"][-1] == "source_ref" for e in response.json()["error"]["detail"]["errors"])


@pytest.mark.parametrize("protocol", ["bgp", "ospf", "isis"])
async def test_redistribution_source_notation_change_retains_identity(adapter_client, protocol):
    device_id = await seed_device(nso_device_name="placeholder-device")
    for source_ref in ["64086.59904", "4200000000"]:
        endpoint, body = _redistribution_request(protocol, source_ref)
        response = await adapter_client.put(
            f"/api/v1/devices/{device_id}/{endpoint}?store_only=true", json=body, headers=AUTH | push_seq()
        )
        assert_text_free_of(response.text, ["placeholder-device"])
        assert response.status_code == 200
        async with session() as db:
            row = (await db.execute(select(RedistributionIntent))).scalar_one()
            if source_ref == "64086.59904":
                initial_id = row.id
            else:
                assert row.id == initial_id
                assert row.source_ref == source_ref


@pytest.mark.parametrize("protocol", ["bgp", "ospf", "isis"])
async def test_redistribution_bgp_source_requires_an_as_number(adapter_client, protocol):
    device_id = await seed_device(nso_device_name="placeholder-device")
    endpoint, body = _redistribution_request(protocol, "64512")
    if protocol == "bgp":
        entries = body["routers"][0]["scopes"][0]["address_families"][0]["redistribution"]
    else:
        entries = body["instances" if protocol == "ospf" else "processes"][0]["redistribution"]
    del entries[0]["source_ref"]
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/{endpoint}?store_only=true", json=body, headers=AUTH | push_seq()
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 422, response.text
    assert any(e["loc"][-1] == "source_ref" for e in response.json()["error"]["detail"]["errors"])


@pytest.mark.parametrize("protocol", ["bgp", "ospf", "isis"])
async def test_redistribution_source_duplicate_spellings_are_refused(adapter_client, protocol):
    device_id = await seed_device(nso_device_name="placeholder-device")
    endpoint, body = _redistribution_request(protocol, "4200000000")
    if protocol == "bgp":
        entries = body["routers"][0]["scopes"][0]["address_families"][0]["redistribution"]
    else:
        entries = body["instances" if protocol == "ospf" else "processes"][0]["redistribution"]
    entries.append({"source_protocol": "bgp", "source_ref": "64086.59904"})
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/{endpoint}?store_only=true", json=body, headers=AUTH | push_seq()
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "conflict"
    async with session() as db:
        assert list((await db.execute(select(RedistributionIntent))).scalars()) == []


async def test_bgp_delete_origin_refuses_malformed_authorized_projection(adapter_client):
    from nso_adapter.store.models import DeviceProjectionStream
    from tests.core.test_generation_protocol import seed_settings

    device_id = await seed_device(nso_device_name="placeholder-device")
    await seed_settings(device_id, auto_apply=True)
    endpoint = f"/api/v1/devices/{device_id}/bgp-intent"
    response = await adapter_client.put(endpoint, json={"routers": [{"asn": "64512"}]}, headers=AUTH | push_seq())
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 200, response.text

    async with session() as db:
        router = (await db.execute(select(BgpRouterIntent))).scalar_one()
        router.asn = "064512"
        projection = (
            await db.execute(select(DeviceProjectionStream).where(DeviceProjectionStream.stream == "bgp"))
        ).scalar_one()
        document = deepcopy(projection.authorized_document)
        assert document
        document["bgp_router_intent"][0]["asn"] = "064512"
        projection.authorized_document = document
        await db.commit()
    response = await adapter_client.put(
        endpoint + "?delete_origin=true", json={"routers": []}, headers=AUTH | push_seq()
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "asn_rule_violation"
    async with session() as db:
        assert (await db.scalar(select(BgpRouterIntent))).asn == "064512"


@pytest.mark.parametrize("store_only", [False, True])
@pytest.mark.parametrize("replacement", [[], [{"asn": "4200000000"}]])
@pytest.mark.parametrize("duplicate", ["router", "destination", "source"])
async def test_bgp_replacement_refuses_authorized_as_collisions(adapter_client, store_only, replacement, duplicate):

    from nso_adapter.core.projection import _row_dict
    from nso_adapter.store.models import DeviceProjectionStream
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head, seed_settings

    device_id = await seed_device(nso_device_name="placeholder-device")
    await seed_settings(device_id, auto_apply=True)
    endpoint = f"/api/v1/devices/{device_id}/bgp-intent"
    router = _router_with_redist([{"source_protocol": "bgp", "source_ref": "4200000000"}], asn="4200000000")
    response = await adapter_client.put(endpoint, json={"routers": [router]}, headers=AUTH | push_seq())
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 200
    client, _ = recorded_client("placeholder-device")
    initial_job = await job_row(await run_head(device_id, client))
    assert initial_job.status is JobStatus.succeeded
    async with session() as db:
        if duplicate == "router":
            db.add(BgpRouterIntent(device_id=device_id, asn="64086.59904"))
        else:
            db.add(
                RedistributionIntent(
                    device_id=device_id,
                    dest_protocol="bgp",
                    dest_ref=f"{'64086.59904' if duplicate == 'destination' else '4200000000'}::ipv4-unicast",
                    source_protocol="bgp",
                    source_ref="64086.59904" if duplicate == "source" else "4200000000",
                )
            )
        await db.flush()
        projection = await db.scalar(select(DeviceProjectionStream).where(DeviceProjectionStream.stream == "bgp"))
        projection.authorized_document = {
            **deepcopy(projection.authorized_document),
            "bgp_router_intent": [_row_dict(row) for row in (await db.scalars(select(BgpRouterIntent))).all()],
            "redistribution_intent": [_row_dict(row) for row in (await db.scalars(select(RedistributionIntent))).all()],
        }
        await db.commit()
    if replacement:
        replacement = [_router_with_redist([{"source_protocol": "bgp", "source_ref": "4200000000"}], asn="4200000000")]
    response = await adapter_client.put(
        endpoint + ("?store_only=true" if store_only else ""),
        json={"routers": replacement},
        headers=AUTH | push_seq(),
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "asn_rule_violation"
    async with session() as db:
        model = BgpRouterIntent if duplicate == "router" else RedistributionIntent
        assert len((await db.scalars(select(model))).all()) == 2


@pytest.mark.parametrize("store_only", [False, True])
@pytest.mark.parametrize("field", ["remote_as", "local_as"])
async def test_bgp_replacement_refuses_authorized_peer_as(adapter_client, store_only, field):

    from nso_adapter.store.models import BgpPeerIntent, DeviceProjectionStream
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head, seed_settings

    device_id = await seed_device(nso_device_name="placeholder-device")
    await seed_settings(device_id, auto_apply=True)
    endpoint = f"/api/v1/devices/{device_id}/bgp-intent"
    router = {"asn": "64512", "scopes": [{"peers": [{"peer_address": "198.18.0.1", field: "64513"}]}]}
    response = await adapter_client.put(endpoint, json={"routers": [router]}, headers=AUTH | push_seq())
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 200
    client, _ = recorded_client("placeholder-device")
    initial_job = await job_row(await run_head(device_id, client))
    assert initial_job.status is JobStatus.succeeded
    async with session() as db:
        peer = await db.scalar(select(BgpPeerIntent))
        setattr(peer, field, "064513")
        projection = await db.scalar(select(DeviceProjectionStream).where(DeviceProjectionStream.stream == "bgp"))
        document = deepcopy(projection.authorized_document)
        document["bgp_peer_intent"][0][field] = "064513"
        projection.authorized_document = document
        await db.commit()
    response = await adapter_client.put(
        endpoint + ("?store_only=true" if store_only else ""),
        json={"routers": [router]},
        headers=AUTH | push_seq(),
    )
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 409, response.text
    error = response.json()["error"]
    assert error["code"] == "asn_rule_violation"
    assert error["detail"]["table"] == "bgp_peer_intent"
    assert error["detail"]["field"] == field
    async with session() as db:
        assert getattr(await db.scalar(select(BgpPeerIntent)), field) == "064513"


async def test_apply_diff_refuses_malformed_frozen_asn(adapter_client, monkeypatch):
    from nso_adapter.config import NsoInstanceConfig
    from nso_adapter.core.generation import digest_document
    from nso_adapter.nso.client import NsoClient
    from nso_adapter.store.models import DeploymentGeneration
    from tests.core.test_generation_protocol import seed_settings

    device_id = await seed_device(nso_device_name="placeholder-device")
    await seed_settings(device_id, auto_apply=True)
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [{"asn": "64512"}]},
        headers=AUTH | push_seq(),
    )
    assert response.status_code == 200
    async with session() as db:
        generation = await db.scalar(select(DeploymentGeneration))
        document = deepcopy(generation.document)
        document["bgp"]["bgp_router_intent"][0]["asn"] = "064512"
        fields = {
            column.name: getattr(generation, column.name)
            for column in DeploymentGeneration.__table__.columns
            if column.name not in {"id", "created_at", "updated_at"}
        }
        fields.update(
            document=document,
            seq=generation.seq + 1,
            digest=digest_document(generation.mode, document, generation.allowed_removal_keys),
        )
        db.add(DeploymentGeneration(**fields))
        await db.commit()
    client = NsoClient(
        NsoInstanceConfig(
            name="test",
            base_url="https://nso.example.test",
            username_ref="TEST_NSO_USERNAME",
            password_ref="TEST_NSO_PASSWORD",
        ),
        "test",
        "test",
    )
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda instance: client)
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/actions/apply-diff", headers=AUTH)
    assert_text_free_of(response.text, ["placeholder-device"])
    assert response.status_code == 409, response.text
    error = response.json()["error"]
    assert error["code"] == "asn_rule_violation"
    assert error["detail"]["table"] == "bgp_router_intent"
    assert error["detail"]["field"] == "asn"


@pytest.mark.parametrize("protocol", ["ospf", "isis"])
def test_redistribution_duplicates_are_handler_conflicts(protocol):
    from fastapi import HTTPException

    from nso_adapter.api.isis import IsisInterfaceIntentUpdate
    from nso_adapter.api.ospf import OspfIntentUpdate
    from nso_adapter.api.redistribution import validate_unique_redistribution_sources

    _, body = _redistribution_request(protocol, "4200000000")
    processes = body["instances" if protocol == "ospf" else "processes"]
    processes[0]["redistribution"].append({"source_protocol": "bgp", "source_ref": "64086.59904"})
    model = OspfIntentUpdate if protocol == "ospf" else IsisInterfaceIntentUpdate
    payload = model.model_validate(body)
    process = payload.instances[0] if protocol == "ospf" else payload.processes[0]
    with pytest.raises(HTTPException) as caught:
        validate_unique_redistribution_sources(process.redistribution)
    assert caught.value.status_code == 409
    assert caught.value.detail["error"]["code"] == "conflict"


_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("PUT", "bgp-intent"),
        ("PUT", "ospf-intent"),
        ("PUT", "isis-interface-intent"),
        ("PUT", "isis-flex-algo-intent"),
        ("GET", "bgp-config"),
        ("GET", "redistribution"),
        ("GET", "actions/apply-diff"),
    ],
)
def test_asn_refusing_endpoint_headings_list_409(method, path):
    doc = (_REPO_ROOT / "docs" / "api-contract.md").read_text()
    match = re.search(rf"^### `{method} /api/v1/devices/{{id}}/{re.escape(path)}` → `([^`]+)`$", doc, re.MULTILINE)
    assert match, f"no heading for {method} {path}"
    statuses = {s.strip() for s in match.group(1).split("|")}
    assert "409" in statuses
    snapshot = json.loads((_REPO_ROOT / "tests" / "api" / "openapi_snapshot.json").read_text())
    responses = snapshot["paths"][f"/api/v1/devices/{{device_id}}/{path}"][method.lower()]["responses"]
    assert statuses <= set(responses)
