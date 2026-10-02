# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Stored AS violations refuse work without changing authority or mirrors."""

import asyncio
from copy import deepcopy

import pytest
from sqlalchemy import select

from nso_adapter.core.apply import _reader_compare_expected
from nso_adapter.core.bgp import refresh_bgp_config_for_device
from nso_adapter.core.generation import _fragment_deletions, _retain_rows
from nso_adapter.core.projection import snapshot_stream
from nso_adapter.core.removal import _document_orphans, promotion_removal_context
from nso_adapter.domain.asn import AsnRuleViolation, checked_asn
from nso_adapter.store.models import BgpRouterIntent, Device, DeviceBgpRouter, JobStatus
from tests._secret_discipline import assert_chain_free_of, assert_text_free_of
from tests.conftest import AUTH, push_seq, seed_device, session
from tests.core.test_generation_protocol import job_row, recorded_client, run_head, seed_settings


def test_asn_refusal_has_no_parser_exception_context():
    with pytest.raises(AsnRuleViolation) as caught:
        checked_asn("064512", "bgp_router_intent", 7, "asn")
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


def test_asn_refusal_accumulates_sorted_degraded_surfaces_without_rejected_value():
    refusal = AsnRuleViolation("device_read.bgp", "router[0]", "asn", "064512")

    refusal.include_degraded_surfaces(["static_route", "bgp", "bgp"])
    refusal.include_degraded_surfaces(["sync_from", "interface_attributes"])

    assert refusal.error["detail"] == {
        "table": "device_read.bgp",
        "row_id": "router[0]",
        "field": "asn",
        "degraded_surfaces": ["bgp", "interface_attributes", "static_route", "sync_from"],
    }
    assert_text_free_of(refusal.error, ["064512"])


@pytest.mark.parametrize("other_row_id", [None, "router[1]"])
def test_asn_refusal_classifier_preserves_only_the_authored_envelope(other_row_id):
    from nso_adapter.domain.asn import asn_refusal_detail

    refusal = AsnRuleViolation(
        "device_read.bgp", "router[0]", "asn", "placeholder-rejected-AS", other_row_id=other_row_id
    )
    refusal.include_degraded_surfaces(["redistribution", "bgp"])
    classified = asn_refusal_detail(refusal)
    assert_text_free_of(classified, ["placeholder-rejected-AS"])
    assert classified == refusal.error
    assert classified["detail"]["degraded_surfaces"] == ["bgp", "redistribution"]
    if other_row_id is not None:
        assert classified["detail"]["other_row_id"] == other_row_id


@pytest.mark.parametrize("exception_kind", ["unrelated", "http", "decoder", "subclass"])
def test_asn_refusal_classifier_rejects_untrusted_exception_shapes(exception_kind):
    import httpx

    from nso_adapter.domain.asn import asn_refusal_detail

    class UntrustedRefusal(AsnRuleViolation):
        pass

    request = httpx.Request("GET", "https://nso.example.test/placeholder-secret")
    exceptions = {
        "unrelated": ValueError("placeholder-secret"),
        "http": httpx.HTTPStatusError(
            "placeholder-secret", request=request, response=httpx.Response(500, request=request)
        ),
        "decoder": UnicodeDecodeError("utf-8", b"placeholder-secret", 0, 1, "placeholder-secret"),
        "subclass": UntrustedRefusal("placeholder-table", 1, "asn", "placeholder-secret"),
    }
    with pytest.raises(TypeError) as caught:
        asn_refusal_detail(exceptions[exception_kind])
    assert_text_free_of(caught.value, ["placeholder-secret"])
    assert str(caught.value) == "exc must be an AsnRuleViolation"


async def test_projection_refuses_stored_router(adapter_client):
    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        row = BgpRouterIntent(device_id=device_id, asn="064512")
        db.add(row)
        await db.commit()
        with pytest.raises(AsnRuleViolation) as caught:
            await snapshot_stream(db, device_id, "bgp")
        assert_chain_free_of(caught.value, ["064512"])
        assert_text_free_of(caught.value.error, ["064512"])
        assert caught.value.error["detail"] == {"table": "bgp_router_intent", "row_id": row.id, "field": "asn"}


async def test_removal_context_refuses_stored_router():
    with pytest.raises(AsnRuleViolation) as caught:
        await promotion_removal_context(None, 1, "bgp", {"bgp_router_intent": [{"id": 7, "asn": "064512"}]})
    assert caught.value.error["detail"]["row_id"] == 7


def test_reader_compare_refuses_stored_peer():
    from nso_adapter.store.models import BgpPeerIntent, BgpScopeIntent

    peer = BgpPeerIntent(id=9, remote_as="064513", peer_address="198.18.0.1")
    row = BgpRouterIntent(id=7, asn="64512", scopes=[BgpScopeIntent(peers=[peer])])
    with pytest.raises(AsnRuleViolation) as caught:
        _reader_compare_expected("bgp", [row])
    assert caught.value.error["detail"] == {"table": "bgp_peer_intent", "row_id": 9, "field": "remote_as"}


@pytest.mark.parametrize("invalid_row", ["router", "peer", "peer_group"])
async def test_refresh_replaces_invalid_stored_bgp_mirror(adapter_client, invalid_row):
    from nso_adapter.store.models import DeviceBgpPeer, DeviceBgpPeerGroup, DeviceBgpScope

    class ValidDeviceRead:
        async def get_device_state_section(self, device_name, section):
            return {"status": "ok", "router": [{"asn": "64512"}]}

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        router = DeviceBgpRouter(device_id=device_id, asn="064512" if invalid_row == "router" else "64512")
        db.add(router)
        await db.flush()
        if invalid_row != "router":
            scope = DeviceBgpScope(router_id=router.id, vrf="")
            db.add(scope)
            await db.flush()
            row = (
                DeviceBgpPeer(scope_id=scope.id, peer_address="198.18.0.1", remote_as="064513")
                if invalid_row == "peer"
                else DeviceBgpPeerGroup(scope_id=scope.id, name="placeholder-group", remote_as="064513")
            )
            db.add(row)
        await db.commit()
        response = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "asn_rule_violation"
        assert (
            response.json()["error"]["detail"]["table"]
            == {
                "router": "device_bgp_router",
                "peer": "device_bgp_peer",
                "peer_group": "device_bgp_peer_group",
            }[invalid_row]
        )
        assert await refresh_bgp_config_for_device(db, device, ValidDeviceRead())
        assert [row.asn for row in (await db.scalars(select(DeviceBgpRouter))).all()] == ["64512"]
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert response.status_code == 200
    assert [row["asn"] for row in response.json()["routers"]] == ["64512"]


async def test_refresh_refuses_invalid_incoming_bgp_and_retains_mirror(adapter_client):
    class InvalidDeviceRead:
        async def get_device_state_section(self, device_name, section):
            return {"status": "ok", "router": [{"asn": "064512"}]}

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        db.add(DeviceBgpRouter(device_id=device_id, asn="64512"))
        await db.commit()
        with pytest.raises(AsnRuleViolation):
            await refresh_bgp_config_for_device(db, device, InvalidDeviceRead())
        assert [row.asn for row in (await db.scalars(select(DeviceBgpRouter))).all()] == ["64512"]


async def test_apply_job_surfaces_stored_document_refusal(adapter_client):
    from nso_adapter.core.generation import digest_document
    from nso_adapter.store.models import DeploymentGeneration

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
    client, recorder = recorded_client("placeholder-device")
    job = await job_row(await run_head(device_id, client))
    assert job.status is JobStatus.failed
    assert job.error["code"] == "asn_rule_violation"
    assert job.error["detail"]["table"] == "bgp_router_intent"
    assert recorder.commits == []


def test_collateral_guard_refuses_invalid_device_read():
    document = {"bgp": {"router": [{"asn": "064512"}]}}
    with pytest.raises(AsnRuleViolation) as caught:
        _document_orphans(document, document, {})
    assert caught.value.error["code"] == "asn_rule_violation"


def test_detach_only_redistribution_keeps_valid_rows():
    from nso_adapter.store.models import IntentPushReceipt

    rows = [
        {
            "id": 8,
            "dest_protocol": "bgp",
            "dest_ref": "4200000000::ipv4-unicast",
            "source_protocol": "connected",
            "source_ref": "",
        },
        {
            "id": 9,
            "dest_protocol": "bgp",
            "dest_ref": "64086.59904::ipv4-unicast",
            "source_protocol": "static",
            "source_ref": "",
        },
    ]
    old = {"redistribution_intent": rows}
    networked, detached = _fragment_deletions(old, {}, IntentPushReceipt(delete_origin=False))
    assert networked == {}
    retained = _retain_rows({}, detached, "bgp", old)
    assert retained["redistribution_intent"] == rows


def test_delete_origin_refuses_malformed_prior_asn():
    from nso_adapter.store.models import IntentPushReceipt

    with pytest.raises(AsnRuleViolation) as caught:
        _fragment_deletions(
            {"bgp_router_intent": [{"id": 7, "asn": "064512"}]}, {}, IntentPushReceipt(delete_origin=True)
        )
    assert caught.value.error["code"] == "asn_rule_violation"


@pytest.mark.parametrize("protocol", ["bgp", "ospf", "isis"])
def test_collateral_guard_refuses_colliding_device_sources(protocol):
    sources = [{"source-protocol": "bgp", "source-ref": asn} for asn in ["4200000000", "64086.59904"]]
    if protocol == "bgp":
        section = {
            "router": [
                {"asn": "64512", "scope": [{"address-family": [{"afi": "ipv4-unicast", "redistribute": sources}]}]}
            ]
        }
    else:
        key = "process-id" if protocol == "ospf" else "process-tag"
        section = {"process-config": [{key: "placeholder-process", "redistribute": sources}]}
    with pytest.raises(AsnRuleViolation):
        _document_orphans({protocol: section}, {protocol: section}, {})


async def test_apply_job_fails_closed_on_invalid_device_read(adapter_client):
    device_id = await seed_device(nso_device_name="placeholder-device")
    await seed_settings(device_id, auto_apply=True)
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [{"asn": "64512"}]},
        headers=AUTH | push_seq(),
    )
    assert response.status_code == 200
    client, recorder = recorded_client(
        "placeholder-device",
        device_state={"bgp-config": {"status": "ok", "router": [{"asn": "064512"}]}},
    )
    job = await job_row(await run_head(device_id, client))
    assert recorder.commits
    assert job.status is JobStatus.failed
    assert job.error["code"] == "asn_rule_violation"
    assert job.error["detail"]["table"] == "device_read.bgp"


async def test_redistribution_refresh_replaces_invalid_stored_mirror(adapter_client):
    from nso_adapter.core.redistribution import refresh_redistribution_from_outcomes
    from nso_adapter.nso.read_outcome import Freshness, Present
    from nso_adapter.store.models import DeviceRedistribution

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        db.add(
            DeviceRedistribution(
                device_id=device_id,
                dest_protocol="ospf",
                dest_ref="placeholder-process",
                source_protocol="bgp",
                source_ref="064512",
            )
        )
        await db.commit()
        response = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "asn_rule_violation"
        ospf = {
            "instance": [
                {
                    "process-id": "placeholder-process",
                    "redistribute": [{"source-protocol": "bgp", "source-ref": "64512"}],
                }
            ]
        }
        outcomes = {
            protocol: Present(ospf if protocol == "ospf" else {}, Freshness.fresh)
            for protocol in ["ospf", "isis", "bgp"]
        }
        assert await refresh_redistribution_from_outcomes(db, device, outcomes)
        assert [row.source_ref for row in (await db.scalars(select(DeviceRedistribution))).all()] == ["64512"]
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
    assert response.status_code == 200
    assert response.json()["entries"][0]["source_ref"] == "64512"


@pytest.mark.parametrize("reason", ["unsupported", "read_error"])
async def test_redistribution_refresh_keeps_unavailable_partition(adapter_client, reason):
    from structlog.testing import capture_logs

    from nso_adapter.core.redistribution import refresh_redistribution_from_outcomes
    from nso_adapter.nso.read_outcome import Freshness, Present, Unavailable, UnavailableReason
    from nso_adapter.store.models import DeviceRedistribution

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        db.add(
            DeviceRedistribution(
                device_id=device_id,
                dest_protocol="ospf",
                dest_ref="placeholder-process",
                source_protocol="bgp",
                source_ref="064512",
            )
        )
        await db.commit()
        outcomes = {protocol: Present({}, Freshness.fresh) for protocol in ["isis", "bgp"]}
        outcomes["ospf"] = Unavailable(UnavailableReason(reason))
        with capture_logs() as logs:
            assert await refresh_redistribution_from_outcomes(db, device, outcomes) == (reason == "unsupported")
        assert (await db.scalar(select(DeviceRedistribution))).source_ref == "064512"
        done = next(record for record in logs if record["event"] == "redistribution.refresh.done")
        assert done["row_count"] == 0


async def test_redistribution_refresh_refuses_invalid_incoming_source(adapter_client):
    from nso_adapter.core.redistribution import refresh_redistribution_from_outcomes
    from nso_adapter.nso.read_outcome import Freshness, Present
    from nso_adapter.store.models import DeviceRedistribution

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        db.add(
            DeviceRedistribution(
                device_id=device_id,
                dest_protocol="ospf",
                dest_ref="placeholder-process",
                source_protocol="bgp",
                source_ref="64512",
            )
        )
        await db.commit()
        ospf = {
            "instance": [
                {
                    "process-id": "placeholder-process",
                    "redistribute": [{"source-protocol": "bgp", "source-ref": "064512"}],
                }
            ]
        }
        outcomes = {
            protocol: Present(ospf if protocol == "ospf" else {}, Freshness.fresh)
            for protocol in ["ospf", "isis", "bgp"]
        }
        with pytest.raises(AsnRuleViolation):
            await refresh_redistribution_from_outcomes(db, device, outcomes)
        assert (await db.scalar(select(DeviceRedistribution))).source_ref == "64512"


def test_detach_only_colliding_prior_rows_refuse_without_retirement():
    from nso_adapter.store.models import IntentPushReceipt

    rows = [
        {
            "id": index,
            "dest_protocol": "bgp",
            "dest_ref": f"{asn}::ipv4-unicast",
            "source_protocol": "connected",
            "source_ref": "",
        }
        for index, asn in enumerate(["4200000000", "64086.59904"], 1)
    ]
    with pytest.raises(AsnRuleViolation):
        _fragment_deletions({"redistribution_intent": rows}, {}, IntentPushReceipt(delete_origin=False))


async def test_comprehensive_refresh_surfaces_the_typed_refusal(adapter_client):
    from structlog.testing import capture_logs

    from nso_adapter.core.importer import refresh_all_surfaces_for_device
    from tests.core.test_importer import _ALL_PROJECTED_WIRES

    class InvalidDeviceRead:
        async def get_device_state_doc(self, device_name):
            sections = {wire: {"status": "ok"} for wire in _ALL_PROJECTED_WIRES}
            sections["bgp-config"] = {"status": "ok", "router": [{"asn": "064512"}]}
            sections["static-route"] = {"status": "error", "error-reason": "placeholder-read-failure"}
            return sections

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        with capture_logs() as logs, pytest.raises(AsnRuleViolation) as caught:
            await refresh_all_surfaces_for_device(db, device, InvalidDeviceRead())
        assert caught.value.error["code"] == "asn_rule_violation"
        assert caught.value.error["detail"]["degraded_surfaces"] == ["bgp", "redistribution", "static_route"]
    refusals = [record for record in logs if record["event"] == "sync.surface_refresh_refused"]
    assert_text_free_of(refusals, ["064512"])
    assert {record["surface"] for record in refusals} == {"bgp", "redistribution"}
    for record in refusals:
        assert record["error"]["code"] == "asn_rule_violation"
        assert record["error"]["detail"]["field"] == "asn"
        assert record["error"]["detail"]["row_id"] == "router[0]"


@pytest.mark.parametrize("caller", ["apply", "sse"])
async def test_partial_refresh_commits_config_before_notifying(adapter_client, monkeypatch, caller):
    from structlog.testing import capture_logs

    from nso_adapter.core import importer as imp
    from nso_adapter.core.apply import _post_apply_refresh_and_notify
    from nso_adapter.main import _DeviceRefreshCoalescer
    from nso_adapter.store.models import DeviceSvi
    from tests.core.test_importer import _ALL_PROJECTED_WIRES

    class InvalidDeviceRead:
        async def get_device_state_doc(self, device_name):
            sections = {wire: {"status": "ok"} for wire in _ALL_PROJECTED_WIRES}
            sections["bgp-config"] = {"status": "ok", "router": [{"asn": "064512"}]}
            sections["svi"] = {
                "status": "ok",
                "interface": [{"interface-name": "Vlan42", "vlan-id": 42}],
            }
            return sections

        async def run_device_state_read(self, device_name, wire_names, *, timeout):
            return {"atomic": True, **await self.get_device_state_doc(device_name)}

    device_id = await seed_device(nso_device_name="placeholder-device", netbox_device_id=42)
    notifications = []

    class NetBoxNotification:
        async def notify_sync_complete(self, netbox_device_id):
            async with session() as db:
                rows = (await db.scalars(select(DeviceSvi).where(DeviceSvi.device_id == device_id))).all()
                notifications.append((netbox_device_id, [(row.interface_name, row.vlan_id) for row in rows]))

    client = InvalidDeviceRead()
    monkeypatch.setattr(imp, "get_nso_client", lambda _: client)
    monkeypatch.setattr(imp, "get_netbox_client", NetBoxNotification)
    with capture_logs() as logs:
        if caller == "apply":
            async with session() as db:
                await _post_apply_refresh_and_notify(db, device_id)
        else:
            tasks = set()
            coalescer = _DeviceRefreshCoalescer({"nso-dev": client}, tasks, None)
            coalescer.trigger(device_id, "nso-dev", 42)
            await asyncio.gather(*tasks)

    assert notifications == [(42, [("Vlan42", 42)])]
    event = "apply.post_refresh_failed" if caller == "apply" else "sse.coalesced_refresh_failed"
    refusal = next(record for record in logs if record["event"] == event)
    assert_text_free_of(refusal, ["064512"])
    assert refusal["error"]["code"] == "asn_rule_violation"
    assert refusal["error"]["detail"] == {
        "table": "device_read.bgp",
        "row_id": "router[0]",
        "field": "asn",
        "degraded_surfaces": ["bgp", "redistribution"],
    }


@pytest.mark.parametrize("caller", ["onboard", "apply"])
async def test_mirror_refresh_logs_asn_refusal_with_degraded_surfaces(adapter_client, monkeypatch, caller):
    from structlog.testing import capture_logs

    from nso_adapter.core import importer as imp
    from nso_adapter.core.apply import _post_apply_refresh_and_notify
    from nso_adapter.core.onboarding import _initial_mirror_refresh
    from tests.core.test_importer import _ALL_PROJECTED_WIRES

    class InvalidDeviceRead:
        async def get_device_state_doc(self, device_name):
            sections = {wire: {"status": "ok"} for wire in _ALL_PROJECTED_WIRES}
            sections["bgp-config"] = {"status": "ok", "router": [{"asn": "064512"}]}
            sections["static-route"] = {"status": "error", "error-reason": "placeholder-read-failure"}
            return sections

        async def run_device_state_read(self, device_name, wire_names, *, timeout):
            return {"atomic": True, **await self.get_device_state_doc(device_name)}

    client = InvalidDeviceRead()
    monkeypatch.setattr(imp, "get_nso_client", lambda _: client)
    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        with capture_logs() as logs:
            if caller == "onboard":
                await _initial_mirror_refresh(db, device_id, client)
            else:
                await _post_apply_refresh_and_notify(db, device_id)
    event = "device.onboard_mirror.failed" if caller == "onboard" else "apply.post_refresh_failed"
    record = next(record for record in logs if record["event"] == event)
    assert record["error"]["code"] == "asn_rule_violation"
    assert record["error"]["detail"]["degraded_surfaces"] == ["bgp", "redistribution", "static_route"]
    assert_text_free_of(record, ["064512"])


def test_reader_compare_refuses_colliding_stored_routers():
    rows = [BgpRouterIntent(id=index, asn=asn, scopes=[]) for index, asn in enumerate(["4200000000", "64086.59904"], 1)]
    with pytest.raises(AsnRuleViolation):
        _reader_compare_expected("bgp", rows)


@pytest.mark.parametrize("protocol", ["ospf", "isis"])
@pytest.mark.parametrize("singleton", [False, True])
async def test_protocol_refresh_refuses_invalid_bgp_source_and_retains_mirror(adapter_client, protocol, singleton):
    from nso_adapter.core.isis import refresh_isis_interfaces_for_device
    from nso_adapter.core.ospf import refresh_ospf_for_device
    from nso_adapter.store.models import DeviceIsisProcess, DeviceOspfInstance

    refresh, model, list_key, identity_key = (
        (refresh_ospf_for_device, DeviceOspfInstance, "instance", "process-id")
        if protocol == "ospf"
        else (refresh_isis_interfaces_for_device, DeviceIsisProcess, "process", "process-tag")
    )
    source = {"source-protocol": "bgp", "source-ref": "064512"}
    process = {identity_key: "placeholder-new", "redistribute": source if singleton else [source]}

    class InvalidDeviceRead:
        async def get_device_state_section(self, device_name, section):
            return {"status": "ok", list_key: process if singleton else [process]}

    device_id = await seed_device(nso_device_name="placeholder-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        identity_attr = identity_key.replace("-", "_")
        row = model(device_id=device_id, **{identity_attr: "placeholder-kept"})
        db.add(row)
        await db.commit()
        with pytest.raises(AsnRuleViolation) as caught:
            await refresh(db, device, InvalidDeviceRead())
        assert_chain_free_of(caught.value, ["064512"])
        assert_text_free_of(caught.value.error, ["064512"])
        assert caught.value.error["detail"]["field"] == "source-ref"
        rows = (await db.scalars(select(model).where(model.device_id == device_id))).all()
        assert [getattr(item, identity_attr) for item in rows] == ["placeholder-kept"]


async def test_asn_refusal_handler_matches_error_envelope():
    from nso_adapter.api.errors import ErrorEnvelope, asn_rule_violation_handler

    refusal = AsnRuleViolation("device_read.bgp", "router[0]", "asn", "064512")
    response = await asn_rule_violation_handler(None, refusal)
    assert_text_free_of(response.body, ["064512"])
    envelope = ErrorEnvelope.model_validate_json(response.body)
    assert response.status_code == 409
    assert envelope.error.code == "asn_rule_violation"


@pytest.mark.parametrize("kind,key", [("peer", "peer-address"), ("peer-group", "name")])
@pytest.mark.parametrize("field", ["remote-as", "local-as"])
def test_device_peer_refusal_does_not_disclose_rejected_value(kind, key, field):
    from nso_adapter.core.bgp import validate_bgp_as_numbers

    rejected = "198.18.0.1"
    routers = [{"asn": "64512", "scope": [{kind: [{key: rejected, field: rejected}]}]}]
    with pytest.raises(AsnRuleViolation) as caught:
        validate_bgp_as_numbers(routers)
    assert_chain_free_of(caught.value, [rejected])
    assert_text_free_of(caught.value.error, [rejected])
    assert caught.value.error["detail"]["row_id"] == f"router[0].scope[0].{kind}[0]"


@pytest.mark.parametrize(
    "scope,list_key,identity_key",
    [
        ("isis", "process", "process-tag"),
        ("isis", "process-config", "process-tag"),
        ("ospf", "instance", "process-id"),
        ("ospf", "process-config", "process-id"),
    ],
)
@pytest.mark.parametrize("singleton", [False, True])
def test_removal_source_refusal_uses_structural_process_location(scope, list_key, identity_key, singleton):
    from nso_adapter.core.removal import _leaf_keys, section_guard_lists

    rejected = "placeholder-process"
    source = {"source-protocol": "bgp", "source-ref": rejected}
    process = {identity_key: rejected, "redistribute": source if singleton else [source]}
    entry = {list_key: process if singleton else [process]}
    with pytest.raises(AsnRuleViolation) as caught:
        _leaf_keys(scope, entry, section_guard_lists(scope)[0])
    assert_chain_free_of(caught.value, [rejected])
    assert_text_free_of(caught.value.error, [rejected])
    assert caught.value.error["detail"]["row_id"] == f"{list_key}[0]"


@pytest.mark.parametrize("protocol", ["isis", "ospf", "bgp"])
@pytest.mark.parametrize("source_protocol", ["bgp", " bgp "])
def test_redistribution_source_refusal_uses_structural_location(protocol, source_protocol):
    from datetime import UTC, datetime

    from nso_adapter.core.redistribution import (
        _bgp_redistribution_rows,
        _isis_redistribution_rows,
        _ospf_redistribution_rows,
    )

    rejected = "placeholder-process"
    source = {"source-protocol": source_protocol, "source-ref": rejected}
    scope = {"vrf": rejected, "address-family": [{"afi": "ipv4-unicast", "redistribute": [source]}]}
    builders = {
        "isis": (_isis_redistribution_rows, {"process": [{"process-tag": rejected, "redistribute": [source]}]}),
        "ospf": (_ospf_redistribution_rows, {"instance": [{"process-id": rejected, "redistribute": [source]}]}),
        "bgp": (
            _bgp_redistribution_rows,
            {"router": [{"asn": "64512", "scope": [scope]}]},
        ),
    }
    builder, entry = builders[protocol]
    with pytest.raises(AsnRuleViolation) as caught:
        builder(1, entry, datetime.now(UTC), "test")
    assert_chain_free_of(caught.value, [rejected])
    assert_text_free_of(caught.value.error, [rejected])
    assert caught.value.error["detail"]["field"] == "source-ref"
    location = {"isis": "process[0]", "ospf": "instance[0]", "bgp": "router[0].scope[0]"}[protocol]
    if protocol == "bgp" and source_protocol != "bgp":
        location += ".address-family[0]"
    assert caught.value.error["detail"]["row_id"] == location
