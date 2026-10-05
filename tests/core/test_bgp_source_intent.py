# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""NED-conditioned BGP source intent through the PostgreSQL deployment lifecycle."""

from copy import deepcopy

import pytest
from sqlalchemy import select

from nso_adapter.store.models import Device, JobStatus, RedistributionIntent
from tests._secret_discipline import assert_text_free_of
from tests.api.test_bgp_asn_validation import _redistribution_request
from tests.conftest import AUTH, push_seq, seed_device, session
from tests.core.test_action_apply_promotion import _apply
from tests.core.test_execution_context import _drain, _drain_writes, _next_queued_type, _set_ned
from tests.core.test_generation_protocol import generations, job_row, recorded_client, run_head, seed_settings
from tests.core.test_static_route_removal import SrFake

_ALLOWED_NEDS = ["juniper-junos-nc", "timos-nc-23.10"]
_REFUSED_NEDS = [
    "cisco-ios-cli-6.95",
    "cisco-iosxr-cli-7.0",
    "cisco-nx-cli-5.0",
    "arcos-cli-1.0",
    None,
    "",
    "arcos-v8.1.2X-nc-1.0",
]
_PROTOCOLS = ["isis", "ospf", "bgp"]


def _entries(protocol, body):
    if protocol == "bgp":
        return body["routers"][0]["scopes"][0]["address_families"][0]["redistribution"]
    return body["instances" if protocol == "ospf" else "processes"][0]["redistribution"]


def _wire_sources(value):
    if isinstance(value, dict):
        if value.get("source-protocol") == "bgp":
            yield value["source-ref"]
        for child in value.values():
            yield from _wire_sources(child)
    elif isinstance(value, list):
        for child in value:
            yield from _wire_sources(child)


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize("ned_id", _ALLOWED_NEDS + _REFUSED_NEDS)
def test_encoder_source_policy_uses_section_execution(protocol, ned_id):
    from nso_adapter.core.community_dialect import community_dialect_for
    from nso_adapter.core.projection import section_registry
    from nso_adapter.domain.asn import AsnRuleViolation
    from nso_adapter.nso.apply import SectionExecution
    from tests.nso.test_device_intent_encoders import _rows

    rows = _rows(protocol)
    rows["redistribution_intent"] = [
        RedistributionIntent(
            dest_protocol=protocol,
            dest_ref="64512::ipv4-unicast" if protocol == "bgp" else "placeholder-process",
            source_protocol="bgp",
            source_ref="",
        )
    ]
    execution = SectionExecution(ned_id, community_dialect_for(ned_id))
    encode = section_registry()[protocol].encode
    if ned_id in _ALLOWED_NEDS:
        assert list(_wire_sources(encode(rows, execution))) == [""]
    else:
        with pytest.raises(AsnRuleViolation) as caught:
            encode(rows, execution)
        assert caught.value.error["code"] == "bgp_source_as_required"
        assert (ned_id or "no learned NED") in caught.value.error["message"]


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize("ned_id", _ALLOWED_NEDS + _REFUSED_NEDS)
@pytest.mark.parametrize("omitted", [False, True])
async def test_empty_source_put_is_conditioned_on_resolved_ned(adapter_client, protocol, ned_id, omitted):
    device_id = await seed_device(nso_device_name="placeholder-device")
    await _set_ned(device_id, ned_id)
    endpoint, body = _redistribution_request(protocol, "")
    if omitted:
        del _entries(protocol, body)[0]["source_ref"]
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/{endpoint}?store_only=true", json=body, headers=AUTH | push_seq()
    )
    assert_text_free_of(response.text, ["placeholder-device", "placeholder-process"])
    async with session() as db:
        rows = (await db.scalars(select(RedistributionIntent))).all()
    if ned_id in _ALLOWED_NEDS:
        assert response.status_code == 200, response.text
        assert [(r.source_protocol, r.source_ref) for r in rows] == [("bgp", "")]
    else:
        assert response.status_code == 409, response.text
        error = response.json()["error"]
        assert error["code"] == "bgp_source_as_required"
        assert (ned_id or "no learned NED") in error["message"]
        assert "source AS" in error["message"]
        assert rows == []


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize("ned_id", _ALLOWED_NEDS + _REFUSED_NEDS)
@pytest.mark.parametrize("source_ref", ["abc", "65536.0.1", "64512"])
async def test_nonempty_source_schema_is_unchanged(adapter_client, protocol, ned_id, source_ref):
    device_id = await seed_device(nso_device_name="placeholder-device")
    await _set_ned(device_id, ned_id)
    endpoint, body = _redistribution_request(protocol, source_ref)
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/{endpoint}?store_only=true", json=body, headers=AUTH | push_seq()
    )
    assert_text_free_of(response.text, ["placeholder-device", "placeholder-process"])
    assert response.status_code == (200 if source_ref == "64512" else 422), response.text


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize("ned_id", _ALLOWED_NEDS)
async def test_duplicate_empty_source_is_an_atomic_conflict(adapter_client, protocol, ned_id):
    device_id = await seed_device(nso_device_name="placeholder-device")
    await _set_ned(device_id, ned_id)
    endpoint, body = _redistribution_request(protocol, "")
    _entries(protocol, body).append({"source_protocol": "bgp", "source_ref": ""})
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/{endpoint}?store_only=true", json=body, headers=AUTH | push_seq()
    )
    assert_text_free_of(response.text, ["placeholder-device", "placeholder-process"])
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "conflict"
    async with session() as db:
        assert (await db.scalars(select(RedistributionIntent))).all() == []


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize("operation", ["replay", "replacement", "deletion"])
async def test_live_ned_change_refuses_empty_source_before_images(adapter_client, protocol, operation):
    device_id = await seed_device(nso_device_name="placeholder-device")
    await _set_ned(device_id, "juniper-junos-nc")
    endpoint, body = _redistribution_request(protocol, "")
    url = f"/api/v1/devices/{device_id}/{endpoint}?store_only=true"
    accepted = await adapter_client.put(url, json=body, headers=AUTH | push_seq(1))
    assert_text_free_of(accepted.text, ["placeholder-device", "placeholder-process"])
    assert accepted.status_code == 200, accepted.text
    await _set_ned(device_id, "cisco-ios-cli-6.95")
    if operation == "replacement":
        _entries(protocol, body)[0]["source_ref"] = "64512"
    elif operation == "deletion":
        _entries(protocol, body).clear()
    refused = await adapter_client.put(url, json=body, headers=AUTH | push_seq(1 if operation == "replay" else 2))
    assert_text_free_of(refused.text, ["placeholder-device", "placeholder-process"])
    assert refused.status_code == 409, refused.text
    assert refused.json()["error"]["code"] == "bgp_source_as_required"
    async with session() as db:
        assert (await db.scalar(select(RedistributionIntent))).source_ref == ""


class _WireRecorder(SrFake):
    def __init__(self):
        super().__init__("placeholder-device", service=None)
        self.payloads = []

    async def handle(self, method, url, content=None, headers=None):
        if content is not None and "dry-run=" not in url:
            self.payloads.append(content)
        return await super().handle(method, url, content, headers)


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize("ned_id", _ALLOWED_NEDS)
async def test_empty_source_lifecycle_preserves_frozen_payload(adapter_client, protocol, ned_id):
    from nso_adapter.core.projection import hydrate_section

    device_id = await seed_device(nso_device_name="placeholder-device")
    await _set_ned(device_id, ned_id)
    await seed_settings(device_id, auto_apply=False)
    endpoint, body = _redistribution_request(protocol, "")
    url = f"/api/v1/devices/{device_id}/{endpoint}"
    response = await adapter_client.put(url, json=body, headers=AUTH | push_seq(1))
    assert_text_free_of(response.text, ["placeholder-device", "placeholder-process"])
    assert response.status_code == 200, response.text
    replay = await adapter_client.put(url, json=body, headers=AUTH | push_seq(1))
    assert replay.content == response.content
    assert (await _apply(adapter_client, device_id, {protocol: 1})).status_code == 202
    (generation,) = await generations(device_id)
    rows = hydrate_section(generation.document, protocol)
    assert rows[RedistributionIntent][0].source_ref == ""

    fake = _WireRecorder()
    fake.reject_containers.add(protocol)
    client, _ = recorded_client("placeholder-device", sr_fake=fake)
    failed = await job_row(await run_head(device_id, client))
    assert failed.status is JobStatus.failed, failed.error
    first_payload = fake.payloads[0]
    fake.reject_containers.clear()
    retry = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/retry-generation",
        json={"generation_id": generation.id},
        headers=AUTH,
    )
    assert_text_free_of(retry.text, ["placeholder-device", "placeholder-process"])
    assert retry.status_code == 202, retry.text
    await _set_ned(device_id, "cisco-ios-cli-6.95")
    retried = await job_row(await run_head(device_id, client))
    assert retried.status is JobStatus.succeeded, retried.error
    assert fake.payloads[-1] == first_payload
    assert list(_wire_sources((await client.service_instance_state("placeholder-device")).entry)) == [""]
    await _set_ned(device_id, ned_id)

    flush = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/force-removal", json={"scope": protocol}, headers=AUTH
    )
    assert_text_free_of(flush.text, ["placeholder-device", "placeholder-process"])
    assert flush.status_code == 202, flush.text
    rebuilt = await _drain_writes(device_id, "placeholder-device")
    assert rebuilt
    assert any(list(_wire_sources(w["instance"])) == [""] for w in rebuilt)
    from nso_adapter.store.models import JobType

    assert await _next_queued_type(device_id) in (None, JobType.sync)
    await _drain(device_id)

    for seq, source_ref in [(2, "0.64512"), (3, "64512"), (4, "")]:
        _, replacement = _redistribution_request(protocol, source_ref)
        changed = await adapter_client.put(url, json=replacement, headers=AUTH | push_seq(seq))
        assert_text_free_of(changed.text, ["placeholder-device", "placeholder-process"])
        assert changed.status_code == 200
        async with session() as db:
            stored = await db.scalar(select(RedistributionIntent))
            assert stored.source_ref == source_ref
            if seq == 2:
                canonical_row_id = stored.id
            elif seq == 3:
                assert stored.id == canonical_row_id
        applied = await _apply(adapter_client, device_id, {protocol: seq})
        assert_text_free_of(applied.text, ["placeholder-device", "placeholder-process"])
        assert applied.status_code in (200, 202)
        writes = await _drain_writes(device_id, "placeholder-device")
        assert writes
        assert list(_wire_sources(writes[-1]["instance"])) == ["64512" if source_ref else ""]
        assert await _next_queued_type(device_id) in (None, JobType.sync)
        await _drain(device_id)

    deletion = deepcopy(body)
    _entries(protocol, deletion).clear()
    deleted = await adapter_client.put(url, json=deletion, headers=AUTH | push_seq(5))
    assert_text_free_of(deleted.text, ["placeholder-device", "placeholder-process"])
    assert deleted.status_code == 200, deleted.text
    applied = await _apply(adapter_client, device_id, {protocol: 5})
    assert_text_free_of(applied.text, ["placeholder-device", "placeholder-process"])
    assert applied.status_code in (200, 202), applied.text
    writes = await _drain_writes(device_id, "placeholder-device")
    assert writes and list(_wire_sources(writes[-1]["instance"])) == []
    async with session() as db:
        assert (await db.scalars(select(RedistributionIntent))).all() == []


@pytest.mark.parametrize("protocol", _PROTOCOLS)
async def test_ios_frozen_section_refuses_empty_source_before_send(adapter_client, protocol):
    from nso_adapter.core.generation import digest_document
    from nso_adapter.store.models import DeploymentGeneration

    device_id = await seed_device(nso_device_name="placeholder-device")
    await seed_settings(device_id)
    await _set_ned(device_id, "cisco-ios-cli-6.95")
    endpoint, body = _redistribution_request(protocol, "64512")
    response = await adapter_client.put(f"/api/v1/devices/{device_id}/{endpoint}", json=body, headers=AUTH | push_seq())
    assert_text_free_of(response.text, ["placeholder-device", "placeholder-process"])
    assert response.status_code == 200, response.text
    async with session() as db:
        generation = await db.scalar(select(DeploymentGeneration))
        document = deepcopy(generation.document)
        document[protocol]["redistribution_intent"][0]["source_ref"] = ""
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
        device = await db.get(Device, device_id)
        device.ned_id = "juniper-junos-nc"
        await db.commit()
    client, recorder = recorded_client("placeholder-device")
    failed = await job_row(await run_head(device_id, client))
    assert failed.status is JobStatus.failed, failed.error
    assert failed.error["code"] == "bgp_source_as_required"
    assert "cisco-ios-cli-6.95" in failed.error["message"]
    assert recorder.commits == []


async def test_source_as_refusal_subclass_envelope_round_trip():
    from nso_adapter.api.errors import ErrorEnvelope, asn_rule_violation_handler
    from nso_adapter.domain.asn import BgpSourceAsRequired, asn_refusal_detail, checked_intent_source_ref

    with pytest.raises(BgpSourceAsRequired) as caught:
        checked_intent_source_ref("", "cisco-ios-cli-6.95")
    refusal = caught.value
    assert asn_refusal_detail(refusal) is refusal.error
    response = await asn_rule_violation_handler(None, refusal)
    assert response.status_code == 409
    envelope = ErrorEnvelope.model_validate_json(response.body)
    assert envelope.error.code == "bgp_source_as_required"
