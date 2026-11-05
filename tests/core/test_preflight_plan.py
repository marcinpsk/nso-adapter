# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Detached Apply planning against PostgreSQL and the RESTCONF boundary."""

import json
from dataclasses import FrozenInstanceError, replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from nso_adapter.core.apply import build_device_containers
from nso_adapter.core.generation import (
    create_action_apply,
    execution_policy,
    generation_execution_context,
)
from nso_adapter.core.preflight import _device_context, normalized_plan_digest, plan_action_apply, read_plan_inputs
from nso_adapter.nso.origins import AttributionFailure, resolve_refusals
from nso_adapter.nso.refusal_protocol import refusal, row_ref
from nso_adapter.store.db import get_engine, session
from nso_adapter.store.models import Base, Device, DeviceProjectionStream, GenerationMode
from tests.api.test_static_route_deleted_routes import deleted
from tests.api.test_static_route_identity import entry
from tests.conftest import seed_device
from tests.core.test_action_apply_promotion import (
    _A,
    _B,
    _C,
    _apply,
    _generations,
    _prepare,
    _put_interface_attrs,
    _put_ip,
    _put_routes,
    _put_vlans,
    _settle,
)
from tests.core.test_generation_protocol import AUTH, job_row, seed_settings
from tests.core.test_refusal_cutover import RuntimeTransport, run_next
from tests.nso.test_apply_send import _client_with

pytestmark = pytest.mark.anyio


class PlanTransport(RuntimeTransport):
    async def handle_async_request(self, request):
        if request.method == "POST" and "device-state-read" in request.url.path:
            self.device_sections = {"static-route": self.fake.section()}
        return await super().handle_async_request(request)


async def setup(api, monkeypatch, case="plain"):
    name = "placeholder-plan-device"
    device_id = await seed_device(nso_device_name=name, netbox_device_id=None)
    await seed_settings(device_id, auto_apply=False)
    transport = PlanTransport(name)
    transport.refuse = False
    client = _client_with(transport)
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _instance: client)
    if case in {"lag", "switchport"}:
        response = await _prepare(api, device_id, case, {"A": [1]})
        assert response.status_code == 200
        selected = {case: response.json()["selection_revision"]}
    elif case in {"removal", "retention", "detach", "retain_history"}:
        response = await _put_routes(
            api,
            device_id,
            [entry(key, route_id=index, generation=1) for index, key in enumerate((_A, _B, _C), 1)],
            seq=41,
        )
        assert response.status_code == 200
        assert (await _apply(api, device_id, {"static_route": 41})).status_code == 202
        await run_next(device_id)
        response = await _put_routes(
            api,
            device_id,
            [entry(_A, route_id=1, generation=2)] if case != "detach" else [],
            seq=42,
            query="?store_only=true",
            deleted=[deleted(2, [_B]), deleted(3, [_C])]
            if case == "retain_history"
            else [deleted(2, [_B])]
            if case == "removal"
            else [],
        )
        assert response.status_code == 200
        selected = {"static_route": 42}
        if case == "retention":
            from tests.core.removal_helpers import seed_tomb

            await seed_tomb(device_id, _B, route_id=2)
            await seed_tomb(device_id, _C, route_id=3)
            response = await _put_routes(
                api, device_id, [entry(_A, route_id=1, generation=3, metric=20)], seq=43, query="?store_only=true"
            )
            assert response.status_code == 200
            selected = {"static_route": 43}
    else:
        response = await _put_vlans(api, device_id, [101], seq=41, query="?store_only=true")
        assert response.status_code == 200
        selected = {"vlan": 41}
    transport.documents.clear()
    transport.fake.reads.clear()
    transport.fake.calls.clear()
    async with session() as db:
        device = await db.get(Device, device_id)
    return device, selected, client, transport


async def plan(device, selected, client):
    live = await read_plan_inputs(client, device)
    return await plan_action_apply(
        get_engine(), device.id, selected, live.section, expected_context=_device_context(device)
    )


@pytest.mark.parametrize("case", ["plain", "removal", "lag", "switchport", "retention"])
async def test_planned_chain_matches_created_chain(adapter_client, monkeypatch, case):
    device, selected, client, transport = await setup(adapter_client, monkeypatch, case)
    planned = await plan(device, selected, client)
    async with session() as db:
        created = await create_action_apply(db, device.id, selected, uuid4())
        assert [link.digest for link in planned.links] == [link.digest for link in created.generations]
        for link, generation in zip(planned.links, created.generations, strict=True):
            policy = execution_policy(generation)
            body = await build_device_containers(
                client,
                device,
                generation.document,
                retain_static_routes=policy.retain_static_routes,
                origin_context=await generation_execution_context(db, generation.job_id),
            )
            assert link.containers == body.containers
            assert link.document == generation.document
            assert link.policy == policy
        assert planned.skipped == created.skipped
    assert not transport.documents
    if case == "removal":
        assert [link.mode for link in planned.links] == [GenerationMode.networked, GenerationMode.detach]
    if case == "retention":
        assert len(planned.links) == 1
        assert planned.links[0].policy.retain_static_routes
        assert len(planned.links[0].containers["static-route"]["route"]) == 3
        assert any(binding.provenance == "retained" for binding in planned.links[0].origins.bindings.values())


async def database_state():
    async with session() as db:
        counts = {
            table.name: await db.scalar(select(func.count()).select_from(table))
            for table in Base.metadata.sorted_tables
        }
        streams = (await db.execute(select(DeviceProjectionStream.__table__).order_by(DeviceProjectionStream.id))).all()
        return counts, [dict(row._mapping) for row in streams]


@pytest.mark.parametrize("case", ["plain", "removal", "lag"])
async def test_plan_persists_nothing(adapter_client, monkeypatch, case):
    device, selected, client, _ = await setup(adapter_client, monkeypatch, case)
    before = await database_state()
    planned = await plan(device, selected, client)
    assert planned.links
    assert await database_state() == before
    if case == "plain":
        assert before[0]["jobs"] == before[0]["deployment_generation"] == before[0]["apply_execution_attempt"] == 0
    async with session() as db:
        await db.execute(select(Device).where(Device.id == device.id).with_for_update(nowait=True))
        await db.execute(text("SET LOCAL lock_timeout = '100ms'"))
        from nso_adapter.core.generation import lock_projection

        await lock_projection(db, device.id)
    with pytest.raises(FrozenInstanceError):
        planned.links = ()
    original = planned.links[0].containers
    planned.links[0].containers.clear()
    assert planned.links[0].containers == original


@pytest.mark.parametrize("fail_after_commit", [False, True])
async def test_outer_rollback_survives_session_commit(adapter_client, monkeypatch, fail_after_commit):
    import nso_adapter.core.preflight as preflight

    device, selected, client, _ = await setup(adapter_client, monkeypatch)
    before = await database_state()

    async def commit_plan(*args):
        result = await create_action_apply(*args)
        await args[0].commit()
        if fail_after_commit:
            raise RuntimeError("placeholder planning failure")
        return result

    monkeypatch.setattr(preflight, "create_action_apply", commit_plan)
    if fail_after_commit:
        with pytest.raises(RuntimeError, match="planning failure"):
            await plan(device, selected, client)
    else:
        assert (await plan(device, selected, client)).links
    assert await database_state() == before
    async with session() as db:
        await db.execute(select(Device).where(Device.id == device.id).with_for_update(nowait=True))


async def test_plan_captures_composed_authorized_interface_baseline(adapter_client, monkeypatch):
    from nso_adapter.store.models import GenerationStatus

    device, selected, client, _ = await setup(adapter_client, monkeypatch)
    response = await _put_interface_attrs(adapter_client, device.id, "baseline", seq=41, query="?store_only=true")
    assert response.status_code == 200
    assert (await _apply(adapter_client, device.id, {"interface_config": 41})).status_code == 202
    await _settle((await _generations(device.id))[-1].job_id, GenerationStatus.settled)
    response = await _put_ip(
        adapter_client,
        device.id,
        [
            {
                "interface": "GigabitEthernet0/1",
                "address": "192.0.2.1/24",
                "family": "ipv4",
                "parent_binding": "lag-99",
                "encap_tag": "99",
            }
        ],
        seq=41,
    )
    assert response.status_code == 200
    from nso_adapter.core.generation import create_automatic_apply

    async with session() as db:
        await create_automatic_apply(db, device.id, "ip", 41)
        await db.commit()
    await _settle((await _generations(device.id))[-1].job_id, GenerationStatus.settled)
    before = await database_state()
    planned = await plan(device, selected, client)
    baseline = planned.baseline_interface_proofs
    assert baseline["interface_config"] == baseline["ip"]
    (record,) = baseline["ip"].values()
    assert record["name"] == "GigabitEthernet0/1"
    assert record["parent_binding"] == "lag-99"
    assert record["encap_tag"] == "99"
    assert planned.links[0].document["interface_config"]["_execution"]["proof"]["interfaces"] == baseline["ip"]
    assert await database_state() == before
    assert normalized_plan_digest(planned) == normalized_plan_digest(await plan(device, selected, client))


async def test_plan_does_not_leak_provisional_generation_context(adapter_client, monkeypatch):
    from nso_adapter.core.generation import LAST_ENQUEUED_GENERATION_ID

    device, selected, client, _ = await setup(adapter_client, monkeypatch)
    token = LAST_ENQUEUED_GENERATION_ID.set(123456789)
    try:
        planned = await plan(device, selected, client)
        assert planned.links
        assert LAST_ENQUEUED_GENERATION_ID.get() == 123456789
    finally:
        LAST_ENQUEUED_GENERATION_ID.reset(token)


async def test_plan_preserves_apply_conflict_and_selection_skips(adapter_client, monkeypatch):
    from nso_adapter.core.generation import ApplyJobConflict

    device, selected, client, _ = await setup(adapter_client, monkeypatch)
    assert (await _apply(adapter_client, device.id, selected)).status_code == 202
    assert (await _put_vlans(adapter_client, device.id, [102], seq=42, query="?store_only=true")).status_code == 200
    before = await database_state()
    with pytest.raises(ApplyJobConflict) as planned_error:
        await plan(device, {"vlan": 42}, client)
    async with session() as db:
        with pytest.raises(ApplyJobConflict) as apply_error:
            await create_action_apply(db, device.id, {"vlan": 42}, uuid4())
    assert planned_error.value.job_id == apply_error.value.job_id
    skipped = await plan(device, {"vlan": 999}, client)
    assert not skipped.links
    async with session() as db:
        real = await create_action_apply(db, device.id, {"vlan": 999}, uuid4())
    assert skipped.skipped == real.skipped
    assert await database_state() == before


@pytest.mark.parametrize("status", ["failed", "outcome_unknown"])
async def test_plan_behind_blocked_head_matches_apply(adapter_client, monkeypatch, status):
    from nso_adapter.store.models import GenerationStatus

    device, selected, client, _ = await setup(adapter_client, monkeypatch)
    assert (await _apply(adapter_client, device.id, selected)).status_code == 202
    await _settle((await _generations(device.id))[-1].job_id, GenerationStatus(status))
    assert (await _put_vlans(adapter_client, device.id, [102], seq=42, query="?store_only=true")).status_code == 200
    before = await database_state()
    planned = await plan(device, {"vlan": 42}, client)
    assert await database_state() == before
    async with session() as db:
        created = await create_action_apply(db, device.id, {"vlan": 42}, uuid4())
        assert [link.digest for link in planned.links] == [link.digest for link in created.generations]
        await db.rollback()


async def test_plan_captures_body_build_errors(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch)
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "1")
    response = await adapter_client.put(
        f"/api/v1/devices/{device.id}/logging-intent?store_only=true",
        json={"hosts": [], "local_levels": {"console_severity": "CRITICAL"}},
        headers=AUTH | {"X-Push-Seq": "41"},
    )
    assert response.status_code == 200
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "0")
    planned = await plan(device, selected | {"logging": 41}, client)
    assert planned.links[0].errors["logging"]["code"] == "local_levels_gated"
    assert not transport.fake.calls


@pytest.mark.parametrize("provenance", ["prior", "retained"])
async def test_plan_binds_history_inside_unit(adapter_client, monkeypatch, provenance):
    from tests.core.removal_helpers import seed_tomb

    device, selected, client, transport = await setup(
        adapter_client, monkeypatch, "removal" if provenance == "prior" else "retain_history"
    )
    if provenance == "retained":
        await seed_tomb(device.id, _B, route_id=2)
        assert (await _put_vlans(adapter_client, device.id, [101], seq=41, query="?store_only=true")).status_code == 200
        selected = {"vlan": 41}
    transport.documents.clear()
    planned = await plan(device, selected, client)
    origins = planned.links[0].origins
    record = refusal("static-route", row_ref("static-route", "static-route.route", (("route", _B),)), "write-blocked")
    resolved = resolve_refusals((record,), origins)
    assert not isinstance(resolved, AttributionFailure), resolved
    assert any(binding.provenance == provenance for key, binding in origins.bindings.items() if key.row == record.row)
    assert not transport.documents


async def test_plan_reports_collateral_guard_block(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch)
    transport.fake.service = []
    transport.fake.instance["vlan"] = {"vlan": [{"vlan-id": 999, "name": "unowned"}]}
    planned = await plan(device, selected, client)
    assert planned.links[0].guard_orphans == {"vlan/vlan": [["999"]]}
    assert not transport.documents
    from nso_adapter.core.generation import create_automatic_apply

    async with session() as db:
        await create_automatic_apply(db, device.id, "vlan", selected["vlan"])
        await db.commit()
    job = await job_row(await run_next(device.id))
    assert job.error["code"] == "removal_blocked_collateral"
    assert job.error["detail"]["orphans"] == planned.links[0].guard_orphans
    assert not transport.documents


async def test_plan_detach_link_uses_no_networking_policy(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch, "detach")
    planned = await plan(device, selected, client)
    assert len(planned.links) == 1
    assert planned.links[0].mode == GenerationMode.detach
    assert planned.links[0].policy.no_networking
    assert planned.links[0].guard_orphans is None
    assert not transport.documents


async def test_plan_single_live_read(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch, "removal")
    planned = await plan(device, selected, client)
    assert len(planned.links) == 2
    assert len(transport.fake.reads) == 1
    assert transport.fake.calls == []


@pytest.mark.parametrize("case", ["plain", "removal"])
async def test_sender_and_preflight_share_body_and_guard(adapter_client, monkeypatch, case):
    device, selected, client, transport = await setup(adapter_client, monkeypatch, case)
    planned = await plan(device, selected, client)
    assert (await _apply(adapter_client, device.id, selected)).status_code == 202
    for link in planned.links:
        await run_next(device.id)
        sent = dict(transport.documents[-1])
        sent.pop("device")
        assert sent == link.containers
        assert transport.fake.writes[-1]["no_networking"] == link.policy.no_networking


@pytest.mark.parametrize("case", ["plain", "removal", "lag", "switchport", "retention"])
async def test_normalized_plan_digest_is_stable_and_binds_complete_plan(adapter_client, monkeypatch, case):
    device, selected, client, _ = await setup(adapter_client, monkeypatch, case)
    first = await plan(device, selected, client)
    second = await plan(device, selected, client)
    assert normalized_plan_digest(first) == normalized_plan_digest(second)


ORPHAN = {"vlan/vlan": [["999"]]}
LINK_MUTATIONS = {
    "containers": lambda link: link.update(containers={}),
    "policy_no_networking": lambda link: link["policy"].update(no_networking=not link["policy"]["no_networking"]),
    "policy_retain_static_routes": lambda link: link["policy"].update(
        retain_static_routes=not link["policy"]["retain_static_routes"]
    ),
    "policy_context": lambda link: link["policy"]["context"].update(scope="vlan"),
    "allowed_removal_keys": lambda link: link.update(allowed_removal_keys=ORPHAN),
    "guard_orphans": lambda link: link.update(guard_orphans=ORPHAN),
    "guard_contributions": lambda link: link.update(guard_contributions={"vlan/vlan": {"vlan": [["999"]]}}),
    "origin_provenance": lambda link: link["origins"][0].update(provenance="prior"),
    "errors": lambda link: link.update(
        errors={"logging": {"code": "local_levels_gated", "message": "placeholder", "detail": {}}}
    ),
    "mode": lambda link: link.update(mode=GenerationMode.detach.value),
    "streams": lambda link: link.update(streams=[*link["streams"], "ip"]),
    "order": lambda link: link.update(order=link["order"] + 1),
}
CHAIN_MUTATIONS = {
    "skipped": lambda chain: chain.update(skipped={"snmp": "no_receipt"}),
    "skipped_detail": lambda chain: chain.update(skipped_detail={"snmp": {"reason": "no_receipt"}}),
    "baseline_interface_proofs": lambda chain: chain.update(
        baseline_interface_proofs={"ip": {"7": {"id": 7, "name": "GigabitEthernet0/1", "vrf": None}}}
    ),
}


@pytest.mark.parametrize("field", [*LINK_MUTATIONS, *CHAIN_MUTATIONS])
async def test_normalized_plan_digest_binds_each_plan_field(adapter_client, monkeypatch, field):
    device, selected, client, _ = await setup(adapter_client, monkeypatch)
    planned = await plan(device, selected, client)
    link, chain = json.loads(planned.links[0]._json), json.loads(planned._json)
    assert link["origins"] and not link["policy"]["no_networking"] and "scope" not in link["policy"]["context"]
    if field in LINK_MUTATIONS:
        LINK_MUTATIONS[field](link)
        changed = replace(planned, links=(replace(planned.links[0], _json=json.dumps(link)), *planned.links[1:]))
    else:
        CHAIN_MUTATIONS[field](chain)
        changed = replace(planned, _json=json.dumps(chain))
    assert normalized_plan_digest(changed) != normalized_plan_digest(planned)


async def test_normalized_digest_ignores_carrier_job_identity(adapter_client, monkeypatch):
    device, selected, client, _ = await setup(adapter_client, monkeypatch, "retention")
    planned = await plan(device, selected, client)
    payload = json.loads(planned.links[0]._json)
    payload["document"]["static_route"]["static_route_tombstone"][0]["job_id"] = 123456789
    changed = replace(planned.links[0], _json=json.dumps(payload))
    assert normalized_plan_digest(planned) == normalized_plan_digest(replace(planned, links=(changed,)))


@pytest.mark.parametrize("field", ["interface_proof", "relation", "identities"])
def test_normalized_digest_replaces_database_identities(field):
    from nso_adapter.core.preflight import PlannedChain, PlannedLink

    def chain(identity, *, changed=False):
        payload = {
            "order": 0,
            "mode": "networked",
            "streams": ["ip"],
            "digest": str(identity),
            "containers": {"vlan": {"vlan": [{"vlan-id": 101}]}},
            "policy": {"context": {}, "no_networking": False, "retain_static_routes": True},
            "document": {
                "interface_config": {
                    "interface_ip_intent": [{"id": identity + 1, "interface_id": identity, "address": "192.0.2.1/24"}],
                    "_execution": {
                        "context": {"ned_id": "placeholder-ned"},
                        "proof": {
                            "interfaces": {
                                str(identity): {
                                    "id": identity,
                                    "name": "Ethernet1",
                                    "vrf": "red" if changed and field == "interface_proof" else None,
                                }
                            },
                            "attribute_eligibility": {f"{identity}/description": False},
                        },
                    },
                }
            },
            "origins": [],
            "allowed_removal_keys": {},
            "guard_orphans": None,
            "errors": {},
        }
        if field == "identities":
            payload["origins"] = [{"content_identity": str(identity), "generation_id": identity, "seq": identity}]
        if field == "relation":
            payload["document"]["interface_config"]["_execution"]["proof"]["interfaces"][str(identity + 1)] = {
                "id": identity + 1,
                "name": "Ethernet2",
                "vrf": None,
            }
            payload["document"]["interface_config"]["interface_ip_intent"][0]["interface_id"] += int(changed)
        return PlannedChain(
            (PlannedLink(json.dumps(payload)),), '{"baseline_interface_proofs":{},"skipped":{},"skipped_detail":{}}'
        )

    assert normalized_plan_digest(chain(7)) == normalized_plan_digest(chain(999))
    if field != "identities":
        assert normalized_plan_digest(chain(7)) != normalized_plan_digest(chain(7, changed=True))


async def test_live_read_reports_inconclusive(adapter_client, monkeypatch):
    device, selected, client, transport = await setup(adapter_client, monkeypatch)
    transport.fake.service_status = "inconclusive"
    live = await read_plan_inputs(client, device)
    assert live.section.inconclusive
    assert live.digest is None
    with pytest.raises(ValueError, match="certified"):
        await plan_action_apply(
            get_engine(), device.id, selected, live.section, expected_context=_device_context(device)
        )
