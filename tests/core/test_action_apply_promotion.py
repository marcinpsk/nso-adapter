# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Selected manual-Apply promotion and mixed decomposition."""

from __future__ import annotations

import asyncio
import re
import time
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from tests.api.test_static_route_deleted_routes import deleted as deleted_route
from tests.api.test_static_route_identity import entry as route_entry
from tests.conftest import VALID_TOKEN, seed_device, session
from tests.core.test_generation_protocol import seed_settings

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

_A = ("", "198.18.1.0/24", "192.0.2.1")
_B = ("", "198.18.2.0/24", "192.0.2.2")
_C = ("", "198.18.3.0/24", "192.0.2.3")
_D = ("", "198.18.4.0/24", "192.0.2.4")


async def _put_routes(client, device_id: int, routes: list[dict], *, seq: int, query: str = "", deleted=None):
    return await client.put(
        f"/api/v1/devices/{device_id}/static-route-intent{query}",
        json={"routes": routes, "deleted_routes": [] if deleted is None else deleted},
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _put_vlans(client, device_id: int, vids: list[int], *, seq: int, query: str = ""):
    return await client.put(
        f"/api/v1/devices/{device_id}/vlan-intent{query}",
        json={"vlans": [{"vlan_id": vid, "name": f"vlan-{vid}"} for vid in vids]},
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _put_snmp(client, device_id: int, labels: list[str], *, seq: int, query: str = ""):
    return await client.put(
        f"/api/v1/devices/{device_id}/snmp-intent{query}",
        json={"communities": [{"label": label, "vault_ref": f"kv/snmp#{label}", "access": "ro"} for label in labels]},
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _put_bgp(client, device_id: int, remote_as: str, *, seq: int, query: str = ""):
    return await client.put(
        f"/api/v1/devices/{device_id}/bgp-intent{query}",
        json={"routers": [_bgp_router(remote_as)]},
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _put_interface_attrs(client, device_id: int, value: str, *, seq: int, query: str = ""):
    return await client.put(
        f"/api/v1/devices/{device_id}/intent{query}",
        json={"attributes": [{"interface": "GigabitEthernet0/1", "attribute": "description", "intent_value": value}]},
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _put_svis(client, device_id: int, vlan_ids: list[int], *, seq: int, query: str = ""):
    return await client.put(
        f"/api/v1/devices/{device_id}/svi-intent{query}",
        json={
            "interfaces": [
                {"interface_name": f"Vlan{vlan_id}", "vlan_id": vlan_id, "type": "svi"} for vlan_id in vlan_ids
            ]
        },
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _apply(client, device_id: int, selected: dict[str, int], *, attempt_id=None):
    return await client.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={"apply_attempt_id": str(attempt_id or uuid4()), "selected": selected},
        headers=AUTH,
    )


def _reject_restconf_patches(client, message: str) -> None:
    async def reject(url, content=None, headers=None, **kwargs):
        return httpx.Response(
            400,
            request=httpx.Request("PATCH", url),
            json={"errors": {"error": [{"error-message": message}]}},
        )

    client._client.return_value.__aenter__.return_value.patch.side_effect = reject


def _bgp_router(remote_as: str) -> dict:
    return {
        "asn": "64512",
        "router_id": "192.0.2.254",
        "scopes": [
            {
                "vrf": "",
                "address_families": [{"af": "ipv4-unicast"}],
                "peers": [
                    {
                        "peer_address": "192.0.2.1",
                        "remote_as": remote_as,
                        "address_families": [{"af": "ipv4-unicast", "enabled": True}],
                    }
                ],
            }
        ],
    }


async def _generations(device_id: int) -> list:
    from nso_adapter.store.models import DeploymentGeneration

    async with session() as db:
        return list(
            (
                await db.execute(
                    sa.select(DeploymentGeneration)
                    .where(DeploymentGeneration.device_id == device_id)
                    .order_by(DeploymentGeneration.seq)
                )
            )
            .scalars()
            .all()
        )


async def _jobs(device_id: int) -> list:
    from nso_adapter.store.models import Job

    async with session() as db:
        return list(
            (await db.execute(sa.select(Job).where(Job.device_id == device_id).order_by(Job.created_at, Job.id)))
            .scalars()
            .all()
        )


async def _stream(device_id: int, stream: str):
    from nso_adapter.store.models import DeviceProjectionStream

    async with session() as db:
        return await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == stream,
            )
        )


async def _settle(job_id: int, outcome) -> None:
    from nso_adapter.core.generation import settle_job_generations
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        job = await db.get(Job, job_id)
        job.status = JobStatus.succeeded if outcome.value == "settled" else JobStatus.failed
        await settle_job_generations(db, job_id, outcome=outcome)
        await db.commit()


async def _mixed_case(client, *, suffix: int):
    """Directly promote an A-update/B-delete/C-detach/D-add push after A/B/C."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name=f"apply-mixed-{suffix}", netbox_device_id=9950 + suffix)
    await seed_settings(device_id, auto_apply=True)

    baseline_seq = suffix * 100 + 1
    baseline = await _put_routes(
        client,
        device_id,
        [
            route_entry(_A, route_id=1, generation=1),
            route_entry(_B, route_id=2, generation=1),
            route_entry(_C, route_id=3, generation=1),
        ],
        seq=baseline_seq,
    )
    assert baseline.status_code == 200, baseline.text
    (baseline_generation,) = await _generations(device_id)
    await _settle(baseline_generation.job_id, GenerationStatus.settled)

    promoted_seq = suffix * 100 + 2
    promoted = await _put_routes(
        client,
        device_id,
        [route_entry(_A, route_id=1, generation=2, metric=20), route_entry(_D, route_id=4, generation=1)],
        seq=promoted_seq,
        deleted=[deleted_route(2, [_B])],
    )
    assert promoted.status_code == 200, promoted.text
    chain = await _generations(device_id)
    return device_id, promoted, chain[1:]


def _route_ids(generation) -> set[int]:
    return {row["route_id"] for row in generation.document["static_route"]["static_route_intent"]}


async def test_mixed_direct_promotion_decomposes_into_shared_cohort_chain(adapter_client):
    from nso_adapter.store.models import GenerationMode, JobType

    device_id, _response, chain = await _mixed_case(adapter_client, suffix=1)

    assert len(chain) == 3
    networked, detach, apply = chain
    assert networked.settlement_cohort is not None
    assert detach.settlement_cohort == networked.settlement_cohort
    assert apply.settlement_cohort == networked.settlement_cohort
    assert (networked.mode, detach.mode, apply.mode) == (
        GenerationMode.networked,
        GenerationMode.detach,
        GenerationMode.networked,
    )
    assert networked.seq < detach.seq < apply.seq
    assert _route_ids(networked) == {1, 4}
    assert _route_ids(detach) == {1, 4}
    assert _route_ids(apply) == {1, 4}
    assert {row["route_id"] for row in networked.document["static_route"]["static_route_tombstone"]} == {2, 3}
    updated = next(row for row in networked.document["static_route"]["static_route_intent"] if row["route_id"] == 1)
    assert updated["metric"] == 20
    assert networked.digest != detach.digest
    jobs = {job.id: job for job in await _jobs(device_id)}
    assert [jobs[generation.job_id].job_type for generation in chain] == [
        JobType.removal,
        JobType.removal,
        JobType.apply,
    ]


async def test_mixed_promotion_stamps_applied_only_after_the_whole_chain_settles(adapter_client):
    from nso_adapter.store.models import GenerationStatus

    device_id, _response, chain = await _mixed_case(adapter_client, suffix=2)
    networked, detach, apply = chain

    await _settle(networked.job_id, GenerationStatus.settled)
    assert (await _stream(device_id, "static_route")).applied_revision == 1

    await _settle(detach.job_id, GenerationStatus.settled)
    assert (await _stream(device_id, "static_route")).applied_revision == 1

    await _settle(apply.job_id, GenerationStatus.settled)
    stream = await _stream(device_id, "static_route")
    assert (stream.desired_revision, stream.authorized_revision, stream.applied_revision) == (2, 2, 2)


async def test_request_settlement_preparation_uses_one_auto_apply_predicate(adapter_client):
    from nso_adapter.core.generation import prepare_request_settlement

    device_id = await seed_device(nso_device_name="request-settlement-helper", netbox_device_id=9989)
    await seed_settings(device_id, auto_apply=True)
    async with session() as db:
        assert await prepare_request_settlement(
            db,
            device_id,
            mutation_count=0,
            removal_generation_count=1,
        ) == (False, None)
        apply_requested, cohort = await prepare_request_settlement(
            db,
            device_id,
            mutation_count=1,
            removal_generation_count=1,
        )

    assert apply_requested is True
    assert isinstance(cohort, int)


async def test_request_atomic_cohort_stamps_no_stream_until_every_member_succeeds(adapter_client):
    """One failed cohort member withholds every stream until that member is retried."""
    from nso_adapter.core.claim import terminalize
    from nso_adapter.core.generation import (
        allocate_settlement_cohort,
        attach_to_job,
        create_generation,
        mark_job_generations_running,
        note_write,
        retry_generation,
    )
    from nso_adapter.store.models import (
        GenerationMode,
        Job,
        JobStatus,
        JobType,
        SviIntent,
        VlanIntent,
    )

    device_id = await seed_device(nso_device_name="request-atomic-cohort", netbox_device_id=9990)
    async with session() as db:
        db.add(VlanIntent(device_id=device_id, vlan_id=10, accepted_at=sa.func.now()))
        db.add(SviIntent(device_id=device_id, interface_name="Vlan10", vlan_id=10, accepted_at=sa.func.now()))
        await db.flush()
        cohort = await allocate_settlement_cohort(db)

        await note_write(db, device_id, "vlan")
        vlan_generation = await create_generation(
            db,
            device_id,
            streams=("vlan",),
            mode=GenerationMode.networked,
            settlement_cohort=cohort,
        )
        vlan_job = Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.running, coalescible=True)
        db.add(vlan_job)
        await db.flush()
        await attach_to_job(db, vlan_generation, vlan_job)

        await note_write(db, device_id, "svi")
        svi_generation = await create_generation(
            db,
            device_id,
            streams=("svi",),
            mode=GenerationMode.networked,
            settlement_cohort=cohort,
        )
        svi_job = Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.queued, coalescible=False)
        db.add(svi_job)
        await db.flush()
        await attach_to_job(db, svi_generation, svi_job)
        await db.commit()

    async with session() as db:
        assert await terminalize(db, vlan_job.id, status=JobStatus.succeeded, expect=JobStatus.running) is not None
        await db.commit()
    assert (await _stream(device_id, "vlan")).applied_revision == 0
    assert (await _stream(device_id, "svi")).applied_revision == 0

    async with session() as db:
        queued = await db.get(Job, svi_job.id)
        queued.status = JobStatus.running
        await mark_job_generations_running(db, svi_job.id)
        await db.flush()
        assert await terminalize(db, svi_job.id, status=JobStatus.failed, expect=JobStatus.running) is not None
        await db.commit()
    assert (await _stream(device_id, "vlan")).applied_revision == 0
    assert (await _stream(device_id, "svi")).applied_revision == 0

    async with session() as db:
        retry = await retry_generation(db, svi_generation.id)
        assert retry is not None
        await db.commit()
        retry.status = JobStatus.running
        await mark_job_generations_running(db, retry.id)
        await db.flush()
        assert await terminalize(db, retry.id, status=JobStatus.succeeded, expect=JobStatus.running) is not None
        await db.commit()

    assert (await _stream(device_id, "vlan")).applied_revision == 1
    assert (await _stream(device_id, "svi")).applied_revision == 1


async def test_svi_request_with_removal_and_apply_uses_one_cohort(adapter_client):
    """Every generation promoted by one non-static request shares one request cohort."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="svi-request-cohort", netbox_device_id=9991)
    await seed_settings(device_id, auto_apply=True)
    assert (await _put_svis(adapter_client, device_id, [100, 200], seq=6401)).status_code == 200
    (baseline,) = await _generations(device_id)
    await _settle(baseline.job_id, GenerationStatus.settled)

    assert (await _put_svis(adapter_client, device_id, [100], seq=6402)).status_code == 200
    removal, apply = (await _generations(device_id))[1:]
    assert removal.settlement_cohort is not None
    assert apply.settlement_cohort == removal.settlement_cohort


async def test_svi_action_apply_executes_the_selected_document(adapter_client):
    """A store-only successor landing after Apply cannot leak into the selected run."""
    from tests.core.test_generation_protocol import recorded_client, run_head

    device_id = await seed_device(nso_device_name="svi-exact-selection", netbox_device_id=9992)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_svis(adapter_client, device_id, [100], seq=6501, query="?store_only=true")).status_code == 200
    response = await _apply(adapter_client, device_id, {"svi": 6501})
    assert response.status_code == 202, response.text

    async def successor():
        result = await _put_svis(adapter_client, device_id, [200], seq=6502, query="?store_only=true")
        assert result.status_code == 200

    client, recorder = recorded_client("svi-exact-selection", on_sync_from=successor)
    assert await run_head(device_id, client) is not None
    bodies = recorder.bodies("svi-reconciler:svi-config")
    assert [[entry["vlan-id"] for entry in body["svi-reconciler:svi-config"][0]["interface"]] for body in bodies] == [
        [100]
    ]


async def test_snmp_action_apply_executes_the_selected_document(adapter_client):
    """A later SNMP push cannot replace the selected rows or Vault references at send time."""
    from tests.core.test_generation_protocol import recorded_client, run_head

    device_id = await seed_device(nso_device_name="snmp-exact-selection", netbox_device_id=9995)
    await seed_settings(device_id, auto_apply=False)
    assert (
        await _put_snmp(adapter_client, device_id, ["selected"], seq=6551, query="?store_only=true")
    ).status_code == 200
    response = await _apply(adapter_client, device_id, {"snmp": 6551})
    assert response.status_code == 202, response.text

    async def successor():
        result = await _put_snmp(
            adapter_client,
            device_id,
            ["successor"],
            seq=6552,
            query="?store_only=true",
        )
        assert result.status_code == 200

    client, recorder = recorded_client("snmp-exact-selection", on_sync_from=successor)
    assert await run_head(device_id, client) is not None
    bodies = recorder.bodies("snmp-reconciler:snmp-config")
    assert [entry["name"] for entry in bodies[0]["snmp-reconciler:snmp-config"][0]["community"]] == ["selected"]
    assert bodies[0]["snmp-reconciler:snmp-config"][0]["community"][0] == {
        "name": "selected",
        "access": "ro",
        "vault-mount": "kv",
        "vault-path": "snmp",
        "vault-key": "selected",
    }


async def test_bgp_action_apply_executes_the_selected_graph(adapter_client):
    """A later BGP tree cannot replace the selected graph at worker claim time."""
    from nso_adapter.store.models import JobStatus
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(nso_device_name="bgp-exact-selection", netbox_device_id=9996)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_bgp(adapter_client, device_id, "64513", seq=6561, query="?store_only=true")).status_code == 200
    response = await _apply(adapter_client, device_id, {"bgp": 6561})
    assert response.status_code == 202, response.text

    async def successor():
        result = await _put_bgp(adapter_client, device_id, "64514", seq=6562, query="?store_only=true")
        assert result.status_code == 200

    client, recorder = recorded_client("bgp-exact-selection", on_sync_from=successor)
    job_id = await run_head(device_id, client)
    assert job_id is not None
    job = await job_row(job_id)
    assert job.status is JobStatus.succeeded
    assert job.result["bgp_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
    bodies = recorder.bodies("bgp-reconciler:bgp-config")
    router = bodies[0]["bgp-reconciler:bgp-config"][0]["router"][0]
    assert router["asn"] == 64512
    assert router["scope"][0]["address-family"] == [{"afi": "ipv4-unicast"}]
    assert router["scope"][0]["peer"][0]["remote-as"] == "64513"
    assert router["scope"][0]["peer"][0]["peer-address-family"] == [{"afi": "ipv4-unicast", "enabled": True}]


async def test_interface_config_action_apply_executes_the_selected_document(adapter_client):
    """A later interface push cannot replace the selected attribute at worker claim time."""
    from nso_adapter.store.models import JobStatus
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(nso_device_name="interface-exact-selection", netbox_device_id=9997)
    await seed_settings(device_id, auto_apply=False)
    selected = await _put_interface_attrs(
        adapter_client,
        device_id,
        "selected description",
        seq=6571,
        query="?store_only=true",
    )
    assert selected.status_code == 200, selected.text
    response = await _apply(adapter_client, device_id, {"interface_config": 6571})
    assert response.status_code == 202, response.text

    async def successor():
        result = await _put_interface_attrs(
            adapter_client,
            device_id,
            "successor description",
            seq=6572,
            query="?store_only=true",
        )
        assert result.status_code == 200, result.text

    client, recorder = recorded_client("interface-exact-selection", on_sync_from=successor)
    job_id = await run_head(device_id, client)
    assert job_id is not None
    job = await job_row(job_id)
    assert job.status is JobStatus.succeeded
    bodies = recorder.bodies("interface-reconciler:interface-config")
    assert [body["interface-reconciler:interface-config"][0]["description"] for body in bodies] == [
        "selected description"
    ]


async def test_static_route_action_apply_executes_the_selected_plan(adapter_client):
    """A later static-route push cannot replace the selected rows or PUT classification."""
    from nso_adapter.store.models import JobStatus
    from tests.api.test_static_route_identity import seed_intent
    from tests.core.test_generation_protocol import job_row, run_head
    from tests.core.test_static_route_put import wire
    from tests.core.test_static_route_removal import SrFake, sr_client

    device_id = await seed_device(nso_device_name="static-route-exact-selection", netbox_device_id=9999)
    await seed_settings(device_id, auto_apply=False)
    await seed_intent(device_id, [{"triple": _B, "route_id": 1, "deployed_key": list(_A)}])
    selected = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_B, route_id=1, generation=1)],
        seq=6591,
        query="?store_only=true",
    )
    assert selected.status_code == 200, selected.text
    response = await _apply(adapter_client, device_id, {"static_route": 6591})
    assert response.status_code == 202, response.text

    successor = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_C, route_id=1, generation=2)],
        seq=6592,
        query="?store_only=true",
    )
    assert successor.status_code == 200, successor.text

    fake = SrFake("static-route-exact-selection", service=[wire(_A)])
    job_id = await run_head(device_id, sr_client(fake))
    assert job_id is not None
    assert (await job_row(job_id)).status is JobStatus.succeeded
    assert fake.sent_keys() == {_B}


async def test_static_route_removal_generation_records_deployed_predecessor_authority(adapter_client):
    """The removal document records both the removed triple and its deployed predecessor."""
    from tests.api.test_static_route_identity import seed_intent

    device_id = await seed_device(nso_device_name="static-route-recorded-removal", netbox_device_id=10000)
    await seed_settings(device_id, auto_apply=False)
    await seed_intent(device_id, [{"triple": _B, "route_id": 1, "deployed_key": list(_A)}])

    deleted = await _put_routes(
        adapter_client,
        device_id,
        [],
        seq=6601,
        deleted=[deleted_route(1, [_B])],
    )
    assert deleted.status_code == 200, deleted.text
    (generation,) = await _generations(device_id)
    recorded = generation.document["static_route"]["_execution"]["removal"]
    assert recorded["authorized_removal_keys"] == [list(_A), list(_B)]
    assert len(recorded["tombstone_ids"]) == 1


async def test_static_route_action_removal_records_store_only_deletion_authority(adapter_client):
    """Manual Apply records both keys without widening the pinned job-context wire shape."""
    from nso_adapter.store.models import GenerationStatus
    from tests.api.test_static_route_identity import seed_intent

    device_id = await seed_device(nso_device_name="static-route-action-removal", netbox_device_id=10002)
    await seed_settings(device_id, auto_apply=True)
    await seed_intent(device_id, [{"triple": _B, "route_id": 1, "deployed_key": list(_A)}])
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_B, route_id=1, generation=1)],
        seq=6600,
    )
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)
    deleted = await _put_routes(
        adapter_client,
        device_id,
        [],
        seq=6602,
        query="?store_only=true",
        deleted=[deleted_route(1, [_B])],
    )
    assert deleted.status_code == 200, deleted.text

    response = await _apply(adapter_client, device_id, {"static_route": 6602})

    assert response.status_code == 202, response.text
    generation = (await _generations(device_id))[-1]
    job = {row.id: row for row in await _jobs(device_id)}[generation.job_id]
    assert job.context == {"scope": "static_route", "removed": {"route": [list(_B)]}}
    assert generation.allowed_removal_keys == {"route": [list(_A), list(_B)]}
    recorded = generation.document["static_route"]["_execution"]["removal"]
    assert recorded["authorized_removal_keys"] == [list(_A), list(_B)]
    assert recorded["tombstone_ids"] == []


async def test_static_route_removal_executes_recorded_authority_after_a_later_reclaim(adapter_client):
    """A store-only reclaim after generation creation cannot weaken the recorded removal."""
    from nso_adapter.store.models import JobStatus
    from tests.api.test_static_route_identity import seed_intent
    from tests.core.test_generation_protocol import job_row, run_head
    from tests.core.test_static_route_put import wire
    from tests.core.test_static_route_removal import SrFake, sr_client

    device_id = await seed_device(nso_device_name="static-route-removal-exact", netbox_device_id=10001)
    await seed_settings(device_id, auto_apply=False)
    await seed_intent(device_id, [{"triple": _B, "route_id": 1, "deployed_key": list(_A)}])

    deleted = await _put_routes(
        adapter_client,
        device_id,
        [],
        seq=6611,
        deleted=[deleted_route(1, [_B])],
    )
    assert deleted.status_code == 200, deleted.text

    reclaimed = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_B, route_id=1, generation=2)],
        seq=6612,
        query="?store_only=true",
    )
    assert reclaimed.status_code == 200, reclaimed.text

    fake = SrFake("static-route-removal-exact", service=[wire(_A), wire(_B)])
    job_id = await run_head(device_id, sr_client(fake))
    assert job_id is not None
    assert (await job_row(job_id)).status is JobStatus.succeeded
    assert fake.sent_keys() == set()


async def test_mixed_generation_executes_interface_section_from_its_document(adapter_client):
    """A static-route lane cannot make the interface lane read live rows."""
    from nso_adapter.core.generation import attach_to_job, create_generation
    from nso_adapter.store.models import GenerationMode, Job, JobStatus, JobType
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(nso_device_name="interface-mixed-exact-selection", netbox_device_id=9981)
    await seed_settings(device_id, auto_apply=False)
    selected = await _put_interface_attrs(
        adapter_client,
        device_id,
        "selected description",
        seq=6573,
        query="?store_only=true",
    )
    assert selected.status_code == 200, selected.text
    addresses = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ip-intent?store_only=true",
        json={"addresses": [{"interface": "GigabitEthernet0/1", "address": "198.18.10.1/30", "family": "ipv4"}]},
        headers=AUTH | {"X-Push-Seq": "6574"},
    )
    assert addresses.status_code == 200, addresses.text
    routes = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=6575,
        query="?store_only=true",
    )
    assert routes.status_code == 200, routes.text

    async with session() as db:
        generation = await create_generation(
            db,
            device_id,
            streams=("interface_config", "ip", "static_route"),
            mode=GenerationMode.networked,
        )
        job = Job(job_type=JobType.apply, device_id=device_id, coalescible=True)
        db.add(job)
        await db.flush()
        assert await attach_to_job(db, generation, job)
        await db.commit()
        assert generation.stream_revisions == {"interface_config": 1, "ip": 1, "static_route": 1}

    successor = await _put_interface_attrs(
        adapter_client,
        device_id,
        "live successor description",
        seq=6576,
        query="?store_only=true",
    )
    assert successor.status_code == 200, successor.text

    client, recorder = recorded_client("interface-mixed-exact-selection")
    job_id = await run_head(device_id, client)
    assert job_id is not None
    assert (await job_row(job_id)).status is JobStatus.succeeded
    bodies = recorder.bodies("interface-reconciler:interface-config")
    assert [
        body["interface-reconciler:interface-config"][0]["description"]
        for body in bodies
        if "description" in body["interface-reconciler:interface-config"][0]
    ] == ["selected description"]


async def test_interface_config_generation_records_creation_time_attribute_eligibility(adapter_client):
    """The worker sends the recorded eligible subset after live eligibility changes."""
    from nso_adapter.store.models import DbInterface, InterfaceAttrState, JobStatus, SyncState
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(
        nso_device_name="interface-recorded-eligibility",
        netbox_device_id=9998,
        attributes=["description", "enabled"],
    )
    await seed_settings(device_id, auto_apply=False)
    stored = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent?store_only=true",
        json={
            "attributes": [
                {
                    "interface": "GigabitEthernet0/1",
                    "attribute": "description",
                    "intent_value": "selected description",
                },
                {"interface": "GigabitEthernet0/1", "attribute": "enabled", "intent_value": True},
            ]
        },
        headers=AUTH | {"X-Push-Seq": "6581"},
    )
    assert stored.status_code == 200, stored.text
    async with session() as db:
        iface = await db.scalar(
            sa.select(DbInterface).where(
                DbInterface.device_id == device_id,
                DbInterface.name == "GigabitEthernet0/1",
            )
        )
        enabled_state = await db.scalar(
            sa.select(InterfaceAttrState).where(
                InterfaceAttrState.interface_id == iface.id,
                InterfaceAttrState.attribute == "enabled",
            )
        )
        enabled_state.sync_state = SyncState.error
        iface_id = iface.id
        await db.commit()

    response = await _apply(adapter_client, device_id, {"interface_config": 6581})

    assert response.status_code == 202, response.text
    (generation,) = await _generations(device_id)
    execution = generation.document["interface_config"]["_execution"]
    assert execution["eligible_interface_attributes"] == [{"interface_id": iface_id, "attribute": "description"}]
    async with session() as db:
        enabled_state = await db.scalar(
            sa.select(InterfaceAttrState).where(
                InterfaceAttrState.interface_id == iface_id,
                InterfaceAttrState.attribute == "enabled",
            )
        )
        description_state = await db.scalar(
            sa.select(InterfaceAttrState).where(
                InterfaceAttrState.interface_id == iface_id,
                InterfaceAttrState.attribute == "description",
            )
        )
        enabled_state.sync_state = SyncState.accepted
        description_state.sync_state = SyncState.error
        await db.commit()

    client, recorder = recorded_client("interface-recorded-eligibility")
    job_id = await run_head(device_id, client)
    assert job_id is not None
    assert (await job_row(job_id)).status is JobStatus.succeeded
    bodies = recorder.bodies("interface-reconciler:interface-config")
    assert [body["interface-reconciler:interface-config"][0] for body in bodies] == [
        {
            "device": "interface-recorded-eligibility",
            "interface-name": "GigabitEthernet0/1",
            "kind": "base",
            "description": "selected description",
        }
    ]


async def test_interface_config_generation_refuses_unresolvable_attribute_eligibility(adapter_client):
    """A missing attr-state row refuses generation creation and leaves authority unchanged."""
    from structlog.testing import capture_logs

    from nso_adapter.store.models import DbInterface, InterfaceAttrState

    device_id = await seed_device(nso_device_name="interface-unresolved-eligibility", netbox_device_id=9999)
    await seed_settings(device_id, auto_apply=False)
    stored = await _put_interface_attrs(
        adapter_client,
        device_id,
        "selected description",
        seq=6591,
        query="?store_only=true",
    )
    assert stored.status_code == 200, stored.text
    async with session() as db:
        iface_id = await db.scalar(
            sa.select(DbInterface.id).where(
                DbInterface.device_id == device_id,
                DbInterface.name == "GigabitEthernet0/1",
            )
        )
        await db.execute(sa.delete(InterfaceAttrState).where(InterfaceAttrState.interface_id == iface_id))
        await db.commit()

    with capture_logs() as logs:
        response = await _apply(adapter_client, device_id, {"interface_config": 6591})

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "apply_unexecutable",
            "message": "Selected stream(s) cannot be applied faithfully: interface_config",
            "detail": {"streams": {"interface_config": "interface_attribute_eligibility_unresolved"}},
        }
    }
    assert await _generations(device_id) == []
    assert await _jobs(device_id) == []
    assert (await _stream(device_id, "interface_config")).authorized_revision == 0
    warning = next(log for log in logs if log["event"] == "generation.interface_eligibility_unresolved")
    assert warning["device_id"] == device_id
    assert f"interface {iface_id}" in warning["detail"]
    assert "attribute 'description'" in warning["detail"]
    assert warning["exc_info"] is True


async def test_unrelated_promotion_preserves_recorded_interface_eligibility(adapter_client):
    """A VLAN-only promotion carries the interface plan without reading changed live state."""
    from nso_adapter.store.models import DbInterface, InterfaceAttrState, JobStatus
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(nso_device_name="unselected-interface-eligibility", netbox_device_id=10000)
    await seed_settings(device_id, auto_apply=False)
    stored_interface = await _put_interface_attrs(
        adapter_client,
        device_id,
        "unselected description",
        seq=6592,
    )
    assert stored_interface.status_code == 200, stored_interface.text
    promoted_interface = await _apply(adapter_client, device_id, {"interface_config": 6592})
    assert promoted_interface.status_code == 202, promoted_interface.text
    interface_execution = (await _generations(device_id))[-1].document["interface_config"]["_execution"]
    client, _ = recorded_client("unselected-interface-eligibility")
    job_id = await run_head(device_id, client)
    assert job_id is not None
    assert (await job_row(job_id)).status is JobStatus.succeeded
    async with session() as db:
        interface_id = await db.scalar(
            sa.select(DbInterface.id).where(
                DbInterface.device_id == device_id,
                DbInterface.name == "GigabitEthernet0/1",
            )
        )
        await db.execute(sa.delete(InterfaceAttrState).where(InterfaceAttrState.interface_id == interface_id))
        await db.commit()

    stored_vlan = await _put_vlans(adapter_client, device_id, [10], seq=6593)
    assert stored_vlan.status_code == 200, stored_vlan.text

    response = await _apply(adapter_client, device_id, {"vlan": 6593})

    assert response.status_code == 202, response.text
    generation = (await _generations(device_id))[-1]
    assert generation.stream_revisions == {"vlan": 1}
    assert generation.document["interface_config"]["_execution"] == interface_execution


async def test_coalesced_vlan_successor_preserves_interface_execution(adapter_client):
    """The highest document keeps the immutable plan for every section the shared job executes."""
    from nso_adapter.core.generation import generation_execution_sections
    from nso_adapter.store.models import JobStatus
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(nso_device_name="coalesced-interface-plan", netbox_device_id=10001)
    await seed_settings(device_id, auto_apply=True)

    interface_response = await _put_interface_attrs(
        adapter_client,
        device_id,
        "selected description",
        seq=6594,
    )
    assert interface_response.status_code == 200, interface_response.text
    (interface_generation,) = await _generations(device_id)
    interface_execution = interface_generation.document["interface_config"]["_execution"]

    vlan_response = await _put_vlans(adapter_client, device_id, [10], seq=6595)
    assert vlan_response.status_code == 200, vlan_response.text
    interface_generation, vlan_generation = await _generations(device_id)
    assert vlan_generation.job_id == interface_generation.job_id
    assert vlan_generation.document["interface_config"]["_execution"] == interface_execution

    async with session() as db:
        assert await generation_execution_sections(db, vlan_generation.job_id) == frozenset(
            {"interface_config", "vlan"}
        )

    client, _recorder = recorded_client("coalesced-interface-plan")
    assert await run_head(device_id, client) == vlan_generation.job_id
    assert (await job_row(vlan_generation.job_id)).status is JobStatus.succeeded


async def test_failed_svi_document_send_with_no_stamp_rows_fails_generation(adapter_client):
    from nso_adapter.store.models import GenerationStatus, JobStatus
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(nso_device_name="svi-failed-empty-stamp", netbox_device_id=9993)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_svis(adapter_client, device_id, [100], seq=6601, query="?store_only=true")).status_code == 200
    response = await _apply(adapter_client, device_id, {"svi": 6601})
    assert response.status_code == 202, response.text

    async def successor():
        result = await _put_svis(adapter_client, device_id, [200], seq=6602, query="?store_only=true")
        assert result.status_code == 200

    client, _recorder = recorded_client("svi-failed-empty-stamp", on_sync_from=successor)
    _reject_restconf_patches(client, "svi commit rejected")
    job_id = await run_head(device_id, client)
    assert job_id is not None

    job = await job_row(job_id)
    assert job.status is JobStatus.failed
    assert job.result["svi_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
    assert (await _generations(device_id))[0].status is GenerationStatus.failed
    assert (await _stream(device_id, "svi")).applied_revision == 0


async def test_failed_vlan_document_send_with_no_stamp_rows_fails_generation(adapter_client):
    from nso_adapter.store.models import GenerationStatus, JobStatus
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(nso_device_name="vlan-failed-empty-stamp", netbox_device_id=9994)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=6701, query="?store_only=true")).status_code == 200
    response = await _apply(adapter_client, device_id, {"vlan": 6701})
    assert response.status_code == 202, response.text

    async def successor():
        result = await _put_vlans(adapter_client, device_id, [20], seq=6702, query="?store_only=true")
        assert result.status_code == 200

    client, _recorder = recorded_client("vlan-failed-empty-stamp", on_sync_from=successor)
    _reject_restconf_patches(client, "vlan commit rejected")
    job_id = await run_head(device_id, client)
    assert job_id is not None

    job = await job_row(job_id)
    assert job.status is JobStatus.failed
    assert job.result["vlan_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
    assert (await _generations(device_id))[0].status is GenerationStatus.failed
    assert (await _stream(device_id, "vlan")).applied_revision == 0


async def test_failed_static_route_document_send_without_a_live_stamp_reports_the_route_error(adapter_client):
    """The current send error belongs to the result even when no live row can store it."""
    from nso_adapter.store.models import JobStatus
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(nso_device_name="route-failed-empty-stamp", netbox_device_id=9995)
    await seed_settings(device_id, auto_apply=True)
    first = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=6901,
    )
    assert first.status_code == 200, first.text

    successor = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_B, route_id=1, generation=2)],
        seq=6902,
        query="?store_only=true",
    )
    assert successor.status_code == 200, successor.text

    client, _recorder = recorded_client("route-failed-empty-stamp")
    _reject_restconf_patches(client, "static route commit rejected")
    job_id = await run_head(device_id, client)
    assert job_id is not None

    job = await job_row(job_id)
    assert job.status is JobStatus.failed
    result = job.result["static_route_results"][0]
    assert result["outcome"] == "apply_failed"
    assert result["error"]["message"] == "NSO PATCH for static_route failed with status 400"


async def test_reader_compare_miss_without_a_live_stamp_reports_the_route_error(adapter_client):
    """A verification miss belongs to the result even when no live row can store it."""
    from nso_adapter.store.models import JobStatus
    from tests.core.test_generation_protocol import job_row, recorded_client, run_head

    device_id = await seed_device(nso_device_name="route-rc-empty-stamp", netbox_device_id=9993)
    await seed_settings(device_id, auto_apply=True)
    first = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=6903,
    )
    assert first.status_code == 200, first.text

    successor = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_B, route_id=1, generation=2)],
        seq=6904,
        query="?store_only=true",
    )
    assert successor.status_code == 200, successor.text

    # The commit reports success while the device view never gains the key (#26 class).
    client, _recorder = recorded_client(
        "route-rc-empty-stamp",
        device_state={"static-route": {"status": "ok", "route": []}},
    )
    job_id = await run_head(device_id, client)
    assert job_id is not None

    job = await job_row(job_id)
    assert job.status is JobStatus.failed
    result = job.result["static_route_results"][0]
    assert result["outcome"] == "apply_failed"
    assert result["error"]["code"] == "reader_compare_missing"


async def test_recorded_static_route_put_is_refused_if_verification_is_disabled_at_execution(
    adapter_client,
    monkeypatch,
):
    """A creation-time PUT decision cannot bypass the worker's live replace gate."""
    from nso_adapter.core.generation import attach_to_job, create_generation, note_write
    from nso_adapter.store.models import GenerationMode, GenerationStatus, Job, JobStatus, JobType
    from tests.core.test_generation_protocol import job_row, run_head
    from tests.core.test_static_route_put import seed_rows, wire
    from tests.core.test_static_route_removal import SrFake, sr_client

    device_id = await seed_device(nso_device_name="recorded-put-gate", netbox_device_id=9996)
    await seed_settings(device_id, auto_apply=True)
    await seed_rows(device_id, [{"triple": _B, "route_id": 1, "deployed_key": list(_A)}])
    async with session() as db:
        await note_write(db, device_id, "static_route")
        generation = await create_generation(
            db,
            device_id,
            streams=("static_route",),
            mode=GenerationMode.networked,
        )
        job = Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.queued, coalescible=True)
        db.add(job)
        await db.flush()
        assert await attach_to_job(db, generation, job)
        await db.commit()
        generation_id = generation.id
        job_id = job.id
        assert generation.document["static_route"]["_execution"]["apply"]["mode"] == "PUT"

    monkeypatch.setattr("nso_adapter.nso.apply.VERIFY_AFTER_APPLY", False)
    fake = SrFake("recorded-put-gate", service=[wire(_A)])
    client = sr_client(fake)
    assert await run_head(device_id, client) == job_id

    assert fake.writes == [], "the worker executed the recorded destructive PUT"
    client.sync_from.assert_not_awaited()
    failed_job = await job_row(job_id)
    assert failed_job.status is JobStatus.failed
    assert failed_job.error["code"] == "static_route_put_verify_disabled"
    assert (await _generations(device_id))[0].id == generation_id
    assert (await _generations(device_id))[0].status is GenerationStatus.failed


async def test_failed_networked_link_blocks_detach_successor(adapter_client):
    from nso_adapter.core.generation import job_admissible
    from nso_adapter.store.models import GenerationStatus

    device_id, _response, chain = await _mixed_case(adapter_client, suffix=3)
    networked, detach, apply = chain

    await _settle(networked.job_id, GenerationStatus.failed)
    async with session() as db:
        assert await job_admissible(db, detach.job_id, device_id) is False
        assert await job_admissible(db, apply.job_id, device_id) is False


async def test_store_only_repair_is_promoted_only_when_selected_by_action_apply(adapter_client):
    device_id = await seed_device(nso_device_name="apply-store-only", netbox_device_id=9960)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4001, query="?store_only=true")).status_code == 200
    assert (await _put_snmp(adapter_client, device_id, ["kept"], seq=4002, query="?store_only=true")).status_code == 200
    assert await _generations(device_id) == []

    response = await _apply(adapter_client, device_id, {"vlan": 4001})

    assert response.status_code == 202
    (generation,) = await _generations(device_id)
    assert generation.stream_revisions == {"vlan": 1}
    assert (await _stream(device_id, "vlan")).authorized_revision == 1
    assert (await _stream(device_id, "snmp")).authorized_revision == 0


async def test_action_apply_does_not_promote_a_later_unselected_push(adapter_client):
    device_id = await seed_device(nso_device_name="apply-selection-boundary", netbox_device_id=9961)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4101, query="?store_only=true")).status_code == 200
    assert (await _put_vlans(adapter_client, device_id, [20], seq=4102, query="?store_only=true")).status_code == 200

    response = await _apply(adapter_client, device_id, {"vlan": 4101})

    assert response.status_code == 200
    assert response.json() == {
        "device_id": device_id,
        "outcome": "no_op",
        "selected": {"vlan": 4101},
        "skipped": {"vlan": "superseded"},
        "skipped_detail": None,
        "generations": [],
    }
    assert await _generations(device_id) == []
    stream = await _stream(device_id, "vlan")
    assert (stream.desired_revision, stream.authorized_revision) == (2, 0)


async def test_action_apply_rolls_back_promotion_when_generation_flush_fails(adapter_client):
    from nso_adapter.core.generation import create_action_apply, digest_document
    from nso_adapter.store.apply_attempt_store import begin_apply_attempt
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode, GenerationStatus

    device_id = await seed_device(nso_device_name="apply-atomic-rollback", netbox_device_id=9962)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4201, query="?store_only=true")).status_code == 200

    attempt_id = uuid4()
    async with session() as db:
        assert await begin_apply_attempt(db, attempt_id, device_id, {"vlan": 4201}) is None
        db.add(
            DeploymentGeneration(
                device_id=device_id,
                seq=1,
                mode=GenerationMode.networked,
                status=GenerationStatus.settled,
                document={},
                digest=digest_document(GenerationMode.networked, {}, {}),
                allowed_removal_keys={},
                source_push_seq={},
                stream_revisions={},
            )
        )
        await db.commit()

    async with session() as db:
        with pytest.raises(IntegrityError) as exc_info:
            await create_action_apply(db, device_id, {"vlan": 4201}, attempt_id)
        assert "uq_generation_seq_per_device" in str(exc_info.value)
        await db.rollback()

    assert (await _stream(device_id, "vlan")).authorized_revision == 0
    assert len(await _generations(device_id)) == 1


async def test_action_apply_endpoint_returns_selected_generation_contract(adapter_client):
    device_id = await seed_device(nso_device_name="apply-contract", netbox_device_id=9963)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4301, query="?store_only=true")).status_code == 200

    response = await _apply(adapter_client, device_id, {"vlan": 4301})

    assert response.status_code == 202
    body = response.json()
    assert body["device_id"] == device_id
    assert body["outcome"] == "promoted"
    assert body["selected"] == {"vlan": 4301}
    assert body["skipped"] == {}
    assert body["skipped_detail"] is None
    assert len(body["generations"]) == 1
    link = body["generations"][0]
    assert body["job_id"] == link["job_id"]
    assert link["mode"] == "networked"
    assert link["source_push_seq"] == {"vlan": 4301}
    assert link["stream_revisions"] == {"vlan": 1}
    assert len(link["digest"]) == 64
    assert link["job_id"] is not None
    assert (await _generations(device_id))[0].digest == link["digest"]


async def test_action_apply_endpoint_reports_the_queued_incumbent(adapter_client):
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="apply-queued-conflict", netbox_device_id=None)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4351, query="?store_only=true")).status_code == 200
    async with session() as db:
        incumbent = Job(device_id=device_id, job_type=JobType.apply, status=JobStatus.queued, coalescible=True)
        db.add(incumbent)
        await db.commit()
        incumbent_id = incumbent.id

    response = await _apply(adapter_client, device_id, {"vlan": 4351})

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "conflict",
            "message": "A job is already queued or running for this device",
            "detail": {"job_id": incumbent_id},
        }
    }


async def test_action_apply_wraps_a_missing_head_job_in_the_error_contract(adapter_client, monkeypatch):
    from nso_adapter.core import generation as generation_module
    from nso_adapter.store.models import GenerationMode

    device_id = await seed_device(nso_device_name="apply-missing-head-job", netbox_device_id=None)

    async def create_missing_head(*_args):
        return SimpleNamespace(
            generations=[
                SimpleNamespace(
                    id=81,
                    seq=4,
                    job_id=None,
                    mode=GenerationMode.networked,
                    source_push_seq={"vlan": 4301},
                    stream_revisions={"vlan": 1},
                    digest="a" * 64,
                )
            ],
            skipped={},
            skipped_detail={},
        )

    monkeypatch.setattr(generation_module, "create_action_apply", create_missing_head)

    response = await _apply(adapter_client, device_id, {"vlan": 4301})

    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "internal",
            "message": "The promoted generation chain has no executable head job",
            "detail": {},
        }
    }


async def test_action_apply_rejects_a_sequence_above_the_receipt_domain(adapter_client):
    device_id = await seed_device(nso_device_name="apply-sequence-bound", netbox_device_id=None)

    response = await _apply(adapter_client, device_id, {"vlan": 2**63})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_single_mode_selection_creates_one_generation_without_cohort(adapter_client):
    from nso_adapter.store.models import GenerationMode

    device_id = await seed_device(nso_device_name="apply-single-mode", netbox_device_id=9964)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4401, query="?store_only=true")).status_code == 200

    response = await _apply(adapter_client, device_id, {"vlan": 4401})

    assert response.status_code == 202
    (generation,) = await _generations(device_id)
    assert generation.mode is GenerationMode.networked
    assert generation.settlement_cohort is None
    assert len(response.json()["generations"]) == 1


async def test_apply_accumulates_delete_provenance_across_store_only_receipts(adapter_client):
    """A later edit must not hide an earlier marked deletion."""
    from nso_adapter.core.generation import _fragment_deletions
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.core.receipt import latest_receipt
    from nso_adapter.store.models import DeviceProjectionStream, GenerationStatus

    device_id = await seed_device(nso_device_name="apply-provenance", netbox_device_id=9965)
    await seed_settings(device_id, auto_apply=True)
    baseline_seq = 4501
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1), route_entry(_B, route_id=2, generation=1)],
        seq=baseline_seq,
    )
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)

    deleted_seq = 4502
    deleted = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=deleted_seq,
        query="?store_only=true",
        deleted=[deleted_route(2, [_B])],
    )
    assert deleted.status_code == 200, deleted.text
    edited_seq = 4503
    edited = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=2, metric=30)],
        seq=edited_seq,
        query="?store_only=true",
    )
    assert edited.status_code == 200, edited.text

    async with session() as db:
        projection = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "static_route",
            )
        )
        receipt = await latest_receipt(db, device_id, "static_route")
        desired = await snapshot_stream(db, device_id, "static_route")
        networked, detached = _fragment_deletions(projection.authorized_document, desired, receipt)

    assert detached == {}
    assert [row["route_id"] for row in networked["static_route_intent"]] == [2]
    assert {
        "table": "static_route_intent",
        "route_id": 2,
        "key": list(_B),
        "marking": "delete_origin",
    } in receipt.response["_promotion_deletions"]


async def test_auto_apply_refuses_to_discard_carried_deletion_provenance(adapter_client):
    """A direct promotion cannot strand an earlier store-only deletion."""
    from nso_adapter.core.receipt import latest_receipt
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="apply-carried-provenance", netbox_device_id=9979)
    await seed_settings(device_id, auto_apply=True)
    baseline = await _put_vlans(adapter_client, device_id, [10, 20], seq=5901)
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)

    stored = await _put_vlans(adapter_client, device_id, [10], seq=5902, query="?store_only=true")
    assert stored.status_code == 200, stored.text

    promoted = await _put_vlans(adapter_client, device_id, [10, 30], seq=5903)

    assert promoted.status_code == 409
    assert promoted.json() == {
        "error": {
            "code": "apply_unexecutable",
            "message": (
                "Push cannot promote outstanding deletion provenance for vlan. "
                "Apply the stored receipt when vlan is document-executed, then retry this push"
            ),
            "detail": {"streams": {"vlan": "outstanding_deletion_provenance"}},
        }
    }
    assert len(await _generations(device_id)) == 1
    assert len(await _jobs(device_id)) == 1
    stream = await _stream(device_id, "vlan")
    assert (stream.desired_revision, stream.authorized_revision, stream.applied_revision) == (2, 1, 1)
    async with session() as db:
        receipt = await latest_receipt(db, device_id, "vlan")
        assert receipt.push_seq == 5902
        (deletion,) = receipt.response["_promotion_deletions"]
        assert deletion["table"] == "vlan_intent"
        assert isinstance(deletion["id"], int)
        assert deletion["marking"] == "detach"


async def test_apply_does_not_reuse_provenance_consumed_by_an_immediate_promotion(adapter_client):
    """Delivered provenance cannot mark a later deletion of the same route."""
    from nso_adapter.core.receipt import latest_receipt
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="apply-consumed-provenance", netbox_device_id=9973)
    await seed_settings(device_id, auto_apply=True)
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1), route_entry(_B, route_id=2, generation=1)],
        seq=5301,
    )
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[-1].job_id, GenerationStatus.settled)

    marked_delete = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=2)],
        seq=5302,
        query="?delete_origin=true",
        deleted=[deleted_route(2, [_B])],
    )
    assert marked_delete.status_code == 200, marked_delete.text
    for generation in (await _generations(device_id))[1:]:
        await _settle(generation.job_id, GenerationStatus.settled)

    restored = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=3), route_entry(_B, route_id=2, generation=2)],
        seq=5303,
    )
    assert restored.status_code == 200, restored.text
    await _settle((await _generations(device_id))[-1].job_id, GenerationStatus.settled)

    detached_delete = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=4)],
        seq=5304,
        query="?store_only=true",
    )
    assert detached_delete.status_code == 200, detached_delete.text

    async with session() as db:
        receipt = await latest_receipt(db, device_id, "static_route")
        assert receipt.response["_promotion_deletions"] == [
            {"table": "static_route_intent", "route_id": 2, "key": list(_B), "marking": "detach"}
        ]


async def test_restored_row_retires_accumulated_deletion_provenance(adapter_client):
    """A restored route takes its next deletion's current marking."""
    from nso_adapter.core.receipt import latest_receipt
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="apply-restored-provenance", netbox_device_id=9977)
    await seed_settings(device_id, auto_apply=True)
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1), route_entry(_B, route_id=2, generation=1)],
        seq=5701,
    )
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)

    detached = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=5702,
        query="?store_only=true",
    )
    assert detached.status_code == 200, detached.text
    restored = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1), route_entry(_B, route_id=2, generation=1)],
        seq=5703,
        query="?store_only=true",
    )
    assert restored.status_code == 200, restored.text
    deleted = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=5704,
        query="?store_only=true&delete_origin=true",
        deleted=[deleted_route(2, [_B])],
    )
    assert deleted.status_code == 200, deleted.text

    async with session() as db:
        receipt = await latest_receipt(db, device_id, "static_route")
        assert receipt.response["_promotion_deletions"] == [
            {"table": "static_route_intent", "route_id": 2, "key": list(_B), "marking": "delete_origin"}
        ]


async def test_deletion_retirement_uses_durable_identity_for_internal_row_ids():
    """An in-place key edit is not restoration, but a rebuilt logical row is."""
    from nso_adapter.core.receipt import _restored_deletion_identities

    record = ("static_route_intent", "id", 1)
    previous = {
        "_promotion_deletions": [
            {"table": "static_route_intent", "id": 1, "marking": "detach"},
        ]
    }
    authorized = {
        "static_route_intent": [
            {"id": 1, "vrf": _A[0], "prefix": _A[1], "next_hop": _A[2]},
        ]
    }
    identity_edit = {
        "static_route_intent": [
            {"id": 1, "vrf": _B[0], "prefix": _B[1], "next_hop": _B[2]},
        ]
    }
    rebuilt_restoration = {
        "static_route_intent": [
            {"id": 2, "vrf": _A[0], "prefix": _A[1], "next_hop": _A[2]},
        ]
    }

    assert record not in _restored_deletion_identities(previous, authorized, identity_edit)
    assert record in _restored_deletion_identities(previous, authorized, rebuilt_restoration)


async def test_apply_routes_a_store_only_scalar_clear_through_replacement(adapter_client):
    """A clear cannot settle on the merge-writer path."""
    from nso_adapter.store.models import GenerationStatus, JobType

    device_id = await seed_device(nso_device_name="apply-clear", netbox_device_id=9966)
    await seed_settings(device_id, auto_apply=False)
    initial_seq = 4601
    initial = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent?store_only=true",
        json={"vlans": [{"vlan_id": 10, "name": "managed"}]},
        headers=AUTH | {"X-Push-Seq": str(initial_seq)},
    )
    assert initial.status_code == 200, initial.text
    assert (await _apply(adapter_client, device_id, {"vlan": initial_seq})).status_code == 202
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)

    clear_seq = 4602
    cleared = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent?store_only=true",
        json={"vlans": [{"vlan_id": 10, "name": ""}]},
        headers=AUTH | {"X-Push-Seq": str(clear_seq)},
    )
    assert cleared.status_code == 200, cleared.text

    response = await _apply(adapter_client, device_id, {"vlan": clear_seq})

    assert response.status_code == 202, response.text
    replacement = (await _jobs(device_id))[-1]
    assert replacement.job_type is JobType.removal
    assert replacement.context == {"scope": "vlan"}


async def test_apply_delta_ignores_apply_bookkeeping_changes():
    """Apply-owned result fields are not operator replacement work."""
    from nso_adapter.core.generation import _content_losing_rows, _has_positive_delta

    old = {
        "static_route_intent": [
            {
                "id": 1,
                "vrf": "",
                "prefix": "198.18.0.0/24",
                "next_hop": "192.0.2.1",
                "last_apply_at": "2026-08-12T12:00:00+00:00",
                "last_apply_error": {},
                "pending_clear": {"authorized": ["metric"]},
                "deployed_key": None,
            }
        ]
    }
    desired = {
        "static_route_intent": [
            {
                "id": 1,
                "vrf": "",
                "prefix": "198.18.0.0/24",
                "next_hop": "192.0.2.1",
                "last_apply_at": None,
                "last_apply_error": None,
                "pending_clear": None,
                "deployed_key": ["", "198.18.0.0/24", "192.0.2.1"],
            }
        ]
    }

    assert _content_losing_rows(old, desired) == {}
    assert _has_positive_delta(old, desired) is False


async def test_apply_delta_ignores_correlation_only_changes():
    """``static_route_entry`` renders neither column, so repairing lineage is not device work."""
    from nso_adapter.core.generation import _has_positive_delta

    row = {
        "id": 1,
        "vrf": "",
        "prefix": "198.18.0.0/24",
        "next_hop": "192.0.2.1",
        "route_id": None,
        "intent_generation": None,
    }
    correlated = {**row, "route_id": 77, "intent_generation": 4}

    assert _has_positive_delta({"static_route_intent": [row]}, {"static_route_intent": [correlated]}) is False
    # A real payload field still reads as a delta.
    assert (
        _has_positive_delta({"static_route_intent": [row]}, {"static_route_intent": [{**correlated, "metric": 10}]})
        is True
    )


async def test_apply_settlement_fields_do_not_block_a_later_detach(adapter_client):
    """Settled clear bookkeeping is not successor replacement work."""
    from nso_adapter.store.models import DeviceSettings, GenerationMode, GenerationStatus, JobStatus, JobType
    from tests.core.test_static_route_put import wire
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client

    device_id = await seed_device(nso_device_name="apply-settled-clear-detach", netbox_device_id=9978)
    await seed_settings(device_id, auto_apply=True)
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1, metric=10), route_entry(_B, route_id=2, generation=1)],
        seq=5801,
    )
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)

    cleared = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1), route_entry(_B, route_id=2, generation=1)],
        seq=5802,
    )
    assert cleared.status_code == 200, cleared.text
    clear_generations = (await _generations(device_id))[1:]
    jobs = {job.id: job for job in await _jobs(device_id)}
    clear_generation = next(
        generation for generation in clear_generations if jobs[generation.job_id].job_type is JobType.removal
    )
    fake = SrFake("apply-settled-clear-detach", service=[wire(_A, metric=10), wire(_B)])
    clear_job = await run_removal_job(device_id, clear_generation.job_id, sr_client(fake))
    assert clear_job.status is JobStatus.succeeded, clear_job.error
    for generation in clear_generations:
        if generation.id != clear_generation.id:
            await _settle(generation.job_id, GenerationStatus.settled)

    from nso_adapter.store.models import Job, StaticRouteIntent

    async with session() as db:
        settled_row = await db.scalar(
            sa.select(StaticRouteIntent).where(
                StaticRouteIntent.device_id == device_id,
                StaticRouteIntent.route_id == 1,
            )
        )
        assert settled_row.pending_clear is None
        await db.execute(
            sa.update(Job)
            .where(
                Job.device_id == device_id,
                Job.job_type == JobType.sync,
                Job.status == JobStatus.queued,
            )
            .values(status=JobStatus.succeeded)
        )
        await db.execute(
            sa.update(DeviceSettings).where(DeviceSettings.device_id == device_id).values(auto_apply=False)
        )
        await db.commit()

    detached = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=5803,
    )
    assert detached.status_code == 200, detached.text

    generation = (await _generations(device_id))[-1]
    job = {job.id: job for job in await _jobs(device_id)}[generation.job_id]
    assert generation.mode is GenerationMode.detach
    assert job.job_type is JobType.removal
    assert job.context == {"scope": "static_route", "removed": {"route": [list(_B)]}, "detach": True}


async def test_clear_removal_then_apply_reports_the_route_in_sync(adapter_client):
    """A predecessor's proof fulfills the clear carried by the immutable apply document."""
    from nso_adapter.store.models import GenerationStatus, JobStatus, JobType, StaticRouteIntent
    from tests.core.test_generation_protocol import job_row, run_head
    from tests.core.test_static_route_put import wire
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client

    device_id = await seed_device(nso_device_name="apply-clear-chain", netbox_device_id=9984)
    await seed_settings(device_id, auto_apply=True)
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1, metric=10)],
        seq=6801,
    )
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)

    cleared = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=2)],
        seq=6802,
    )
    assert cleared.status_code == 200, cleared.text
    removal, apply = (await _generations(device_id))[1:]
    jobs = {job.id: job for job in await _jobs(device_id)}
    assert jobs[removal.job_id].job_type is JobType.removal
    assert jobs[apply.job_id].job_type is JobType.apply

    fake = SrFake("apply-clear-chain", service=[wire(_A, metric=10)])
    removal_job = await run_removal_job(device_id, removal.job_id, sr_client(fake))
    assert removal_job.status is JobStatus.succeeded, removal_job.error
    async with session() as db:
        row = await db.scalar(sa.select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))
        assert row.pending_clear is None

    assert await run_head(device_id, sr_client(fake)) == apply.job_id
    apply_job = await job_row(apply.job_id)
    assert apply_job.status is JobStatus.succeeded, apply_job.error
    assert apply_job.result["static_route_results"][0]["outcome"] == "in_sync"


async def test_action_apply_accepts_every_selected_document_stream(adapter_client):
    """The final document section makes a mixed VLAN/static-route selection executable."""
    device_id = await seed_device(nso_device_name="apply-all-documents", netbox_device_id=9967)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4701, query="?store_only=true")).status_code == 200
    routes = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=4702,
        query="?store_only=true",
    )
    assert routes.status_code == 200, routes.text

    response = await _apply(adapter_client, device_id, {"vlan": 4701, "static_route": 4702})

    assert response.status_code == 202, response.text
    (generation,) = await _generations(device_id)
    assert generation.stream_revisions == {"static_route": 1, "vlan": 1}
    assert len(await _jobs(device_id)) == 1
    assert (await _stream(device_id, "vlan")).authorized_revision == 1
    assert (await _stream(device_id, "static_route")).authorized_revision == 1


async def test_action_apply_accepts_static_route_once_it_executes_from_the_document(adapter_client):
    """The manual Apply boundary includes static-route after its plan is recorded."""
    device_id = await seed_device(nso_device_name="apply-static-document", netbox_device_id=9980)
    await seed_settings(device_id, auto_apply=False)
    stored = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=6001,
        query="?store_only=true",
    )
    assert stored.status_code == 200, stored.text

    response = await _apply(adapter_client, device_id, {"static_route": 6001})

    assert response.status_code == 202, response.text
    (generation,) = await _generations(device_id)
    assert generation.stream_revisions == {"static_route": 1}
    assert len(await _jobs(device_id)) == 1
    assert (await _stream(device_id, "static_route")).authorized_revision == 1


async def test_action_apply_settles_name_only_static_route_without_device_work(adapter_client):
    """A store-only name edit advances settlement without another generation or job."""
    from nso_adapter.store.models import GenerationStatus, StaticRouteIntent

    device_id = await seed_device(nso_device_name="apply-static-name-only", netbox_device_id=9983)
    await seed_settings(device_id, auto_apply=False)
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1, name="old name")],
        seq=6011,
        query="?store_only=true",
    )
    assert baseline.status_code == 200, baseline.text
    assert (await _apply(adapter_client, device_id, {"static_route": 6011})).status_code == 202
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)

    renamed = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=2, name="new name")],
        seq=6012,
        query="?store_only=true",
    )
    assert renamed.status_code == 200, renamed.text
    async with session() as db:
        assert (
            await db.scalar(sa.select(StaticRouteIntent.name).where(StaticRouteIntent.device_id == device_id))
            == "new name"
        )

    response = await _apply(adapter_client, device_id, {"static_route": 6012})

    assert response.status_code == 200, response.text
    assert response.json() == {
        "device_id": device_id,
        "outcome": "no_op",
        "selected": {"static_route": 6012},
        "skipped": {"static_route": "already_applied"},
        "skipped_detail": None,
        "generations": [],
    }
    assert len(await _generations(device_id)) == 1
    assert len(await _jobs(device_id)) == 1
    stream = await _stream(device_id, "static_route")
    assert (stream.desired_revision, stream.authorized_revision, stream.applied_revision) == (2, 2, 2)
    authorized = stream.authorized_document["static_route_intent"][0]
    assert (authorized["name"], authorized["route_id"], authorized["intent_generation"]) == ("new name", 1, 2)

    repeated = await _apply(adapter_client, device_id, {"static_route": 6012})
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()["skipped"] == {"static_route": "already_applied"}
    assert len(await _generations(device_id)) == 1
    assert len(await _jobs(device_id)) == 1


async def test_action_apply_keeps_static_route_metric_edits_generation_backed(adapter_client):
    """A rendered route edit still requires device work after a settled baseline."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="apply-static-metric", netbox_device_id=9984)
    await seed_settings(device_id, auto_apply=False)
    assert (
        await _put_routes(
            adapter_client,
            device_id,
            [route_entry(_A, route_id=1, generation=1, metric=10)],
            seq=6021,
            query="?store_only=true",
        )
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"static_route": 6021})).status_code == 202
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)
    assert (
        await _put_routes(
            adapter_client,
            device_id,
            [route_entry(_A, route_id=1, generation=2, metric=20)],
            seq=6022,
            query="?store_only=true",
        )
    ).status_code == 200

    response = await _apply(adapter_client, device_id, {"static_route": 6022})

    assert response.status_code == 202, response.text
    assert response.json()["outcome"] == "promoted"
    generations = await _generations(device_id)
    assert len(generations) == 2
    assert generations[1].document["static_route"]["static_route_intent"][0]["metric"] == 20
    assert len(await _jobs(device_id)) == 2
    stream = await _stream(device_id, "static_route")
    assert (stream.desired_revision, stream.authorized_revision, stream.applied_revision) == (2, 2, 1)


@pytest.mark.parametrize("blocked_status", ["failed", "outcome_unknown"])
async def test_action_apply_does_not_settle_name_only_route_across_blocked_predecessor(
    adapter_client,
    blocked_status,
):
    """Renderer equivalence cannot advance settlement past an unresolved generation."""
    from nso_adapter.store.models import GenerationStatus

    suffix = 0 if blocked_status == "failed" else 1
    device_id = await seed_device(
        nso_device_name=f"apply-static-name-{blocked_status}",
        netbox_device_id=9985 + suffix,
    )
    await seed_settings(device_id, auto_apply=False)
    assert (
        await _put_routes(
            adapter_client,
            device_id,
            [route_entry(_A, route_id=1, generation=1, name="old name")],
            seq=6031 + suffix * 10,
            query="?store_only=true",
        )
    ).status_code == 200
    baseline_seq = 6031 + suffix * 10
    assert (await _apply(adapter_client, device_id, {"static_route": baseline_seq})).status_code == 202
    baseline = (await _generations(device_id))[0]
    await _settle(baseline.job_id, GenerationStatus(blocked_status))
    changed_seq = baseline_seq + 1
    assert (
        await _put_routes(
            adapter_client,
            device_id,
            [route_entry(_A, route_id=1, generation=2, name="new name")],
            seq=changed_seq,
            query="?store_only=true",
        )
    ).status_code == 200

    response = await _apply(adapter_client, device_id, {"static_route": changed_seq})

    assert response.status_code == 202, response.text
    assert response.json()["outcome"] == "promoted"
    generations = await _generations(device_id)
    assert [generation.status for generation in generations] == [
        GenerationStatus(blocked_status),
        GenerationStatus.pending,
    ]
    assert len(await _jobs(device_id)) == 2
    stream = await _stream(device_id, "static_route")
    assert (stream.desired_revision, stream.authorized_revision, stream.applied_revision) == (2, 2, 0)


async def test_ordinary_auto_apply_name_only_route_edit_stays_generation_backed(adapter_client):
    """The local settlement rule belongs only to the explicit Apply action."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="auto-static-name-only", netbox_device_id=9987)
    await seed_settings(device_id, auto_apply=True)
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1, name="old name")],
        seq=6051,
    )
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)

    renamed = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=2, name="new name")],
        seq=6052,
    )

    assert renamed.status_code == 200, renamed.text
    assert len(await _generations(device_id)) == 2
    assert len(await _jobs(device_id)) == 2
    stream = await _stream(device_id, "static_route")
    assert (stream.desired_revision, stream.authorized_revision, stream.applied_revision) == (2, 2, 1)


async def test_action_apply_reports_every_skipped_selection_reason(adapter_client):
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="apply-skipped", netbox_device_id=9971)
    await seed_settings(device_id, auto_apply=False)
    no_receipt = await _apply(adapter_client, device_id, {"vlan": 5100})
    assert no_receipt.status_code == 200, no_receipt.text
    assert no_receipt.json() == {
        "device_id": device_id,
        "outcome": "no_op",
        "selected": {"vlan": 5100},
        "skipped": {"vlan": "no_receipt"},
        "skipped_detail": None,
        "generations": [],
    }
    assert (await _put_vlans(adapter_client, device_id, [10], seq=5101, query="?store_only=true")).status_code == 200
    assert (await _apply(adapter_client, device_id, {"vlan": 5101})).status_code == 202
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)

    response = await _apply(adapter_client, device_id, {"vlan": 5101})

    assert response.status_code == 200, response.text
    assert response.json() == {
        "device_id": device_id,
        "outcome": "no_op",
        "selected": {"vlan": 5101},
        "skipped": {"vlan": "already_applied"},
        "skipped_detail": None,
        "generations": [],
    }


async def test_action_apply_distinguishes_a_projection_sequence_mismatch(adapter_client):
    from nso_adapter.store.models import DeviceProjectionStream

    device_id = await seed_device(nso_device_name="apply-revision-mismatch", netbox_device_id=None)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=5151, query="?store_only=true")).status_code == 200
    async with session() as db:
        projection = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "vlan",
            )
        )
        projection.source_push_seq = 5150
        await db.commit()

    response = await _apply(adapter_client, device_id, {"vlan": 5151})

    assert response.status_code == 200
    assert response.json()["skipped"] == {"vlan": "revision_mismatch"}
    assert await _generations(device_id) == []


async def test_action_apply_refuses_detach_combined_with_replacement_work(adapter_client):
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="apply-unexecutable", netbox_device_id=9972)
    await seed_settings(device_id, auto_apply=False)
    first = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent?store_only=true",
        json={"vlans": [{"vlan_id": 10, "name": "managed"}, {"vlan_id": 20, "name": "removed"}]},
        headers=AUTH | {"X-Push-Seq": "5201"},
    )
    assert first.status_code == 200, first.text
    assert (await _apply(adapter_client, device_id, {"vlan": 5201})).status_code == 202
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)
    second = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent?store_only=true",
        json={"vlans": [{"vlan_id": 10, "name": ""}]},
        headers=AUTH | {"X-Push-Seq": "5202"},
    )
    assert second.status_code == 200, second.text

    response = await _apply(adapter_client, device_id, {"vlan": 5202})

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "apply_unexecutable",
            "message": "Selected stream(s) cannot be applied faithfully: vlan",
            "detail": {"streams": {"vlan": "mixed_detach_replacement"}},
        }
    }
    assert len(await _generations(device_id)) == 1
    assert (await _stream(device_id, "vlan")).authorized_revision == 1


async def test_apply_non_static_removal_carries_guarded_keys(adapter_client):
    """Composed non-static removals retain collateral-guard authority."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="apply-guard-keys", netbox_device_id=9968)
    await seed_settings(device_id, auto_apply=False)
    assert (
        await _put_vlans(adapter_client, device_id, [10, 20], seq=4801, query="?store_only=true")
    ).status_code == 200
    assert (await _apply(adapter_client, device_id, {"vlan": 4801})).status_code == 202
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4802, query="?store_only=true")).status_code == 200

    response = await _apply(adapter_client, device_id, {"vlan": 4802})

    assert response.status_code == 202, response.text
    assert (await _jobs(device_id))[-1].context == {
        "scope": "vlan",
        "removed": {"vlan": [20]},
        "detach": True,
    }


async def test_interface_promotion_refuses_an_unresolved_interface_id(adapter_client):
    from nso_adapter.core.removal import promotion_removal_context

    device_id = await seed_device(nso_device_name="apply-unresolved-interface", netbox_device_id=None)
    async with session() as db:
        with pytest.raises(ValueError, match="999999"):
            await promotion_removal_context(
                db,
                device_id,
                "interface_config",
                {
                    "interface_ip_intent": [
                        {"interface_id": 999999, "address": "198.18.9.1/32", "vrf": ""},
                    ]
                },
            )


async def test_action_apply_accepts_ip_stream_that_executes_from_interface_document(adapter_client):
    """The IP lane is executable because its containing interface section moved to the document."""
    device_id = await seed_device(nso_device_name="apply-interface-list", netbox_device_id=9969)
    await seed_settings(device_id, auto_apply=False)
    first = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ip-intent?store_only=true",
        json={
            "addresses": [
                {"interface": "Gi0/1", "address": "198.18.10.1/30", "family": "ipv4"},
                {"interface": "Gi0/2", "address": "198.18.10.5/30", "family": "ipv4"},
            ]
        },
        headers=AUTH | {"X-Push-Seq": "4901"},
    )
    assert first.status_code == 200, first.text

    response = await _apply(adapter_client, device_id, {"ip": 4901})

    assert response.status_code == 202, response.text
    (generation,) = await _generations(device_id)
    assert generation.stream_revisions == {"ip": 1}
    assert [row["address"] for row in generation.document["interface_config"]["interface_ip_intent"]] == [
        "198.18.10.1/30",
        "198.18.10.5/30",
    ]
    assert len(await _jobs(device_id)) == 1
    assert (await _stream(device_id, "ip")).authorized_revision == 1


async def test_apply_static_route_removal_keeps_apply_bookkeeping_job(adapter_client):
    """Removal uses SrRemoval, and additions retain the apply CAS and results path."""
    from nso_adapter.store.models import GenerationStatus, JobType

    device_id = await seed_device(nso_device_name="apply-static-bookkeeping", netbox_device_id=9970)
    await seed_settings(device_id, auto_apply=True)
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1), route_entry(_B, route_id=2, generation=1)],
        seq=5001,
    )
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)
    prepared = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=2, metric=40), route_entry(_C, route_id=3, generation=1)],
        seq=5002,
        deleted=[deleted_route(2, [_B])],
    )
    assert prepared.status_code == 200, prepared.text

    chain = (await _generations(device_id))[1:]
    assert len(chain) == 2
    jobs = {job.id: job for job in await _jobs(device_id)}
    removal, apply = chain
    assert jobs[removal.job_id].job_type is JobType.removal
    assert jobs[removal.job_id].context == {"scope": "static_route", "removed": {"route": [list(_B)]}}
    assert jobs[apply.job_id].job_type is JobType.apply
    assert removal.stream_revisions == {"static_route": 2}
    assert apply.stream_revisions == {"static_route": 2}
    assert removal.settlement_cohort is not None
    assert apply.settlement_cohort == removal.settlement_cohort


async def test_non_static_detach_mix_keeps_positive_delta_on_apply_link(adapter_client):
    """A VLAN replacement must detach 10 and still network 20."""
    from nso_adapter.store.models import GenerationMode, GenerationStatus, JobType

    device_id = await seed_device(nso_device_name="apply-vlan-detach-mix", netbox_device_id=9974)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=5401, query="?store_only=true")).status_code == 200
    assert (await _apply(adapter_client, device_id, {"vlan": 5401})).status_code == 202
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)
    assert (await _put_vlans(adapter_client, device_id, [20], seq=5402, query="?store_only=true")).status_code == 200

    response = await _apply(adapter_client, device_id, {"vlan": 5402})

    assert response.status_code == 202, response.text
    apply, detach = (await _generations(device_id))[1:]
    jobs = (await _jobs(device_id))[1:]
    assert [(generation.seq, generation.mode) for generation in (apply, detach)] == [
        (2, GenerationMode.networked),
        (3, GenerationMode.detach),
    ]
    assert [(job.id, job.job_type) for job in jobs] == [
        (apply.job_id, JobType.apply),
        (detach.job_id, JobType.removal),
    ]
    assert [link["job_id"] for link in response.json()["generations"]] == [apply.job_id, detach.job_id]
    assert detach.allowed_removal_keys == {"vlan": [10]}
    assert {row["vlan_id"] for row in apply.document["vlan"]["vlan_intent"]} == {10, 20}
    assert {row["vlan_id"] for row in detach.document["vlan"]["vlan_intent"]} == {20}
    assert detach.settlement_cohort is not None
    assert apply.settlement_cohort == detach.settlement_cohort


async def test_action_apply_accepts_bgp_once_its_graph_executes_from_the_document(adapter_client):
    """The manual Apply boundary includes BGP after its graph becomes document-executed."""
    device_id = await seed_device(nso_device_name="apply-bgp-rebuild", netbox_device_id=9975)
    await seed_settings(device_id, auto_apply=False)
    first = await _put_bgp(adapter_client, device_id, "64513", seq=5501, query="?store_only=true")
    assert first.status_code == 200, first.text

    response = await _apply(adapter_client, device_id, {"bgp": 5501})

    assert response.status_code == 202, response.text
    (generation,) = await _generations(device_id)
    assert generation.stream_revisions == {"bgp": 1}
    assert set(generation.document["bgp"]) == {
        "bgp_router_intent",
        "bgp_scope_intent",
        "bgp_af_intent",
        "bgp_peer_intent",
        "bgp_peer_af_intent",
        "redistribution_intent",
    }
    assert len(await _jobs(device_id)) == 1
    assert (await _stream(device_id, "bgp")).authorized_revision == 1


async def test_promoted_static_route_detach_fails_when_proof_is_inconclusive(adapter_client):
    """Job context is a proof carrier even after provenance consumption."""
    from nso_adapter.core.generation import (
        _compose_authorized_document,
        _enqueue_action_removal_links,
        _RemovalLink,
    )
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.store.apply_attempt_store import begin_apply_attempt, complete_apply_attempt
    from nso_adapter.store.models import DeviceProjectionStream, GenerationMode, GenerationStatus, JobStatus
    from tests.core.test_static_route_put import wire
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client

    device_id = await seed_device(nso_device_name="apply-static-proof-carrier", netbox_device_id=9976)
    await seed_settings(device_id, auto_apply=True)
    baseline = await _put_routes(adapter_client, device_id, [route_entry(_A, route_id=1, generation=1)], seq=5601)
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)
    removed = await _put_routes(adapter_client, device_id, [], seq=5602, query="?store_only=true")
    assert removed.status_code == 200, removed.text
    async with session() as db:
        projection = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "static_route",
            )
        )
        desired = await snapshot_stream(db, device_id, "static_route")
        document = await _compose_authorized_document(db, device_id, {"static_route": desired})
        attempt_id = uuid4()
        assert await begin_apply_attempt(db, attempt_id, device_id, {"static_route": 5602}) is None
        (generation,) = await _enqueue_action_removal_links(
            db,
            device_id,
            [
                _RemovalLink(
                    "static_route",
                    GenerationMode.detach,
                    {"static_route_intent": projection.authorized_document["static_route_intent"]},
                    {},
                )
            ],
            apply_attempt_id=attempt_id,
            cohort=None,
            intermediate_document=document,
            final_document=document,
            removal_authority={},
            frozen_fragments={},
        )
        await complete_apply_attempt(
            db,
            attempt_id,
            admission_state="admitted",
            http_status=202,
            response={"generations": [{"generation_id": generation.id}]},
        )
        await db.commit()

    fake = SrFake("apply-static-proof-carrier", service=[wire(_A)], dry_run_status=500)
    job = await run_removal_job(device_id, generation.job_id, sr_client(fake))

    assert job.status is JobStatus.failed
    assert job.error["code"] == "static_route_removal_unproven"
    assert (await _generations(device_id))[1].status is GenerationStatus.failed
    stream = await _stream(device_id, "static_route")
    assert (stream.authorized_revision, stream.applied_revision) == (2, 1)


async def test_consumed_static_route_tombstone_is_not_planned_as_an_intent_deletion(adapter_client):
    """Proof consumption is not a successor operator deletion."""
    from nso_adapter.core.generation import (
        _content_losing_rows,
        _fragment_deletions,
        _has_positive_delta,
        _plan_action_links,
        _Promotion,
    )
    from nso_adapter.core.projection import snapshot_stream
    from nso_adapter.core.receipt import latest_receipt
    from nso_adapter.store.models import (
        DeviceProjectionStream,
        DeviceSettings,
        GenerationStatus,
        JobStatus,
        StaticRouteTombstone,
    )
    from tests.core.test_static_route_put import wire
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client

    device_id = await seed_device(nso_device_name="apply-consumed-tombstone", netbox_device_id=9981)
    await seed_settings(device_id, auto_apply=True)
    baseline = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1, metric=10), route_entry(_B, route_id=2, generation=1)],
        seq=6101,
    )
    assert baseline.status_code == 200, baseline.text
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)
    async with session() as db:
        await db.execute(
            sa.update(DeviceSettings).where(DeviceSettings.device_id == device_id).values(auto_apply=False)
        )
        await db.commit()

    deleted = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1, metric=10)],
        seq=6102,
        query="?delete_origin=true",
        deleted=[deleted_route(2, [_B])],
    )
    assert deleted.status_code == 200, deleted.text
    deletion_generation = (await _generations(device_id))[-1]
    fake = SrFake("apply-consumed-tombstone", service=[wire(_A, metric=10), wire(_B)])
    deletion_job = await run_removal_job(device_id, deletion_generation.job_id, sr_client(fake))
    assert deletion_job.status is JobStatus.succeeded, deletion_job.error
    async with session() as db:
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(StaticRouteTombstone)
                .where(StaticRouteTombstone.device_id == device_id)
            )
            == 0
        )

    cleared = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=2)],
        seq=6103,
        query="?store_only=true",
    )
    assert cleared.status_code == 200, cleared.text

    async with session() as db:
        projection = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "static_route",
            )
        )
        receipt = await latest_receipt(db, device_id, "static_route")
        desired = await snapshot_stream(db, device_id, "static_route")
        assert projection.authorized_document["static_route_tombstone"]
        assert desired["static_route_tombstone"] == []
        networked, detached = _fragment_deletions(projection.authorized_document, desired, receipt)
        replacement = _content_losing_rows(projection.authorized_document, desired)
        provenance = (receipt.response or {}).get("_promotion_deletions")

    assert (networked, detached, provenance) == ({}, {}, None)
    assert set(replacement) == {"static_route_intent"}
    promotion = _Promotion(
        projection,
        receipt,
        desired,
        networked,
        detached,
        replacement,
        _has_positive_delta(projection.authorized_document, desired),
    )
    networked_links, detach_links, apply_streams, wire_equivalent_streams = _plan_action_links(
        {"static_route": promotion}
    )
    assert len(networked_links) == 1
    assert detach_links == []
    assert wire_equivalent_streams == set()
    # The push clears a metric and bumps the correlation columns; neither ADDS payload, so
    # the replacement link carries the whole desired state and no apply rides beside it.
    assert apply_streams == set()


async def test_action_apply_job_executes_only_the_selected_generation_document(adapter_client):
    """An unselected live row must not ride a document-executed Apply."""
    from nso_adapter.store.models import SnmpCommunityIntent
    from tests.core.test_generation_protocol import recorded_client, run_head

    device_id = await seed_device(nso_device_name="apply-document-only", netbox_device_id=9982)
    await seed_settings(device_id, auto_apply=False)
    assert (
        await _put_snmp(adapter_client, device_id, ["stored"], seq=6201, query="?store_only=true")
    ).status_code == 200
    assert (await _put_vlans(adapter_client, device_id, [10], seq=6202, query="?store_only=true")).status_code == 200

    response = await _apply(adapter_client, device_id, {"vlan": 6202})
    assert response.status_code == 202, response.text

    client, recorder = recorded_client("apply-document-only")
    assert await run_head(device_id, client) is not None

    assert recorder.vlan_ids() == [[10]]
    assert recorder.bodies("snmp-reconciler:snmp-config") == []
    snmp = await _stream(device_id, "snmp")
    vlan = await _stream(device_id, "vlan")
    assert (snmp.desired_revision, snmp.authorized_revision, snmp.applied_revision) == (1, 0, 0)
    assert (vlan.desired_revision, vlan.authorized_revision, vlan.applied_revision) == (1, 1, 1)
    async with session() as db:
        stored = await db.scalar(
            sa.select(SnmpCommunityIntent).where(
                SnmpCommunityIntent.device_id == device_id,
                SnmpCommunityIntent.label == "stored",
            )
        )
        assert stored.last_apply_at is None


async def test_action_apply_settlement_between_projection_and_generation_reads_is_skipped(
    adapter_client,
    rival_engine,
):
    """A concurrent settlement is already authorized. It must never cause a 500."""
    from nso_adapter.core.generation import settle_job_generations
    from nso_adapter.store.models import GenerationStatus, Job, JobStatus

    device_id = await seed_device(nso_device_name="apply-settlement-race", netbox_device_id=9983)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [20], seq=6301, query="?store_only=true")).status_code == 200
    assert (await _apply(adapter_client, device_id, {"vlan": 6301})).status_code == 202
    (generation,) = await _generations(device_id)

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with session() as receipt_blocker, rival() as settler:
        await receipt_blocker.execute(sa.text("LOCK TABLE intent_push_receipt IN ACCESS EXCLUSIVE MODE"))
        applying = asyncio.create_task(_apply(adapter_client, device_id, {"vlan": 6301}))

        # A DEADLINE, not an iteration count: how long the Apply task takes to reach its
        # blocked read is scheduling-dependent, so a fixed budget only measures how loaded
        # the runner is. A healthy run still breaks out on the first poll; a busy one gets
        # the room it needs instead of failing a correctness assertion for being slow.
        waiting = False
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            await settler.execute(sa.text("SELECT pg_stat_clear_snapshot()"))
            waiting = await settler.scalar(
                sa.text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_stat_activity "
                    "WHERE datname = current_database() "
                    "AND pid <> pg_backend_pid() "
                    "AND wait_event_type = 'Lock' "
                    "AND query LIKE '%intent_push_receipt%'"
                    ")"
                )
            )
            if waiting:
                break
            await asyncio.sleep(0.02)
        assert waiting, "the Apply did not reach its blocked receipt read"

        job = await settler.get(Job, generation.job_id)
        job.status = JobStatus.succeeded
        # Settlement must not acquire lock_projection. This transaction runs while Apply
        # owns that lock, so adding it there would reverse this test's lock order and deadlock.
        await settle_job_generations(settler, generation.job_id, outcome=GenerationStatus.settled)
        await settler.commit()
        await receipt_blocker.commit()
        # Still a deadlock guard (a lock-order inversion must fail, not hang), but the
        # budget only has to be shorter than "forever". It is not a latency assertion.
        response = await asyncio.wait_for(applying, timeout=30)

    assert response.status_code == 200, response.text
    assert response.json() == {
        "device_id": device_id,
        "outcome": "no_op",
        "selected": {"vlan": 6301},
        "skipped": {"vlan": "already_authorized"},
        "skipped_detail": None,
        "generations": [],
    }


async def test_settlement_stamps_only_generations_whose_terminalization_cas_lands(
    adapter_client,
    rival_engine,
):
    from nso_adapter.core.generation import settle_job_generations
    from nso_adapter.store.models import DeploymentGeneration, GenerationStatus, Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="apply-settlement-carrier-swap", netbox_device_id=9984)
    await seed_settings(device_id, auto_apply=True)
    assert (await _put_vlans(adapter_client, device_id, [30], seq=6311)).status_code == 200
    assert (await _put_snmp(adapter_client, device_id, ["selected"], seq=6312)).status_code == 200
    carried = await _generations(device_id)
    assert len(carried) == 2
    assert carried[0].job_id == carried[1].job_id
    carrier_job_id = carried[0].job_id
    landed_generation, moved_generation = carried
    landed_stream = next(iter(landed_generation.stream_revisions))
    moved_stream = next(iter(moved_generation.stream_revisions))

    async with session() as db:
        carrier = await db.get(Job, carrier_job_id)
        carrier.status = JobStatus.succeeded
        await db.commit()

    async def settle() -> None:
        async with session() as db:
            await settle_job_generations(db, carrier_job_id, outcome=GenerationStatus.settled)
            await db.commit()

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with rival() as mover:
        await mover.execute(sa.text("LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE"))
        replacement = Job(
            job_type=JobType.apply,
            status=JobStatus.running,
            coalescible=False,
            device_id=device_id,
        )
        mover.add(replacement)
        await mover.flush()
        await mover.execute(
            sa.update(DeploymentGeneration)
            .where(DeploymentGeneration.id == moved_generation.id)
            .values(job_id=replacement.id, status=GenerationStatus.running)
        )

        settling = asyncio.create_task(settle())
        waiting = False
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            await mover.execute(sa.text("SELECT pg_stat_clear_snapshot()"))
            waiting = await mover.scalar(
                sa.text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_stat_activity "
                    "WHERE datname = current_database() "
                    "AND pid <> pg_backend_pid() "
                    "AND wait_event_type = 'Lock' "
                    "AND position('jobs' IN lower(query)) > 0"
                    ")"
                )
            )
            if waiting:
                break
            await asyncio.sleep(0.02)
        assert waiting, "settlement did not read the carried generations before the job swap"
        await mover.commit()
        await asyncio.wait_for(settling, timeout=30)
        replacement_job_id = replacement.id

    async with session() as db:
        landed = await db.get(DeploymentGeneration, landed_generation.id)
        moved = await db.get(DeploymentGeneration, moved_generation.id)
    assert landed.status is GenerationStatus.settled
    assert landed.job_id == carrier_job_id
    assert moved.status is GenerationStatus.running
    assert moved.job_id == replacement_job_id
    assert (await _stream(device_id, landed_stream)).applied_revision == 1
    assert (await _stream(device_id, moved_stream)).applied_revision == 0


def test_every_apply_unexecutable_reason_is_documented_and_live():
    """The reason list is a stable machine contract, so both directions are pinned.

    ``live_read_execution`` sat in the doc with no producer left, and
    ``interface_attribute_eligibility_unresolved`` was raised without being documented.
    ``outstanding_deletion_provenance`` is emitted through the shared API-error helper rather
    than ``ApplyUnexecutable``. Neither shape can drift silently: the doc list and every
    ``apply_unexecutable`` producer must match exactly.
    """
    import ast
    from pathlib import Path

    source_dir = Path(__file__).resolve().parents[2] / "nso_adapter"
    contract = (Path(__file__).resolve().parents[2] / "docs" / "api-contract.md").read_text()

    raised: set[str] = set()
    accumulator_sites = 0
    for path in source_dir.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            # Both shapes: the literal dict handed to the exception, and the dict a caller
            # accumulates reasons into before raising.
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplyUnexecutable":
                raised |= {
                    value.value
                    for argument in node.args
                    if isinstance(argument, ast.Dict)
                    for value in argument.values
                    if isinstance(value, ast.Constant) and isinstance(value.value, str)
                }
            if (
                isinstance(node, ast.Call)
                and getattr(node.func, "id", None) == "api_error"
                and len(node.args) >= 4
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value == "apply_unexecutable"
            ):
                raised |= {
                    value.value
                    for detail in ast.walk(node.args[3])
                    if isinstance(detail, ast.Dict)
                    for value in detail.values
                    if isinstance(value, ast.Constant) and isinstance(value.value, str)
                }
            if (
                isinstance(node, ast.Assign)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
                and any(
                    isinstance(target, ast.Subscript) and getattr(target.value, "id", "") == "unexecutable"
                    for target in node.targets
                )
            ):
                accumulator_sites += 1
                raised.add(node.value.value)

    assert raised, "the reason scan found no raise site — the scan itself has drifted"
    assert accumulator_sites == 1, "an ApplyUnexecutable accumulator producer changed shape"
    reason_list = contract.split("<!-- apply-unexecutable-reasons:start -->", 1)[1].split(
        "<!-- apply-unexecutable-reasons:end -->", 1
    )[0]
    documented = set(re.findall(r"^\s*- `([a-z_]+)`\s*$", reason_list, re.MULTILINE))
    assert documented == raised, f"documented {sorted(documented)} != raised {sorted(raised)}"


def _widen_apply_boundary(monkeypatch, *sections: str) -> None:
    """Admit *sections* past the live-read gate for one test.

    The gate is a rollout boundary, not the behavior under test: it currently holds every
    section but ``vlan``, so the paths below are unreachable over HTTP until the aggregate
    document builder lands. The widening only unions sections in and never replaces the
    set, so it degrades to a no-op and these tests run unpatched once the real boundary
    holds the section.
    """
    from nso_adapter.core import generation as generation_module
    from nso_adapter.core import projection as projection_module

    widened = projection_module.ACTION_APPLY_EXECUTABLE_SECTIONS | set(sections)
    monkeypatch.setattr(projection_module, "ACTION_APPLY_EXECUTABLE_SECTIONS", widened)
    # `from … import` binds a copy; rebind it only where the consuming module still has one.
    if hasattr(generation_module, "ACTION_APPLY_EXECUTABLE_SECTIONS"):
        monkeypatch.setattr(generation_module, "ACTION_APPLY_EXECUTABLE_SECTIONS", widened)


async def test_action_apply_names_a_backfill_only_receipt_by_its_own_skip_code(adapter_client, monkeypatch):
    """A backfill receipt holds the selected sequence, so 'no_receipt' misdescribes it."""
    from tests.api.test_static_route_identity import seed_intent

    _widen_apply_boundary(monkeypatch, "static_route")
    device_id = await seed_device(nso_device_name="apply-backfill-skip", netbox_device_id=9984)
    await seed_settings(device_id, auto_apply=False)
    await seed_intent(device_id, [{"triple": _A, "route_id": 1}])
    backfill = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=6401,
        query="?backfill_only=true",
    )
    assert backfill.status_code == 200, backfill.text

    response = await _apply(adapter_client, device_id, {"static_route": 6401})

    assert response.status_code == 200, response.text
    assert response.json() == {
        "device_id": device_id,
        "outcome": "no_op",
        "selected": {"static_route": 6401},
        "skipped": {"static_route": "backfill_only"},
        "skipped_detail": None,
        "generations": [],
    }
    assert await _generations(device_id) == []
    assert await _jobs(device_id) == []


async def _put_ip(client, device_id: int, addresses: list[dict], *, seq: int):
    return await client.put(
        f"/api/v1/devices/{device_id}/ip-intent?store_only=true",
        json={"addresses": addresses},
        headers=AUTH | {"X-Push-Seq": str(seq)},
    )


async def _rebind_authorized_ip_rows(device_id: int, interface_id) -> None:
    """Point every authorized ``ip`` row at *interface_id*, the way a lost interface reads."""
    from nso_adapter.store.models import DeviceProjectionStream

    async with session() as db:
        row = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "ip",
            )
        )
        document = {
            table: [dict(entry, interface_id=interface_id) for entry in rows]
            for table, rows in row.authorized_document.items()
        }
        assert document["interface_ip_intent"], "the promotion authorized no address to lose"
        await db.execute(
            sa.update(DeviceProjectionStream)
            .where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "ip",
            )
            .values(authorized_document=document)
        )
        await db.commit()


async def _authorized_ip_address(client, device_id: int, monkeypatch, *, seq: int) -> None:
    """Promote and settle one stored address, so the next push decomposes into a removal."""
    from nso_adapter.store.models import GenerationStatus

    _widen_apply_boundary(monkeypatch, "interface_config")
    await seed_settings(device_id, auto_apply=False)
    stored = await _put_ip(
        client,
        device_id,
        [{"interface": "Gi0/1", "address": "198.18.11.1/30", "family": "ipv4"}],
        seq=seq,
    )
    assert stored.status_code == 200, stored.text
    assert (await _apply(client, device_id, {"ip": seq})).status_code == 202
    await _settle((await _generations(device_id))[0].job_id, GenerationStatus.settled)


async def test_action_apply_refuses_a_removal_whose_interface_identity_is_gone(adapter_client, monkeypatch):
    """The removal names an interface row that no longer exists, so nothing may execute."""
    device_id = await seed_device(nso_device_name="apply-interface-gone", netbox_device_id=9985)
    await _authorized_ip_address(adapter_client, device_id, monkeypatch, seq=6501)
    await _rebind_authorized_ip_rows(device_id, 999999)
    assert (await _put_ip(adapter_client, device_id, [], seq=6502)).status_code == 200

    response = await _apply(adapter_client, device_id, {"ip": 6502})

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "apply_unexecutable",
            "message": "Selected stream(s) cannot be applied faithfully: ip",
            "detail": {"streams": {"ip": "unresolved_interface_identity"}},
        }
    }
    assert len(await _generations(device_id)) == 1, "the refused promotion left a generation behind"
    assert len(await _jobs(device_id)) == 1, "the refused promotion left a job behind"
    assert (await _stream(device_id, "ip")).authorized_revision == 1


async def test_action_apply_refuses_an_interface_removal_with_no_executable_instance(adapter_client, monkeypatch):
    """interface-reconciler is keyed per interface, so a nameless removal would send nothing."""
    device_id = await seed_device(nso_device_name="apply-interface-nameless", netbox_device_id=9986)
    await _authorized_ip_address(adapter_client, device_id, monkeypatch, seq=6601)
    await _rebind_authorized_ip_rows(device_id, None)
    assert (await _put_ip(adapter_client, device_id, [], seq=6602)).status_code == 200

    response = await _apply(adapter_client, device_id, {"ip": 6602})

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "apply_unexecutable",
            "message": "Selected stream(s) cannot be applied faithfully: ip",
            "detail": {"streams": {"ip": "no_executable_interface"}},
        }
    }
    assert len(await _generations(device_id)) == 1, "the refused promotion left a generation behind"
    assert len(await _jobs(device_id)) == 1, "the refused promotion left a job behind"
    assert (await _stream(device_id, "ip")).authorized_revision == 1


@pytest.mark.parametrize(
    ("selected_seq", "expected", "netbox_device_id"),
    [(6410, "superseded", 9987), (6412, "no_receipt", 9988)],
)
async def test_backfill_only_never_answers_for_a_sequence_the_receipt_does_not_hold(
    adapter_client, monkeypatch, selected_seq, expected, netbox_device_id
):
    """A neighboring sequence keeps its own answer: the older one is superseded, the newer retryable."""
    from tests.api.test_static_route_identity import seed_intent

    _widen_apply_boundary(monkeypatch, "static_route")
    device_id = await seed_device(
        nso_device_name=f"apply-backfill-seq-{selected_seq}", netbox_device_id=netbox_device_id
    )
    await seed_settings(device_id, auto_apply=False)
    await seed_intent(device_id, [{"triple": _A, "route_id": 1}])
    backfill = await _put_routes(
        adapter_client,
        device_id,
        [route_entry(_A, route_id=1, generation=1)],
        seq=6411,
        query="?backfill_only=true",
    )
    assert backfill.status_code == 200, backfill.text

    response = await _apply(adapter_client, device_id, {"static_route": selected_seq})

    assert response.status_code == 200, response.text
    assert response.json()["skipped"] == {"static_route": expected}
    assert await _generations(device_id) == []
    assert await _jobs(device_id) == []


# ── #1612: the two out-of-protocol switching streams ──────────────────────────
#
# The POST prepares and Apply authorizes. Every case runs through the real app and the real
# store, parametrized over both streams: the two differ only in their wire shape.

_SWITCHING_STREAMS = ["lag", "switchport"]


def _lag_body(roots: dict[str, list[int]], *, deleted_roots=(), scalar=None, extra=None) -> dict:
    return {
        "bundles": [
            {
                "name": name,
                "lag_id": 1,
                **({"admin_key": scalar} if scalar is not None else {}),
                "members": [{"interface_name": f"Gi0/{child}"} for child in children],
                **(extra or {}),
            }
            for name, children in roots.items()
        ],
        "deleted_roots": list(deleted_roots),
    }


def _switchport_body(roots: dict[str, list[int]], *, deleted_roots=(), scalar=None, extra=None) -> dict:
    return {
        "interfaces": [
            {
                "interface_name": name,
                **({"mode": scalar} if scalar is not None else {}),
                "tagged_vlans": list(children),
                **(extra or {}),
            }
            for name, children in roots.items()
        ],
        "deleted_roots": list(deleted_roots),
    }


#: Per-stream wire shape. ``scalar`` is a retained-root leaf a preparation can CLEAR, which
#: is a table delta that is not positive (contract control 1c).
_SHAPE = {
    "lag": {
        "path": "lag-config/apply",
        "body": _lag_body,
        "root_table": "lag_bundle_intent",
        "child_table": "lag_member_intent",
        "root_field": "name",
        "child_field": "interface_name",
        "child": lambda n: f"Gi0/{n}",
        "scalar": 7,
        "text_leaf": "timer",
    },
    "switchport": {
        "path": "switchport/apply",
        "body": _switchport_body,
        "root_table": "switchport_intent",
        "child_table": "switchport_tagged_vlan_intent",
        "root_field": "interface_name",
        "child_field": "vlan_id",
        "child": lambda n: n,
        "scalar": "trunk",
        "text_leaf": "mode",
    },
}


async def _prepare(
    client, device_id: int, stream: str, roots, *, deleted_roots=(), scalar=None, extra=None, query: str = ""
):
    shape = _SHAPE[stream]
    return await client.post(
        f"/api/v1/devices/{device_id}/{shape['path']}{query}",
        json=shape["body"](roots, deleted_roots=deleted_roots, scalar=scalar, extra=extra),
        headers=AUTH,
    )


def _roots_in(document: dict, stream: str) -> list:
    shape = _SHAPE[stream]
    section = document.get(stream) or {}
    return [row[shape["root_field"]] for row in section.get(shape["root_table"], [])]


def _children_in(document: dict, stream: str) -> list:
    shape = _SHAPE[stream]
    section = document.get(stream) or {}
    return [row[shape["child_field"]] for row in section.get(shape["child_table"], [])]


async def _live_roots(device_id: int, stream: str) -> list:
    shape = _SHAPE[stream]
    async with session() as db:
        return list(
            (
                await db.execute(
                    sa.text(
                        f"SELECT {shape['root_field']} FROM {shape['root_table']} "
                        "WHERE device_id = :device_id ORDER BY id"
                    ),
                    {"device_id": device_id},
                )
            )
            .scalars()
            .all()
        )


async def _set_ned_id(device_id: int, ned_id: str | None) -> None:
    from nso_adapter.store.models import Device

    async with session() as db:
        await db.execute(sa.update(Device).where(Device.id == device_id).values(ned_id=ned_id))
        await db.commit()


async def _receipts(device_id: int) -> int:
    from nso_adapter.store.models import IntentPushReceipt

    async with session() as db:
        return await db.scalar(
            sa.select(sa.func.count()).select_from(IntentPushReceipt).where(IntentPushReceipt.device_id == device_id)
        )


# ── scenario 1: a prepared snapshot survives a store-only overwrite ───────────


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_prepared_snapshot_survives_a_store_only_overwrite(adapter_client, sender_enabled_sections, stream):
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name=f"prep-store-only-{stream}", netbox_device_id=None)
    prepared = await _prepare(adapter_client, device_id, stream, {"A": [1]})
    assert prepared.json()["status"] == "prepared"
    revision = prepared.json()["selection_revision"]
    stored = await _prepare(adapter_client, device_id, stream, {"B": [2]}, query="?store_only=true")
    assert stored.json() == {**stored.json(), "status": "stored", "selection_revision": None}

    row = await _stream(device_id, stream)
    assert (row.prepared_revision, row.desired_revision, row.authorized_revision) == (revision, revision + 1, 0)
    assert "_execution" not in row.prepared_tables
    assert await _live_roots(device_id, stream) == ["B"]

    response = await _apply(adapter_client, device_id, {stream: revision})

    assert response.status_code == 202, response.text
    assert response.json()["skipped"] == {}
    generations = await _generations(device_id)
    assert generations, "the prepared snapshot promotes"
    for generation in generations:
        assert generation.stream_revisions[stream] == revision
        assert generation.source_push_seq[stream] is None
        assert _roots_in(generation.document, stream) == ["A"], "the composed section is what was prepared"
    assert await _live_roots(device_id, stream) == ["B"], "the live rows stay the store-only replacement"
    row = await _stream(device_id, stream)
    assert (row.desired_revision, row.authorized_revision, row.prepared_revision) == (revision + 1, revision, revision)
    assert await _receipts(device_id) == 0, "these two streams carry no receipt"

    for generation in generations:
        await _settle(generation.job_id, GenerationStatus.settled)
    row = await _stream(device_id, stream)
    assert row.applied_revision == revision
    assert row.prepared_revision == revision, "the slot survives until the next normal preparation"

    superseded = await _apply(adapter_client, device_id, {stream: revision + 1})
    assert superseded.json()["skipped"] == {stream: "no_prepared_revision"}


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_the_execution_context_is_frozen_at_authorization_not_at_preparation(
    adapter_client, sender_enabled_sections, stream
):
    """Control 1a: the NED id in the authorized fragment is the one read by Apply."""
    device_id = await seed_device(nso_device_name=f"freeze-time-{stream}", netbox_device_id=None)
    await _set_ned_id(device_id, "cisco-ios-cli-6.95")
    revision = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]
    await _set_ned_id(device_id, "timos-nc-23.10")

    assert (await _apply(adapter_client, device_id, {stream: revision})).status_code == 202

    row = await _stream(device_id, stream)
    assert row.authorized_document["_execution"] == {"context": {"ned_id": "timos-nc-23.10", "dialect": "nokia_timos"}}
    for generation in await _generations(device_id):
        assert generation.document[stream]["_execution"]["context"]["ned_id"] == "timos-nc-23.10"


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_an_identical_re_preparation_settles_without_a_generation(
    adapter_client, sender_enabled_sections, stream
):
    """Control 1b: no removal work, no table delta and an equal context settle wire-equivalently."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name=f"wire-equivalent-{stream}", netbox_device_id=None)
    first = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]
    assert (await _apply(adapter_client, device_id, {stream: first})).status_code == 202
    for generation in await _generations(device_id):
        await _settle(generation.job_id, GenerationStatus.settled)
    before = len(await _generations(device_id))

    again = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]
    response = await _apply(adapter_client, device_id, {stream: again})

    assert response.status_code == 200, response.text
    assert response.json()["skipped"] == {stream: "already_applied"}
    assert len(await _generations(device_id)) == before, "an identical re-preparation creates no generation"
    row = await _stream(device_id, stream)
    assert (row.authorized_revision, row.applied_revision) == (again, again)


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_context_change_over_identical_tables_creates_a_generation(
    adapter_client, sender_enabled_sections, stream
):
    """Control 1c, first half: table equality is not wire equivalence."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name=f"context-delta-{stream}", netbox_device_id=None)
    await _set_ned_id(device_id, "cisco-ios-cli-6.95")
    first = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]
    assert (await _apply(adapter_client, device_id, {stream: first})).status_code == 202
    for generation in await _generations(device_id):
        await _settle(generation.job_id, GenerationStatus.settled)
    before = len(await _generations(device_id))
    await _set_ned_id(device_id, "timos-nc-23.10")

    again = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]
    response = await _apply(adapter_client, device_id, {stream: again})

    assert response.status_code == 202, response.text
    assert len(await _generations(device_id)) == before + 1
    assert (await _generations(device_id))[-1].document[stream]["_execution"]["context"]["dialect"] == "nokia_timos"


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_retained_root_scalar_clear_creates_a_generation(adapter_client, sender_enabled_sections, stream):
    """Control 1c, second half: a clear is a table delta that is not positive."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name=f"scalar-clear-{stream}", netbox_device_id=None)
    first = (await _prepare(adapter_client, device_id, stream, {"A": [1]}, scalar=_SHAPE[stream]["scalar"])).json()[
        "selection_revision"
    ]
    assert (await _apply(adapter_client, device_id, {stream: first})).status_code == 202
    for generation in await _generations(device_id):
        await _settle(generation.job_id, GenerationStatus.settled)
    before = len(await _generations(device_id))

    cleared = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]
    response = await _apply(adapter_client, device_id, {stream: cleared})

    assert response.status_code == 202, response.text
    assert len(await _generations(device_id)) > before


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_no_generation_settlement_stamps_the_selected_revision(adapter_client, sender_enabled_sections, stream):
    """Control 1d: never desired_revision, so a store-only successor stays unselectable."""
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name=f"settle-stamps-{stream}", netbox_device_id=None)
    first = (await _prepare(adapter_client, device_id, stream, {})).json()["selection_revision"]
    assert (await _apply(adapter_client, device_id, {stream: first})).status_code == 202
    for generation in await _generations(device_id):
        await _settle(generation.job_id, GenerationStatus.settled)

    revision = (await _prepare(adapter_client, device_id, stream, {})).json()["selection_revision"]
    assert (await _prepare(adapter_client, device_id, stream, {"B": [2]}, query="?store_only=true")).status_code == 200

    response = await _apply(adapter_client, device_id, {stream: revision})

    assert response.status_code == 200, response.text
    row = await _stream(device_id, stream)
    assert (row.authorized_revision, row.applied_revision, row.desired_revision) == (
        revision,
        revision,
        revision + 1,
    )
    later = await _apply(adapter_client, device_id, {stream: revision + 1})
    assert later.json()["skipped"] == {stream: "no_prepared_revision"}


# ── scenario 2: durable mixed deletion and detach ─────────────────────────────


async def _authorize(client, device_id: int, stream: str, roots, *, scalar=None):
    """Prepare, Apply and settle one snapshot, so its roots become AUTHORIZED."""
    from nso_adapter.store.models import GenerationStatus

    prepared = await _prepare(client, device_id, stream, roots, scalar=scalar)
    assert prepared.status_code == 200, prepared.text
    revision = prepared.json()["selection_revision"]
    response = await _apply(client, device_id, {stream: revision})
    assert response.status_code in (200, 202), response.text
    for generation in await _generations(device_id):
        if generation.status is not GenerationStatus.settled and generation.job_id is not None:
            await _settle(generation.job_id, GenerationStatus.settled)
    return revision


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_marked_root_retracts_and_an_unmarked_one_detaches(adapter_client, sender_enabled_sections, stream):
    from nso_adapter.core.generation import digest_document
    from nso_adapter.core.projection import hydrate_section
    from nso_adapter.store.models import GenerationMode

    shape = _SHAPE[stream]
    device_id = await seed_device(nso_device_name=f"mixed-detach-{stream}", netbox_device_id=None)
    await _authorize(adapter_client, device_id, stream, {"A": [1, 2], "B": [3, 4]})
    before = len(await _generations(device_id))

    revision = (await _prepare(adapter_client, device_id, stream, {}, deleted_roots=["A"])).json()["selection_revision"]
    row = await _stream(device_id, stream)
    assert [item[shape["root_field"]] for item in row.prepared_deletions["delete_origin"][shape["root_table"]]] == ["A"]
    assert len(row.prepared_deletions["delete_origin"][shape["child_table"]]) == 2
    assert [item[shape["root_field"]] for item in row.prepared_deletions["detach"][shape["root_table"]]] == ["B"]
    assert row.prepared_deletions["owned_content"] == {}

    assert (await _apply(adapter_client, device_id, {stream: revision})).status_code == 202

    chain = (await _generations(device_id))[before:]
    assert [generation.mode for generation in chain] == [GenerationMode.networked, GenerationMode.detach]
    networked, detach = chain
    assert _roots_in(networked.document, stream) == ["B"], "the intermediate retains the detached root"
    assert sorted(_children_in(networked.document, stream)) == sorted(shape["child"](n) for n in (3, 4))
    assert _roots_in(detach.document, stream) == [], "the final omits every removed root"
    assert networked.settlement_cohort is not None
    assert networked.settlement_cohort == detach.settlement_cohort
    assert networked.allowed_removal_keys == {
        f"{stream}/{shape['root_table']}": [["A"]],
        f"{stream}/{shape['child_table']}": [["A", shape["child"](1)], ["A", shape["child"](2)]],
    }
    assert detach.allowed_removal_keys == {}, "a detach link carries no removal authority"
    for generation in chain:
        assert digest_document(generation.mode, generation.document, generation.allowed_removal_keys) == (
            generation.digest
        )
        assert hydrate_section(generation.document, stream) is not None

    # F9: the immutable generation keeps its authority when the slot is replaced.
    assert (await _prepare(adapter_client, device_id, stream, {"C": [5]})).status_code == 200
    assert (await _generations(device_id))[before].allowed_removal_keys == networked.allowed_removal_keys


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_the_removal_chain_settles_only_when_every_link_does(adapter_client, sender_enabled_sections, stream):
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name=f"chain-settle-{stream}", netbox_device_id=None)
    await _authorize(adapter_client, device_id, stream, {"A": [1], "B": [2]})
    before = len(await _generations(device_id))
    revision = (await _prepare(adapter_client, device_id, stream, {}, deleted_roots=["A"])).json()["selection_revision"]
    assert (await _apply(adapter_client, device_id, {stream: revision})).status_code == 202
    networked, detach = (await _generations(device_id))[before:]

    await _settle(networked.job_id, GenerationStatus.settled)
    assert (await _stream(device_id, stream)).applied_revision != revision

    await _settle(detach.job_id, GenerationStatus.settled)
    assert (await _stream(device_id, stream)).applied_revision == revision


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_failed_first_link_blocks_the_second(adapter_client, sender_enabled_sections, stream):
    from nso_adapter.core.generation import job_admissible
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name=f"chain-blocks-{stream}", netbox_device_id=None)
    await _authorize(adapter_client, device_id, stream, {"A": [1], "B": [2]})
    before = len(await _generations(device_id))
    revision = (await _prepare(adapter_client, device_id, stream, {}, deleted_roots=["A"])).json()["selection_revision"]
    assert (await _apply(adapter_client, device_id, {stream: revision})).status_code == 202
    networked, detach = (await _generations(device_id))[before:]

    await _settle(networked.job_id, GenerationStatus.failed)

    async with session() as db:
        assert await job_admissible(db, detach.job_id, device_id) is False
    assert (await _stream(device_id, stream)).applied_revision != revision


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_an_addition_beside_a_detach_reaches_every_networked_link(
    adapter_client, sender_enabled_sections, stream
):
    """Control 2a: a new root and an edit ride the networked links; detach subtrees stay intact."""
    device_id = await seed_device(nso_device_name=f"add-beside-detach-{stream}", netbox_device_id=None)
    await _authorize(adapter_client, device_id, stream, {"A": [1], "B": [2]})
    before = len(await _generations(device_id))

    revision = (await _prepare(adapter_client, device_id, stream, {"A": [1], "C": [5]})).json()["selection_revision"]
    assert (await _apply(adapter_client, device_id, {stream: revision})).status_code == 202

    chain = (await _generations(device_id))[before:]
    networked = [generation for generation in chain if _roots_in(generation.document, stream) != []]
    assert networked, "the addition produces at least one networked link"
    for generation in networked:
        roots = _roots_in(generation.document, stream)
        assert "C" in roots, "the addition rides every networked link"
    intermediate = next(generation for generation in chain if "B" in _roots_in(generation.document, stream))
    assert sorted(_roots_in(intermediate.document, stream)) == ["A", "B", "C"]


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_retained_root_clear_beside_a_detach_is_not_refused(adapter_client, sender_enabled_sections, stream):
    """Control 2b: mixed_detach_replacement is gated to the sixteen receipt lanes."""
    shape = _SHAPE[stream]
    device_id = await seed_device(nso_device_name=f"clear-detach-{stream}", netbox_device_id=None)
    await _authorize(adapter_client, device_id, stream, {"A": [1], "B": [2]}, scalar=shape["scalar"])
    before = len(await _generations(device_id))

    revision = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]
    response = await _apply(adapter_client, device_id, {stream: revision})

    assert response.status_code == 202, response.text
    chain = (await _generations(device_id))[before:]
    assert len(chain) == 2
    intermediate, final = chain
    assert sorted(_roots_in(intermediate.document, stream)) == ["A", "B"]
    assert _roots_in(final.document, stream) == ["A"], "the final retains the cleared root and its children"
    assert _children_in(final.document, stream) == [shape["child"](1)]
    for generation in chain:
        cleared = next(
            item for item in generation.document[stream][shape["root_table"]] if item[shape["root_field"]] == "A"
        )
        assert cleared["admin_key" if stream == "lag" else "mode"] is None


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_child_dropped_under_a_retained_root_is_networked_by_itself(
    adapter_client, sender_enabled_sections, stream
):
    """Controls 2c and 2d: an owned-content removal schedules an intermediate on its own."""
    shape = _SHAPE[stream]
    device_id = await seed_device(nso_device_name=f"owned-content-{stream}", netbox_device_id=None)
    await _authorize(adapter_client, device_id, stream, {"A": [1, 2]})
    before = len(await _generations(device_id))

    revision = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]
    row = await _stream(device_id, stream)
    assert [item[shape["child_field"]] for item in row.prepared_deletions["owned_content"][shape["child_table"]]] == [
        shape["child"](2)
    ]
    assert row.prepared_deletions["delete_origin"] == {}
    assert row.prepared_deletions["detach"] == {}

    assert (await _apply(adapter_client, device_id, {stream: revision})).status_code == 202

    chain = (await _generations(device_id))[before:]
    assert len(chain) == 1
    (networked,) = chain
    assert _roots_in(networked.document, stream) == ["A"]
    assert _children_in(networked.document, stream) == [shape["child"](1)]
    assert networked.allowed_removal_keys == {f"{stream}/{shape['child_table']}": [["A", shape["child"](2)]]}


async def test_one_apply_carries_both_switching_scopes_removal_authority(adapter_client, sender_enabled_sections):
    """Control 2e: every networked generation carries the scope-qualified union."""
    device_id = await seed_device(nso_device_name="multi-stream-union", netbox_device_id=None)
    await _authorize(adapter_client, device_id, "lag", {"A": [1]})
    await _authorize(adapter_client, device_id, "switchport", {"B": [2]})
    before = len(await _generations(device_id))

    lag_revision = (await _prepare(adapter_client, device_id, "lag", {}, deleted_roots=["A"])).json()[
        "selection_revision"
    ]
    switchport_revision = (await _prepare(adapter_client, device_id, "switchport", {}, deleted_roots=["B"])).json()[
        "selection_revision"
    ]
    response = await _apply(adapter_client, device_id, {"lag": lag_revision, "switchport": switchport_revision})

    assert response.status_code == 202, response.text
    chain = (await _generations(device_id))[before:]
    union = {
        "lag/lag_bundle_intent": [["A"]],
        "lag/lag_member_intent": [["A", "Gi0/1"]],
        "switchport/switchport_intent": [["B"]],
        "switchport/switchport_tagged_vlan_intent": [["B", 2]],
    }
    networked = [generation for generation in chain if generation.allowed_removal_keys]
    assert networked, "the deletion produces networked links"
    for generation in networked:
        assert generation.allowed_removal_keys == union


async def test_a_companion_apply_generation_carries_the_removal_authority_on_a_dedicated_carrier(
    adapter_client, sender_enabled_sections
):
    """Controls 2f and 2g: the companion carries the same authority and no one may join it."""
    from nso_adapter.core.generation import advance_generations_locked, lock_projection
    from nso_adapter.store.models import GenerationStatus, Job

    device_id = await seed_device(nso_device_name="companion-authority", netbox_device_id=None)
    await seed_settings(device_id, auto_apply=True)
    await _authorize(adapter_client, device_id, "lag", {"A": [1]})
    before = len(await _generations(device_id))

    revision = (await _prepare(adapter_client, device_id, "lag", {"C": [5]}, deleted_roots=["A"])).json()[
        "selection_revision"
    ]
    assert (await _apply(adapter_client, device_id, {"lag": revision})).status_code == 202

    removal, companion = (await _generations(device_id))[before:]
    authority = {"lag/lag_bundle_intent": [["A"]], "lag/lag_member_intent": [["A", "Gi0/1"]]}
    assert removal.allowed_removal_keys == authority
    assert companion.allowed_removal_keys == authority, "the companion carries the same authority"
    async with session() as db:
        carrier = await db.get(Job, companion.job_id)
    assert carrier.coalescible is False, "a companion carrying authority takes a dedicated carrier"

    await _settle(removal.job_id, GenerationStatus.failed)
    isis = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        json={"interfaces": [{"interface_name": "Gi0/9", "af": "ipv4"}]},
        headers=AUTH | {"X-Push-Seq": "7701"},
    )
    assert isis.status_code == 200, isis.text
    isis_generation = (await _generations(device_id))[-1]
    assert isis_generation.job_id != companion.job_id, "an unrelated auto-Apply never joins that carrier"

    abandon = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/abandon-generation",
        json={"generation_id": removal.id},
        headers=AUTH,
    )
    assert abandon.status_code == 202, abandon.text

    async with session() as db:
        await lock_projection(db, device_id)
        await advance_generations_locked(db, device_id)
        await db.commit()
    released = next(generation for generation in await _generations(device_id) if generation.id == companion.id)
    assert "A" not in _roots_in(released.document, "lag"), "the released generation still omits the deleted root"
    assert released.allowed_removal_keys == authority


# ── scenario 3: restart after preparation, before authorization ───────────────


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_preparation_survives_a_connection_restart(adapter_client, sender_enabled_sections, stream):
    from nso_adapter.store.db import get_engine

    device_id = await seed_device(nso_device_name=f"restart-prepare-{stream}", netbox_device_id=None)
    revision = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]
    await get_engine().dispose()

    row = await _stream(device_id, stream)
    assert (row.prepared_revision, row.desired_revision, row.authorized_revision) == (revision, revision, 0)
    assert await _live_roots(device_id, stream) == ["A"]

    attempt_id = uuid4()
    first = await _apply(adapter_client, device_id, {stream: revision}, attempt_id=attempt_id)
    assert first.status_code == 202, first.text
    await get_engine().dispose()

    replay = await _apply(adapter_client, device_id, {stream: revision}, attempt_id=attempt_id)
    assert (replay.status_code, replay.json()) == (first.status_code, first.json())

    before = len(await _generations(device_id))
    fresh = await _apply(adapter_client, device_id, {stream: revision})
    assert fresh.json()["skipped"] == {stream: "already_authorized"}
    assert len(await _generations(device_id)) == before, "a new UUID creates no duplicate chain"


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_a_failed_promotion_commits_neither_the_promotion_nor_a_generation(
    adapter_client, sender_enabled_sections, stream
):
    from nso_adapter.core.generation import create_action_apply, digest_document
    from nso_adapter.store.apply_attempt_store import begin_apply_attempt
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode, GenerationStatus

    device_id = await seed_device(nso_device_name=f"restart-atomic-{stream}", netbox_device_id=None)
    revision = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]

    attempt_id = uuid4()
    async with session() as db:
        assert await begin_apply_attempt(db, attempt_id, device_id, {stream: revision}) is None
        db.add(
            DeploymentGeneration(
                device_id=device_id,
                seq=1,
                mode=GenerationMode.networked,
                status=GenerationStatus.settled,
                document={},
                digest=digest_document(GenerationMode.networked, {}, {}),
                allowed_removal_keys={},
                source_push_seq={},
                stream_revisions={},
            )
        )
        await db.commit()

    async with session() as db:
        with pytest.raises(IntegrityError) as exc_info:
            await create_action_apply(db, device_id, {stream: revision}, attempt_id)
        assert "uq_generation_seq_per_device" in str(exc_info.value)
        await db.rollback()

    row = await _stream(device_id, stream)
    assert (row.authorized_revision, row.prepared_revision) == (0, revision)
    assert len(await _generations(device_id)) == 1


# ── scenario 4: composition with an IS-IS generation in flight ────────────────


async def test_a_switching_apply_composes_the_authorized_isis_fragment_only(adapter_client, sender_enabled_sections):
    from nso_adapter.store.models import GenerationStatus

    device_id = await seed_device(nso_device_name="switching-with-isis", netbox_device_id=None)
    await seed_settings(device_id, auto_apply=False)
    isis = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        json={"interfaces": [{"interface_name": "Gi0/1", "af": "ipv4"}]},
        headers=AUTH | {"X-Push-Seq": "8801"},
    )
    assert isis.status_code == 200, isis.text
    assert (await _apply(adapter_client, device_id, {"isis": 8801})).status_code == 202
    (isis_generation,) = await _generations(device_id)

    lag_revision = (await _prepare(adapter_client, device_id, "lag", {"L1": [1]})).json()["selection_revision"]
    later_isis = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent?store_only=true",
        json={"interfaces": [{"interface_name": "Gi0/2", "af": "ipv4"}]},
        headers=AUTH | {"X-Push-Seq": "8802"},
    )
    assert later_isis.status_code == 200, later_isis.text
    assert (await _prepare(adapter_client, device_id, "lag", {"L2": [2]}, query="?store_only=true")).status_code == 200

    conflicted = await _apply(adapter_client, device_id, {"lag": lag_revision})
    assert conflicted.status_code == 409, conflicted.text
    assert (await _stream(device_id, "lag")).prepared_revision == lag_revision

    await _settle(isis_generation.job_id, GenerationStatus.settled)
    retried = await _apply(adapter_client, device_id, {"lag": lag_revision})

    assert retried.status_code == 202, retried.text
    promoted = (await _generations(device_id))[-1]
    assert _roots_in(promoted.document, "lag") == ["L1"], "the store-only successor never enters the document"
    isis_interfaces = [row["interface_name"] for row in promoted.document["isis"]["isis_interface_intent"]]
    assert isis_interfaces == ["Gi0/1"], "only the authorized IS-IS fragment composes in"
    assert promoted.stream_revisions == {"lag": lag_revision}


# ── the production refusal: no sender, no selection, no job ───────────────────


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_apply_refuses_a_section_awaiting_its_aggregate_sender(adapter_client, stream):
    device_id = await seed_device(nso_device_name=f"awaiting-sender-{stream}", netbox_device_id=None)
    revision = (await _prepare(adapter_client, device_id, stream, {"A": [1]})).json()["selection_revision"]

    response = await _apply(adapter_client, device_id, {stream: revision})

    assert response.status_code == 200, response.text
    assert response.json()["skipped"] == {stream: "awaiting_aggregate_sender"}
    assert response.json()["outcome"] == "no_op"
    assert await _generations(device_id) == []
    assert await _jobs(device_id) == []
    row = await _stream(device_id, stream)
    assert (row.prepared_revision, row.authorized_revision) == (revision, 0)


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
async def test_force_removal_refuses_a_section_awaiting_its_aggregate_sender(adapter_client, stream):
    device_id = await seed_device(nso_device_name=f"awaiting-sender-force-{stream}", netbox_device_id=None)
    assert (await _prepare(adapter_client, device_id, stream, {"A": [1]})).status_code == 200

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/force-removal",
        json={"scope": stream},
        headers=AUTH,
    )

    assert response.status_code == 400, response.text
    assert response.json()["error"]["detail"] == {"scope": stream, "reason": "awaiting_aggregate_sender"}
    assert await _generations(device_id) == []
    assert await _jobs(device_id) == []


# ── review r1: table equality is its own predicate ────────────────────────────


@pytest.mark.parametrize("stream", _SWITCHING_STREAMS)
@pytest.mark.parametrize(("before", "after"), [(None, ""), ("", None)], ids=["null-to-empty", "empty-to-null"])
async def test_a_null_to_empty_scalar_change_is_not_wire_equivalent(
    adapter_client, sender_enabled_sections, stream, before, after
):
    """Neither predicate sees it: a clear is not cleared and an empty value is not positive."""
    from nso_adapter.store.models import GenerationStatus

    leaf = _SHAPE[stream]["text_leaf"]
    device_id = await seed_device(nso_device_name=f"null-empty-{stream}-{before!r}", netbox_device_id=None)
    first = (await _prepare(adapter_client, device_id, stream, {"A": [1]}, extra={leaf: before})).json()[
        "selection_revision"
    ]
    assert (await _apply(adapter_client, device_id, {stream: first})).status_code == 202
    for generation in await _generations(device_id):
        await _settle(generation.job_id, GenerationStatus.settled)
    baseline = len(await _generations(device_id))

    changed = (await _prepare(adapter_client, device_id, stream, {"A": [1]}, extra={leaf: after})).json()[
        "selection_revision"
    ]
    response = await _apply(adapter_client, device_id, {stream: changed})

    assert response.status_code == 202, response.text
    assert len(await _generations(device_id)) == baseline + 1, "a table delta must reach the device"
    promoted = (await _generations(device_id))[-1]
    row = next(
        item
        for item in promoted.document[stream][_SHAPE[stream]["root_table"]]
        if item[_SHAPE[stream]["root_field"]] == "A"
    )
    assert row[leaf] == after


async def test_the_frozen_context_reads_the_ned_id_committed_under_the_lock(
    adapter_client, sender_enabled_sections, rival_engine
):
    """The device row is re-read under the projection lock, never served from the identity map."""
    from nso_adapter.core.generation import create_action_apply, lock_device_document
    from nso_adapter.store.apply_attempt_store import begin_apply_attempt
    from nso_adapter.store.db import get_engine
    from nso_adapter.store.models import Device
    from tests.core.test_projection_lock_order import _backend_pid, _wait_for_blocked_query

    device_id = await seed_device(nso_device_name="frozen-context-race", netbox_device_id=None)
    await _set_ned_id(device_id, "cisco-ios-cli-6.95")
    revision = (await _prepare(adapter_client, device_id, "lag", {"A": [1]})).json()["selection_revision"]
    attempt_id = uuid4()
    async with session() as db:
        assert await begin_apply_attempt(db, attempt_id, device_id, {"lag": revision}) is None
        await db.commit()

    rival = async_sessionmaker(rival_engine, expire_on_commit=False)
    async with session() as writer, rival() as blocker:
        # The endpoint loads the device BEFORE it takes the projection lock, and keeps the
        # reference for the whole handler, so the identity map really does hold it.
        loaded = await writer.get(Device, device_id)
        assert loaded.ned_id == "cisco-ios-cli-6.95"
        writer_pid = await _backend_pid(writer)
        blocker_pid = await _backend_pid(blocker)
        await lock_device_document(blocker, device_id)

        applying = asyncio.create_task(create_action_apply(writer, device_id, {"lag": revision}, attempt_id))
        try:
            await _wait_for_blocked_query(
                get_engine(),
                blocker_pid=blocker_pid,
                waiter_pid=writer_pid,
                relation="devices",
                fragments=("from devices", "for no key update"),
            )
            assert not applying.done()
            await blocker.execute(sa.update(Device).where(Device.id == device_id).values(ned_id="timos-nc-23.10"))
            await blocker.commit()
            await asyncio.wait_for(applying, timeout=10)
        finally:
            if not applying.done():
                applying.cancel()
                await asyncio.gather(applying, return_exceptions=True)
        await writer.commit()
        assert loaded is not None  # the handler's reference outlives the promotion

    row = await _stream(device_id, "lag")
    assert row.authorized_document["_execution"]["context"] == {
        "ned_id": "timos-nc-23.10",
        "dialect": "nokia_timos",
    }
