# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1558 slice 2a: selected manual-Apply promotion and mixed decomposition."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

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


async def _apply(client, device_id: int, selected: dict[str, int]):
    return await client.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={"selected": selected},
        headers=AUTH,
    )


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
        "generations": [],
    }
    assert await _generations(device_id) == []
    stream = await _stream(device_id, "vlan")
    assert (stream.desired_revision, stream.authorized_revision) == (2, 0)


async def test_action_apply_rolls_back_promotion_when_generation_flush_fails(adapter_client):
    from nso_adapter.core.generation import create_action_apply, digest_document
    from nso_adapter.store.models import DeploymentGeneration, GenerationMode, GenerationStatus

    device_id = await seed_device(nso_device_name="apply-atomic-rollback", netbox_device_id=9962)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4201, query="?store_only=true")).status_code == 200

    async with session() as db:
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
        with pytest.raises(IntegrityError):
            await create_action_apply(db, device_id, {"vlan": 4201})
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
    assert len(body["generations"]) == 1
    link = body["generations"][0]
    assert body["job_id"] == link["job_id"]
    assert link["mode"] == "networked"
    assert link["source_push_seq"] == {"vlan": 4301}
    assert link["stream_revisions"] == {"vlan": 1}
    assert len(link["digest"]) == 64
    assert link["job_id"] is not None
    assert (await _generations(device_id))[0].digest == link["digest"]


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
    """Finding 1: a later edit must not hide an earlier marked deletion."""
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
    """R5 finding 3: a direct promotion cannot strand an earlier store-only deletion."""
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
    """Finding 1: delivered provenance cannot mark a later deletion of the same route."""
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
    """R4 P1: a restored route takes its next deletion's current marking."""
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
    """Finding 2: a clear cannot settle on the merge-writer path."""
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
    """Finding 2: apply-owned result fields are not operator replacement work."""
    from nso_adapter.core.generation import _has_positive_delta, _replacement_rows

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

    assert _replacement_rows(old, desired) == {}
    assert _has_positive_delta(old, desired) is False


async def test_apply_settlement_fields_do_not_block_a_later_detach(adapter_client):
    """R4 P2: settled clear bookkeeping is not successor replacement work."""
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


async def test_action_apply_refuses_a_live_read_stream_without_promoting_an_executable_subset(adapter_client):
    """R5 boundary: one live-read selection refuses the whole mixed request."""
    device_id = await seed_device(nso_device_name="apply-live-read-refusal", netbox_device_id=9967)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_vlans(adapter_client, device_id, [10], seq=4701, query="?store_only=true")).status_code == 200
    assert (await _put_snmp(adapter_client, device_id, ["new"], seq=4702, query="?store_only=true")).status_code == 200

    response = await _apply(adapter_client, device_id, {"vlan": 4701, "snmp": 4702})

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "apply_unexecutable",
            "message": "Selected stream(s) cannot be applied faithfully: snmp",
            "detail": {"streams": {"snmp": "live_read_execution"}},
        }
    }
    assert await _generations(device_id) == []
    assert await _jobs(device_id) == []
    assert (await _stream(device_id, "vlan")).authorized_revision == 0
    assert (await _stream(device_id, "snmp")).authorized_revision == 0


async def test_action_apply_refuses_static_route_until_it_executes_from_the_document(adapter_client):
    """R6 boundary: static-route workers still read live intent at execution time."""
    device_id = await seed_device(nso_device_name="apply-static-live-read-refusal", netbox_device_id=9980)
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

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "apply_unexecutable",
            "message": "Selected stream(s) cannot be applied faithfully: static_route",
            "detail": {"streams": {"static_route": "live_read_execution"}},
        }
    }
    assert await _generations(device_id) == []
    assert await _jobs(device_id) == []
    assert (await _stream(device_id, "static_route")).authorized_revision == 0


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
    """Finding 4: composed non-static removals retain collateral-guard authority."""
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


async def test_action_apply_refuses_interface_streams_that_execute_from_live_rows(adapter_client):
    """R5 rescope: interface execution cannot certify the selected stored revision."""
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

    assert response.status_code == 409
    assert response.json()["error"]["detail"] == {
        "streams": {"ip": "live_read_execution"},
    }
    assert await _generations(device_id) == []
    assert await _jobs(device_id) == []
    assert (await _stream(device_id, "ip")).authorized_revision == 0


async def test_apply_static_route_removal_keeps_apply_bookkeeping_job(adapter_client):
    """Finding 6: removal uses SrRemoval and additions retain the apply CAS/results path."""
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
    """R3 P1-1: a VLAN replacement must detach 10 and still network 20."""
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
    assert detach.allowed_removal_keys == {"vlan": [10]}
    assert {row["vlan_id"] for row in apply.document["vlan"]["vlan_intent"]} == {10, 20}
    assert {row["vlan_id"] for row in detach.document["vlan"]["vlan_intent"]} == {20}
    assert detach.settlement_cohort is not None
    assert apply.settlement_cohort == detach.settlement_cohort


async def test_action_apply_refuses_bgp_until_its_graph_executes_from_the_document(adapter_client):
    """R5 rescope: the live-read BGP graph cannot execute an exact selected revision."""
    device_id = await seed_device(nso_device_name="apply-bgp-rebuild", netbox_device_id=9975)
    await seed_settings(device_id, auto_apply=False)
    first = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent?store_only=true",
        json={"routers": [_bgp_router("64513")]},
        headers=AUTH | {"X-Push-Seq": "5501"},
    )
    assert first.status_code == 200, first.text

    response = await _apply(adapter_client, device_id, {"bgp": 5501})

    assert response.status_code == 409
    assert response.json()["error"]["detail"] == {
        "streams": {"bgp": "live_read_execution"},
    }
    assert await _generations(device_id) == []
    assert await _jobs(device_id) == []
    assert (await _stream(device_id, "bgp")).authorized_revision == 0


async def test_promoted_static_route_detach_fails_when_proof_is_inconclusive(adapter_client):
    """R3 P1-3: job context is a proof carrier even after provenance consumption."""
    from nso_adapter.core.generation import (
        _compose_authorized_document,
        _enqueue_action_removal_links,
        _RemovalLink,
    )
    from nso_adapter.core.projection import snapshot_stream
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
            cohort=None,
            intermediate_document=document,
            final_document=document,
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
    """R6 lifecycle: proof consumption is not a successor operator deletion."""
    from nso_adapter.core.generation import (
        _fragment_deletions,
        _has_positive_delta,
        _plan_action_links,
        _Promotion,
        _replacement_rows,
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
        replacement = _replacement_rows(projection.authorized_document, desired)
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
    networked_links, detach_links, apply_streams = _plan_action_links({"static_route": promotion})
    assert len(networked_links) == 1
    assert detach_links == []
    assert apply_streams == {"static_route"}


async def test_action_apply_job_executes_only_the_selected_generation_document(adapter_client):
    """R7 item 1: an unselected live row must not ride a document-executed Apply."""
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
    """R7 item 2: a concurrent settlement is already-authorized, never a 500."""
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
        # budget only has to be shorter than "forever" — it is not a latency assertion.
        response = await asyncio.wait_for(applying, timeout=30)

    assert response.status_code == 200, response.text
    assert response.json() == {
        "device_id": device_id,
        "outcome": "no_op",
        "selected": {"vlan": 6301},
        "skipped": {"vlan": "already_authorized"},
        "generations": [],
    }


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
