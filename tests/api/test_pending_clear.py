# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""End-to-end tests for durable pending-clear recording."""

from __future__ import annotations

import sqlalchemy as sa

from tests.conftest import VALID_TOKEN, note_projection_write, push_seq, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _pending_clears(device_id: int):
    from nso_adapter.store.models import StreamPendingClear

    async with session() as db:
        return list(
            (
                await db.execute(
                    sa.select(StreamPendingClear)
                    .where(StreamPendingClear.device_id == device_id)
                    .order_by(StreamPendingClear.stream, StreamPendingClear.provenance)
                )
            )
            .scalars()
            .all()
        )


async def test_record_pending_clears_replaces_store_only_with_authorized(adapter_client):
    from nso_adapter.core.removal import _record_pending_clears
    from nso_adapter.store.models import StreamPendingClear

    device_id = await seed_device(nso_device_name="pending-clear-replace", netbox_device_id=14758)
    async with session() as db:
        await note_projection_write(db, device_id, "ospf")
        db.add(
            StreamPendingClear(
                device_id=device_id,
                stream="ospf",
                provenance="store_only",
                revision=1,
            )
        )
        await db.commit()

    async with session() as db:
        await _record_pending_clears(
            db,
            device_id,
            ("ospf",),
            provenance="authorized",
        )
        await db.commit()

    (pending,) = await _pending_clears(device_id)
    assert (pending.stream, pending.provenance, pending.revision) == ("ospf", "authorized", 1)


async def test_mixed_ospf_clear_records_one_authorized_stream_obligation(adapter_client):
    """A detach that cannot carry its retained-row clear records the clear durably."""
    from nso_adapter.store.models import DeviceProjectionStream, Job, JobType

    device_id = await seed_device(nso_device_name="pending-ospf-mixed", netbox_device_id=14750)
    first = {
        "instances": [
            {"process_id": "1", "router_id": "198.18.0.1", "vrf": "", "areas": []},
            {"process_id": "2", "router_id": "198.18.0.2", "vrf": "", "areas": []},
        ],
        "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0", "cost": 100}],
    }
    second = {
        "instances": [{"process_id": "1", "router_id": "198.18.0.1", "vrf": "", "areas": []}],
        "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0"}],
    }
    assert (
        await adapter_client.put(
            f"/api/v1/devices/{device_id}/ospf-intent",
            headers=AUTH | push_seq(147501),
            json=first,
        )
    ).status_code == 200
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(147502),
        json=second,
    )
    assert response.status_code == 200

    async with session() as db:
        job = await db.scalar(sa.select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal))
        stream = await db.scalar(
            sa.select(DeviceProjectionStream).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "ospf",
            )
        )
    assert job is not None
    assert job.context["detach"] is True
    assert job.context["retract_deferred"] is True

    (pending,) = await _pending_clears(device_id)
    assert (pending.stream, pending.provenance, pending.revision) == (
        "ospf",
        "authorized",
        stream.desired_revision,
    )

    replay = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(147502),
        json=second,
    )
    assert replay.status_code == 200
    assert len(await _pending_clears(device_id)) == 1


async def test_store_only_clear_parks_until_an_authorized_deferred_clear_supersedes_it(adapter_client):
    """Store-only provenance never deploys, and it cannot demote an authorized row."""
    from nso_adapter.store.models import DeploymentGeneration, DeviceProjectionStream, Job

    device_id = await seed_device(nso_device_name="pending-ospf-store-only", netbox_device_id=14751)
    url = f"/api/v1/devices/{device_id}/ospf-intent"
    instances = [
        {"process_id": "1", "router_id": "198.18.1.1", "vrf": "", "areas": []},
        {"process_id": "2", "router_id": "198.18.1.2", "vrf": "", "areas": []},
    ]

    def body(*, processes=instances, costs=(100, 200, 300)):
        return {
            "instances": processes,
            "interfaces": [
                {
                    "interface_name": f"Gi0/{index}",
                    "process_id": "1",
                    "area_id": "0",
                    **({"cost": cost} if cost is not None else {}),
                }
                for index, cost in enumerate(costs)
            ],
        }

    assert (await adapter_client.put(url, headers=AUTH | push_seq(147511), json=body())).status_code == 200
    assert (
        await adapter_client.put(
            f"{url}?store_only=true",
            headers=AUTH | push_seq(147512),
            json=body(costs=(None, 200, 300)),
        )
    ).status_code == 200

    (parked,) = await _pending_clears(device_id)
    assert (parked.stream, parked.provenance, parked.revision) == ("ospf", "store_only", 2)
    async with session() as db:
        assert await db.scalar(sa.select(sa.func.count()).select_from(Job)) == 0
        assert await db.scalar(sa.select(sa.func.count()).select_from(DeploymentGeneration)) == 0

    # Clear Gi0/1 while un-owning process 2. The detach cannot carry either clear, so the
    # authorized obligation supersedes the parked store-only one and keeps the newer revision.
    assert (
        await adapter_client.put(
            url,
            headers=AUTH | push_seq(147513),
            json=body(processes=instances[:1], costs=(None, None, 300)),
        )
    ).status_code == 200
    (authorized,) = await _pending_clears(device_id)
    assert (authorized.stream, authorized.provenance, authorized.revision) == ("ospf", "authorized", 3)

    # A later store-only clear is a real store revision, but it cannot weaken the frozen
    # authorization or claim that revision 4 was authorized.
    assert (
        await adapter_client.put(
            f"{url}?store_only=true",
            headers=AUTH | push_seq(147514),
            json=body(processes=instances[:1], costs=(None, None, None)),
        )
    ).status_code == 200
    (still_authorized,) = await _pending_clears(device_id)
    assert (still_authorized.provenance, still_authorized.revision) == ("authorized", 3)
    async with session() as db:
        stream_revision = await db.scalar(
            sa.select(DeviceProjectionStream.desired_revision).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.stream == "ospf",
            )
        )
    assert stream_revision == 4


async def test_authorized_push_with_the_same_omission_promotes_a_parked_clear(adapter_client):
    """A later authorized detach carrying the SAME omission (no new set-to-unset
    transition, so retract is False) must still promote the parked store-only row."""
    device_id = await seed_device(nso_device_name="pending-ospf-promote-same", netbox_device_id=14755)
    url = f"/api/v1/devices/{device_id}/ospf-intent"
    instances = [
        {"process_id": "1", "router_id": "198.18.5.1", "vrf": "", "areas": []},
        {"process_id": "2", "router_id": "198.18.5.2", "vrf": "", "areas": []},
    ]
    iface = {"interface_name": "Gi0/0", "process_id": "1", "area_id": "0"}
    assert (
        await adapter_client.put(
            url, headers=AUTH | push_seq(147551), json={"instances": instances, "interfaces": [iface | {"cost": 100}]}
        )
    ).status_code == 200
    assert (
        await adapter_client.put(
            f"{url}?store_only=true",
            headers=AUTH | push_seq(147552),
            json={"instances": instances, "interfaces": [iface]},
        )
    ).status_code == 200
    (parked,) = await _pending_clears(device_id)
    assert (parked.provenance, parked.revision) == ("store_only", 2)

    assert (
        await adapter_client.put(
            url,
            headers=AUTH | push_seq(147553),
            json={"instances": instances[:1], "interfaces": [iface]},
        )
    ).status_code == 200

    (promoted,) = await _pending_clears(device_id)
    assert (promoted.stream, promoted.provenance, promoted.revision) == ("ospf", "authorized", 3)


async def test_networked_marked_removal_with_the_same_omission_discharges_a_parked_clear(adapter_client):
    """A delete_origin removal PUT-replaces the stream from the omitting store, so the
    parked obligation is delivered and must be discharged, even with retract False."""
    device_id = await seed_device(nso_device_name="pending-ospf-discharge-same", netbox_device_id=14756)
    url = f"/api/v1/devices/{device_id}/ospf-intent"
    instances = [
        {"process_id": "1", "router_id": "198.18.6.1", "vrf": "", "areas": []},
        {"process_id": "2", "router_id": "198.18.6.2", "vrf": "", "areas": []},
    ]
    iface = {"interface_name": "Gi0/0", "process_id": "1", "area_id": "0"}
    assert (
        await adapter_client.put(
            url, headers=AUTH | push_seq(147561), json={"instances": instances, "interfaces": [iface | {"cost": 100}]}
        )
    ).status_code == 200
    assert (
        await adapter_client.put(
            f"{url}?store_only=true",
            headers=AUTH | push_seq(147562),
            json={"instances": instances, "interfaces": [iface]},
        )
    ).status_code == 200
    assert len(await _pending_clears(device_id)) == 1

    assert (
        await adapter_client.put(
            f"{url}?delete_origin=true",
            headers=AUTH | push_seq(147563),
            json={"instances": instances[:1], "interfaces": [iface]},
        )
    ).status_code == 200

    assert await _pending_clears(device_id) == []


async def test_force_removal_discharges_all_pending_streams_in_the_scope(adapter_client):
    """The operator's reviewed flush discharges the section without promoting it."""
    from nso_adapter.store.models import DeploymentGeneration, StreamPendingClear

    device_id = await seed_device(nso_device_name="pending-force-isis", netbox_device_id=14752)
    async with session() as db:
        db.add_all(
            [
                StreamPendingClear(
                    device_id=device_id,
                    stream="isis",
                    provenance="authorized",
                    revision=3,
                ),
                StreamPendingClear(
                    device_id=device_id,
                    stream="isis_flex_algo",
                    provenance="store_only",
                    revision=2,
                ),
            ]
        )
        await db.commit()

    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/force-removal",
        headers=AUTH,
        json={"scope": "isis"},
    )
    assert response.status_code == 202
    assert await _pending_clears(device_id) == []
    async with session() as db:
        generation = await db.scalar(sa.select(DeploymentGeneration).where(DeploymentGeneration.device_id == device_id))
    assert generation.stream_revisions == {}


async def test_networked_pure_clear_discharges_an_existing_stream_obligation(adapter_client):
    """A pure clear has a networked carrier, so no pending-clear row remains."""
    from nso_adapter.store.models import Job, JobType, StreamPendingClear

    device_id = await seed_device(nso_device_name="pending-ospf-pure", netbox_device_id=14753)
    url = f"/api/v1/devices/{device_id}/ospf-intent"
    before = {
        "instances": [{"process_id": "1", "router_id": "198.18.2.1", "vrf": "", "areas": []}],
        "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0", "cost": 100}],
    }
    after = {
        "instances": before["instances"],
        "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0"}],
    }
    assert (await adapter_client.put(url, headers=AUTH | push_seq(147531), json=before)).status_code == 200
    async with session() as db:
        db.add(
            StreamPendingClear(
                device_id=device_id,
                stream="ospf",
                provenance="store_only",
                revision=1,
            )
        )
        await db.commit()

    assert (await adapter_client.put(url, headers=AUTH | push_seq(147532), json=after)).status_code == 200
    async with session() as db:
        job = await db.scalar(sa.select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal))
    assert job.context.get("detach") is None
    assert job.context.get("retract_deferred") is None
    assert await _pending_clears(device_id) == []


async def test_isis_recording_does_not_promote_the_flex_algo_sibling_stream(adapter_client):
    """Each endpoint records only its own authorization lane."""
    device_id = await seed_device(nso_device_name="pending-isis-sibling", netbox_device_id=14754)
    flex_url = f"/api/v1/devices/{device_id}/isis-flex-algo-intent"
    isis_url = f"/api/v1/devices/{device_id}/isis-interface-intent"

    assert (
        await adapter_client.put(
            flex_url,
            headers=AUTH | push_seq(147541),
            json={"flex_algos": [{"process_tag": "1", "algo_id": 128, "priority": 100}]},
        )
    ).status_code == 200
    assert (
        await adapter_client.put(
            f"{flex_url}?store_only=true",
            headers=AUTH | push_seq(147542),
            json={"flex_algos": [{"process_tag": "1", "algo_id": 128}]},
        )
    ).status_code == 200

    before = {
        "interfaces": [
            {"interface_name": "Gi0/0", "af": "ipv4", "metric": 10},
            {"interface_name": "Gi0/1", "af": "ipv4", "metric": 20},
        ]
    }
    after = {"interfaces": [{"interface_name": "Gi0/0", "af": "ipv4"}]}
    assert (await adapter_client.put(isis_url, headers=AUTH | push_seq(147543), json=before)).status_code == 200
    assert (await adapter_client.put(isis_url, headers=AUTH | push_seq(147544), json=after)).status_code == 200

    rows = await _pending_clears(device_id)
    assert [(row.stream, row.provenance, row.revision) for row in rows] == [
        ("isis", "authorized", 2),
        ("isis_flex_algo", "store_only", 2),
    ]
