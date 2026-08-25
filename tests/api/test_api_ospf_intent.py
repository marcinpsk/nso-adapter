# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for PUT /api/v1/devices/{id}/ospf-intent."""

from __future__ import annotations

from sqlalchemy import select

from tests.conftest import VALID_TOKEN, push_seq, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def test_put_ospf_intent_string_process_id(adapter_client):
    """process_id is a STRING end-to-end — a numeric value must store, not raise.

    Regression: the Pydantic schema declared process_id as int, so asyncpg rejected the
    coerced value against the String column. Only surfaced once OSPF intent was first
    pushed (greenfield Nokia OSPF).
    """
    from nso_adapter.store.models import OspfInstanceIntent, OspfInterfaceIntent

    device_id = await seed_device(nso_device_name="ospf-intent-dev", netbox_device_id=920)
    payload = {
        "instances": [{"process_id": "1", "router_id": "198.18.250.117", "vrf": "", "areas": []}],
        "interfaces": [{"interface_name": "LAG99:99", "process_id": "1", "area_id": "0", "passive": False}],
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ospf-intent", headers=AUTH | push_seq(), json=payload)
    assert resp.status_code == 200

    async with session() as db:
        inst = (
            await db.execute(select(OspfInstanceIntent).where(OspfInstanceIntent.device_id == device_id))
        ).scalar_one()
        assert inst.process_id == "1"
        assert inst.router_id == "198.18.250.117"
        iface = (
            await db.execute(select(OspfInterfaceIntent).where(OspfInterfaceIntent.device_id == device_id))
        ).scalar_one()
        assert iface.interface_name == "LAG99:99"
        assert iface.process_id == "1"
        assert iface.area_id == "0"


async def test_put_ospf_intent_admin_state(adapter_client):
    """OSPF process admin-state (enabled) round-trips through put/get + into the apply body."""
    import json
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, patch

    from nso_adapter.nso.apply import apply_ospf_config
    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await seed_device(nso_device_name="ospf-admin-dev", netbox_device_id=921)
    payload = {
        "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "enabled": True, "areas": []}],
        "interfaces": [],
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ospf-intent", headers=AUTH | push_seq(), json=payload)
    assert resp.status_code == 200

    async with session() as db:
        inst = (
            await db.execute(select(OspfInstanceIntent).where(OspfInstanceIntent.device_id == device_id))
        ).scalar_one()
        assert inst.enabled is True
        # The dry-run apply body must carry the admin-state for the reconciler to write it.
        with patch("nso_adapter.nso.apply.native_dry_run", new_callable=AsyncMock, return_value="OK") as mock_dry:
            await apply_ospf_config(
                client=SimpleNamespace(_base="http://nso/restconf/data"),
                device_name="ospf-admin-dev",
                process_intent_rows=[inst],
                interface_intent_rows=[],
                dry_run=True,
            )
        sent = json.loads(mock_dry.call_args.args[2])  # native_dry_run(client, url, payload, ...)
        proc = sent["ospf-reconciler:ospf-config"][0]["process-config"][0]
        assert proc["enabled"] is True


async def test_put_ospf_intent_removal_enqueues_job_not_inline(adapter_client):
    """Dropping an OSPF instance queues an async `removal` job (no inline device commit).

    The PUT must return promptly; the PUT-replace that reverts the dropped process on
    the device runs in the background via the removal job.
    """
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="ospf-removal-dev", netbox_device_id=922)
    # Seed two processes, then PUT a payload that drops one.
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            "instances": [
                {"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []},
                {"process_id": "2", "router_id": "2.2.2.2", "vrf": "", "areas": []},
            ],
            "interfaces": [],
        },
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
            "interfaces": [],
        },
    )
    assert resp.status_code == 200

    async with session() as db:
        jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        assert len(jobs) == 1
        assert jobs[0].status == JobStatus.queued
        # process 2 was just dropped — threaded for the collateral guard
        assert jobs[0].context == {"scope": "ospf", "removed": {"process-config": ["2"]}, "detach": True}


# ── retracting a cleared owned OSPF scalar (#83's flow, ported from IS-IS) ───
#
# OSPF only ever tracked DELETED keys, so a cleared owned scalar (cost back to blank)
# enqueued no removal job at all — and the next apply is a merge-PATCH, which never drops
# a leaf. The device kept the old cost forever and the operator could not clear it.


async def _ospf_removal_jobs(device_id: int):
    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        return [j for j in jobs if (j.context or {}).get("scope") == "ospf"]
    return []


async def test_clearing_an_owned_ospf_interface_scalar_retracts(adapter_client):
    """Clearing an owned interface cost must reach the device: the row stays owned and
    accepted — only the leaf was blanked — so nothing is un-owned and the job must not detach."""
    device_id = await seed_device(nso_device_name="ospf-clear-iface", netbox_device_id=930)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
            "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0", "cost": 100}],
        },
    )
    assert await _ospf_removal_jobs(device_id) == []  # the initial set is not a retraction

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
            "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0"}],  # cost cleared
        },
    )
    assert resp.status_code == 200

    jobs = await _ospf_removal_jobs(device_id)
    assert len(jobs) == 1
    assert jobs[0].context.get("detach") is None  # a real, networking replace
    assert not jobs[0].context.get("removed")  # nothing was un-owned


async def test_clearing_an_owned_ospf_instance_scalar_retracts(adapter_client):
    """Same contract on the instance row (router_id blanked)."""
    device_id = await seed_device(nso_device_name="ospf-clear-inst", netbox_device_id=931)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
            "interfaces": [],
        },
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={"instances": [{"process_id": "1", "vrf": "", "areas": []}], "interfaces": []},  # router_id cleared
    )
    assert resp.status_code == 200

    jobs = await _ospf_removal_jobs(device_id)
    assert len(jobs) == 1
    assert jobs[0].context.get("detach") is None


async def test_ospf_enabled_false_to_none_is_an_update_not_a_clear(adapter_client):
    """The writer maps None to explicit enabled=true, so no replace is needed."""
    from nso_adapter.store.models import StreamPendingClear

    device_id = await seed_device(nso_device_name="ospf-enabled-update", netbox_device_id=934)
    url = f"/api/v1/devices/{device_id}/ospf-intent"
    assert (
        await adapter_client.put(
            url,
            headers=AUTH | push_seq(),
            json={"instances": [{"process_id": "1", "enabled": False, "areas": []}], "interfaces": []},
        )
    ).status_code == 200
    assert (
        await adapter_client.put(
            url,
            headers=AUTH | push_seq(),
            json={"instances": [{"process_id": "1", "areas": []}], "interfaces": []},
        )
    ).status_code == 200

    assert await _ospf_removal_jobs(device_id) == []
    async with session() as db:
        pending = (
            (await db.execute(select(StreamPendingClear).where(StreamPendingClear.device_id == device_id)))
            .scalars()
            .all()
        )
    assert pending == []


async def test_ospf_unown_riding_along_with_a_clear_defers_the_retract(adapter_client):
    """An un-own in the same push cannot be networked (it would strip the dropped process off
    the device) — safety wins, and the deferred retract is recorded, not silently dropped."""
    device_id = await seed_device(nso_device_name="ospf-mixed", netbox_device_id=932)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            "instances": [
                {"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []},
                {"process_id": "2", "router_id": "2.2.2.2", "vrf": "", "areas": []},
            ],
            "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0", "cost": 100}],
        },
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            # process 2 un-owned AND Gi0/0's cost cleared, in the same push
            "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
            "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0"}],
        },
    )
    assert resp.status_code == 200

    jobs = await _ospf_removal_jobs(device_id)
    assert len(jobs) == 1
    assert jobs[0].context["detach"] is True
    assert jobs[0].context["retract_deferred"] is True


async def test_clearing_an_ospf_scalar_under_store_only_touches_nothing(adapter_client):
    device_id = await seed_device(nso_device_name="ospf-clear-so", netbox_device_id=933)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
            "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0", "cost": 100}],
        },
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent?store_only=true",
        headers=AUTH | push_seq(),
        json={
            "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
            "interfaces": [{"interface_name": "Gi0/0", "process_id": "1", "area_id": "0"}],
        },
    )
    assert resp.status_code == 200
    assert await _ospf_removal_jobs(device_id) == []


async def test_put_ospf_intent_device_not_found(adapter_client):
    """Non-existent device → 404."""
    resp = await adapter_client.put(
        "/api/v1/devices/99999/ospf-intent",
        headers=AUTH | push_seq(),
        json={"instances": [], "interfaces": []},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_put_ospf_intent_creates_redistribution(adapter_client):
    """Per-instance redistribution entries become RedistributionIntent rows (dest_protocol=ospf)."""
    from nso_adapter.store.models import RedistributionIntent

    device_id = await seed_device(nso_device_name="ospf-redist-create", netbox_device_id=930)
    payload = {
        "instances": [
            {
                "process_id": "1",
                "router_id": "1.1.1.1",
                "vrf": "",
                "areas": [],
                "redistribution": [
                    {
                        "source_protocol": "bgp",
                        "source_ref": "65001",
                        "route_map": "RM",
                        "metric": 20,
                        "metric_type": "2",
                    },
                    {"source_protocol": "connected", "source_ref": "", "route_map": None, "metric": None},
                ],
            }
        ],
        "interfaces": [],
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ospf-intent", headers=AUTH | push_seq(), json=payload)
    assert resp.status_code == 200

    async with session() as db:
        rows = (
            (await db.execute(select(RedistributionIntent).where(RedistributionIntent.device_id == device_id)))
            .scalars()
            .all()
        )
    by_src = {r.source_protocol: r for r in rows}
    assert set(by_src) == {"bgp", "connected"}
    assert all(r.dest_protocol == "ospf" and r.dest_ref == "1" for r in rows)  # dest_ref = process_id
    assert (by_src["bgp"].source_ref, by_src["bgp"].route_map, by_src["bgp"].metric, by_src["bgp"].metric_type) == (
        "65001",
        "RM",
        20,
        "2",
    )
    assert (by_src["connected"].route_map, by_src["connected"].metric) == (None, None)


async def test_put_ospf_intent_redistribution_full_replace_and_update(adapter_client):
    """Re-PUT drops absent redistribution rows, updates the kept one, and queues a removal job."""
    from nso_adapter.store.models import Job, JobType, RedistributionIntent

    device_id = await seed_device(nso_device_name="ospf-redist-replace", netbox_device_id=931)

    def _body(redist: list[dict]) -> dict:
        return {
            "instances": [
                {"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": [], "redistribution": redist}
            ],
            "interfaces": [],
        }

    await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json=_body(
            [
                {"source_protocol": "bgp", "source_ref": "65001", "route_map": "RM-A", "metric": 10},
                {"source_protocol": "static", "source_ref": "", "route_map": None, "metric": None},
            ]
        ),
    )
    # Keep bgp (changed route_map/metric/type), drop static.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json=_body(
            [{"source_protocol": "bgp", "source_ref": "65001", "route_map": "RM-B", "metric": 99, "metric_type": "1"}]
        ),
    )
    assert resp.status_code == 200

    async with session() as db:
        rows = (
            (await db.execute(select(RedistributionIntent).where(RedistributionIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal))
        ).scalar_one_or_none()
    assert len(rows) == 1  # static dropped
    assert rows[0].source_protocol == "bgp"
    assert (rows[0].route_map, rows[0].metric, rows[0].metric_type) == ("RM-B", 99, "1")  # updated in place
    assert job is not None  # redistribution removal alone enqueues the ospf removal job


async def test_put_ospf_intent_interface_full_replace_and_update(adapter_client):
    """Re-PUT drops an absent interface, updates the kept one in place, and queues a removal job."""
    from nso_adapter.store.models import Job, JobType, OspfInterfaceIntent

    device_id = await seed_device(nso_device_name="ospf-iface-replace", netbox_device_id=932)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            "instances": [],
            "interfaces": [
                {"interface_name": "GE0/0", "area_id": "0", "passive": False, "cost": 10},
                {"interface_name": "GE0/1", "area_id": "0", "passive": False},
            ],
        },
    )
    # Keep GE0/0 (changed cost/passive), drop GE0/1.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={
            "instances": [],
            "interfaces": [{"interface_name": "GE0/0", "area_id": "0", "passive": True, "cost": 50}],
        },
    )
    assert resp.status_code == 200
    assert resp.json()["interface_count"] == 1

    async with session() as db:
        ifaces = (
            (await db.execute(select(OspfInterfaceIntent).where(OspfInterfaceIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal))
        ).scalar_one_or_none()
    assert [i.interface_name for i in ifaces] == ["GE0/0"]  # GE0/1 dropped
    assert (ifaces[0].cost, ifaces[0].passive) == (50, True)  # updated in place
    assert job is not None


async def test_put_ospf_intent_auto_apply_enqueues_apply_job(adapter_client):
    """PUT with auto_apply=True and a non-empty payload enqueues an apply job, so accepted
    OSPF config actually reaches the device — parity with every other intent scope (s3-2)."""
    from nso_adapter.store.models import DeviceSettings, Job, JobType

    device_id = await seed_device(nso_device_name="ospf-intent-autoapply", netbox_device_id=930)
    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()

    payload = {
        "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
        "interfaces": [{"interface_name": "GE0/0", "process_id": "1", "area_id": "0", "passive": False}],
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ospf-intent", headers=AUTH | push_seq(), json=payload)
    assert resp.status_code == 200

    async with session() as db:
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        ).scalar_one_or_none()
        assert job is not None


async def test_put_ospf_intent_no_apply_job_when_auto_apply_disabled(adapter_client):
    """auto_apply=False → no apply job enqueued (operator applies manually)."""
    from nso_adapter.store.models import DeviceSettings, Job, JobType

    device_id = await seed_device(nso_device_name="ospf-intent-noapply", netbox_device_id=931)
    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=False))
        await db.commit()

    payload = {
        "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
        "interfaces": [],
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ospf-intent", headers=AUTH | push_seq(), json=payload)
    assert resp.status_code == 200

    async with session() as db:
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        ).scalar_one_or_none()
        assert job is None


async def test_put_ospf_intent_empty_payload_no_apply_job(adapter_client):
    """auto_apply=True but an empty payload → no apply job (nothing to push)."""
    from nso_adapter.store.models import DeviceSettings, Job, JobType

    device_id = await seed_device(nso_device_name="ospf-intent-empty", netbox_device_id=932)
    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH | push_seq(),
        json={"instances": [], "interfaces": []},
    )
    assert resp.status_code == 200

    async with session() as db:
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        ).scalar_one_or_none()
        assert job is None
