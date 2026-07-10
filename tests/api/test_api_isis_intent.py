# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""End-to-end tests for the IS-IS write endpoints.

PUT /isis-interface-intent (interfaces + processes + redistribution full-replace,
auto-apply) and PUT /isis-flex-algo-intent (flex-algo full-replace + the
removal -> replace_isis_service path). Real route, real SQLite session; only the
NSO HTTP boundary is faked, and only where the on-device service replace is asserted.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _seed_settings(device_id: int, *, auto_apply: bool):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSettings

    async for db in get_session():
        db.add(DeviceSettings(device_id=device_id, auto_apply=auto_apply))
        await db.commit()
        return


# ── PUT /isis-interface-intent ────────────────────────────────────────────────


@pytest.mark.anyio
async def test_put_isis_intent_device_not_found(adapter_client):
    resp = await adapter_client.put(
        "/api/v1/devices/99999/isis-interface-intent", headers=AUTH, json={"interfaces": []}
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_put_isis_intent_creates_interfaces_and_processes(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import IsisInterfaceIntent, IsisProcessIntent

    device_id = await seed_device(nso_device_name="isis-create", netbox_device_id=980)
    body = {
        "interfaces": [
            {"interface_name": "Gi0/0", "af": "ipv4", "process_tag": "1", "metric": 10, "passive": False},
            {"interface_name": "Gi0/0", "af": "ipv6", "process_tag": "1", "passive": True},
        ],
        "processes": [{"process_tag": "1", "net": "49.0001.0000.0000.0001.00", "is_type": "level-2"}],
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/isis-interface-intent", headers=AUTH, json=body)
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, "interface_count": 2, "process_count": 1}

    async for db in get_session():
        ifaces = (
            (await db.execute(select(IsisInterfaceIntent).where(IsisInterfaceIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        procs = (
            (await db.execute(select(IsisProcessIntent).where(IsisProcessIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        break
    assert {(i.interface_name, i.af) for i in ifaces} == {("Gi0/0", "ipv4"), ("Gi0/0", "ipv6")}
    assert all(i.accepted_at is not None for i in ifaces)  # defaulted to now
    assert [p.process_tag for p in procs] == ["1"]
    assert procs[0].net == "49.0001.0000.0000.0001.00"


@pytest.mark.anyio
async def test_put_isis_intent_full_replace_and_update(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import IsisInterfaceIntent, IsisProcessIntent

    device_id = await seed_device(nso_device_name="isis-replace", netbox_device_id=981)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={
            "interfaces": [
                {"interface_name": "Gi0/0", "af": "ipv4", "metric": 10},
                {"interface_name": "Gi0/1", "af": "ipv4"},
            ],
            "processes": [{"process_tag": "1", "is_type": "level-1"}, {"process_tag": "2"}],
        },
    )
    # Keep Gi0/0 (changed metric) + process 1 (changed is_type); drop Gi0/1 + process 2.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={
            "interfaces": [{"interface_name": "Gi0/0", "af": "ipv4", "metric": 99}],
            "processes": [{"process_tag": "1", "is_type": "level-2"}],
        },
    )
    assert resp.status_code == 200
    assert resp.json()["interface_count"] == 1 and resp.json()["process_count"] == 1

    async for db in get_session():
        ifaces = (
            (await db.execute(select(IsisInterfaceIntent).where(IsisInterfaceIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        procs = (
            (await db.execute(select(IsisProcessIntent).where(IsisProcessIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        break
    assert {(i.interface_name, i.af): i.metric for i in ifaces} == {("Gi0/0", "ipv4"): 99}  # Gi0/1 dropped, updated
    assert {p.process_tag: p.is_type for p in procs} == {"1": "level-2"}  # process 2 dropped, updated


async def _isis_removal_jobs(device_id: int):
    """All queued IS-IS ``removal`` jobs for this device (real Job rows)."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobType

    async for db in get_session():
        jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        return [j for j in jobs if (j.context or {}).get("scope") == "isis"]
    return []


@pytest.mark.anyio
async def test_clearing_owned_scalar_enqueues_isis_removal(adapter_client):
    """Clearing a previously-set owned scalar (metric back to blank) queues an ``isis``
    removal job — a merge-PATCH apply would never drop the metric leaf on-device."""
    device_id = await seed_device(nso_device_name="isis-clear", netbox_device_id=987)
    # Own an interface with metric 10 (accepted, applied).
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={"interfaces": [{"interface_name": "system", "af": "ipv4", "metric": 10, "passive": True}]},
    )
    assert await _isis_removal_jobs(device_id) == []  # the initial add is not a retraction

    # Clear the metric (omitted → None) while keeping the interface owned.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={"interfaces": [{"interface_name": "system", "af": "ipv4", "passive": True}]},
    )
    assert resp.status_code == 200
    jobs = await _isis_removal_jobs(device_id)
    assert len(jobs) == 1, "clearing an owned metric must enqueue an isis removal (PUT-replace)"


@pytest.mark.anyio
async def test_deleting_owned_interface_enqueues_isis_removal(adapter_client):
    """Dropping a previously-owned interface from the full-replace queues an ``isis`` removal."""
    device_id = await seed_device(nso_device_name="isis-del", netbox_device_id=988)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={
            "interfaces": [
                {"interface_name": "system", "af": "ipv4", "metric": 10, "passive": True},
                {"interface_name": "lag1", "af": "ipv4", "metric": 5, "passive": False},
            ]
        },
    )
    # Drop lag1.
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={"interfaces": [{"interface_name": "system", "af": "ipv4", "metric": 10, "passive": True}]},
    )
    assert len(await _isis_removal_jobs(device_id)) == 1


@pytest.mark.anyio
async def test_deleting_rows_threads_removed_keys_into_removal_context(adapter_client):
    """The removal job must know WHAT this PUT just deleted so its collateral guard
    can tell an intended retraction from PATCH-no-op-era orphans (the ra1 lo0
    incident: orphaned service rows were silently flushed off the live device)."""
    device_id = await seed_device(nso_device_name="isis-ctx", netbox_device_id=990)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={
            "interfaces": [
                {"interface_name": "system", "af": "ipv4", "passive": True},
                {"interface_name": "lag1", "af": "ipv4", "metric": 5, "passive": False},
            ]
        },
    )
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={"interfaces": [{"interface_name": "system", "af": "ipv4", "passive": True}]},
    )
    (job,) = await _isis_removal_jobs(device_id)
    assert job.context["removed"] == {"interface-config": [["lag1", "ipv4"]]}


@pytest.mark.anyio
async def test_pure_add_or_widen_does_not_enqueue_isis_removal(adapter_client):
    """A pure add / a set-from-blank (None→value) is NOT a retraction → no removal job.

    Adds/updates ride the normal (merge-PATCH) apply; only a drop/clear needs the
    PUT-replace, so we must not churn a device commit on every additive edit."""
    device_id = await seed_device(nso_device_name="isis-add", netbox_device_id=989)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={"interfaces": [{"interface_name": "system", "af": "ipv4", "passive": True}]},
    )
    # Set the metric from blank → 20 (widen) and add a new interface — both additive.
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={
            "interfaces": [
                {"interface_name": "system", "af": "ipv4", "metric": 20, "passive": True},
                {"interface_name": "lag1", "af": "ipv4", "metric": 5, "passive": False},
            ]
        },
    )
    assert await _isis_removal_jobs(device_id) == []


@pytest.mark.anyio
async def test_put_isis_intent_levels_create_and_full_replace(adapter_client):
    """Per-level process tuning rides the process entries ('levels') and lands in
    IsisLevelIntent rows keyed (device, process_tag, level) with full-replace
    semantics — a second PUT without a level deletes its row."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import IsisLevelIntent

    device_id = await seed_device(nso_device_name="isis-levels", netbox_device_id=982)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={
            "interfaces": [],
            "processes": [
                {
                    "process_tag": "",
                    "levels": [
                        {"level": 2, "wide_metrics_only": True, "labeled_preference": 7},
                        {"level": 1, "disabled": True},
                    ],
                }
            ],
        },
    )
    assert resp.status_code == 200

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={
            "interfaces": [],
            "processes": [{"process_tag": "", "levels": [{"level": 2, "labeled_preference": 9}]}],
        },
    )
    assert resp.status_code == 200

    async for db in get_session():
        rows = (await db.execute(select(IsisLevelIntent).where(IsisLevelIntent.device_id == device_id))).scalars().all()
        break
    assert {(r.process_tag, r.level): (r.wide_metrics_only, r.labeled_preference, r.disabled) for r in rows} == {
        ("", 2): (None, 9, None)  # level 1 dropped; level 2 updated
    }


@pytest.mark.anyio
async def test_put_isis_intent_redistribution_create_and_replace(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import RedistributionIntent

    device_id = await seed_device(nso_device_name="isis-redist", netbox_device_id=982)

    def _body(redist):
        return {
            "interfaces": [],
            "processes": [{"process_tag": "1", "redistribution": redist}],
        }

    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json=_body(
            [
                {
                    "source_protocol": "bgp",
                    "source_ref": "65001",
                    "route_map": "RM-A",
                    "metric": 10,
                    "metric_type": "1",
                },
                {"source_protocol": "static", "source_ref": "", "route_map": None},
            ]
        ),
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json=_body([{"source_protocol": "bgp", "source_ref": "65001", "route_map": "RM-B", "metric": 50}]),
    )
    assert resp.status_code == 200

    async for db in get_session():
        rows = (
            (await db.execute(select(RedistributionIntent).where(RedistributionIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        break
    assert len(rows) == 1  # static dropped
    assert rows[0].dest_protocol == "isis" and rows[0].dest_ref == "1"
    assert (rows[0].source_protocol, rows[0].route_map, rows[0].metric) == ("bgp", "RM-B", 50)  # updated in place


@pytest.mark.anyio
async def test_put_isis_intent_auto_apply_enqueues_job(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="isis-auto", netbox_device_id=983)
    await _seed_settings(device_id, auto_apply=True)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={"interfaces": [{"interface_name": "Gi0/0", "af": "ipv4"}], "processes": []},
    )
    assert resp.status_code == 200

    async for db in get_session():
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        ).scalar_one_or_none()
        break
    assert job is not None


# ── PUT /isis-flex-algo-intent ────────────────────────────────────────────────


@pytest.mark.anyio
async def test_put_flex_algo_device_not_found(adapter_client):
    resp = await adapter_client.put(
        "/api/v1/devices/99999/isis-flex-algo-intent", headers=AUTH, json={"flex_algos": []}
    )
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_put_flex_algo_creates_and_updates_in_place(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import IsisFlexAlgoIntent

    device_id = await seed_device(nso_device_name="isis-flex", netbox_device_id=984)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent",
        headers=AUTH,
        json={"flex_algos": [{"process_tag": "1", "algo_id": 128, "priority": 100, "metric_type": "delay"}]},
    )
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, "flex_algo_count": 1, "service_replaced": False}

    # Re-PUT same (process_tag, algo_id) with a changed priority → updated in place.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent",
        headers=AUTH,
        json={"flex_algos": [{"process_tag": "1", "algo_id": 128, "priority": 200, "metric_type": "delay"}]},
    )
    assert resp.status_code == 200

    async for db in get_session():
        rows = (
            (await db.execute(select(IsisFlexAlgoIntent).where(IsisFlexAlgoIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        break
    assert len(rows) == 1  # not duplicated
    assert rows[0].priority == 200


@pytest.mark.anyio
async def test_put_flex_algo_auto_apply_enqueues_job(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="isis-flex-auto", netbox_device_id=985)
    await _seed_settings(device_id, auto_apply=True)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent",
        headers=AUTH,
        json={"flex_algos": [{"process_tag": "1", "algo_id": 128}]},
    )
    assert resp.status_code == 200

    async for db in get_session():
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        ).scalar_one_or_none()
        break
    assert job is not None


@pytest.mark.anyio
async def test_put_flex_algo_removal_triggers_service_replace(adapter_client, monkeypatch):
    """Dropping a flex-algo PUT-replaces the IS-IS service in NSO (service_replaced=True)."""
    device_id = await seed_device(nso_device_name="isis-flex-removal", netbox_device_id=986)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent",
        headers=AUTH,
        json={"flex_algos": [{"process_tag": "1", "algo_id": 128}]},
    )

    captured = {}

    async def _fake_replace(client, device_name, interfaces, processes):
        captured["device_name"] = device_name

    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda instance: object())
    monkeypatch.setattr("nso_adapter.nso.apply.replace_isis_service", _fake_replace)

    # PUT an empty set → the seeded flex-algo is removed → service replace fires.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent",
        headers=AUTH,
        json={"flex_algos": []},
    )
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, "flex_algo_count": 0, "service_replaced": True}
    assert captured["device_name"] == "isis-flex-removal"


@pytest.mark.anyio
async def test_put_flex_algo_removal_replace_failure_is_swallowed(adapter_client):
    """If the NSO service replace fails (instance unregistered), service_replaced=False, no raise."""
    device_id = await seed_device(nso_device_name="isis-flex-fail", netbox_device_id=987)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent",
        headers=AUTH,
        json={"flex_algos": [{"process_tag": "1", "algo_id": 128}]},
    )
    # No NSO client registered for 'nso-dev' → get_nso_client raises → caught.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent",
        headers=AUTH,
        json={"flex_algos": []},
    )
    assert resp.status_code == 200
    assert resp.json()["service_replaced"] is False


@pytest.mark.anyio
async def test_put_isis_intent_stores_bfd_enabled(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import IsisInterfaceIntent

    device_id = await seed_device(nso_device_name="isis-bfd", netbox_device_id=990)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={"interfaces": [{"interface_name": "Gi0/0", "af": "ipv4", "bfd_enabled": True, "passive": False}]},
    )
    assert resp.status_code == 200
    async for db in get_session():
        row = (
            (await db.execute(select(IsisInterfaceIntent).where(IsisInterfaceIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        break
    assert row.bfd_enabled is True


@pytest.mark.anyio
async def test_clearing_bfd_enabled_enqueues_isis_removal(adapter_client):
    """Clearing an owned bfd_enabled (True -> omitted/None) must retract BFD from the
    device — a merge-PATCH can't drop it, so an isis removal (PUT-replace) is queued."""
    device_id = await seed_device(nso_device_name="isis-bfd-clear", netbox_device_id=991)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={"interfaces": [{"interface_name": "Gi0/0", "af": "ipv4", "bfd_enabled": True, "passive": False}]},
    )
    assert await _isis_removal_jobs(device_id) == []  # the initial enable is not a retraction
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-interface-intent",
        headers=AUTH,
        json={"interfaces": [{"interface_name": "Gi0/0", "af": "ipv4", "passive": False}]},  # bfd_enabled omitted
    )
    assert resp.status_code == 200
    assert len(await _isis_removal_jobs(device_id)) == 1
