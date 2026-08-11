# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""End-to-end tests for the ``?store_only=true`` request flag (tracker #103).

The plugin's "Re-sync adapter intent" (NSOIntentResyncView) promises it never touches the
device: it re-pushes the owned snapshot only to reconcile the adapter's intent STORE with
NetBox ownership. Without the flag, that shrinking PUT auto-enqueued a removal job whose
PUT-replace retracted FASTMAP-owned config from the real device (ra1.lab, removal job
31686). A store-only request must therefore suppress every device-touching job enqueue —
removal (both the ``replace_on_removal`` path and the direct ``enqueue_removal`` path) and
auto-apply — while the store full-replace still happens.

These drive the real FastAPI routes through the real PostgreSQL-backed session
(``adapter_client`` + ``get_db``); nothing NSO-facing is needed because the whole point
is that NO device-touching job may ever be created.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, push_seq, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _jobs(device_id: int, job_type=None) -> list:
    from nso_adapter.store.models import Job

    async with session() as db:
        stmt = select(Job).where(Job.device_id == device_id)
        if job_type is not None:
            stmt = stmt.where(Job.job_type == job_type)
        return (await db.execute(stmt)).scalars().all()
    return []


async def _logging_rows(device_id: int) -> list[str]:
    from nso_adapter.store.models import LoggingHostIntent

    async with session() as db:
        rows = (
            (await db.execute(select(LoggingHostIntent).where(LoggingHostIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return sorted(r.address for r in rows)
    return []


async def _flex_algo_keys(device_id: int) -> list[tuple[str, int]]:
    from nso_adapter.store.models import IsisFlexAlgoIntent

    async with session() as db:
        rows = (
            (await db.execute(select(IsisFlexAlgoIntent).where(IsisFlexAlgoIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return sorted((r.process_tag, r.algo_id) for r in rows)
    return []


async def _seed_settings(device_id: int, *, auto_apply: bool) -> None:
    from nso_adapter.store.models import DeviceSettings

    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=auto_apply))
        await db.commit()
        return


def _snmp_body() -> dict:
    return {
        "communities": [
            {"label": "ro1", "vault_ref": "snmp/ro#community", "access": "RO"},
            {"label": "rw1", "vault_ref": "snmp/rw#community", "access": "RW"},
        ],
        "v3_users": [],
        "hosts": [],
        "system_info": None,
    }


# ── control: without the flag a shrink still enqueues the removal ────────────


@pytest.mark.anyio
async def test_logging_shrink_without_flag_enqueues_removal(adapter_client):
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="so-ctl-dev", netbox_device_id=980)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.9.0.1"}, {"address": "10.9.0.2"}]},
        headers=AUTH | push_seq(),
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.9.0.2"}]},
        headers=AUTH | push_seq(),
    )
    assert resp.status_code == 200
    assert resp.json()["replaced"] is True
    assert len(await _jobs(device_id, JobType.removal)) == 1


# ── store_only suppresses the removal enqueue (replace_on_removal path) ─────


@pytest.mark.anyio
async def test_logging_shrink_store_only_skips_removal_but_updates_store(adapter_client):
    device_id = await seed_device(nso_device_name="so-log-dev", netbox_device_id=981)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.9.1.1"}, {"address": "10.9.1.2"}]},
        headers=AUTH | push_seq(),
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent?store_only=true",
        json={"hosts": [{"address": "10.9.1.2"}]},
        headers=AUTH | push_seq(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["removed"] == 1  # the store shrink DID happen …
    assert body["replaced"] is False  # … but no removal job was enqueued
    assert await _logging_rows(device_id) == ["10.9.1.2"]
    assert await _jobs(device_id) == []


@pytest.mark.anyio
async def test_logging_clear_store_only_skips_removal(adapter_client):
    """The re-sync of a fully orphaned scope pushes an EMPTY snapshot — still no job."""
    device_id = await seed_device(nso_device_name="so-empty-dev", netbox_device_id=982)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.9.2.1"}]},
        headers=AUTH | push_seq(),
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent?store_only=true",
        json={"hosts": []},
        headers=AUTH | push_seq(),
    )
    assert resp.status_code == 200
    assert await _logging_rows(device_id) == []
    assert await _jobs(device_id) == []


# ── store_only suppresses the DIRECT enqueue_removal path (snmp) ────────────


@pytest.mark.anyio
async def test_snmp_shrink_store_only_skips_direct_removal(adapter_client):
    from nso_adapter.store.models import SnmpCommunityIntent

    device_id = await seed_device(nso_device_name="so-snmp-dev", netbox_device_id=983)
    await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=_snmp_body(), headers=AUTH | push_seq())

    trimmed = _snmp_body()
    trimmed["communities"] = [trimmed["communities"][0]]  # drop rw1
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/snmp-intent?store_only=true", json=trimmed, headers=AUTH | push_seq()
    )
    assert resp.status_code == 200

    async with session() as db:
        labels = sorted(
            r.label
            for r in (
                (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
                .scalars()
                .all()
            )
        )
    assert labels == ["ro1"]  # store shrank
    assert await _jobs(device_id) == []  # no removal job


# ── store_only suppresses the flex-algo INLINE service replace ──────────────


@pytest.mark.anyio
async def test_flex_algo_shrink_store_only_does_not_touch_the_device(adapter_client, monkeypatch):
    """The flex-algo PUT must honour store_only like every other intent endpoint.

    It used to PUT-replace the isis-reconciler INLINE (bypassing the enqueue choke point
    where STORE_ONLY is guarded), so the plugin's "does not touch the device" re-sync —
    which re-pushes every scope, isis_flex_algo included, under ?store_only=true — played
    FASTMAP's reverse diff against the live router and retracted real IS-IS config.

    The inline path swallows every exception, so a raising sentinel would be masked:
    RECORD the NSO boundary crossings instead and assert there were none.
    """
    touched: list = []

    monkeypatch.setattr(
        "nso_adapter.core.importer.get_nso_client",
        lambda instance: touched.append(instance) or object(),
    )

    device_id = await seed_device(nso_device_name="so-flex-dev", netbox_device_id=987)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent",
        json={"flex_algos": [{"process_tag": "1", "algo_id": 128}, {"process_tag": "1", "algo_id": 129}]},
        headers=AUTH | push_seq(),
    )

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/isis-flex-algo-intent?store_only=true",
        json={"flex_algos": [{"process_tag": "1", "algo_id": 129}]},
        headers=AUTH | push_seq(),
    )
    assert resp.status_code == 200
    assert resp.json()["flex_algo_count"] == 1

    assert await _flex_algo_keys(device_id) == [("1", 129)]  # the store shrink DID happen …
    assert touched == []  # … but the device was never reached …
    assert await _jobs(device_id) == []  # … and no device-touching job was queued


# ── store_only suppresses the auto-apply enqueue ────────────────────────────


@pytest.mark.anyio
async def test_store_only_skips_auto_apply(adapter_client):
    device_id = await seed_device(nso_device_name="so-auto-dev", netbox_device_id=984)
    await _seed_settings(device_id, auto_apply=True)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent?store_only=true",
        json={"hosts": [{"address": "10.9.3.1"}]},
        headers=AUTH | push_seq(),
    )
    assert resp.status_code == 200
    assert await _logging_rows(device_id) == ["10.9.3.1"]
    assert await _jobs(device_id) == []


@pytest.mark.anyio
async def test_store_only_manual_apply_is_rejected_without_a_job(adapter_client):
    """Manual Apply is a device write. It cannot run under a store-only request."""
    device_id = await seed_device(nso_device_name="so-manual-apply", netbox_device_id=988)
    stored = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent?store_only=true",
        json={"vlans": [{"vlan_id": 10, "name": "ten"}]},
        headers=AUTH,
    )
    assert stored.status_code == 200, stored.text

    response = await adapter_client.post(f"/api/v1/devices/{device_id}/actions/apply?store_only=true", headers=AUTH)

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "validation_error",
            "message": "store_only is not valid for the Apply action",
            "detail": {},
        }
    }
    assert await _jobs(device_id) == []


# ── flag parsing + the force-removal exemption ──────────────────────────────


@pytest.mark.anyio
async def test_store_only_false_behaves_like_absent(adapter_client):
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="so-false-dev", netbox_device_id=985)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.9.4.1"}, {"address": "10.9.4.2"}]},
        headers=AUTH | push_seq(),
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent?store_only=false",
        json={"hosts": [{"address": "10.9.4.2"}]},
        headers=AUTH | push_seq(),
    )
    assert resp.status_code == 200
    assert len(await _jobs(device_id, JobType.removal)) == 1


@pytest.mark.anyio
async def test_force_removal_action_exempt_from_store_only(adapter_client):
    """The operator force-removal is an explicit 'flush the device' override — a stray
    store_only flag must not turn it into a job-less no-op (its response needs job_id)."""
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="so-force-dev", netbox_device_id=986)
    resp = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/force-removal?store_only=true",
        json={"scope": "logging"},
        headers=AUTH,
    )
    assert resp.status_code == 202
    jobs = await _jobs(device_id, JobType.removal)
    assert len(jobs) == 1
    assert jobs[0].context.get("force") is True
