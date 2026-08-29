# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""End-to-end tests for PUT /api/v1/devices/{id}/snmp-intent.

These drive the real FastAPI route through the real PostgreSQL-backed session
(``adapter_client`` + ``get_db``), exercising the full-replace upsert, the
auto-apply enqueue, and the removal-propagation re-apply. Only the NSO HTTP
boundary (``get_nso_client`` / ``apply_snmp_config``) is faked, and only in the
one test that asserts the on-device revert is attempted.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import (
    SnmpCommunityIntent,
    SnmpHostIntent,
    SnmpSystemInfoIntent,
    SnmpV3UserIntent,
)
from tests.conftest import VALID_TOKEN, push_seq, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


def _full_body() -> dict:
    return {
        "communities": [
            {"label": "ro1", "vault_ref": "snmp/ro#community", "access": "RO", "acl": "20"},
            {"label": "rw1", "vault_ref": "snmp/rw#community", "access": "RW"},
        ],
        "v3_users": [
            {"username": "monitor", "auth_vault_ref": "snmp/v3#auth", "priv_vault_ref": "snmp/v3#priv"},
        ],
        "hosts": [
            {"address": "10.0.1.100", "version": "2c", "notify_type": "trap", "community_or_user": "ro1"},
        ],
        "system_info": {"location": "ITC-Lab", "contact": "noc@example.com"},
    }


async def _read_intent(device_id: int):
    """Return (communities, v3_users, hosts, system_info) intent rows for a device."""

    async with session() as db:
        comms = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        users = (
            (await db.execute(select(SnmpV3UserIntent).where(SnmpV3UserIntent.device_id == device_id))).scalars().all()
        )
        hosts = (await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))).scalars().all()
        sysinfo = (
            await db.execute(select(SnmpSystemInfoIntent).where(SnmpSystemInfoIntent.device_id == device_id))
        ).scalar_one_or_none()
        return comms, users, hosts, sysinfo


# ── boundary validation: a ref/enum the writer can never render is rejected HERE ──
#
# apply_snmp_config hard-fails any vault_ref that is not mount/path#key and any enum
# spelling it cannot map. Both fields were unvalidated `str`, so the PUT returned 200 and
# the bad value sat in the store failing EVERY apply forever — and with atomic apply on,
# taking the whole job (interfaces, IPs, BGP, IS-IS) down with it. The apply-diff preview
# swallowed the error too, so the operator got no warning before hitting Apply. Reject at
# the boundary: the store must never hold intent the writer cannot render.


@pytest.mark.anyio
@pytest.mark.parametrize(
    "bad_ref",
    [
        "snmp/ro",  # no '#key' — the mandatory vault triple cannot be built
        "no-mount#community",  # no '/' — mount cannot be determined
        "snmp/ro#a#b",  # more than one '#'
        "snmp/ro#",  # empty key after '#'
        "snmp/ ro#community",  # whitespace
    ],
)
async def test_put_rejects_a_community_vault_ref_the_writer_cannot_render(adapter_client, bad_ref):
    device_id = await seed_device(nso_device_name=f"snmp-badref-{abs(hash(bad_ref)) % 9999}", netbox_device_id=970)
    body = _full_body()
    body["communities"] = [{"label": "ro1", "vault_ref": bad_ref, "access": "RO"}]

    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=body, headers=AUTH | push_seq())

    assert resp.status_code == 422
    comms, _, _, _ = await _read_intent(device_id)
    assert comms == []  # nothing was stored


@pytest.mark.anyio
async def test_put_rejects_an_unmappable_host_version(adapter_client):
    device_id = await seed_device(nso_device_name="snmp-badver", netbox_device_id=971)
    body = _full_body()
    body["hosts"] = [{"address": "10.0.1.100", "version": "9", "notify_type": "trap", "community_or_user": "ro1"}]

    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=body, headers=AUTH | push_seq())

    assert resp.status_code == 422


@pytest.mark.anyio
async def test_put_accepts_the_bare_2_version_spelling(adapter_client):
    """ "2" is a legitimate v2c spelling — the writer maps it, so the API must take it."""
    device_id = await seed_device(nso_device_name="snmp-ver2", netbox_device_id=972)
    body = _full_body()
    body["hosts"] = [{"address": "10.0.1.100", "version": "2", "notify_type": "trap", "community_or_user": "ro1"}]

    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=body, headers=AUTH | push_seq())

    assert resp.status_code == 200
    _, _, hosts, _ = await _read_intent(device_id)
    assert hosts[0].version == "2"


# ── happy path ──────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_put_creates_all_collections(adapter_client):
    device_id = await seed_device(nso_device_name="snmp-put-dev", netbox_device_id=960)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/snmp-intent", json=_full_body(), headers=AUTH | push_seq()
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["community_count"] == 2
    assert body["v3_user_count"] == 1
    assert body["host_count"] == 1
    assert body["has_system_info"] is True

    comms, users, hosts, sysinfo = await _read_intent(device_id)
    assert {c.label: (c.access, c.acl, c.vault_ref) for c in comms} == {
        "ro1": ("RO", "20", "snmp/ro#community"),
        "rw1": ("RW", None, "snmp/rw#community"),
    }
    assert {u.username: (u.auth_vault_ref, u.priv_vault_ref) for u in users} == {
        "monitor": ("snmp/v3#auth", "snmp/v3#priv")
    }
    assert {h.address: (h.version, h.notify_type, h.community_or_user) for h in hosts} == {
        "10.0.1.100": ("2c", "trap", "ro1")
    }
    assert (sysinfo.location, sysinfo.contact) == ("ITC-Lab", "noc@example.com")
    # accepted_at defaulted to ~now (naive UTC) for every row
    assert all(c.accepted_at is not None for c in comms)


@pytest.mark.anyio
async def test_v3_protocol_and_host_port_fields_roundtrip(adapter_client):
    """v3 users without protocols are unusable on-device (the reconciler skips
    auth/priv entirely) — the intent must carry auth/priv protocols and group,
    and hosts must carry the optional UDP port."""
    device_id = await seed_device(nso_device_name="snmp-v3proto-dev", netbox_device_id=962)
    body = {
        "v3_users": [
            {
                "username": "monitor",
                "group": "v3-test-group",
                "auth_protocol": "sha-256",
                "priv_protocol": "aes-128",
                "auth_vault_ref": "network/netbox/snmp/v3/monitor#auth",
                "priv_vault_ref": "network/netbox/snmp/v3/monitor#priv",
            },
        ],
        "hosts": [
            {
                "address": "10.0.1.100",
                "version": "2c",
                "notify_type": "trap",
                "community_or_user": "ro1",
                "port": 1162,
            },
        ],
    }

    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=body, headers=AUTH | push_seq())
    assert resp.status_code == 200

    _, users, hosts, _ = await _read_intent(device_id)
    assert (users[0].group_name, users[0].auth_protocol, users[0].priv_protocol) == (
        "v3-test-group",
        "sha-256",
        "aes-128",
    )
    assert hosts[0].port == 1162


@pytest.mark.anyio
async def test_put_unknown_device_404(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/9999/snmp-intent", json=_full_body(), headers=AUTH | push_seq())
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_put_requires_auth(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/1/snmp-intent", json=_full_body())
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_put_explicit_accepted_at_is_preserved(adapter_client):
    device_id = await seed_device(nso_device_name="snmp-acc-dev", netbox_device_id=961)
    body = {
        "communities": [
            {
                "label": "ro1",
                "vault_ref": "snmp/ro#c",
                "access": "RO",
                "accepted_at": "2026-06-01T12:00:00Z",
            }
        ],
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=body, headers=AUTH | push_seq())
    assert resp.status_code == 200

    comms, *_ = await _read_intent(device_id)
    assert comms[0].accepted_at == datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)  # the wire 'Z' is kept as UTC


# ── full-replace semantics ───────────────────────────────────────────────────


@pytest.mark.anyio
async def test_put_updates_existing_rows_in_place(adapter_client):
    device_id = await seed_device(nso_device_name="snmp-upd-dev", netbox_device_id=962)
    await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=_full_body(), headers=AUTH | push_seq())

    # Re-PUT same keys with changed fields → updated, not duplicated.
    changed = _full_body()
    changed["communities"][0]["access"] = "RW"
    changed["communities"][0]["acl"] = "99"
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=changed, headers=AUTH | push_seq())
    assert resp.status_code == 200

    comms, *_ = await _read_intent(device_id)
    assert len(comms) == 2  # no duplicate ro1
    ro1 = next(c for c in comms if c.label == "ro1")
    assert (ro1.access, ro1.acl) == ("RW", "99")


@pytest.mark.anyio
async def test_put_drops_absent_rows(adapter_client):
    device_id = await seed_device(nso_device_name="snmp-drop-dev", netbox_device_id=963)
    await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=_full_body(), headers=AUTH | push_seq())

    # PUT a body keeping only ro1 + dropping users/hosts/system_info entirely.
    trimmed = {
        "communities": [{"label": "ro1", "vault_ref": "snmp/ro#community", "access": "RO", "acl": "20"}],
        "v3_users": [],
        "hosts": [],
        "system_info": None,
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=trimmed, headers=AUTH | push_seq())
    assert resp.status_code == 200
    assert resp.json() == {
        "device_id": device_id,
        "community_count": 1,
        "v3_user_count": 0,
        "host_count": 0,
        "has_system_info": False,
        "updated_at": resp.json()["updated_at"],
    }

    comms, users, hosts, sysinfo = await _read_intent(device_id)
    assert [c.label for c in comms] == ["ro1"]  # rw1 deleted
    assert users == [] and hosts == [] and sysinfo is None


@pytest.mark.anyio
async def test_put_system_info_null_deletes_existing(adapter_client):
    device_id = await seed_device(nso_device_name="snmp-sys-dev", netbox_device_id=964)
    await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=_full_body(), headers=AUTH | push_seq())

    keep_rest = _full_body()
    keep_rest["system_info"] = None
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/snmp-intent", json=keep_rest, headers=AUTH | push_seq()
    )
    assert resp.status_code == 200
    assert resp.json()["has_system_info"] is False

    _, _, _, sysinfo = await _read_intent(device_id)
    assert sysinfo is None


# ── auto-apply + removal propagation ─────────────────────────────────────────


async def _seed_settings(device_id: int, *, auto_apply: bool):
    from nso_adapter.store.models import DeviceSettings

    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=auto_apply))
        await db.commit()
        return


@pytest.mark.anyio
async def test_put_auto_apply_enqueues_job(adapter_client):
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="snmp-auto-dev", netbox_device_id=965)
    await _seed_settings(device_id, auto_apply=True)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/snmp-intent", json=_full_body(), headers=AUTH | push_seq()
    )
    assert resp.status_code == 200

    async with session() as db:
        jobs = (await db.execute(select(Job).where(Job.device_id == device_id))).scalars().all()
    assert len(jobs) == 1
    assert jobs[0].job_type == JobType.apply
    assert jobs[0].status == JobStatus.queued


@pytest.mark.anyio
async def test_put_no_auto_apply_enqueues_nothing(adapter_client):
    from nso_adapter.store.models import Job

    device_id = await seed_device(nso_device_name="snmp-noauto-dev", netbox_device_id=966)
    # auto_apply defaults off (no DeviceSettings row at all)

    await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=_full_body(), headers=AUTH | push_seq())

    async with session() as db:
        jobs = (await db.execute(select(Job).where(Job.device_id == device_id))).scalars().all()
    assert jobs == []


@pytest.mark.anyio
async def test_put_removal_enqueues_async_removal_job(adapter_client, monkeypatch):
    """s3-23: dropping a row must ENQUEUE an async removal job (not block the PUT on an inline
    replace-mode device commit that can stall past the plugin timeout). The worker then
    PUT-replaces the snmp service with the remaining accepted intent (replace=True)."""
    from nso_adapter.core.removal import run_removal
    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="snmp-prop-dev", netbox_device_id=967)

    captured = {}

    def _fake_get_client(instance):
        # The removal worker's collateral guard reads the current service instance;
        # None (no instance in NSO) short-circuits the guard to a plain replace.
        client = AsyncMock(spec=NsoClient)
        client.get_service_config.return_value = None
        return client

    async def _fake_apply(client, device_name, comms, users, hosts, sysinfo, replace):
        captured["device_name"] = device_name
        captured["replace"] = replace
        captured["labels"] = [c.label for c in comms]

    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", _fake_get_client)
    monkeypatch.setattr("nso_adapter.nso.apply.apply_snmp_config", _fake_apply)

    await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=_full_body(), headers=AUTH | push_seq())
    # No device call during a pure-add PUT.
    assert captured == {}

    # Drop rw1 → a removal → an async removal job is queued (the PUT does NOT call the device).
    trimmed = _full_body()
    trimmed["communities"] = [trimmed["communities"][0]]  # keep ro1 only
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=trimmed, headers=AUTH | push_seq())
    assert resp.status_code == 200
    assert captured == {}  # still no inline device commit — deferred to the worker

    async with session() as db:
        jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        assert len(jobs) == 1
        # community rw1 was just dropped — threaded for the collateral guard, and (CR-A17) with the
        # dropped row's vault_ref alongside it. The intent ROW is deleted by this same PUT, so if
        # the ref is not lifted out here, the worker has no way back to the secret — and no way to
        # compute the sha256 the device export keys the community by, which is the only thing that
        # can answer "did it actually leave the router?". A vault_ref is a PATH, not a secret; the
        # same value already sits in plaintext in snmp_community_intent and in the push payload.
        assert jobs[0].context == {
            "scope": "snmp",
            "removed": {"community": ["rw1"]},
            "vault_refs": {"rw1": "snmp/rw#community"},
            "detach": True,
        }
        job_id = jobs[0].id

    # The worker runs the removal → PUT-replaces with the remaining intent.
    await run_removal(job_id, device_id)
    assert captured["replace"] is True
    assert captured["device_name"] == "snmp-prop-dev"
    assert captured["labels"] == ["ro1"]  # rw1 gone from the re-applied set


@pytest.mark.anyio
async def test_put_no_removal_enqueues_nothing(adapter_client):
    """A pure-add/update PUT (no removals) must NOT enqueue a removal job."""
    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="snmp-norm-dev", netbox_device_id=968)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/snmp-intent", json=_full_body(), headers=AUTH | push_seq()
    )
    assert resp.status_code == 200

    async with session() as db:
        jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        assert jobs == []  # first write, nothing removed → no removal job
