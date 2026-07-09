# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""End-to-end tests for PUT /api/v1/devices/{id}/route-policy-intent.

Drive the real route through the real SQLite session (``adapter_client`` +
``get_db``): full-replace upsert of per-object route-policy intent, the
validation 422s, and the removal-propagation that enqueues a `removal` job.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


def _obj(family: str, name: str, *, entries=None, accepted=False, invert_match=False) -> dict:
    return {
        "family": family,
        "name": name,
        "entries": entries if entries is not None else [],
        "accepted": accepted,
        "invert_match": invert_match,
    }


async def _read_intent(device_id: int):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import RoutePolicyObjectIntent

    async for db in get_session():
        return (
            (await db.execute(select(RoutePolicyObjectIntent).where(RoutePolicyObjectIntent.device_id == device_id)))
            .scalars()
            .all()
        )
    raise RuntimeError("no session")


# ── validation ────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_put_route_policy_intent_device_not_found(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/99999/route-policy-intent", headers=AUTH, json={"objects": []})
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_put_route_policy_intent_requires_objects_list(adapter_client):
    device_id = await seed_device(nso_device_name="rp-noobj", netbox_device_id=7950)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent", headers=AUTH, json={"not_objects": 1}
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "invalid_payload"


@pytest.mark.anyio
async def test_put_route_policy_intent_rejects_unknown_family(adapter_client):
    device_id = await seed_device(nso_device_name="rp-badfam", netbox_device_id=7951)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("bogus_family", "X")]},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "invalid_family"


@pytest.mark.anyio
async def test_put_route_policy_intent_rejects_empty_name(adapter_client):
    device_id = await seed_device(nso_device_name="rp-noname", netbox_device_id=7952)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("prefix_list", "")]},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "invalid_name"


@pytest.mark.anyio
async def test_put_route_policy_intent_rejects_non_list_entries(adapter_client):
    device_id = await seed_device(nso_device_name="rp-badentries", netbox_device_id=7953)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [{"family": "prefix_list", "name": "PL", "entries": "nope"}]},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "invalid_entries"


# ── upsert + response ─────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_put_route_policy_intent_creates_objects(adapter_client):
    device_id = await seed_device(nso_device_name="rp-create", netbox_device_id=7954)
    entries = [{"sequence": 10, "action": "permit", "prefix": "10.0.0.0/8"}]
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={
            "objects": [
                _obj("prefix_list", "PL-1", entries=entries, accepted=True),
                _obj("community_list", "CL-1", accepted=False, invert_match=True),
            ]
        },
    )
    assert resp.status_code == 200
    objs = {o["name"]: o for o in resp.json()["objects"]}
    assert set(objs) == {"PL-1", "CL-1"}
    assert objs["PL-1"]["family"] == "prefix_list"
    assert objs["PL-1"]["entries"] == entries
    assert objs["PL-1"]["accepted_at"] is not None  # accepted=True stamped
    assert objs["CL-1"]["accepted_at"] is None  # accepted=False → unstamped
    assert {"id", "family", "name", "entries", "accepted_at", "last_apply_at", "last_apply_error"} <= set(
        objs["PL-1"].keys()
    )

    rows = await _read_intent(device_id)
    by_name = {r.name: r for r in rows}
    assert by_name["CL-1"].invert_match is True


@pytest.mark.anyio
async def test_put_route_policy_intent_updates_in_place(adapter_client):
    device_id = await seed_device(nso_device_name="rp-update", netbox_device_id=7955)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("prefix_list", "PL-1", entries=[{"sequence": 10}], accepted=False)]},
    )
    # Re-PUT same (family, name) with new entries + accepted now true.
    new_entries = [{"sequence": 20, "action": "deny", "prefix": "0.0.0.0/0"}]
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("prefix_list", "PL-1", entries=new_entries, accepted=True)]},
    )
    assert resp.status_code == 200

    rows = await _read_intent(device_id)
    assert len(rows) == 1  # updated in place, not duplicated
    assert rows[0].entries == new_entries
    assert rows[0].accepted_at is not None  # now stamped


@pytest.mark.anyio
async def test_put_route_policy_intent_full_replace_removes_and_enqueues(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="rp-replace", netbox_device_id=7956)
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("prefix_list", "PL-1"), _obj("as_path", "AP-1")]},
    )
    # Re-PUT keeping only PL-1 → AP-1 dropped → removal propagation.
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("prefix_list", "PL-1")]},
    )
    assert resp.status_code == 200
    assert {o["name"] for o in resp.json()["objects"]} == {"PL-1"}  # AP-1 gone from mirror

    rows = await _read_intent(device_id)
    assert {r.name for r in rows} == {"PL-1"}

    async for db in get_session():
        job = (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal))
        ).scalar_one_or_none()
        break
    assert job is not None
    assert job.context == {"scope": "route_policy"}


@pytest.mark.anyio
async def test_put_route_policy_intent_no_removal_when_nothing_dropped(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="rp-noremoval", netbox_device_id=7957)
    # Two PUTs that only add/keep objects → never a removal.
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("prefix_list", "PL-1")]},
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("prefix_list", "PL-1"), _obj("route_map", "RM-1")]},
    )
    assert resp.status_code == 200

    async for db in get_session():
        jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        break
    assert jobs == []


async def _set_ned(device_id: int, ned_id: str):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device

    async for db in get_session():
        dev = await db.get(Device, device_id)
        dev.ned_id = ned_id
        await db.commit()
        return


@pytest.mark.anyio
async def test_put_reports_unrepresentable_community_members_for_nokia(adapter_client):
    """A community member the device's NED can't hold (wildcard color on Nokia) is
    reported in the PUT response so the plugin flags it "unsupported on <ned>" instead
    of showing a phantom "pending apply" that the codec-skipped member can never clear."""
    device_id = await seed_device(nso_device_name="rp-nokia", netbox_device_id=771)
    await _set_ned(device_id, "timos-nc-23.10")

    entries = [
        {"sequence": 1, "action": "permit", "community": "6830:*"},
        {"sequence": 2, "action": "permit", "community": "color:0:12."},  # unrepresentable
        {"sequence": 3, "action": "permit", "community": "color:0:128"},  # exact color → representable
        {"sequence": 4, "action": "permit", "community": "no-export"},
    ]
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("community_list", "cnad-test", entries=entries, accepted=True)]},
    )
    assert resp.status_code == 200
    assert resp.json()["unsupported_members"] == {"cnad-test": ["color:0:12."]}


@pytest.mark.anyio
async def test_put_reports_no_unrepresentable_members_for_identity_ned(adapter_client):
    """On an identity-dialect NED (IOS-XR) every canonical member is representable —
    the response carries an empty unsupported map (no false "unsupported" badges)."""
    device_id = await seed_device(nso_device_name="rp-iosxr", netbox_device_id=772)
    await _set_ned(device_id, "cisco-iosxr-nc-7.3")

    entries = [{"sequence": 1, "action": "permit", "community": "color:0:12."}]
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [_obj("community_list", "cnad-test", entries=entries, accepted=True)]},
    )
    assert resp.status_code == 200
    assert resp.json()["unsupported_members"] == {}
