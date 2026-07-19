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


# ── behavior matrix: validation PRECEDENCE ──────────────────────────────────────
#
# The endpoint keeps ``body: dict`` (never a Pydantic body model) on purpose: the request
# schema is documented via ``openapi_extra`` so the runtime precedence below is preserved.
# A Pydantic body model would validate the body BEFORE the handler runs, turning every
# "missing device + malformed body" case into a 422 (body validation) instead of the 404 the
# handler produces by checking the device FIRST. These tests pin that order so the OpenAPI
# schema documentation stays decoupled from — and honest about — runtime behavior.


@pytest.mark.anyio
async def test_missing_device_beats_non_list_objects(adapter_client):
    """404 (device) wins over 422 (non-list ``objects``): the device lookup precedes the
    body-shape check. A Pydantic ``objects: list`` body model would 422 here instead."""
    resp = await adapter_client.put(
        "/api/v1/devices/999999/route-policy-intent", headers=AUTH, json={"objects": "not-a-list"}
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_missing_device_beats_bad_per_object(adapter_client):
    """404 (device) wins over per-object 422s (invalid_family/name/entries): the device
    lookup precedes the per-object validation loop."""
    resp = await adapter_client.put(
        "/api/v1/devices/999999/route-policy-intent",
        headers=AUTH,
        json={"objects": [{"family": "bogus", "name": ""}]},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_non_dict_body_is_framework_validation_error(adapter_client):
    """``body: dict`` still enforces a JSON object at the framework layer: a non-object body
    (here a JSON array) is rejected as the S0 ``validation_error`` envelope, matching the
    ``type: object`` requestBody the openapi_extra schema documents."""
    resp = await adapter_client.put("/api/v1/devices/999999/route-policy-intent", headers=AUTH, json=[1, 2, 3])
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


@pytest.mark.anyio
async def test_invalid_family_beats_invalid_name_within_object(adapter_client):
    """Within one object, family is validated before name: a bad family AND an empty name
    reports ``invalid_family`` (not ``invalid_name``)."""
    device_id = await seed_device(nso_device_name="rp-fam-then-name", netbox_device_id=7960)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [{"family": "bogus", "name": ""}]},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "invalid_family"


@pytest.mark.anyio
async def test_invalid_name_beats_invalid_entries_within_object(adapter_client):
    """Within one object, name is validated before entries: an empty name AND non-list
    entries reports ``invalid_name`` (not ``invalid_entries``)."""
    device_id = await seed_device(nso_device_name="rp-name-then-entries", netbox_device_id=7961)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [{"family": "prefix_list", "name": "", "entries": "nope"}]},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "invalid_name"


@pytest.mark.anyio
async def test_primitive_list_items_rejected_as_invalid_payload(adapter_client):
    """A non-object item in ``objects`` (e.g. ``{"objects": [1, 2]}``) is rejected with a clean
    422 ``invalid_payload`` before any DB mutation — previously ``o.get("family")`` was called on
    an int while building the full-replace key set, an unhandled AttributeError (500)."""
    device_id = await seed_device(nso_device_name="rp-primitive-items", netbox_device_id=7962)
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent",
        headers=AUTH,
        json={"objects": [1, 2]},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "invalid_payload"


@pytest.mark.anyio
async def test_missing_device_beats_primitive_list_items(adapter_client):
    """404 (device) still wins over the non-object-item 422: the device lookup precedes the
    per-item shape check, exactly as it does for the non-list-objects case."""
    resp = await adapter_client.put(
        "/api/v1/devices/999999/route-policy-intent", headers=AUTH, json={"objects": [1, 2]}
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


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
    # as-path AP-1 was just dropped — threaded (per-family list) for the collateral guard
    assert job.context == {"scope": "route_policy", "removed": {"as-path": ["AP-1"]}, "detach": True}


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


# ── entry-level shrink must retract (#83, the route-policy leg) ───────────────
#
# The full-replace diff is keyed on (family, name), so it only ever notices an object that
# vanished WHOLE. Deleting one route-map TERM — or a prefix-list line, or a community member —
# leaves the key untouched and only shrinks `entries`. A merge-PATCH apply cannot drop any of
# that: the deleted term stays in the service's CDB input and FASTMAP keeps creating it, so
# the device went on matching a term the operator had removed in NetBox.


async def _removal_job(device_id: int):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobType

    async for db in get_session():
        return (
            await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal))
        ).scalar_one_or_none()
    return None


async def _put(adapter_client, device_id: int, objects: list[dict]):
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/route-policy-intent", headers=AUTH, json={"objects": objects}
    )
    assert resp.status_code == 200, resp.text
    return resp


@pytest.mark.anyio
async def test_deleting_a_route_map_term_queues_a_retract(adapter_client):
    device_id = await seed_device(nso_device_name="rp-term-drop", netbox_device_id=7970)
    two_terms = [
        {"sequence": 10, "action": "permit", "match": {"prefix-list": "PL-A"}},
        {"sequence": 20, "action": "deny", "match": {"prefix-list": "PL-B"}},
    ]
    await _put(adapter_client, device_id, [_obj("route_map", "RM-1", entries=two_terms, accepted=True)])
    assert await _removal_job(device_id) is None, "the initial push only adds intent"

    # The object survives under the same (family, name) — only term 20 is gone.
    await _put(adapter_client, device_id, [_obj("route_map", "RM-1", entries=two_terms[:1], accepted=True)])

    job = await _removal_job(device_id)
    assert job is not None, "a deleted route-map term must queue a removal — merge-PATCH cannot drop it"
    # The policy is still owned and accepted: only a term went away. So this is a retract that
    # must reach the device, not an un-own that detaches with no-networking (#106).
    assert job.context.get("detach") is not True
    assert job.context.get("retract_deferred") is not True


@pytest.mark.anyio
async def test_dropping_a_community_member_queues_a_retract(adapter_client):
    device_id = await seed_device(nso_device_name="rp-member-drop", netbox_device_id=7971)
    members = [{"community": "64500:100"}, {"community": "64500:200"}]
    await _put(adapter_client, device_id, [_obj("community_list", "CL-1", entries=members, accepted=True)])
    assert await _removal_job(device_id) is None

    await _put(adapter_client, device_id, [_obj("community_list", "CL-1", entries=members[:1], accepted=True)])
    assert await _removal_job(device_id) is not None


@pytest.mark.anyio
async def test_blanking_a_set_clause_inside_a_surviving_term_queues_a_retract(adapter_client):
    """The term still exists and keeps its sequence — it just lost its set-clause leaf."""
    device_id = await seed_device(nso_device_name="rp-set-clear", netbox_device_id=7972)
    with_set = [{"sequence": 10, "action": "permit", "set": {"local-preference": 200}}]
    await _put(adapter_client, device_id, [_obj("route_map", "RM-2", entries=with_set, accepted=True)])
    assert await _removal_job(device_id) is None

    without_set = [{"sequence": 10, "action": "permit"}]
    await _put(adapter_client, device_id, [_obj("route_map", "RM-2", entries=without_set, accepted=True)])
    assert await _removal_job(device_id) is not None


@pytest.mark.anyio
async def test_dropping_one_match_prefix_list_from_a_leaf_list_queues_a_retract(adapter_client):
    """A leaf-list MERGES: [A, B] -> [A] would leave B on the device."""
    device_id = await seed_device(nso_device_name="rp-leaflist-drop", netbox_device_id=7973)
    both = [{"sequence": 10, "action": "permit", "match_prefix_lists": ["PL-A", "PL-B"]}]
    await _put(adapter_client, device_id, [_obj("route_map", "RM-3", entries=both, accepted=True)])
    assert await _removal_job(device_id) is None

    one = [{"sequence": 10, "action": "permit", "match_prefix_lists": ["PL-A"]}]
    await _put(adapter_client, device_id, [_obj("route_map", "RM-3", entries=one, accepted=True)])
    assert await _removal_job(device_id) is not None


@pytest.mark.anyio
async def test_growing_or_republishing_a_policy_queues_nothing(adapter_client):
    """The guard against the opposite failure: adding a term, or re-pushing the same intent,
    must not manufacture a device-touching PUT-replace out of nothing."""
    device_id = await seed_device(nso_device_name="rp-grow", netbox_device_id=7974)
    one = [{"sequence": 10, "action": "permit"}]
    await _put(adapter_client, device_id, [_obj("route_map", "RM-4", entries=one, accepted=True)])
    await _put(adapter_client, device_id, [_obj("route_map", "RM-4", entries=one, accepted=True)])  # republish
    assert await _removal_job(device_id) is None

    two = [*one, {"sequence": 20, "action": "deny"}]
    await _put(adapter_client, device_id, [_obj("route_map", "RM-4", entries=two, accepted=True)])
    assert await _removal_job(device_id) is None, "a grow is not a shrink"
