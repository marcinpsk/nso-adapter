# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/route-policy.

Pins the JSON shape the adapter emits for the four route-policy families
(prefix-list / community-list / as-path / route-map, each with their entries), which
the plugin consumes in ``route_policy_reconciler.reconcile_route_policy``. Only the
prefix-list entry has optional keys (``ge``/``le``, omitted when unset); every other
level emits a fixed key set.

NOTE: unlike the other read endpoints, this one has **no** top-level ``refresh_source``.

Canonical contract: ``docs/api-contract.md`` § "GET .../route-policy".
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_route_policy.py`` —
the ``*_KEYS`` sets MUST stay identical across both files.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

REQUIRED_TOP_KEYS = {"device_id", "last_refreshed_at", "prefix_lists", "community_lists", "as_paths", "route_maps"}
REQUIRED_PL_KEYS = {"name", "family", "entries"}
REQUIRED_PL_ENTRY_KEYS = {"sequence", "action", "prefix"}
OPTIONAL_PL_ENTRY_KEYS = {"ge", "le"}
REQUIRED_CL_KEYS = {"name", "entries"}
REQUIRED_CL_ENTRY_KEYS = {"sequence", "action", "community"}
REQUIRED_AP_KEYS = {"name", "entries"}
REQUIRED_AP_ENTRY_KEYS = {"sequence", "action", "pattern"}
REQUIRED_RM_KEYS = {"name", "entries"}
REQUIRED_RM_ENTRY_KEYS = {
    "sequence", "action", "match_prefix_lists", "match_community_lists", "match_as_paths", "match", "set",
}


async def _seed_route_policy(device_id: int) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import (
        DeviceRoutePolicyASPath,
        DeviceRoutePolicyASPathEntry,
        DeviceRoutePolicyCommunityList,
        DeviceRoutePolicyCommunityListEntry,
        DeviceRoutePolicyPrefixList,
        DeviceRoutePolicyPrefixListEntry,
        DeviceRoutePolicyRouteMap,
        DeviceRoutePolicyRouteMapEntry,
    )

    ts = datetime(2026, 6, 1, 10, 0, 0)
    async for db in get_session():
        pl = DeviceRoutePolicyPrefixList(device_id=device_id, name="PL-1", family=4, last_refreshed_at=ts)
        db.add(pl)
        await db.flush()
        # Maximal entry (ge+le) + minimal entry (omitted).
        db.add(DeviceRoutePolicyPrefixListEntry(prefix_list_id=pl.id, sequence=10, action="permit",
                                                prefix="10.0.0.0/8", ge=16, le=24))
        db.add(DeviceRoutePolicyPrefixListEntry(prefix_list_id=pl.id, sequence=20, action="deny",
                                                prefix="0.0.0.0/0"))

        cl = DeviceRoutePolicyCommunityList(device_id=device_id, name="CL-1", last_refreshed_at=ts)
        db.add(cl)
        await db.flush()
        db.add(DeviceRoutePolicyCommunityListEntry(community_list_id=cl.id, sequence=10, action="permit",
                                                   community="65000:100"))

        ap = DeviceRoutePolicyASPath(device_id=device_id, name="AP-1", last_refreshed_at=ts)
        db.add(ap)
        await db.flush()
        db.add(DeviceRoutePolicyASPathEntry(as_path_id=ap.id, sequence=10, action="permit", pattern="^65000_"))

        rm = DeviceRoutePolicyRouteMap(device_id=device_id, name="RM-1", last_refreshed_at=ts)
        db.add(rm)
        await db.flush()
        db.add(DeviceRoutePolicyRouteMapEntry(route_map_id=rm.id, sequence=10, action="permit",
                                              match_prefix_lists=["PL-1"], match_community_lists=["CL-1"],
                                              match_as_paths=["AP-1"], match_json='{"prefix": "PL-1"}',
                                              set_json='{"local_preference": 200}'))
        await db.commit()
        break


@pytest.mark.anyio
async def test_route_policy_payload_matches_contract_exactly(adapter_client):
    """Every family + entry exposes its documented keys (prefix-list ge/le optional)."""
    device_id = await seed_device(nso_device_name="rp-contract", netbox_device_id=7940)
    await _seed_route_policy(device_id)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/route-policy", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()

    assert set(body.keys()) == REQUIRED_TOP_KEYS  # NB: no refresh_source

    pl = body["prefix_lists"][0]
    assert set(pl.keys()) == REQUIRED_PL_KEYS
    assert isinstance(pl["family"], int)  # 4 or 6
    pl_entries = {e["sequence"]: e for e in pl["entries"]}
    assert set(pl_entries[10].keys()) == REQUIRED_PL_ENTRY_KEYS | OPTIONAL_PL_ENTRY_KEYS
    assert set(pl_entries[20].keys()) == REQUIRED_PL_ENTRY_KEYS  # ge/le omitted

    cl = body["community_lists"][0]
    assert set(cl.keys()) == REQUIRED_CL_KEYS
    assert set(cl["entries"][0].keys()) == REQUIRED_CL_ENTRY_KEYS

    ap = body["as_paths"][0]
    assert set(ap.keys()) == REQUIRED_AP_KEYS
    assert set(ap["entries"][0].keys()) == REQUIRED_AP_ENTRY_KEYS

    rm = body["route_maps"][0]
    assert set(rm.keys()) == REQUIRED_RM_KEYS
    rm_entry = rm["entries"][0]
    assert set(rm_entry.keys()) == REQUIRED_RM_ENTRY_KEYS
    # match_* are lists; match/set are JSON strings (the plugin json.loads them).
    assert isinstance(rm_entry["match_prefix_lists"], list)
    assert isinstance(rm_entry["match"], str) and isinstance(rm_entry["set"], str)


@pytest.mark.anyio
async def test_route_policy_no_data_shape(adapter_client):
    """Empty shape keeps the top-level keys (all family lists empty)."""
    device_id = await seed_device(nso_device_name="rp-contract-empty", netbox_device_id=7941)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/route-policy", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == REQUIRED_TOP_KEYS
    assert body["prefix_lists"] == [] and body["route_maps"] == []
    assert body["last_refreshed_at"] is None
