# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/route-policy.

Four families (prefix-list/community-list/as-path/route-map). Only prefix-list
entries have optional keys (ge/le, omitted when unset) -> exclude_unset. The
response has NO top-level refresh_source. ``family`` is an INT; a route-map
entry's ``match``/``set`` are JSON *strings* (the plugin json.loads them) while
``match_*`` are lists. ``last_refreshed_at`` is a "<iso>Z" string.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import (
    GOLDEN_BORN_ISO,
    GOLDEN_INCARNATION,
    VALID_TOKEN,
    pin_store_incarnation,
    seed_device,
    session,
)

_SYNTH_READ_STATE = {
    "outcome": "unavailable",
    "reason": "not_ready",
    "freshness": None,
    "result": None,
    "succeeded": None,
    "read_at": None,
    "attempt_id": None,
    "source_epoch": 1,
    "payload_revision": None,
    "incarnation": GOLDEN_INCARNATION,
    "incarnation_born": GOLDEN_BORN_ISO,
}

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)


async def _seed_route_policy(device_id: int) -> None:
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

    async with session() as db:
        pl = DeviceRoutePolicyPrefixList(device_id=device_id, name="PL-1", family=4, last_refreshed_at=TS)
        db.add(pl)
        await db.flush()
        db.add(
            DeviceRoutePolicyPrefixListEntry(
                prefix_list_id=pl.id, sequence=10, action="permit", prefix="10.0.0.0/8", ge=16, le=24
            )
        )
        db.add(DeviceRoutePolicyPrefixListEntry(prefix_list_id=pl.id, sequence=20, action="deny", prefix="0.0.0.0/0"))

        cl = DeviceRoutePolicyCommunityList(device_id=device_id, name="CL-1", last_refreshed_at=TS)
        db.add(cl)
        await db.flush()
        db.add(
            DeviceRoutePolicyCommunityListEntry(
                community_list_id=cl.id, sequence=10, action="permit", community="65000:100"
            )
        )

        ap = DeviceRoutePolicyASPath(device_id=device_id, name="AP-1", last_refreshed_at=TS)
        db.add(ap)
        await db.flush()
        db.add(DeviceRoutePolicyASPathEntry(as_path_id=ap.id, sequence=10, action="permit", pattern="^65000_"))

        rm = DeviceRoutePolicyRouteMap(device_id=device_id, name="RM-1", last_refreshed_at=TS)
        db.add(rm)
        await db.flush()
        db.add(
            DeviceRoutePolicyRouteMapEntry(
                route_map_id=rm.id,
                sequence=10,
                action="permit",
                match_prefix_lists=["PL-1"],
                match_community_lists=["CL-1"],
                match_as_paths=["AP-1"],
                match_json='{"prefix": "PL-1"}',
                set_json='{"local_preference": 200}',
            )
        )
        await db.commit()


@pytest.mark.anyio
async def test_route_policy_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="rp-golden", netbox_device_id=7945)
    await pin_store_incarnation()
    await _seed_route_policy(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/route-policy", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "read_state": _SYNTH_READ_STATE,
        "prefix_lists": [
            {
                "name": "PL-1",
                "family": 4,
                "entries": [
                    {"sequence": 10, "action": "permit", "prefix": "10.0.0.0/8", "ge": 16, "le": 24},
                    {"sequence": 20, "action": "deny", "prefix": "0.0.0.0/0"},
                ],
            }
        ],
        "community_lists": [
            {
                "name": "CL-1",
                "invert_match": False,
                "entries": [{"sequence": 10, "action": "permit", "community": "65000:100"}],
            }
        ],
        "as_paths": [{"name": "AP-1", "entries": [{"sequence": 10, "action": "permit", "pattern": "^65000_"}]}],
        "route_maps": [
            {
                "name": "RM-1",
                "entries": [
                    {
                        "sequence": 10,
                        "action": "permit",
                        "match_prefix_lists": ["PL-1"],
                        "match_community_lists": ["CL-1"],
                        "match_as_paths": ["AP-1"],
                        "match": '{"prefix": "PL-1"}',
                        "set": '{"local_preference": 200}',
                    }
                ],
            }
        ],
    }


@pytest.mark.anyio
async def test_route_policy_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="rp-golden-empty", netbox_device_id=7946)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/route-policy", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "read_state": _SYNTH_READ_STATE,
        "prefix_lists": [],
        "community_lists": [],
        "as_paths": [],
        "route_maps": [],
    }
