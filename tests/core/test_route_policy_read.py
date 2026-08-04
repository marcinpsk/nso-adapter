# SPDX-License-Identifier: Apache-2.0
"""Read path: NSO route-policy data -> DB read-mirror (invert-match + dialect canon)."""

from __future__ import annotations

import pytest
from sqlalchemy import select

# adapter_client inits the DB (runs app lifespan -> init_db -> create_all).
from tests.conftest import session


@pytest.mark.asyncio
async def test_read_carries_invert_match_and_canonicalizes_amp_large(adapter_client):  # noqa: F811
    from nso_adapter.core.route_policy import _upsert_route_policy_data
    from nso_adapter.store.models import (
        Device,
        DeviceRoutePolicyCommunityList,
        DeviceRoutePolicyCommunityListEntry,
    )

    nso_data = {
        "community-list": [
            {
                "name": "SCRUBBER",
                "invert-match": True,
                "entry": [
                    {"sequence": 10, "action": "permit", "community": "no-export"},
                    {"sequence": 20, "action": "permit", "community": "64500&.*&[0-4]"},
                ],
            },
            {
                "name": "PLAIN",
                "entry": [{"sequence": 10, "action": "permit", "community": "64500:100"}],
            },
        ]
    }

    async with session() as db:
        device = Device(
            nso_instance="default",
            nso_device_name="ra1",
            netbox_device_id=9991,
            ned_id="timos-nc-23.10",
        )
        db.add(device)
        await db.flush()

        await _upsert_route_policy_data(db, device, nso_data, "test")

        rows = (
            (
                await db.execute(
                    select(DeviceRoutePolicyCommunityList).where(DeviceRoutePolicyCommunityList.device_id == device.id)
                )
            )
            .scalars()
            .all()
        )
        by_name = {r.name: r for r in rows}
        assert by_name["SCRUBBER"].invert_match is True
        assert by_name["PLAIN"].invert_match is False

        entries = (
            (
                await db.execute(
                    select(DeviceRoutePolicyCommunityListEntry)
                    .where(DeviceRoutePolicyCommunityListEntry.community_list_id == by_name["SCRUBBER"].id)
                    .order_by(DeviceRoutePolicyCommunityListEntry.sequence)
                )
            )
            .scalars()
            .all()
        )
        # `&`-large is canonicalized to large:a:b:c on read (timos dialect).
        assert [e.community for e in entries] == ["no-export", "large:64500:.*:[0-4]"]
        return


@pytest.mark.asyncio
async def test_duplicate_object_names_are_deduped_not_crashing(adapter_client):  # noqa: F811
    """A reader reporting the same name twice (SR OS as-path vs as-path-group, or a doubled
    prefix-list read) must not abort the WHOLE full-replace refresh on the (device, name) key
    — dedup keeps the first and the refresh still lands every other object."""
    from nso_adapter.core.route_policy import _upsert_route_policy_data
    from nso_adapter.store.models import Device, DeviceRoutePolicyASPath, DeviceRoutePolicyPrefixList

    nso_data = {
        "prefix-list": [
            {"name": "DUP", "family": 6, "entry": [{"sequence": 10, "action": "permit", "prefix": "2001:db8::/32"}]},
            {"name": "DUP", "family": 6, "entry": []},
        ],
        "as-path": [
            {"name": "AP", "entry": [{"sequence": 10, "action": "permit", "pattern": "^1234"}]},
            {"name": "AP", "entry": [{"sequence": 10, "action": "permit", "pattern": "^5678"}]},
        ],
    }

    async with session() as db:
        device = Device(nso_instance="default", nso_device_name="ra1", netbox_device_id=9992, ned_id="timos-nc-23.10")
        db.add(device)
        await db.flush()

        await _upsert_route_policy_data(db, device, nso_data, "test")  # must not raise

        pls = (
            (
                await db.execute(
                    select(DeviceRoutePolicyPrefixList).where(DeviceRoutePolicyPrefixList.device_id == device.id)
                )
            )
            .scalars()
            .all()
        )
        aps = (
            (await db.execute(select(DeviceRoutePolicyASPath).where(DeviceRoutePolicyASPath.device_id == device.id)))
            .scalars()
            .all()
        )
        assert [p.name for p in pls] == ["DUP"]  # deduped to one, refresh did not crash
        assert [a.name for a in aps] == ["AP"]
        return


@pytest.mark.asyncio
async def test_read_skips_malformed_entries_without_wiping_mirror(adapter_client):  # noqa: F811
    """A malformed entry — a missing required leaf, a list without a name, or a null community
    that would crash the dialect (None.strip()) — must be skipped, not KeyError/AttributeError-
    abort the 4-family full-replace and wipe the whole mirror (s2-4)."""
    from nso_adapter.core.route_policy import _upsert_route_policy_data
    from nso_adapter.store.models import (
        Device,
        DeviceRoutePolicyCommunityListEntry,
        DeviceRoutePolicyPrefixList,
        DeviceRoutePolicyPrefixListEntry,
    )

    nso_data = {
        "prefix-list": [
            {
                "name": "PL",
                "family": 4,
                "entry": [
                    {"sequence": 10, "action": "permit"},  # missing prefix → skip entry
                    {"sequence": 20, "action": "permit", "prefix": "10.0.0.0/8"},
                ],
            },
            {"family": 4, "entry": []},  # missing name → skip whole list
        ],
        "community-list": [
            {
                "name": "CL",
                "entry": [
                    {"sequence": 10, "action": "permit", "community": None},  # null → skip, no crash
                    {"sequence": 20, "action": "permit", "community": "no-export"},
                ],
            }
        ],
    }

    async with session() as db:
        device = Device(
            nso_instance="default", nso_device_name="ra-malformed", netbox_device_id=9993, ned_id="timos-nc-23.10"
        )
        db.add(device)
        await db.flush()

        await _upsert_route_policy_data(db, device, nso_data, "test")  # must not raise

        pls = (
            (
                await db.execute(
                    select(DeviceRoutePolicyPrefixList).where(DeviceRoutePolicyPrefixList.device_id == device.id)
                )
            )
            .scalars()
            .all()
        )
        assert [p.name for p in pls] == ["PL"]  # nameless list skipped
        pl_entries = (
            (
                await db.execute(
                    select(DeviceRoutePolicyPrefixListEntry).where(
                        DeviceRoutePolicyPrefixListEntry.prefix_list_id == pls[0].id
                    )
                )
            )
            .scalars()
            .all()
        )
        assert [e.prefix for e in pl_entries] == ["10.0.0.0/8"]  # prefix-less entry skipped
        cl_entries = (await db.execute(select(DeviceRoutePolicyCommunityListEntry))).scalars().all()
        assert [e.community for e in cl_entries] == ["no-export"]  # null-community entry skipped
        return
