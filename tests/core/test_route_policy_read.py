# SPDX-License-Identifier: Apache-2.0
"""Read path: NSO route-policy data -> DB read-mirror (invert-match + dialect canon)."""

from __future__ import annotations

import pytest
from sqlalchemy import select

# adapter_client inits the DB (runs app lifespan -> init_db -> create_all).
from tests.conftest import adapter_client  # noqa: F401


@pytest.mark.asyncio
async def test_read_carries_invert_match_and_canonicalizes_amp_large(adapter_client):  # noqa: F811
    from nso_adapter.core.route_policy import _upsert_route_policy_data
    from nso_adapter.store.db import get_session
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
                    {"sequence": 20, "action": "permit", "community": "6830&.*&[0-4]"},
                ],
            },
            {
                "name": "PLAIN",
                "entry": [{"sequence": 10, "action": "permit", "community": "6830:100"}],
            },
        ]
    }

    async for db in get_session():
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
        assert [e.community for e in entries] == ["no-export", "large:6830:.*:[0-4]"]
        return
