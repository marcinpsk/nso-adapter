# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/lag-config.

Bundles + members OMIT optional keys when unset (min_links/system_priority/
system_id/timer/admin_key on the bundle; mode/port_priority on a member), so the
model uses exclude_unset. ``last_refreshed_at`` is a "<iso>Z" string. Seeded with
a fixed timestamp (the seed_lag_config helper stamps datetime.now(), which would
be non-deterministic).
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0)


async def _seed_lag(device_id: int) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import LagBundleConfig, LagMemberConfig

    async for db in get_session():
        b1 = LagBundleConfig(
            device_id=device_id,
            name="Bundle-Ether1",
            lag_id=1,
            min_links=2,
            system_priority=100,
            system_id="00:11:22:33:44:55",
            timer="fast",
            admin_key=10,
            last_refreshed_at=TS,
            refresh_source="poll",
        )
        db.add(b1)
        await db.flush()
        db.add(LagMemberConfig(lag_bundle_id=b1.id, interface_name="GE0/1", mode="active", port_priority=32))
        db.add(LagMemberConfig(lag_bundle_id=b1.id, interface_name="GE0/2"))  # minimal member
        db.add(
            LagBundleConfig(
                device_id=device_id, name="Bundle-Ether2", lag_id=2, last_refreshed_at=TS, refresh_source="poll"
            )
        )
        await db.commit()
        break


@pytest.mark.anyio
async def test_lag_config_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="lag-golden", netbox_device_id=7992)
    await _seed_lag(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "bundles": [
            {
                "name": "Bundle-Ether1",
                "lag_id": 1,
                "min_links": 2,
                "system_priority": 100,
                "system_id": "00:11:22:33:44:55",
                "timer": "fast",
                "admin_key": 10,
                "members": [
                    {"interface_name": "GE0/1", "mode": "active", "port_priority": 32},
                    {"interface_name": "GE0/2"},
                ],
            },
            {"name": "Bundle-Ether2", "lag_id": 2, "members": []},
        ],
    }


@pytest.mark.anyio
async def test_lag_config_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="lag-golden-empty", netbox_device_id=7993)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "bundles": [],
    }
