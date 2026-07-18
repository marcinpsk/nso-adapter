# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/static-routes.

Deep-equality proof of the EXACT JSON the adapter emits, field by field. The
contract test (``test_contract_static_routes.py``) pins key *sets*; this pins
key *values* — the guard that response-model typing does not silently drop a
field, reformat a timestamp, or coerce a value. Every optional route key
(including ``next_hop_vrf``, which the contract test does not exercise) appears
on the maximal route so a model missing it would fail here.

``last_refreshed_at`` is a formatted string (``.isoformat() + "Z"``) — pinned
literally so a switch to a raw datetime would be caught.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0)


async def _seed_static_routes(device_id: int) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceStaticRoute

    async for db in get_session():
        # MAXIMAL route — every optional key set, incl. next_hop_vrf.
        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="BLUE",
                prefix="10.0.0.0/8",
                next_hop="192.0.2.1",
                interface_next_hop="GE0/0",
                next_hop_vrf="RED",
                metric=10,
                permanent=True,
                tag=99,
                name="RT-1",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        # MINIMAL route — only the required identity keys.
        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="0.0.0.0/0",
                next_hop="192.0.2.254",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        await db.commit()
        break


@pytest.mark.anyio
async def test_static_routes_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="sr-golden", netbox_device_id=7975)
    await _seed_static_routes(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)).json()

    # Routes ordered by (vrf, prefix, next_hop): "" < "BLUE".
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "routes": [
            {"vrf": "", "prefix": "0.0.0.0/0", "next_hop": "192.0.2.254"},
            {
                "vrf": "BLUE",
                "prefix": "10.0.0.0/8",
                "next_hop": "192.0.2.1",
                "interface_next_hop": "GE0/0",
                "next_hop_vrf": "RED",
                "metric": 10,
                "permanent": True,
                "tag": 99,
                "name": "RT-1",
            },
        ],
    }


@pytest.mark.anyio
async def test_static_routes_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="sr-golden-empty", netbox_device_id=7976)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "routes": [],
    }
