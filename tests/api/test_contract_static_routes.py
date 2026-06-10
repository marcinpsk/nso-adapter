# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/static-routes.

Routes OMIT optional keys when unset (not null). Consumed by the plugin in
``template_content._reconcile_static_routes``.

Canonical contract: ``docs/api-contract.md`` § "Static Routing".
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_static_routes.py``.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TOP_KEYS = {"device_id", "last_refreshed_at", "refresh_source", "routes"}
ROUTE_REQUIRED_KEYS = {"vrf", "prefix", "next_hop"}
ROUTE_OPTIONAL_KEYS = {"interface_next_hop", "metric", "permanent", "tag", "name"}


@pytest.mark.anyio
async def test_static_routes_contract(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceStaticRoute

    device_id = await seed_device(nso_device_name="sr-ct", netbox_device_id=7970)
    ts = datetime(2026, 6, 1, 10, 0, 0)
    async for db in get_session():
        # Maximal route (every optional) + minimal route (only required).
        db.add(DeviceStaticRoute(device_id=device_id, vrf="", prefix="10.0.0.0/8", next_hop="192.0.2.1",
                                 interface_next_hop="GE0/0", metric=10, permanent=True, tag=99, name="RT-1",
                                 last_refreshed_at=ts, refresh_source="poll"))
        db.add(DeviceStaticRoute(device_id=device_id, vrf="", prefix="0.0.0.0/0", next_hop="192.0.2.254",
                                 last_refreshed_at=ts, refresh_source="poll"))
        await db.commit()
        break

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS
    routes = {r["prefix"]: r for r in body["routes"]}
    assert set(routes["10.0.0.0/8"].keys()) == ROUTE_REQUIRED_KEYS | ROUTE_OPTIONAL_KEYS
    assert set(routes["0.0.0.0/0"].keys()) == ROUTE_REQUIRED_KEYS  # optionals omitted
    assert isinstance(routes["10.0.0.0/8"]["permanent"], bool)


@pytest.mark.anyio
async def test_static_routes_no_data_shape(adapter_client):
    device_id = await seed_device(nso_device_name="sr-ct-empty", netbox_device_id=7971)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS
    assert body["routes"] == [] and body["refresh_source"] == "never"
