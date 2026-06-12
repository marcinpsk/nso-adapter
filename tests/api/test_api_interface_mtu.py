# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Phase 2b: GET /api/v1/devices/{id}/interface-mtu."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _seed_mtu(device_id: int, rows: list[dict]) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceInterfaceMtu

    async for db in get_session():
        now = datetime.now(UTC).replace(tzinfo=None)
        for r in rows:
            db.add(DeviceInterfaceMtu(device_id=device_id, last_refreshed_at=now, refresh_source="test", **r))
        await db.commit()
        return


@pytest.mark.anyio
async def test_interface_mtu_returns_seeded_rows(adapter_client):
    device_id = await seed_device()
    await _seed_mtu(device_id, [
        {"interface_name": "Port-channel1", "mtu": 9216},
        {"interface_name": "LAG99:99", "ip_mtu": 9170, "bound_port": "lag-99"},
    ])
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interface-mtu", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    by_name = {i["interface_name"]: i for i in body["interfaces"]}
    assert by_name["Port-channel1"] == {
        "interface_name": "Port-channel1", "mtu": 9216, "ip_mtu": None, "mpls_mtu": None, "bound_port": ""
    }
    assert by_name["LAG99:99"]["ip_mtu"] == 9170
    assert by_name["LAG99:99"]["bound_port"] == "lag-99"


@pytest.mark.anyio
async def test_interface_mtu_unknown_device_is_404(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/999999/interface-mtu", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_interface_mtu_requires_auth(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/1/interface-mtu")
    assert resp.status_code in (401, 403)
