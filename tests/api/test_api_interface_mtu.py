# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Phase 2b: GET /api/v1/devices/{id}/interface-mtu."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _seed_mtu(device_id: int, rows: list[dict]) -> None:
    from nso_adapter.store.models import DeviceInterfaceMtu

    async with session() as db:
        now = datetime.now(UTC).replace(tzinfo=None)
        for r in rows:
            db.add(DeviceInterfaceMtu(device_id=device_id, last_refreshed_at=now, refresh_source="test", **r))
        await db.commit()
        return


@pytest.mark.anyio
async def test_interface_mtu_returns_seeded_rows(adapter_client):
    device_id = await seed_device()
    await _seed_mtu(
        device_id,
        [
            {"interface_name": "Port-channel1", "mtu": 9216},
            {"interface_name": "LAG99:99", "ip_mtu": 9170, "bound_port": "lag-99"},
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interface-mtu", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    by_name = {i["interface_name"]: i for i in body["interfaces"]}
    assert by_name["Port-channel1"] == {
        "interface_name": "Port-channel1",
        "mtu": 9216,
        "ip_mtu": None,
        "mpls_mtu": None,
        "bound_port": "",
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


async def _count_mtu_intent(device_id: int) -> int:
    from nso_adapter.store.models import InterfaceMtuIntent

    async with session() as db:
        rows = (
            (await db.execute(select(InterfaceMtuIntent).where(InterfaceMtuIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return len(rows)
    return 0


@pytest.mark.anyio
async def test_put_interface_mtu_intent_stores_and_full_replaces(adapter_client):
    device_id = await seed_device()
    body = {
        "interfaces": [
            {"interface_name": "Port-channel1", "mtu": 9216},
            {"interface_name": "LAG99:99", "ip_mtu": 9170},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/interface-mtu-intent", json=body, headers=AUTH)
    assert resp.status_code == 200 and resp.json()["count"] == 2
    assert await _count_mtu_intent(device_id) == 2
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/interface-mtu-intent",
        json={"interfaces": [{"interface_name": "LAG99:99", "ip_mtu": 9170}]},
        headers=AUTH,
    )
    assert resp.json()["count"] == 1
    assert await _count_mtu_intent(device_id) == 1


@pytest.mark.anyio
async def test_put_interface_mtu_intent_unknown_device_404(adapter_client):
    resp = await adapter_client.put(
        "/api/v1/devices/999999/interface-mtu-intent", json={"interfaces": []}, headers=AUTH
    )
    assert resp.status_code == 404
