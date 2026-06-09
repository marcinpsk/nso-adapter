# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""BFD write path: PUT /api/v1/devices/{id}/bfd-intent."""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _count_bfd_intent(device_id: int) -> int:
    from sqlalchemy import select

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import BfdIntent

    async for db in get_session():
        rows = (await db.execute(select(BfdIntent).where(BfdIntent.device_id == device_id))).scalars().all()
        return len(rows)
    return 0


@pytest.mark.anyio
async def test_put_bfd_intent_stores_and_full_replaces(adapter_client):
    device_id = await seed_device(nso_device_name="bfd-wp", netbox_device_id=1400)
    body = {"interfaces": [
        {"interface_name": "Port-channel1", "min_tx": 300, "min_rx": 300, "multiplier": 3, "micro_bfd": True},
        {"interface_name": "GigabitEthernet0/1", "min_tx": 100, "min_rx": 100, "multiplier": 5},
    ]}
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/bfd-intent", json=body, headers=AUTH)
    assert resp.status_code == 200 and resp.json()["count"] == 2
    assert await _count_bfd_intent(device_id) == 2
    # full-replace
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bfd-intent",
        json={"interfaces": [{"interface_name": "Port-channel1", "min_tx": 300, "min_rx": 300, "multiplier": 3}]},
        headers=AUTH,
    )
    assert resp.json()["count"] == 1
    assert await _count_bfd_intent(device_id) == 1


@pytest.mark.anyio
async def test_put_bfd_intent_unknown_device_404(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/999999/bfd-intent", json={"interfaces": []}, headers=AUTH)
    assert resp.status_code == 404
