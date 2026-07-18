# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/lag-topology.

S1b: this read GET had no test_contract_*.py before now. Fixed shape; every key
always present (member ``mode`` is a non-null str). ``last_refreshed_at`` is a
"<iso>Z" string. Deep-equality pins the exact bytes.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0)


async def _seed_lag_topology(device_id: int) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import LagInterface, LagMember

    async for db in get_session():
        lag1 = LagInterface(
            device_id=device_id, name="Bundle-Ether1", lag_id=1, last_refreshed_at=TS, refresh_source="poll"
        )
        db.add(lag1)
        await db.flush()
        db.add(LagMember(lag_interface_id=lag1.id, interface_name="GE0/1", mode="active"))
        db.add(LagMember(lag_interface_id=lag1.id, interface_name="GE0/2", mode="passive"))
        # Second bundle with no members.
        db.add(
            LagInterface(
                device_id=device_id, name="Bundle-Ether2", lag_id=2, last_refreshed_at=TS, refresh_source="poll"
            )
        )
        await db.commit()
        break


@pytest.mark.anyio
async def test_lag_topology_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="lagtopo-golden", netbox_device_id=7959)
    await _seed_lag_topology(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/lag-topology", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "lags": [
            {
                "name": "Bundle-Ether1",
                "id": 1,
                "members": [
                    {"interface": "GE0/1", "mode": "active"},
                    {"interface": "GE0/2", "mode": "passive"},
                ],
            },
            {"name": "Bundle-Ether2", "id": 2, "members": []},
        ],
    }


@pytest.mark.anyio
async def test_lag_topology_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="lagtopo-golden-empty", netbox_device_id=7960)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/lag-topology", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "lags": [],
    }
