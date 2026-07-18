# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/bfd.

S1b: this read GET had no test_contract_*.py before now. Bundle/member optionals
(bound_port/min_tx/min_rx/multiplier) are OMITTED when unset -> exclude_unset;
micro_bfd/enabled are always present bools. ``last_refreshed_at`` is a "<iso>Z"
string. Deep-equality pins the exact bytes.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0)


async def _seed_bfd(device_id: int) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceBfdInterface

    async for db in get_session():
        db.add(
            DeviceBfdInterface(
                device_id=device_id,
                interface_name="GE0/0",
                bound_port="lag-1:1",
                min_tx=300,
                min_rx=300,
                multiplier=3,
                micro_bfd=True,
                enabled=True,
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        # Minimal: no timers/bound_port; micro_bfd + enabled exercise both bool values.
        db.add(
            DeviceBfdInterface(
                device_id=device_id,
                interface_name="GE0/1",
                micro_bfd=False,
                enabled=False,
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        await db.commit()
        break


@pytest.mark.anyio
async def test_bfd_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="bfd-golden", netbox_device_id=7955)
    await _seed_bfd(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/bfd", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "interfaces": [
            {
                "interface_name": "GE0/0",
                "micro_bfd": True,
                "enabled": True,
                "bound_port": "lag-1:1",
                "min_tx": 300,
                "min_rx": 300,
                "multiplier": 3,
            },
            {"interface_name": "GE0/1", "micro_bfd": False, "enabled": False},
        ],
    }


@pytest.mark.anyio
async def test_bfd_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="bfd-golden-empty", netbox_device_id=7956)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/bfd", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "interfaces": [],
    }
