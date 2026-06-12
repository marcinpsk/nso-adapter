# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Phase 2b: core/interface_mtu.py — refresh + full-replace."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.interface_mtu import refresh_interface_mtu_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceInterfaceMtu
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


async def _rows(db, device_id):
    rows = (
        await db.execute(select(DeviceInterfaceMtu).where(DeviceInterfaceMtu.device_id == device_id))
    ).scalars().all()
    return {r.interface_name: r for r in rows}


@pytest.mark.anyio
async def test_refresh_inserts_mtu(adapter_client):
    device_id = await seed_device(nso_device_name="mtu-rtr01", netbox_device_id=980)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_interface_mtu.return_value = {
            "device-name": "mtu-rtr01",
            "interface": [
                {"interface-name": "Port-channel1", "mtu": 9216},
                {"interface-name": "Port-channel1.100", "ip-mtu": 9000},
                {"interface-name": "LAG99:99", "ip-mtu": 9170, "bound-port": "lag-99"},
            ],
        }
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        rows = await _rows(db, device_id)
        assert set(rows) == {"Port-channel1", "Port-channel1.100", "LAG99:99"}
        assert rows["Port-channel1"].mtu == 9216
        assert rows["Port-channel1"].ip_mtu is None
        assert rows["Port-channel1.100"].ip_mtu == 9000
        assert rows["LAG99:99"].ip_mtu == 9170
        assert rows["LAG99:99"].bound_port == "lag-99"


@pytest.mark.anyio
async def test_refresh_full_replace(adapter_client):
    device_id = await seed_device(nso_device_name="mtu-rtr02", netbox_device_id=981)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_interface_mtu.return_value = {"interface": [{"interface-name": "ae10", "mtu": 9192}]}
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_interface_mtu.return_value = {"interface": [{"interface-name": "ae11", "mtu": 1500}]}
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        assert set(await _rows(db, device_id)) == {"ae11"}


@pytest.mark.anyio
async def test_refresh_none_clears(adapter_client):
    device_id = await seed_device(nso_device_name="mtu-rtr03", netbox_device_id=982)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_interface_mtu.return_value = {"interface": [{"interface-name": "ae10", "mtu": 9192}]}
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_interface_mtu.return_value = None
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        assert await _rows(db, device_id) == {}
