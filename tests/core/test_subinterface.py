# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M36: core/subinterface.py — refresh + full-replace."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.subinterface import refresh_subinterface_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceSubinterface
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
        await db.execute(select(DeviceSubinterface).where(DeviceSubinterface.device_id == device_id))
    ).scalars().all()
    return {r.interface_name: r for r in rows}


@pytest.mark.anyio
async def test_refresh_inserts_subinterfaces(adapter_client):
    device_id = await seed_device(nso_device_name="subif-rtr01", netbox_device_id=970)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_subinterface.return_value = {
            "device-name": "subif-rtr01",
            "interface": [
                {"interface-name": "GigabitEthernet0/1.100", "parent-interface": "GigabitEthernet0/1",
                 "dot1q-vlan": 100, "type": "subinterface", "vrf": "TENANT_A"},
                {"interface-name": "ge-0/0/0.200", "parent-interface": "ge-0/0/0",
                 "dot1q-vlan": 200, "type": "subinterface"},
            ],
        }
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        rows = await _rows(db, device_id)
        assert set(rows) == {"GigabitEthernet0/1.100", "ge-0/0/0.200"}
        assert rows["GigabitEthernet0/1.100"].dot1q_vlan == 100
        assert rows["GigabitEthernet0/1.100"].parent_interface == "GigabitEthernet0/1"
        assert rows["GigabitEthernet0/1.100"].vrf == "TENANT_A"


@pytest.mark.anyio
async def test_refresh_full_replace(adapter_client):
    device_id = await seed_device(nso_device_name="subif-rtr02", netbox_device_id=971)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_subinterface.return_value = {
            "interface": [{"interface-name": "ge-0/0/0.10", "parent-interface": "ge-0/0/0", "dot1q-vlan": 10}]
        }
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_subinterface.return_value = {
            "interface": [{"interface-name": "ge-0/0/0.20", "parent-interface": "ge-0/0/0", "dot1q-vlan": 20}]
        }
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        assert set(await _rows(db, device_id)) == {"ge-0/0/0.20"}


@pytest.mark.anyio
async def test_refresh_none_clears(adapter_client):
    device_id = await seed_device(nso_device_name="subif-rtr03", netbox_device_id=972)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_subinterface.return_value = {
            "interface": [{"interface-name": "ge-0/0/0.10", "parent-interface": "ge-0/0/0", "dot1q-vlan": 10}]
        }
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_subinterface.return_value = None
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        assert await _rows(db, device_id) == {}
