# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""VLAN database + switchport refresh tests."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.vlan import (
    refresh_switchport_for_device,
    refresh_vlan_database_for_device,
)
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceSwitchport, DeviceVlan
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


@pytest.mark.anyio
async def test_refresh_vlan_database_upserts_and_prunes(adapter_client):
    device_id = await seed_device(nso_device_name="vsw", netbox_device_id=1300)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        nso.get_vlan_database.return_value = {
            "vlan": [{"vlan-id": 10, "name": "MGMT"}, {"vlan-id": 20, "name": "DATA"}]
        }
        await refresh_vlan_database_for_device(db, device, nso)
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert {(r.vlan_id, r.name) for r in rows} == {(10, "MGMT"), (20, "DATA")}

        # second refresh drops 20, keeps 10
        nso.get_vlan_database.return_value = {"vlan": [{"vlan-id": 10, "name": "MGMT"}]}
        await refresh_vlan_database_for_device(db, device, nso)
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert {r.vlan_id for r in rows} == {10}


@pytest.mark.anyio
async def test_refresh_switchport_links_vlans(adapter_client):
    device_id = await seed_device(nso_device_name="vsw2", netbox_device_id=1301)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        nso.get_vlan_database.return_value = {
            "vlan": [{"vlan-id": 10, "name": "A"}, {"vlan-id": 20, "name": "B"}, {"vlan-id": 99, "name": "N"}]
        }
        await refresh_vlan_database_for_device(db, device, nso)
        nso.get_switchport.return_value = {
            "interface": [{"interface-name": "Gi0/1", "mode": "trunk", "untagged-vlan": 99, "tagged-vlans": "10,20"}]
        }
        await refresh_switchport_for_device(db, device, nso)

        sp = (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().one()
        assert sp.mode == "trunk"
        uv = await db.get(DeviceVlan, sp.untagged_vlan_id)
        assert uv.vlan_id == 99
