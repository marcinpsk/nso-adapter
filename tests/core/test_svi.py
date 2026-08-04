# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""core/svi.py — refresh + full-replace."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.svi import refresh_svi_for_device
from nso_adapter.store.models import Device, DeviceSvi
from tests.conftest import seed_device, session


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


async def _svis(db, device_id):
    rows = (await db.execute(select(DeviceSvi).where(DeviceSvi.device_id == device_id))).scalars().all()
    return {r.interface_name: r for r in rows}


@pytest.mark.anyio
async def test_refresh_inserts_svis(adapter_client):
    device_id = await seed_device(nso_device_name="svi-sw01", netbox_device_id=980)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "device-name": "svi-sw01",
            "interface": [
                {"interface-name": "Vlan100", "vlan-id": 100, "type": "svi", "vrf": "MGMT"},
                {"interface-name": "Vlan200", "vlan-id": 200, "type": "svi"},
            ],
        }
        await refresh_svi_for_device(db, device, nso_client, refresh_source="test")
        svis = await _svis(db, device_id)
        assert set(svis) == {"Vlan100", "Vlan200"}
        assert svis["Vlan100"].vlan_id == 100 and svis["Vlan100"].vrf == "MGMT"


@pytest.mark.anyio
async def test_refresh_full_replace(adapter_client):
    device_id = await seed_device(nso_device_name="svi-sw02", netbox_device_id=981)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "Vlan10", "vlan-id": 10, "type": "svi"}],
        }
        await refresh_svi_for_device(db, device, nso_client, refresh_source="test")
        # Second refresh with a different set → old row gone.
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "Vlan20", "vlan-id": 20, "type": "svi"}],
        }
        await refresh_svi_for_device(db, device, nso_client, refresh_source="test")
        assert set(await _svis(db, device_id)) == {"Vlan20"}


@pytest.mark.anyio
async def test_refresh_authoritative_empty_clears(adapter_client):
    """An authoritatively-empty read (status=ok, no list keys) clears the rows. (Device-absence, section None, now KEEPS — READSEM S5.)"""
    device_id = await seed_device(nso_device_name="svi-sw03", netbox_device_id=982)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "Vlan10", "vlan-id": 10, "type": "svi"}],
        }
        await refresh_svi_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_device_state_section.return_value = {"status": "ok"}
        await refresh_svi_for_device(db, device, nso_client, refresh_source="test")
        assert await _svis(db, device_id) == {}
