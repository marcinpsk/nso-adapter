# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/logging_config.py — refresh + full-replace upsert."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.logging_config import refresh_logging_config_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceLoggingHost
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


async def _hosts(db, device_id):
    rows = (await db.execute(select(DeviceLoggingHost).where(DeviceLoggingHost.device_id == device_id))).scalars().all()
    return {r.address: r for r in rows}


@pytest.mark.anyio
async def test_refresh_inserts_hosts(adapter_client):
    device_id = await seed_device(nso_device_name="log-sw01", netbox_device_id=970)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "device-name": "log-sw01",
            "host": [{"address": "198.18.251.86", "severity": "warning", "facility": "any", "source": "1.1.1.1"}],
        }
        await refresh_logging_config_for_device(db, device, nso_client, refresh_source="test")
        hosts = await _hosts(db, device_id)
        assert "198.18.251.86" in hosts
        assert hosts["198.18.251.86"].severity == "warning"
        assert hosts["198.18.251.86"].source == "1.1.1.1"


@pytest.mark.anyio
async def test_refresh_full_replace(adapter_client):
    device_id = await seed_device(nso_device_name="log-sw02", netbox_device_id=971)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "host": [{"address": "10.0.0.1"}, {"address": "10.0.0.2"}],
        }
        await refresh_logging_config_for_device(db, device, nso_client, refresh_source="test")
        assert set((await _hosts(db, device_id)).keys()) == {"10.0.0.1", "10.0.0.2"}
        nso_client.get_device_state_section.return_value = {"status": "ok", "host": [{"address": "10.0.0.2"}]}
        await refresh_logging_config_for_device(db, device, nso_client, refresh_source="test")
        assert set((await _hosts(db, device_id)).keys()) == {"10.0.0.2"}


@pytest.mark.anyio
async def test_refresh_none_clears(adapter_client):
    device_id = await seed_device(nso_device_name="log-sw03", netbox_device_id=972)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {"status": "ok", "host": [{"address": "10.0.0.9"}]}
        await refresh_logging_config_for_device(db, device, nso_client, refresh_source="test")
        assert len(await _hosts(db, device_id)) == 1
        nso_client.get_device_state_section.return_value = None
        await refresh_logging_config_for_device(db, device, nso_client, refresh_source="test")
        assert len(await _hosts(db, device_id)) == 0
