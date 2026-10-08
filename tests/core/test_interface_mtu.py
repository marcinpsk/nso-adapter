# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Phase 2b: core/interface_mtu.py — refresh + full-replace."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.interface_mtu import refresh_interface_mtu_for_device
from nso_adapter.store.models import Device, DeviceInterfaceMtu
from tests.conftest import seed_device, session
from tests.fixtures.switching_read_payloads import SWITCHING_READ_PAYLOADS


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


async def _rows(db, device_id):
    rows = (
        (await db.execute(select(DeviceInterfaceMtu).where(DeviceInterfaceMtu.device_id == device_id))).scalars().all()
    )
    return {r.interface_name: r for r in rows}


@pytest.mark.anyio
async def test_refresh_inserts_mtu(adapter_client):
    device_id = await seed_device(nso_device_name="mtu-rtr01", netbox_device_id=980)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "device-name": "mtu-rtr01",
            **deepcopy(SWITCHING_READ_PAYLOADS["interface_mtu"]),
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
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "ae10", "mtu": 9192}],
        }
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "ae11", "mtu": 1500}],
        }
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        assert set(await _rows(db, device_id)) == {"ae11"}


@pytest.mark.anyio
async def test_refresh_authoritative_empty_clears(adapter_client):
    """An authoritatively-empty read (status=ok, no list keys) clears the rows. (Device-absence, section None, now KEEPS — READSEM S5.)"""
    device_id = await seed_device(nso_device_name="mtu-rtr03", netbox_device_id=982)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "ae10", "mtu": 9192}],
        }
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_device_state_section.return_value = {"status": "ok"}
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        assert await _rows(db, device_id) == {}


@pytest.mark.anyio
async def test_a_BOOLEAN_mtu_is_dropped_and_never_stored_as_1(adapter_client):
    """``int(True)`` stored an MTU of 1 byte; the refused value is stored as absent instead."""
    device_id = await seed_device(nso_device_name="mtu-bool", netbox_device_id=984)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "device-name": "mtu-bool",
            "interface": [{"interface-name": "Gi0/1", "mtu": True, "ip-mtu": 9000}],
        }
        await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        rows = await _rows(db, device_id)
        assert rows["Gi0/1"].mtu is None
        assert rows["Gi0/1"].ip_mtu == 9000


@pytest.mark.anyio
async def test_an_invalid_mtu_leaf_is_materialized_without_a_skip_warning(adapter_client):
    """The row is stored with the leaf absent, so it was not kept out of the mirror."""
    from structlog.testing import capture_logs

    device_id = await seed_device(nso_device_name="mtu-invalid-leaf", netbox_device_id=985)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "device-name": "mtu-invalid-leaf",
            "interface": [{"interface-name": "Gi0/1", "mtu": True, "ip-mtu": 9000}, {"mtu": 1500}],
        }
        with capture_logs() as logs:
            await refresh_interface_mtu_for_device(db, device, nso_client, refresh_source="test")
        rows = await _rows(db, device_id)
        assert list(rows) == ["Gi0/1"]
        assert rows["Gi0/1"].mtu is None
        skipped = [record for record in logs if record["event"] == "interface_mtu.entry_skipped"]
        assert [record["reason"] for record in skipped] == ["interface[1]: invalid interface_name"]
