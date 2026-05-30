# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/interface_ip.py — refresh, upsert, and SSE event handler."""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from nso_adapter.core.interface_ip import (
    handle_interface_ip_change,
    refresh_interface_ips_for_device,
)
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, InterfaceIpAddress
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
async def test_refresh_inserts_addresses(adapter_client):
    """Happy-path: NSO returns two interfaces with addresses → rows inserted."""
    device_id = await seed_device(nso_device_name="ip-sw01", netbox_device_id=910)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_interface_ips.return_value = {
            "name": "ip-sw01",
            "interface": [
                {
                    "interface-name": "GigabitEthernet0/1",
                    "address": [
                        {"address": "10.0.1.1/24", "vrf": "", "family": "ipv4", "secondary": False}
                    ],
                },
                {
                    "interface-name": "Loopback0",
                    "address": [
                        {"address": "192.168.1.1/32", "vrf": "", "family": "ipv4", "secondary": False},
                        {"address": "192.168.1.2/32", "vrf": "", "family": "ipv4", "secondary": True},
                    ],
                },
            ],
        }

        await refresh_interface_ips_for_device(db, device, nso_client, refresh_source="poll")

        nso_client.get_interface_ips.assert_awaited_once_with("ip-sw01")
        result = await db.execute(
            select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device.id)
        )
        rows = result.scalars().all()
        assert len(rows) == 3
        addrs = {r.address for r in rows}
        assert addrs == {"10.0.1.1/24", "192.168.1.1/32", "192.168.1.2/32"}
        secondary_row = next(r for r in rows if r.address == "192.168.1.2/32")
        assert secondary_row.secondary is True


@pytest.mark.anyio
async def test_refresh_full_replaces_existing_rows(adapter_client):
    """Second refresh replaces previous rows entirely (no duplicates)."""
    device_id = await seed_device(nso_device_name="ip-sw02", netbox_device_id=911)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_interface_ips.return_value = {
            "name": "ip-sw02",
            "interface": [
                {
                    "interface-name": "GigabitEthernet0/1",
                    "address": [{"address": "10.0.1.1/24", "vrf": "", "family": "ipv4"}],
                }
            ],
        }

        await refresh_interface_ips_for_device(db, device, nso_client)
        # Update — different address
        nso_client.get_interface_ips.return_value = {
            "name": "ip-sw02",
            "interface": [
                {
                    "interface-name": "GigabitEthernet0/1",
                    "address": [{"address": "10.0.99.1/24", "vrf": "", "family": "ipv4"}],
                }
            ],
        }
        await refresh_interface_ips_for_device(db, device, nso_client)

        result = await db.execute(
            select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device.id)
        )
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].address == "10.0.99.1/24"


@pytest.mark.anyio
async def test_refresh_clears_on_404(adapter_client):
    """If NSO returns None (device gone) existing rows are cleared."""
    device_id = await seed_device(nso_device_name="ip-sw03", netbox_device_id=912)
    async with _device_session(device_id) as (db, device):
        from datetime import UTC, datetime

        db.add(
            InterfaceIpAddress(
                device_id=device.id,
                interface_name="GE0/1",
                address="10.0.0.1/24",
                vrf="",
                family="ipv4",
                secondary=False,
                last_refreshed_at=datetime.now(UTC).replace(tzinfo=None),
                refresh_source="poll",
            )
        )
        await db.commit()

        nso_client = AsyncMock()
        nso_client.get_interface_ips.return_value = None

        await refresh_interface_ips_for_device(db, device, nso_client)

        result = await db.execute(
            select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device.id)
        )
        assert result.scalars().all() == []


@pytest.mark.anyio
async def test_refresh_transport_error_leaves_existing_rows(adapter_client):
    """On NSO transport error, existing rows are preserved (no wipe)."""
    device_id = await seed_device(nso_device_name="ip-sw04", netbox_device_id=913)
    async with _device_session(device_id) as (db, device):
        from datetime import UTC, datetime

        db.add(
            InterfaceIpAddress(
                device_id=device.id,
                interface_name="GE0/1",
                address="10.0.0.1/24",
                vrf="",
                family="ipv4",
                secondary=False,
                last_refreshed_at=datetime.now(UTC).replace(tzinfo=None),
                refresh_source="poll",
            )
        )
        await db.commit()

        nso_client = AsyncMock()
        nso_client.get_interface_ips.side_effect = httpx.ConnectError("timeout")

        await refresh_interface_ips_for_device(db, device, nso_client)

        result = await db.execute(
            select(InterfaceIpAddress).where(InterfaceIpAddress.device_id == device.id)
        )
        # Existing row must still be there — error path does NOT wipe
        assert len(result.scalars().all()) == 1


@pytest.mark.anyio
async def test_handle_interface_ip_change_dispatches(adapter_client):
    """SSE handler dispatches to refresh for matching device."""
    device_id = await seed_device(nso_device_name="ip-sw05", netbox_device_id=914)
    async with _device_session(device_id) as (db, _device):
        nso_client = AsyncMock()
        nso_client.get_interface_ips.return_value = {"name": "ip-sw05", "interface": []}
        event = {
            "netconf-config-change": {
                "edit": [
                    {
                        "target": "/ncs:devices/device[name='ip-sw05']/config/ios:interface/GigabitEthernet0/1"
                    }
                ]
            }
        }

        await handle_interface_ip_change(event, db, {"nso-dev": nso_client})

        nso_client.get_interface_ips.assert_awaited_once_with("ip-sw05")


@pytest.mark.anyio
async def test_handle_interface_ip_change_unknown_device_no_dispatch(adapter_client):
    """SSE handler silently skips events for unrecognised device names."""
    async for db in get_session():
        nso_client = AsyncMock()
        event = {
            "netconf-config-change": {
                "edit": [{"target": "/ncs:devices/device[name='ghost-device']/config/..."}]
            }
        }

        await handle_interface_ip_change(event, db, {"nso-dev": nso_client})

        nso_client.get_interface_ips.assert_not_awaited()
        break
