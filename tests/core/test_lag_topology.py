# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from nso_adapter.core.lag_topology import (
    handle_netconf_config_change,
    parse_changed_nso_devices,
    refresh_lag_topology_for_device,
)
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, LagInterface, LagMember
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


def test_parse_changed_devices_ietf_envelope():
    event = {
        "ietf-restconf:notification": {
            "netconf-config-change": {
                "edit": [
                    {
                        "target": "/ncs:devices/device[name='sw03']/config/ios:interface/GigabitEthernet0/1",
                        "operation": "replace",
                    }
                ]
            }
        }
    }

    assert parse_changed_nso_devices(event) == {"sw03"}


def test_parse_changed_devices_bare():
    event = {
        "netconf-config-change": {
            "edit": [
                {"target": "/ncs:devices/device[name='sw03']/config/..."},
                {"target": "/ncs:devices/device[name='rg03']/config/..."},
            ]
        }
    }

    assert parse_changed_nso_devices(event) == {"sw03", "rg03"}


def test_parse_changed_devices_unknown():
    assert parse_changed_nso_devices({"some": "other"}) == set()


@pytest.mark.anyio
async def test_refresh_lag_topology_happy(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=900)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_lag_topology.return_value = {
            "device-name": "sw03",
            "lag": [
                {
                    "name": "Port-channel1",
                    "lag-id": 1,
                    "member": [{"interface-name": "GigabitEthernet0/1", "mode": "active"}],
                },
                {"name": "Port-channel2", "lag-id": 2, "member": []},
            ],
        }

        await refresh_lag_topology_for_device(db, device, nso_client, refresh_source="poll")

        nso_client.get_lag_topology.assert_awaited_once_with("sw03")
        lag_result = await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))
        member_result = await db.execute(select(LagMember))
        lags = lag_result.scalars().all()
        members = member_result.scalars().all()
        assert len(lags) == 2
        assert {lag.name for lag in lags} == {"Port-channel1", "Port-channel2"}
        assert {(member.interface_name, member.mode) for member in members} == {("GigabitEthernet0/1", "active")}


@pytest.mark.anyio
async def test_refresh_lag_topology_clears_on_404(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=901)
    async with _device_session(device_id) as (db, device):
        db.add(
            LagInterface(
                device_id=device.id,
                name="Port-channel1",
                lag_id=1,
                last_refreshed_at=datetime.now(UTC).replace(tzinfo=None),
                refresh_source="poll",
            )
        )
        await db.commit()

        nso_client = AsyncMock()
        nso_client.get_lag_topology.return_value = None

        await refresh_lag_topology_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))
        assert result.scalars().all() == []


@pytest.mark.anyio
async def test_refresh_lag_topology_transport_error(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=902)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_lag_topology.side_effect = httpx.ConnectError("timeout")

        await refresh_lag_topology_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))
        assert result.scalars().all() == []


@pytest.mark.anyio
async def test_handle_netconf_config_change_dispatches(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=903)
    async with _device_session(device_id) as (db, _device):
        nso_client = AsyncMock()
        nso_client.get_lag_topology.return_value = {"device-name": "sw03", "lag": []}
        event = {
            "netconf-config-change": {
                "edit": [{"target": "/ncs:devices/device[name='sw03']/config/ios:interface/GigabitEthernet1"}]
            }
        }

        await handle_netconf_config_change(event, db, {"nso-dev": nso_client})

        nso_client.get_lag_topology.assert_awaited_once_with("sw03")


@pytest.mark.anyio
async def test_handle_netconf_config_change_unknown_device(adapter_client):
    async for db in get_session():
        nso_client = AsyncMock()
        event = {
            "netconf-config-change": {"edit": [{"target": "/ncs:devices/device[name='unknown-device']/config/..."}]}
        }

        await handle_netconf_config_change(event, db, {"nso-dev": nso_client})

        nso_client.get_lag_topology.assert_not_awaited()
        break
