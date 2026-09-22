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
    parse_changed_nso_devices,
    refresh_lag_topology_for_device,
)
from nso_adapter.store.models import Device, LagInterface, LagMember
from tests.conftest import seed_device, session


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


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
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
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

        nso_client.get_device_state_section.assert_awaited_once_with("sw03", "lag-topology")
        lag_result = await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))
        member_result = await db.execute(select(LagMember))
        lags = lag_result.scalars().all()
        members = member_result.scalars().all()
        assert len(lags) == 2
        assert {lag.name for lag in lags} == {"Port-channel1", "Port-channel2"}
        assert {(member.interface_name, member.mode) for member in members} == {("GigabitEthernet0/1", "active")}


@pytest.mark.anyio
async def test_refresh_lag_topology_clears_on_authoritative_empty(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=901)
    async with _device_session(device_id) as (db, device):
        db.add(
            LagInterface(
                device_id=device.id,
                name="Port-channel1",
                lag_id=1,
                last_refreshed_at=datetime.now(UTC),
                refresh_source="poll",
            )
        )
        await db.commit()

        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {"status": "ok"}

        await refresh_lag_topology_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))
        assert result.scalars().all() == []


@pytest.mark.anyio
async def test_refresh_lag_topology_transport_error(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=902)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.side_effect = httpx.ConnectError("timeout")

        await refresh_lag_topology_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))
        assert result.scalars().all() == []


@pytest.mark.anyio
async def test_lag_without_lag_id_is_skipped_not_fatal(adapter_client):
    """Live ra1 shape: a Nokia lag named without digits ('lag-aa') serves NO lag-id.
    The old direct indexing KeyError'd the whole refresh (every lag lost, surface
    degraded); a malformed entry must be SKIPPED with a warning, like bgp's asn-less
    router."""
    device_id = await seed_device(nso_device_name="lag-noid", netbox_device_id=9820)
    async with _device_session(device_id) as (db, device):
        from structlog.testing import capture_logs

        from tests._secret_discipline import assert_keys_absent, assert_records_free_of

        client = AsyncMock()
        client.get_device_state_section.return_value = {
            "status": "ok",
            "lag": [
                {"name": "lag-1", "lag-id": 1, "member": [{"interface-name": "1/1/c1/1", "mode": "active"}]},
                {"name": "lag-aa", "member": [{"interface-name": "1/1/c2/1", "mode": "active"}]},  # no lag-id
            ],
        }

        with capture_logs() as logs:
            ok = await refresh_lag_topology_for_device(db, device, client)

        assert ok is True
        rows = (await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))).scalars().all()
        assert [(r.name, r.lag_id) for r in rows] == [("lag-1", 1)]
        record = next(record for record in logs if record["event"] == "lag_topology.entry_skipped")
        assert record["device_id"] == device_id
        assert record["reason"] == "no lag-id"
        assert_keys_absent(record, ["lag_name"])
        assert_records_free_of([record], ["lag-aa"])


@pytest.mark.anyio
async def test_lag_with_malformed_id_does_not_block_valid_topology(adapter_client):
    from structlog.testing import capture_logs

    from tests._secret_discipline import assert_keys_absent, assert_records_free_of

    device_id = await seed_device(nso_device_name="sw-malformed-lag", netbox_device_id=9821)
    async with _device_session(device_id) as (db, device):
        client = AsyncMock()
        client.get_device_state_section.return_value = {
            "status": "ok",
            "lag": [
                {"name": "lag-invalid", "lag-id": "invalid", "member": []},
                {"name": "lag-2", "lag-id": 2, "member": []},
            ],
        }

        with capture_logs() as logs:
            assert await refresh_lag_topology_for_device(db, device, client) is True
        rows = (await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))).scalars().all()
        assert [(row.name, row.lag_id) for row in rows] == [("lag-2", 2)]
        skipped = [record for record in logs if record["event"] == "lag_topology.entry_skipped"]
        assert len(skipped) == 1
        assert_keys_absent(skipped[0], ["lag_name"])
        assert_records_free_of(skipped, ["lag-invalid"])


@pytest.mark.anyio
async def test_a_BOOLEAN_lag_id_is_refused(adapter_client):
    """``int(True)`` stored LAG 1, a bundle id the device never reported."""
    device_id = await seed_device(nso_device_name="sw-bool-lag", netbox_device_id=1408)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "device-name": "sw-bool-lag",
            "lag": [{"name": "Port-channel1", "lag-id": True, "member": []}],
        }
        assert await refresh_lag_topology_for_device(db, device, nso_client, refresh_source="poll") is True
        rows = (await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))).scalars().all()
        assert rows == []
