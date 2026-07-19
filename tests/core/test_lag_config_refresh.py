# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for refresh_lag_config_for_device (core/lag_config.py)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from nso_adapter.core.lag_config import refresh_lag_config_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, LagBundleConfig, LagMemberConfig
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
async def test_refresh_lag_config_happy(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=910)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_lag_config.return_value = {
            "device-name": "sw03",
            "lag": [
                {
                    "name": "Port-channel1",
                    "lag-id": 1,
                    "min-links": 2,
                    "system-priority": 100,
                    "timer": "fast",
                    "member": [
                        {"interface-name": "GigabitEthernet0/1", "mode": "active", "port-priority": 200},
                        {"interface-name": "GigabitEthernet0/2", "mode": "active"},
                    ],
                },
                {"name": "Port-channel2", "lag-id": 2, "member": []},
            ],
        }

        await refresh_lag_config_for_device(db, device, nso_client, refresh_source="poll")

        nso_client.get_lag_config.assert_awaited_once_with("sw03")
        bundles = (
            (await db.execute(select(LagBundleConfig).where(LagBundleConfig.device_id == device.id))).scalars().all()
        )
        members = (await db.execute(select(LagMemberConfig))).scalars().all()
        assert {b.name for b in bundles} == {"Port-channel1", "Port-channel2"}
        pc1 = next(b for b in bundles if b.name == "Port-channel1")
        assert pc1.min_links == 2
        assert pc1.system_priority == 100
        assert pc1.timer == "fast"
        assert {(m.interface_name, m.mode, m.port_priority) for m in members} == {
            ("GigabitEthernet0/1", "active", 200),
            ("GigabitEthernet0/2", "active", None),
        }


async def test_refresh_lag_config_carries_vpc_sensitive(adapter_client):
    """NX-P2: the reader emits `vpc-sensitive` only for a vPC-protected bundle (absent =
    ordinary). The refresh must store it so the plugin can gate/badge it — a vPC bundle is
    refused zero-write by the lag-reconciler and must never be offered for accept."""
    device_id = await seed_device(nso_device_name="nx-tor", netbox_device_id=913)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_lag_config.return_value = {
            "device-name": "nx-tor",
            "lag": [
                {"name": "port-channel1", "lag-id": 1, "vpc-sensitive": True, "member": []},  # peer-link
                {"name": "port-channel25", "lag-id": 25, "member": []},  # ordinary (flag absent)
            ],
        }

        await refresh_lag_config_for_device(db, device, nso_client, refresh_source="poll")

        bundles = (
            (await db.execute(select(LagBundleConfig).where(LagBundleConfig.device_id == device.id))).scalars().all()
        )
        by_name = {b.name: b for b in bundles}
        assert by_name["port-channel1"].vpc_sensitive is True
        assert by_name["port-channel25"].vpc_sensitive is False  # absent flag → ordinary


async def test_refresh_lag_config_skips_malformed_bundles_and_members(adapter_client):
    """A bundle missing name/lag-id, or a member missing interface-name, must be skipped —
    not KeyError-abort the upsert (which sits OUTSIDE the fetch try/except) and freeze the
    whole LAG mirror for the device (s2-6)."""
    device_id = await seed_device(nso_device_name="sw-malformed", netbox_device_id=912)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_lag_config.return_value = {
            "device-name": "sw-malformed",
            "lag": [
                {"lag-id": 9, "member": []},  # missing name → skip
                {"name": "Port-channel3", "member": []},  # missing lag-id → skip
                {
                    "name": "Port-channel1",
                    "lag-id": 1,
                    "member": [
                        {"mode": "active"},  # missing interface-name → skip
                        {"interface-name": "GigabitEthernet0/1", "mode": "active"},
                    ],
                },
            ],
        }

        await refresh_lag_config_for_device(db, device, nso_client, refresh_source="poll")

        bundles = (
            (await db.execute(select(LagBundleConfig).where(LagBundleConfig.device_id == device.id))).scalars().all()
        )
        members = (await db.execute(select(LagMemberConfig))).scalars().all()
        assert [b.name for b in bundles] == ["Port-channel1"]  # only the well-formed bundle
        assert [m.interface_name for m in members] == ["GigabitEthernet0/1"]  # nameless member skipped


@pytest.mark.anyio
async def test_refresh_lag_config_clears_on_404(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=911)
    async with _device_session(device_id) as (db, device):
        db.add(
            LagBundleConfig(
                device_id=device.id,
                name="Port-channel1",
                lag_id=1,
                last_refreshed_at=datetime.now(UTC).replace(tzinfo=None),
                refresh_source="poll",
            )
        )
        await db.commit()

        nso_client = AsyncMock()
        nso_client.get_lag_config.return_value = None

        await refresh_lag_config_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(select(LagBundleConfig).where(LagBundleConfig.device_id == device.id))
        assert result.scalars().all() == []


@pytest.mark.anyio
async def test_refresh_lag_config_transport_error_no_change(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=912)
    async with _device_session(device_id) as (db, device):
        db.add(
            LagBundleConfig(
                device_id=device.id,
                name="Port-channel1",
                lag_id=1,
                last_refreshed_at=datetime.now(UTC).replace(tzinfo=None),
                refresh_source="poll",
            )
        )
        await db.commit()

        nso_client = AsyncMock()
        nso_client.get_lag_config.side_effect = httpx.ConnectError("timeout")

        await refresh_lag_config_for_device(db, device, nso_client, refresh_source="poll")

        # Existing rows untouched on transport error.
        result = await db.execute(select(LagBundleConfig).where(LagBundleConfig.device_id == device.id))
        assert len(result.scalars().all()) == 1


@pytest.mark.anyio
async def test_refresh_lag_config_skips_without_nso_name(adapter_client):
    device_id = await seed_device(nso_device_name="sw03", netbox_device_id=913)
    async with _device_session(device_id) as (db, device):
        device.nso_device_name = None
        nso_client = AsyncMock()

        await refresh_lag_config_for_device(db, device, nso_client, refresh_source="poll")

        nso_client.get_lag_config.assert_not_awaited()
