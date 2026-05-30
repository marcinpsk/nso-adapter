# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/snmp.py — refresh and SSE event handler."""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.snmp import handle_snmp_config_change, refresh_snmp_config_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, SnmpCommunity, SnmpHost, SnmpSystemInfo, SnmpV3User
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
async def test_refresh_inserts_communities(adapter_client):
    """NSO returns two communities → SnmpCommunity rows inserted."""
    device_id = await seed_device(nso_device_name="snmp-insert-sw01", netbox_device_id=960)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_snmp_config.return_value = {
            "name": "snmp-insert-sw01",
            "community": [
                {"name": "abc123def456abcd", "access": "RO", "acl": "20", "has-secret": True},
                {"name": "def456abc123def4", "access": "RW", "has-secret": True},
            ],
        }

        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")

        nso_client.get_snmp_config.assert_awaited_once_with("snmp-insert-sw01")
        result = await db.execute(
            select(SnmpCommunity).where(SnmpCommunity.device_id == device.id)
        )
        rows = result.scalars().all()
        assert len(rows) == 2
        hashes = {r.community_hash for r in rows}
        assert hashes == {"abc123def456abcd", "def456abc123def4"}
        ro = next(r for r in rows if r.community_hash == "abc123def456abcd")
        assert ro.access == "RO"
        assert ro.acl == "20"
        assert ro.refresh_source == "poll"


@pytest.mark.anyio
async def test_refresh_inserts_v3_users(adapter_client):
    """NSO returns v3 users → SnmpV3User rows inserted, passwords never stored."""
    device_id = await seed_device(nso_device_name="snmp-v3-sw01", netbox_device_id=961)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_snmp_config.return_value = {
            "name": "snmp-v3-sw01",
            "v3-user": [
                {"username": "monitor", "has-auth-secret": True, "has-priv-secret": False},
                {"username": "admin", "has-auth-secret": True, "has-priv-secret": True},
            ],
        }

        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(
            select(SnmpV3User).where(SnmpV3User.device_id == device.id)
        )
        rows = result.scalars().all()
        assert len(rows) == 2
        by_name = {r.username: r for r in rows}
        assert by_name["monitor"].has_auth_secret is True
        assert by_name["monitor"].has_priv_secret is False
        assert by_name["admin"].has_priv_secret is True


@pytest.mark.anyio
async def test_refresh_inserts_hosts(adapter_client):
    """NSO returns host entries → SnmpHost rows inserted."""
    device_id = await seed_device(nso_device_name="snmp-hosts-sw01", netbox_device_id=962)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_snmp_config.return_value = {
            "name": "snmp-hosts-sw01",
            "host": [
                {"address": "10.0.1.100", "version": "2c", "notify-type": "trap", "port": 162},
            ],
        }

        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(
            select(SnmpHost).where(SnmpHost.device_id == device.id)
        )
        rows = result.scalars().all()
        assert len(rows) == 1
        h = rows[0]
        assert h.address == "10.0.1.100"
        assert h.version == "2c"
        assert h.notify_type == "trap"
        assert h.port == 162


@pytest.mark.anyio
async def test_refresh_inserts_system_info(adapter_client):
    """NSO returns location/contact → SnmpSystemInfo row inserted."""
    device_id = await seed_device(nso_device_name="snmp-sysinfo-sw01", netbox_device_id=963)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_snmp_config.return_value = {
            "name": "snmp-sysinfo-sw01",
            "location": "ITC-Lab",
            "contact": "noc@example.com",
        }

        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(
            select(SnmpSystemInfo).where(SnmpSystemInfo.device_id == device.id)
        )
        info = result.scalar_one_or_none()
        assert info is not None
        assert info.location == "ITC-Lab"
        assert info.contact == "noc@example.com"


@pytest.mark.anyio
async def test_refresh_full_replaces_existing_rows(adapter_client):
    """Second refresh full-replaces: old community rows are deleted, new ones inserted."""
    device_id = await seed_device(nso_device_name="snmp-replace-sw01", netbox_device_id=964)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        # First refresh — two communities
        nso_client.get_snmp_config.return_value = {
            "name": "snmp-replace-sw01",
            "community": [
                {"name": "hash_old1_abcd1234", "access": "RO"},
                {"name": "hash_old2_efgh5678", "access": "RW"},
            ],
        }
        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")

        # Second refresh — only one (different) community
        nso_client.get_snmp_config.return_value = {
            "name": "snmp-replace-sw01",
            "community": [
                {"name": "hash_new1_mnop9012", "access": "RO"},
            ],
        }
        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(
            select(SnmpCommunity).where(SnmpCommunity.device_id == device.id)
        )
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].community_hash == "hash_new1_mnop9012"


@pytest.mark.anyio
async def test_refresh_skips_device_without_nso_name(adapter_client):
    """Device with empty nso_device_name → skipped, no NSO call."""
    device_id = await seed_device(nso_device_name="", netbox_device_id=965)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")
        nso_client.get_snmp_config.assert_not_called()


@pytest.mark.anyio
async def test_refresh_handles_nso_error_gracefully(adapter_client):
    """NSO client raises → no exception propagated, no DB rows."""
    device_id = await seed_device(nso_device_name="snmp-err-sw01", netbox_device_id=966)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_snmp_config.side_effect = Exception("RESTCONF error")

        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(
            select(SnmpCommunity).where(SnmpCommunity.device_id == device.id)
        )
        assert result.scalars().all() == []


@pytest.mark.anyio
async def test_refresh_handles_nso_none_gracefully(adapter_client):
    """NSO returns None (404) → no rows inserted, no error."""
    device_id = await seed_device(nso_device_name="snmp-none-sw01", netbox_device_id=967)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_snmp_config.return_value = None

        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(
            select(SnmpCommunity).where(SnmpCommunity.device_id == device.id)
        )
        assert result.scalars().all() == []


@pytest.mark.anyio
async def test_handle_snmp_config_change_dispatches_to_affected_devices(adapter_client):
    """SSE event → refresh is triggered for each affected managed device."""
    device_id = await seed_device(nso_device_name="snmp-sse-sw01", netbox_device_id=968)
    nso_client = AsyncMock()
    nso_client.get_snmp_config.return_value = {
        "name": "snmp-sse-sw01",
        "community": [{"name": "aaabbbccc1112233", "access": "RO"}],
    }

    # Simulate a NETCONF config-change event mentioning this device
    event_data = {
        "ietf-restconf:notification": {
            "eventTime": "2026-06-10T09:00:00.000Z",
            "netconf-config-change": {
                "changed-by": {"username": "admin"},
                "edit": [
                    {
                        "target": "/ncs:devices/device[name='snmp-sse-sw01']/ncs:config",
                        "operation": "merge",
                    }
                ],
            },
        }
    }

    async for db in get_session():
        await handle_snmp_config_change(event_data, db, {"nso-dev": nso_client})
        break

    nso_client.get_snmp_config.assert_awaited_once_with("snmp-sse-sw01")

    async for db in get_session():
        result = await db.execute(
            select(SnmpCommunity).where(SnmpCommunity.device_id == device_id)
        )
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].community_hash == "aaabbbccc1112233"
        break
