# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for nso_instances.py endpoint functions."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from nso_adapter.api.nso_instances import list_instance_devices, list_nso_instances
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device


# ── list_nso_instances ────────────────────────────────────────────────────────


async def test_list_nso_instances_empty_config(adapter_client):
    """list_nso_instances() returns empty list when no NSO instances in config."""
    result = await list_nso_instances()
    assert result == []


async def test_list_nso_instances_with_instance_reachable(adapter_client_with_nso):
    """list_nso_instances() marks instance reachable when list_devices succeeds."""
    mock_client = MagicMock()
    mock_client.list_devices = AsyncMock(return_value=[])

    with patch("nso_adapter.api.nso_instances.get_nso_client", return_value=mock_client):
        result = await list_nso_instances()

    assert len(result) == 1
    assert result[0]["id"] == "nso-dev"
    assert result[0]["reachable"] is True


async def test_list_nso_instances_with_instance_unreachable(adapter_client_with_nso):
    """list_nso_instances() marks instance unreachable on exception."""
    mock_client = MagicMock()
    mock_client.list_devices = AsyncMock(side_effect=ConnectionError("timeout"))

    with patch("nso_adapter.api.nso_instances.get_nso_client", return_value=mock_client):
        result = await list_nso_instances()

    assert result[0]["reachable"] is False


# ── list_instance_devices ─────────────────────────────────────────────────────


async def test_list_instance_devices_unknown_instance(adapter_client_with_nso):
    """list_instance_devices() raises 404 for unknown instance_id."""
    async for db in get_session():
        with pytest.raises(HTTPException) as exc_info:
            await list_instance_devices(instance_id="nonexistent", db=db)
        assert exc_info.value.status_code == 404
        break


async def test_list_instance_devices_nso_connection_error(adapter_client_with_nso):
    """list_instance_devices() raises 502 when NSO is unreachable."""
    mock_client = MagicMock()
    mock_client.list_devices = AsyncMock(side_effect=ConnectionError("NSO down"))

    async for db in get_session():
        with patch("nso_adapter.api.nso_instances.get_nso_client", return_value=mock_client):
            with pytest.raises(HTTPException) as exc_info:
                await list_instance_devices(instance_id="nso-dev", db=db)
        assert exc_info.value.status_code == 502
        break


async def test_list_instance_devices_returns_sorted_list(adapter_client_with_nso):
    """list_instance_devices() returns enriched, sorted device list."""
    mock_client = MagicMock()
    mock_client.list_devices = AsyncMock(return_value=[
        {
            "name": "zzz-router",
            "address": "10.0.0.2",
            "authgroup": "default",
            "device-type": {"cli": {"ned-id": "cisco-ios-cli-6.95"}},
            "state": {"admin-state": "unlocked"},
        },
        {
            "name": "aaa-router",
            "address": "10.0.0.1",
            "authgroup": "default",
            "device-type": {"netconf": {"ned-id": "cisco-iosxr-nc-7.6"}},
            "state": {"admin-state": "locked"},
        },
    ])
    async for db in get_session():
        with patch("nso_adapter.api.nso_instances.get_nso_client", return_value=mock_client):
            result = await list_instance_devices(instance_id="nso-dev", db=db)

        assert result[0]["name"] == "aaa-router"
        assert result[1]["name"] == "zzz-router"
        assert result[0]["onboarded"] is False
        break


async def test_list_instance_devices_marks_onboarded(adapter_client_with_nso):
    """list_instance_devices() sets onboarded=True for known devices."""
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name="known-router", netbox_device_id=500)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        device_id = d.id

    mock_client = MagicMock()
    mock_client.list_devices = AsyncMock(return_value=[
        {
            "name": "known-router",
            "address": "10.0.0.3",
            "authgroup": "default",
            "device-type": {"cli": {"ned-id": "cisco-ios-cli-6.95"}},
            "state": {"admin-state": "unlocked"},
        },
    ])
    async for db in get_session():
        with patch("nso_adapter.api.nso_instances.get_nso_client", return_value=mock_client):
            result = await list_instance_devices(instance_id="nso-dev", db=db)

        assert result[0]["onboarded"] is True
        assert result[0]["onboarded_device_id"] == device_id
        break


async def test_list_instance_devices_skips_invalid_entries(adapter_client_with_nso):
    """list_instance_devices() skips entries without a 'name' key."""
    mock_client = MagicMock()
    mock_client.list_devices = AsyncMock(return_value=[
        {"name": "valid-device", "address": "10.0.0.4"},
        {"no-name-key": "something"},
        {"name": ""},  # falsy name
    ])
    async for db in get_session():
        with patch("nso_adapter.api.nso_instances.get_nso_client", return_value=mock_client):
            result = await list_instance_devices(instance_id="nso-dev", db=db)

    assert len(result) == 1
    assert result[0]["name"] == "valid-device"
