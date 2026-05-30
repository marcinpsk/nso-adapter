# SPDX-License-Identifier: Apache-2.0
"""Tests for bindings/netbox/writer.py — write_interfaces."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nso_adapter.bindings.netbox.writer import WriteResult, write_interfaces


def _make_nb_client():
    client = MagicMock()
    client._base = "http://netbox"
    client.get_interface = AsyncMock()
    client.patch_interface = AsyncMock()
    return client


def _domain_iface(name="GigabitEthernet0/0", description="test", enabled=True):
    iface = MagicMock()
    iface.name = name
    iface.nso = MagicMock()
    iface.nso.description = description
    iface.nso.enabled = enabled
    return iface


@pytest.mark.asyncio
async def test_write_description_and_enabled():
    """Writes both description and enabled when in scope."""
    client = _make_nb_client()
    client.get_interface.return_value = {"id": 5, "name": "GigabitEthernet0/0"}
    iface = _domain_iface()

    with patch("nso_adapter.bindings.netbox.writer.resolve_or_create_interface", AsyncMock(return_value=5)):
        result = await write_interfaces(client, 42, [iface], ["description", "enabled"])

    assert result.interfaces_written == 1
    assert result.interfaces_created == 0
    assert result.interfaces_skipped == 0
    client.patch_interface.assert_called_once_with(5, {"description": "test", "enabled": True})


@pytest.mark.asyncio
async def test_write_creates_new_interface():
    """Counts as 'created' when get_interface returns None (first write)."""
    client = _make_nb_client()
    client.get_interface.return_value = None  # interface doesn't exist in NetBox yet
    iface = _domain_iface()

    with patch("nso_adapter.bindings.netbox.writer.resolve_or_create_interface", AsyncMock(return_value=99)):
        result = await write_interfaces(client, 42, [iface], ["description"])

    assert result.interfaces_created == 1
    assert result.interfaces_written == 0


@pytest.mark.asyncio
async def test_write_skips_when_resolve_returns_none():
    """Skips interface when resolve_or_create returns None."""
    iface = _domain_iface()

    with patch("nso_adapter.bindings.netbox.writer.resolve_or_create_interface", AsyncMock(return_value=None)):
        result = await write_interfaces(MagicMock(), 42, [iface], ["description"])

    assert result.interfaces_skipped == 1
    assert result.interfaces_written == 0


@pytest.mark.asyncio
async def test_write_skips_empty_payload():
    """Skips patch_interface when no scope_attrs match the interface's available data."""
    client = _make_nb_client()
    iface = _domain_iface()
    iface.nso.description = None  # no description
    iface.nso.enabled = None      # no enabled

    with patch("nso_adapter.bindings.netbox.writer.resolve_or_create_interface", AsyncMock(return_value=5)):
        result = await write_interfaces(client, 42, [iface], ["description", "enabled"])

    client.patch_interface.assert_not_called()
    # Skipped count stays 0 — skipping empty payload is silent, not an error
    assert result.interfaces_written == 0
    assert result.interfaces_skipped == 0


@pytest.mark.asyncio
async def test_write_counts_skipped_on_patch_error():
    """Counts as skipped when patch_interface raises an exception."""
    client = _make_nb_client()
    client.get_interface.return_value = {"id": 5}
    client.patch_interface.side_effect = Exception("NetBox 502")
    iface = _domain_iface()

    with patch("nso_adapter.bindings.netbox.writer.resolve_or_create_interface", AsyncMock(return_value=5)):
        result = await write_interfaces(client, 42, [iface], ["description"])

    assert result.interfaces_skipped == 1


@pytest.mark.asyncio
async def test_write_empty_interface_list():
    """Returns zero-count result for empty interface list."""
    result = await write_interfaces(MagicMock(), 42, [], ["description"])
    assert result == WriteResult()
