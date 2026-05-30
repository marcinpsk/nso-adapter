# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for NetboxClient — get_interface, patch_interface, create_interface."""
from __future__ import annotations

import httpx
import pytest
import respx

from nso_adapter.bindings.netbox.client import NetboxClient

BASE = "http://netbox.local"
TOKEN = "nb-test-token"


@pytest.fixture
def client() -> NetboxClient:
    return NetboxClient(url=BASE, token=TOKEN, timeout=5.0)


# ── get_interface ──────────────────────────────────────────────────────────────


@respx.mock
async def test_get_interface_returns_first_result(client):
    """get_interface() returns the first object in results when present."""
    payload = {"results": [{"id": 42, "name": "GigabitEthernet0/0"}]}
    respx.get(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(200, json=payload))

    result = await client.get_interface(netbox_device_id=1, interface_name="GigabitEthernet0/0")
    assert result == {"id": 42, "name": "GigabitEthernet0/0"}


@respx.mock
async def test_get_interface_returns_none_when_empty(client):
    """get_interface() returns None when results list is empty."""
    payload = {"results": []}
    respx.get(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(200, json=payload))

    result = await client.get_interface(netbox_device_id=1, interface_name="NoSuchIface")
    assert result is None


@respx.mock
async def test_get_interface_raises_on_http_error(client):
    """get_interface() propagates httpx.HTTPStatusError on non-2xx."""
    respx.get(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(403))

    with pytest.raises(httpx.HTTPStatusError):
        await client.get_interface(netbox_device_id=1, interface_name="eth0")


# ── patch_interface ────────────────────────────────────────────────────────────


@respx.mock
async def test_patch_interface_returns_response_json(client):
    """patch_interface() PATCHes the interface and returns the JSON body."""
    updated = {"id": 10, "name": "eth0", "description": "new-desc"}
    respx.patch(f"{BASE}/api/dcim/interfaces/10/").mock(return_value=httpx.Response(200, json=updated))

    result = await client.patch_interface(interface_id=10, payload={"description": "new-desc"})
    assert result == updated


@respx.mock
async def test_patch_interface_raises_on_http_error(client):
    """patch_interface() propagates httpx.HTTPStatusError on 404."""
    respx.patch(f"{BASE}/api/dcim/interfaces/99/").mock(return_value=httpx.Response(404))

    with pytest.raises(httpx.HTTPStatusError):
        await client.patch_interface(interface_id=99, payload={"description": "x"})


# ── create_interface ───────────────────────────────────────────────────────────


@respx.mock
async def test_create_interface_returns_created_object(client):
    """create_interface() POSTs and returns the created object."""
    created = {"id": 55, "name": "eth0", "device": 1}
    respx.post(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(201, json=created))

    result = await client.create_interface(payload={"name": "eth0", "device": 1})
    assert result == created


@respx.mock
async def test_create_interface_raises_on_http_error(client):
    """create_interface() propagates httpx.HTTPStatusError on 400."""
    respx.post(f"{BASE}/api/dcim/interfaces/").mock(return_value=httpx.Response(400, json={"detail": "bad payload"}))

    with pytest.raises(httpx.HTTPStatusError):
        await client.create_interface(payload={"name": ""})
