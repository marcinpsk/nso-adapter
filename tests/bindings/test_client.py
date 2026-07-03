# SPDX-License-Identifier: Apache-2.0
"""Tests for bindings/netbox/client.py — device_exists + create_journal_entry.

The pooled ``_client()`` is replaced with an AsyncMock returning REAL httpx.Response
objects (so status_code / .json() / .raise_for_status() behave for real).
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from nso_adapter.bindings.netbox.client import NetboxClient


def _response(json_data, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=json_data, request=httpx.Request("GET", "http://netbox/x"))


def _client_with_http(http) -> NetboxClient:
    client = NetboxClient(url="http://netbox", token="tok")
    http.is_closed = False  # so _client() reuses our mock instead of building a real one
    client._http = http
    return client


@pytest.mark.asyncio
async def test_device_exists_true_on_200():
    http = AsyncMock()
    http.get.return_value = _response({"id": 42}, 200)
    client = _client_with_http(http)

    assert await client.device_exists(42) is True
    assert http.get.await_args.args[0].endswith("/api/dcim/devices/42/")


@pytest.mark.asyncio
async def test_device_exists_false_on_404():
    http = AsyncMock()
    http.get.return_value = _response({"detail": "Not found."}, 404)
    client = _client_with_http(http)

    assert await client.device_exists(999) is False  # gone from NetBox → no raise


@pytest.mark.asyncio
async def test_create_journal_entry_posts_device_entry():
    http = AsyncMock()
    http.post.return_value = _response({"id": 1}, 201)
    client = _client_with_http(http)

    await client.create_journal_entry(42, comments="offboarded", kind="warning")

    http.post.assert_awaited_once()
    body = http.post.await_args.kwargs["json"]
    assert body["assigned_object_type"] == "dcim.device"
    assert body["assigned_object_id"] == 42
    assert body["kind"] == "warning"
    assert body["comments"] == "offboarded"
