# SPDX-License-Identifier: Apache-2.0
"""Tests for bindings/netbox/scope.py — fetch_all_scope."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from nso_adapter.bindings.netbox.client import NetboxClient
from nso_adapter.bindings.netbox.scope import PluginScopeRecord, fetch_all_scope


def _make_nb_client(base="http://netbox"):
    # NetboxClient is a real external HTTP boundary; bind the fake to its interface via
    # spec= so a renamed/removed method can't be fabricated. fetch_all_scope reads ._base
    # and calls ._client() — both real members.
    client = MagicMock(spec=NetboxClient)
    client._base = base
    return client


def _httpx_response(json_data: dict | list, status: int = 200) -> httpx.Response:
    """A REAL httpx.Response (real status_code/.json()/.raise_for_status), not a mock —
    so .raise_for_status() raises a genuine httpx.HTTPStatusError on 4xx/5xx."""
    return httpx.Response(status, json=json_data, request=httpx.Request("GET", "http://netbox/scope"))


def _mock_http_ctx(response):
    # fetch_all_scope uses the shared pooled client DIRECTLY (c = client._client();
    # await c.get(...)) — it must NOT enter it as a context manager (that raises
    # "Cannot open a client instance more than once" on the reused singleton). So the
    # mock for _client() is the http object itself, with no __aenter__/__aexit__.
    mock_http = AsyncMock()
    mock_http.get.return_value = response
    return mock_http


@pytest.mark.asyncio
async def test_fetch_all_scope_results_key():
    """Handles paginated DRF response with a `results` key."""
    data = {
        "count": 2,
        "next": None,
        "results": [
            {"device": {"id": 10}, "managed_attributes": ["description"]},
            {"device": {"id": 20}, "managed_attributes": ["description", "enabled"]},
        ],
    }
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_scope(client)

    assert len(records) == 2
    assert records[0] == PluginScopeRecord(netbox_device_id=10, attributes=["description"])
    assert records[1].attributes == ["description", "enabled"]


@pytest.mark.asyncio
async def test_fetch_all_scope_bare_list():
    """Handles bare list response (no `results` key)."""
    data = [{"netbox_device_id": 42, "attributes": ["enabled"]}]
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_scope(client)

    assert len(records) == 1
    assert records[0].netbox_device_id == 42


@pytest.mark.asyncio
async def test_fetch_all_scope_device_as_int():
    """Handles `device` field as bare integer."""
    data = {"results": [{"device": 77, "managed_attributes": ["description"]}]}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_scope(client)

    assert records[0].netbox_device_id == 77


@pytest.mark.asyncio
async def test_fetch_all_scope_device_id_key():
    """Handles `device` as dict with only `id` key."""
    data = {"results": [{"device": {"id": 33}, "managed_attributes": []}]}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_scope(client)

    assert records[0].netbox_device_id == 33


@pytest.mark.asyncio
async def test_fetch_all_scope_skips_non_dict_items():
    """Non-dict items in the list are skipped."""
    data = {"results": ["garbage", {"device": 1, "managed_attributes": ["description"]}]}
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_scope(client)

    assert len(records) == 1


@pytest.mark.asyncio
async def test_fetch_all_scope_skips_missing_device_id():
    """Items with no resolvable device id are skipped."""
    data = {"results": [{"managed_attributes": ["description"]}]}  # no device field
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response(data))

    records = await fetch_all_scope(client)

    assert len(records) == 0


@pytest.mark.asyncio
async def test_fetch_all_scope_raises_on_http_error():
    """HTTP error propagates to caller (raise_for_status → httpx.HTTPStatusError)."""
    client = _make_nb_client()
    client._client.return_value = _mock_http_ctx(_httpx_response({}, status=500))

    with pytest.raises(httpx.HTTPStatusError):
        await fetch_all_scope(client)


@pytest.mark.asyncio
async def test_fetch_all_scope_uses_pooled_client_directly():
    """Regression: NetboxClient._client() is a long-lived pooled singleton, already
    opened by other requests. Entering it as a context manager raises httpx's
    RuntimeError('Cannot open a client instance more than once.') — which previously
    aborted the scope reconcile. fetch_all_scope must use the client directly."""
    data = {"results": [{"device": {"id": 10}, "managed_attributes": ["description"]}]}
    mock_http = AsyncMock()
    mock_http.get.return_value = _httpx_response(data)
    # Mimic httpx: re-entering an already-opened client raises.
    mock_http.__aenter__.side_effect = RuntimeError("Cannot open a client instance more than once.")
    client = _make_nb_client()
    client._client.return_value = mock_http

    records = await fetch_all_scope(client)

    assert len(records) == 1
    mock_http.__aenter__.assert_not_called()
