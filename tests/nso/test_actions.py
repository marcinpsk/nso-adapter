# SPDX-License-Identifier: Apache-2.0
"""Tests for nso/actions.py — sync_from, compare_config, check_sync, connect."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from nso_adapter.nso.actions import check_sync, compare_config, connect, sync_from
from nso_adapter.nso.client import NsoClient


def _make_nso_client(base="http://nso"):
    # The NSO RESTCONF client is a real external HTTP boundary; bind the fake to NsoClient via
    # spec= so a renamed member can't be fabricated. Only _base/_action_timeout are read; each
    # test fakes the POST round-trip via client._client() (see _stub_pool).
    client = MagicMock(spec=NsoClient)
    client._base = base
    client._action_timeout = 120.0
    return client


def _stub_pool(client, http):
    """Wire client._client() as an async CM yielding *http* via the spec'd client's auto-created
    child (no bare MagicMock CM); __aexit__ returns False so exceptions propagate. Returns *http*."""
    cm = client._client.return_value
    cm.__aenter__.return_value = http
    cm.__aexit__.return_value = False
    return http


def _mock_http_ctx(client, response):
    """Wire client._client() to an http whose .post returns *response*; returns the http object."""
    http = AsyncMock()
    http.post.return_value = response
    return _stub_pool(client, http)


def _resp(status=200, json_data=None, has_content=True) -> httpx.Response:
    """A REAL httpx.Response with genuine status_code/.content/.json()/.raise_for_status.

    has_content=False → no body (so .content == b'' and .json() would raise); a 4xx/5xx
    status makes .raise_for_status() raise a real httpx.HTTPStatusError.
    """
    req = httpx.Request("POST", "http://nso/action")
    if not has_content:
        return httpx.Response(status, request=req)
    return httpx.Response(status, json=json_data if json_data is not None else {}, request=req)


@pytest.mark.asyncio
async def test_sync_from_success():
    output = {"result": "ok"}
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(200, {"tailf-ncs:output": output}))

    result = await sync_from(client, "core-rtr-01")

    assert result == output
    mock_http = client._client.return_value.__aenter__.return_value
    mock_http.post.assert_called_once()
    url = mock_http.post.call_args[0][0]
    assert "core-rtr-01" in url
    assert "sync-from" in url


@pytest.mark.asyncio
async def test_sync_from_empty_output():
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(200, {}))

    result = await sync_from(client, "rtr")

    assert result == {}


@pytest.mark.asyncio
async def test_compare_config_with_output():
    output = {"diff": "no diff"}
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(200, {"tailf-ncs:output": output}))

    result = await compare_config(client, "rtr")

    assert result == output


@pytest.mark.asyncio
async def test_compare_config_204_no_content():
    """204 response returns empty dict."""
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(204, has_content=False))

    result = await compare_config(client, "rtr")

    assert result == {}


@pytest.mark.asyncio
async def test_compare_config_200_no_body():
    """200 with empty body also returns empty dict."""
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(200, has_content=False))

    result = await compare_config(client, "rtr")

    assert result == {}


@pytest.mark.asyncio
async def test_check_sync_in_sync():
    output = {"tailf-ncs:output": {"result": "in-sync"}}
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(200, output))

    result = await check_sync(client, "rtr")

    assert result is True


@pytest.mark.asyncio
async def test_check_sync_not_in_sync():
    output = {"tailf-ncs:output": {"result": "out-of-sync"}}
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(200, output))

    result = await check_sync(client, "rtr")

    assert result is False


@pytest.mark.asyncio
async def test_check_sync_exception_returns_false():
    """A non-2xx response makes check_sync swallow the error and report not-in-sync.

    The 500 body deliberately says "in-sync": only a real raise_for_status() turns this
    into False — if it were skipped, the body would be read and the result would be True.
    """
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(500, {"tailf-ncs:output": {"result": "in-sync"}}))

    result = await check_sync(client, "rtr")

    assert result is False


@pytest.mark.asyncio
async def test_connect_success():
    output = {"result": "connected"}
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(200, {"tailf-ncs:output": output}))

    result = await connect(client, "rtr")

    assert result == output
    mock_http = client._client.return_value.__aenter__.return_value
    url = mock_http.post.call_args[0][0]
    assert "connect" in url


@pytest.mark.asyncio
async def test_connect_empty_output():
    client = _make_nso_client()
    _mock_http_ctx(client, _resp(200, {}))

    result = await connect(client, "rtr")

    assert result == {}
