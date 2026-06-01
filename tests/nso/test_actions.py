# SPDX-License-Identifier: Apache-2.0
"""Tests for nso/actions.py — sync_from, compare_config, check_sync, connect."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nso_adapter.nso.actions import check_sync, compare_config, connect, sync_from


def _make_nso_client(base="http://nso"):
    client = MagicMock()
    client._base = base
    client._action_timeout = 120.0
    return client


def _mock_http_ctx(response):
    mock_http = AsyncMock()
    mock_http.post.return_value = response
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    return ctx


def _resp(status=200, json_data=None, has_content=True):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_data or {}
    resp.raise_for_status.return_value = None
    resp.content = b"content" if has_content else b""
    return resp


@pytest.mark.asyncio
async def test_sync_from_success():
    output = {"result": "ok"}
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_resp(200, {"tailf-ncs:output": output}))

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
    client._client.return_value = _mock_http_ctx(_resp(200, {}))

    result = await sync_from(client, "rtr")

    assert result == {}


@pytest.mark.asyncio
async def test_compare_config_with_output():
    output = {"diff": "no diff"}
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_resp(200, {"tailf-ncs:output": output}))

    result = await compare_config(client, "rtr")

    assert result == output


@pytest.mark.asyncio
async def test_compare_config_204_no_content():
    """204 response returns empty dict."""
    client = _make_nso_client()
    resp = _resp(204, has_content=False)
    resp.content = b""
    client._client.return_value = _mock_http_ctx(resp)

    result = await compare_config(client, "rtr")

    assert result == {}


@pytest.mark.asyncio
async def test_compare_config_200_no_body():
    """200 with empty body also returns empty dict."""
    client = _make_nso_client()
    resp = _resp(200, has_content=False)
    resp.content = b""
    client._client.return_value = _mock_http_ctx(resp)

    result = await compare_config(client, "rtr")

    assert result == {}


@pytest.mark.asyncio
async def test_check_sync_in_sync():
    output = {"tailf-ncs:output": {"result": "in-sync"}}
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_resp(200, output))

    result = await check_sync(client, "rtr")

    assert result is True


@pytest.mark.asyncio
async def test_check_sync_not_in_sync():
    output = {"tailf-ncs:output": {"result": "out-of-sync"}}
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_resp(200, output))

    result = await check_sync(client, "rtr")

    assert result is False


@pytest.mark.asyncio
async def test_check_sync_exception_returns_false():
    client = _make_nso_client()
    resp = _resp(200)
    resp.raise_for_status.side_effect = Exception("connection refused")
    client._client.return_value = _mock_http_ctx(resp)

    result = await check_sync(client, "rtr")

    assert result is False


@pytest.mark.asyncio
async def test_connect_success():
    output = {"result": "connected"}
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_resp(200, {"tailf-ncs:output": output}))

    result = await connect(client, "rtr")

    assert result == output
    mock_http = client._client.return_value.__aenter__.return_value
    url = mock_http.post.call_args[0][0]
    assert "connect" in url


@pytest.mark.asyncio
async def test_connect_empty_output():
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_resp(200, {}))

    result = await connect(client, "rtr")

    assert result == {}
