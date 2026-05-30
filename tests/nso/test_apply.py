# SPDX-License-Identifier: Apache-2.0
"""Tests for nso/apply.py — apply_interface_attribute."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nso_adapter.nso.apply import NsoApplyError, apply_interface_attribute


def _make_nso_client(base="http://nso"):
    client = MagicMock()
    client._base = base
    client._action_timeout = 120.0
    return client


def _mock_httpx_response(status: int = 204, json_data=None):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_data or {}
    resp.text = "error body"
    return resp


def _mock_http_ctx(response):
    mock_http = AsyncMock()
    mock_http.patch.return_value = response
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_http)
    ctx.__aexit__ = AsyncMock(return_value=None)
    return ctx


@pytest.mark.asyncio
async def test_apply_description_success():
    """Applies description attribute without raising."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    await apply_interface_attribute(client, "core-rtr-01", "GigabitEthernet0/0", "description", "uplink")

    mock_http = client._client.return_value.__aenter__.return_value
    mock_http.patch.assert_called_once()
    url, = mock_http.patch.call_args[0]
    assert "interface-reconciler" in url
    import json
    payload = json.loads(mock_http.patch.call_args[1]["content"])
    entries = payload["interface-reconciler:interface-config"]
    assert entries[0]["description"] == "uplink"
    assert entries[0]["device"] == "core-rtr-01"
    assert entries[0]["interface-name"] == "GigabitEthernet0/0"


@pytest.mark.asyncio
async def test_apply_enabled_true():
    """enabled='True' string maps to boolean True."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(200))

    await apply_interface_attribute(client, "rtr", "ge-0/0/0", "enabled", "True")

    mock_http = client._client.return_value.__aenter__.return_value
    import json
    payload = json.loads(mock_http.patch.call_args[1]["content"])
    assert payload["interface-reconciler:interface-config"][0]["enabled"] is True


@pytest.mark.asyncio
async def test_apply_enabled_false():
    """enabled != 'True' string maps to boolean False."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(201))

    await apply_interface_attribute(client, "rtr", "ge-0/0/0", "enabled", "False")

    mock_http = client._client.return_value.__aenter__.return_value
    import json
    payload = json.loads(mock_http.patch.call_args[1]["content"])
    assert payload["interface-reconciler:interface-config"][0]["enabled"] is False


@pytest.mark.asyncio
async def test_apply_description_none_uses_empty_string():
    """None description is converted to empty string in the payload."""
    client = _make_nso_client()
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(204))

    await apply_interface_attribute(client, "rtr", "ge-0/0/0", "description", None)

    mock_http = client._client.return_value.__aenter__.return_value
    import json
    payload = json.loads(mock_http.patch.call_args[1]["content"])
    assert payload["interface-reconciler:interface-config"][0]["description"] == ""


@pytest.mark.asyncio
async def test_apply_unsupported_attribute_raises():
    """Unsupported attribute raises NsoApplyError immediately (no HTTP call)."""
    client = _make_nso_client()

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_interface_attribute(client, "rtr", "ge-0/0/0", "mtu", "1500")

    assert exc_info.value.code == "unsupported_attribute"
    client._client.assert_not_called()


@pytest.mark.asyncio
async def test_apply_nso_error_status_raises():
    """Non-2xx NSO response raises NsoApplyError."""
    client = _make_nso_client()
    error_body = {"error": {"code": "locked", "message": "device locked"}}
    client._client.return_value = _mock_http_ctx(_mock_httpx_response(409, json_data=error_body))

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_interface_attribute(client, "rtr", "ge-0/0/0", "description", "x")

    assert exc_info.value.code == "nso_patch_failed"
    assert "409" in exc_info.value.message


@pytest.mark.asyncio
async def test_apply_nso_error_non_json_body():
    """Non-JSON NSO error body is captured as raw text."""
    client = _make_nso_client()
    resp = _mock_httpx_response(500)
    resp.json.side_effect = Exception("not json")
    client._client.return_value = _mock_http_ctx(resp)

    with pytest.raises(NsoApplyError) as exc_info:
        await apply_interface_attribute(client, "rtr", "ge-0/0/0", "description", "x")

    assert "raw" in exc_info.value.detail.get("nso_error", {})


def test_nso_apply_error_str():
    """NsoApplyError carries code, message, and optional detail."""
    err = NsoApplyError("test_code", "test message", {"k": "v"})
    assert str(err) == "test message"
    assert err.code == "test_code"
    assert err.detail == {"k": "v"}


def test_nso_apply_error_default_detail():
    """NsoApplyError default detail is empty dict."""
    err = NsoApplyError("code", "msg")
    assert err.detail == {}


# ── apply_interface_ips ──────────────────────────────────────────────────────

from nso_adapter.nso.apply import apply_interface_ips


def _make_ip_row(address: str, family: str = "ipv4", secondary: bool = False, vrf: str = "") -> MagicMock:
    row = MagicMock()
    row.address = address
    row.family = family
    row.secondary = secondary
    row.vrf = vrf
    return row


@pytest.mark.asyncio
async def test_apply_interface_ips_ipv4_primary():
    """Single IPv4 primary address produces correct PATCH body."""
    client = _make_nso_client()
    resp = _mock_httpx_response(204)
    client._client.return_value = _mock_http_ctx(resp)

    row = _make_ip_row("10.0.0.1/24", family="ipv4", secondary=False, vrf="")
    await apply_interface_ips(client, "rtr-a", "GigabitEthernet0/1", [row])

    import json
    call_kwargs = client._client.return_value.__aenter__.return_value.patch.call_args
    body = json.loads(call_kwargs.kwargs["content"])
    entry = body["interface-reconciler:interface-config"][0]
    assert entry["device"] == "rtr-a"
    assert entry["interface-name"] == "GigabitEthernet0/1"
    assert entry["ipv4-address"] == [{"address": "10.0.0.1", "prefix-length": 24, "secondary": False}]
    assert "vrf" not in entry


@pytest.mark.asyncio
async def test_apply_interface_ips_ipv6():
    """IPv6 address produces correct PATCH body."""
    client = _make_nso_client()
    resp = _mock_httpx_response(204)
    client._client.return_value = _mock_http_ctx(resp)

    row = _make_ip_row("2001:db8::1/64", family="ipv6")
    await apply_interface_ips(client, "rtr-b", "GigabitEthernet0/2", [row])

    import json
    call_kwargs = client._client.return_value.__aenter__.return_value.patch.call_args
    body = json.loads(call_kwargs.kwargs["content"])
    entry = body["interface-reconciler:interface-config"][0]
    assert entry["ipv6-address"] == [{"address": "2001:db8::1", "prefix-length": 64}]
    assert "ipv4-address" not in entry


@pytest.mark.asyncio
async def test_apply_interface_ips_sets_vrf():
    """Non-empty vrf field is included in the PATCH body."""
    client = _make_nso_client()
    resp = _mock_httpx_response(204)
    client._client.return_value = _mock_http_ctx(resp)

    row = _make_ip_row("10.1.1.1/30", family="ipv4", vrf="MGMT")
    await apply_interface_ips(client, "rtr-c", "GigabitEthernet0/3", [row])

    import json
    call_kwargs = client._client.return_value.__aenter__.return_value.patch.call_args
    body = json.loads(call_kwargs.kwargs["content"])
    entry = body["interface-reconciler:interface-config"][0]
    assert entry["vrf"] == "MGMT"


@pytest.mark.asyncio
async def test_apply_interface_ips_nso_error_raises():
    """Non-2xx response from NSO raises NsoApplyError."""
    client = _make_nso_client()
    resp = _mock_httpx_response(500, json_data={"error": {"code": "internal"}})
    client._client.return_value = _mock_http_ctx(resp)

    row = _make_ip_row("10.2.0.1/24")
    with pytest.raises(NsoApplyError) as exc_info:
        await apply_interface_ips(client, "rtr-d", "GigabitEthernet0/4", [row])

    assert exc_info.value.code == "nso_patch_failed"
    assert "500" in exc_info.value.message
