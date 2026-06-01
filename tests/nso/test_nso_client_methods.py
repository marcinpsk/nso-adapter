# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for NsoClient HTTP methods using httpx mock transport.

These tests exercise list_devices, get_device_config, get_device_ned_id,
and check_sync without hitting a real NSO instance.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import httpx
import pytest

from nso_adapter.nso.client import NsoClient


def _make_cfg(base_url: str = "http://nso:8080", ca_cert=None, host_header=None):
    cfg = MagicMock()
    cfg.base_url = base_url
    cfg.ca_cert = ca_cert
    cfg.host_header = host_header
    return cfg


def _make_client(base_url: str = "http://nso:8080", host_header=None) -> NsoClient:
    return NsoClient(_make_cfg(base_url, host_header=host_header), "admin", "secret")


class MockTransport(httpx.AsyncBaseTransport):
    """Minimal async transport returning a pre-canned response."""

    def __init__(self, status_code: int, body: dict | None = None, raw_bytes: bytes | None = None):
        self.status_code = status_code
        if raw_bytes is not None:
            self._content = raw_bytes
        elif body is not None:
            self._content = json.dumps(body).encode()
        else:
            self._content = b""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            self.status_code,
            content=self._content,
            headers={"content-type": "application/yang-data+json"},
            request=request,
        )


@pytest.fixture
def patch_client():
    """Return a helper that patches NsoClient._client() to use MockTransport."""
    import contextlib

    @contextlib.contextmanager
    def _patcher(nso_client: NsoClient, status: int, body: dict | None = None, raw: bytes | None = None):
        transport = MockTransport(status, body, raw)
        original = nso_client._client

        def _mock_client(timeout=None):
            return httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

        nso_client._client = _mock_client
        try:
            yield
        finally:
            nso_client._client = original

    return _patcher


# ── list_devices ─────────────────────────────────────────────────────────────


async def test_list_devices_returns_device_list(patch_client):
    """list_devices parses tailf-ncs:devices.device list."""
    client = _make_client()
    payload = {
        "tailf-ncs:devices": {
            "device": [
                {"name": "core-rtr-01", "address": "10.0.0.1"},
                {"name": "edge-rtr-02", "address": "10.0.0.2"},
            ]
        }
    }
    with patch_client(client, 200, payload):
        result = await client.list_devices()
    assert len(result) == 2
    assert result[0]["name"] == "core-rtr-01"
    assert result[1]["name"] == "edge-rtr-02"


async def test_list_devices_empty_when_no_device_key(patch_client):
    """list_devices returns [] when 'device' key is missing from payload."""
    client = _make_client()
    payload = {"tailf-ncs:devices": {}}  # no "device" key
    with patch_client(client, 200, payload):
        result = await client.list_devices()
    assert result == []


async def test_list_devices_empty_when_device_not_list(patch_client):
    """list_devices returns [] when 'device' value is not a list."""
    client = _make_client()
    payload = {"tailf-ncs:devices": {"device": "not-a-list"}}
    with patch_client(client, 200, payload):
        result = await client.list_devices()
    assert result == []


async def test_list_devices_raises_on_http_error(patch_client):
    """list_devices propagates httpx.HTTPStatusError on 4xx/5xx."""
    client = _make_client()
    with patch_client(client, 500, {"error": "internal"}):
        with pytest.raises(httpx.HTTPStatusError):
            await client.list_devices()


# ── get_device_config ────────────────────────────────────────────────────────


async def test_get_device_config_returns_config(patch_client):
    """get_device_config returns config subtree from tailf-ncs:config key."""
    client = _make_client()
    payload = {"tailf-ncs:config": {"interface": [{"name": "GE0/0"}]}}
    with patch_client(client, 200, payload):
        result = await client.get_device_config("core-rtr-01")
    assert "interface" in result


async def test_get_device_config_returns_raw_when_no_tailf_key(patch_client):
    """get_device_config returns the whole payload if tailf-ncs:config key is absent."""
    client = _make_client()
    payload = {"something-else": {"data": 42}}
    with patch_client(client, 200, payload):
        result = await client.get_device_config("core-rtr-01")
    assert result == payload


async def test_get_device_config_returns_empty_on_204(patch_client):
    """get_device_config returns {} on 204 No Content (CDB not populated)."""
    client = _make_client()
    with patch_client(client, 204, raw=b""):
        result = await client.get_device_config("core-rtr-01")
    assert result == {}


async def test_get_device_config_raises_on_404(patch_client):
    """get_device_config propagates HTTP error on 404."""
    client = _make_client()
    with patch_client(client, 404, {"error": "not found"}):
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_device_config("unknown-device")


# ── get_device_ned_id ────────────────────────────────────────────────────────


async def test_get_device_ned_id_cli(patch_client):
    """get_device_ned_id returns NED ID from CLI device-type."""
    client = _make_client()
    payload = {"tailf-ncs:device": {"device-type": {"cli": {"ned-id": "cisco-ios-cli-6.95"}}}}
    with patch_client(client, 200, payload):
        result = await client.get_device_ned_id("core-rtr-01")
    assert result == "cisco-ios-cli-6.95"


async def test_get_device_ned_id_netconf(patch_client):
    """get_device_ned_id returns NED ID from NETCONF device-type."""
    client = _make_client()
    payload = {"tailf-ncs:device": {"device-type": {"netconf": {"ned-id": "juniper-junos-nc-4.1"}}}}
    with patch_client(client, 200, payload):
        result = await client.get_device_ned_id("edge-rtr-02")
    assert result == "juniper-junos-nc-4.1"


async def test_get_device_ned_id_list_response(patch_client):
    """get_device_ned_id handles NSO returning device as a list (keyed list entry)."""
    client = _make_client()
    payload = {"tailf-ncs:device": [{"device-type": {"cli": {"ned-id": "cisco-ios-cli-6.95"}}}]}
    with patch_client(client, 200, payload):
        result = await client.get_device_ned_id("core-rtr-01")
    assert result == "cisco-ios-cli-6.95"


async def test_get_device_ned_id_returns_none_when_no_type(patch_client):
    """get_device_ned_id returns None when device-type has no known keys."""
    client = _make_client()
    payload = {"tailf-ncs:device": {"device-type": {}}}
    with patch_client(client, 200, payload):
        result = await client.get_device_ned_id("mystery-device")
    assert result is None


async def test_get_device_ned_id_uses_fields_filter():
    """get_device_ned_id must send ``fields=device-type`` so NSO returns only the
    small device-type subtree — the unfiltered node pulls the device's full
    config + oper-data (~900 KB, streamed) and truncates mid-body, raising
    JSONDecodeError. Regression test for the device-55 sync failure."""
    client = _make_client()
    captured: dict = {}

    class _CapturingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["params"] = dict(request.url.params)
            body = {"tailf-ncs:device": {"device-type": {"netconf": {"ned-id": "timos-nc-23.10:timos-nc-23.10"}}}}
            return httpx.Response(
                200,
                content=json.dumps(body).encode(),
                headers={"content-type": "application/yang-data+json"},
                request=request,
            )

    def _mock_client(timeout=None):
        return httpx.AsyncClient(transport=_CapturingTransport(), base_url="http://nso:8080")

    client._client = _mock_client
    result = await client.get_device_ned_id("prod-lab03c-ra1")

    assert result == "timos-nc-23.10:timos-nc-23.10"
    assert captured["params"].get("fields") == "device-type", captured


# ── check_sync ───────────────────────────────────────────────────────────────


async def test_check_sync_returns_true_when_in_sync(patch_client):
    """check_sync returns True when NSO reports 'in-sync'."""
    client = _make_client()
    payload = {"tailf-ncs:output": {"result": "in-sync"}}
    with patch_client(client, 200, payload):
        result = await client.check_sync("core-rtr-01")
    assert result is True


async def test_check_sync_returns_false_when_out_of_sync(patch_client):
    """check_sync returns False when NSO reports any non-'in-sync' result."""
    client = _make_client()
    payload = {"tailf-ncs:output": {"result": "out-of-sync"}}
    with patch_client(client, 200, payload):
        result = await client.check_sync("core-rtr-01")
    assert result is False


async def test_check_sync_returns_false_on_http_error(patch_client):
    """check_sync returns False (does not raise) on HTTP error responses."""
    client = _make_client()
    with patch_client(client, 500, {"error": "internal"}):
        result = await client.check_sync("core-rtr-01")
    assert result is False


async def test_check_sync_returns_false_on_missing_result_key(patch_client):
    """check_sync returns False when output structure has no 'result' key."""
    client = _make_client()
    payload = {"tailf-ncs:output": {}}
    with patch_client(client, 200, payload):
        result = await client.check_sync("core-rtr-01")
    assert result is False
