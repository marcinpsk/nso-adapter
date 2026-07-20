# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
from __future__ import annotations

import contextlib
import json

import httpx
import pytest

from nso_adapter.config import NsoInstanceConfig
from nso_adapter.nso.client import NsoClient, NsoExportUnavailableError


def _make_cfg(base_url: str = "http://nso:8080", ca_cert=None, host_header=None):
    # A REAL NsoInstanceConfig — NsoClient reads base_url/ca_cert/host_header off it, so a
    # renamed config field surfaces as a real error instead of a fabricated MagicMock attr.
    return NsoInstanceConfig(
        name="nso-dev",
        base_url=base_url,
        ca_cert=ca_cert,
        username_ref="NSO_USERNAME",
        password_ref="NSO_PASSWORD",
        host_header=host_header,
    )


def _make_client(base_url: str = "http://nso:8080", host_header=None) -> NsoClient:
    return NsoClient(_make_cfg(base_url, host_header=host_header), "admin", "secret")


class MockTransport(httpx.AsyncBaseTransport):
    """Answers the keyed device GET and the parent-container probe with independent statuses.

    get_lag_topology confirms a bare device 404 against the parent container before returning
    None, so a test that wants a genuine per-device absence must let the container answer 200
    (``container_status`` defaults to ``status_code`` for the simple 200/500 cases).
    """

    def __init__(self, status_code: int, body: dict | None = None, container_status: int | None = None):
        self.status_code = status_code
        self.container_status = status_code if container_status is None else container_status
        self._content = json.dumps(body).encode() if body is not None else b""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        is_probe = "/device=" not in str(request.url)
        return httpx.Response(
            self.container_status if is_probe else self.status_code,
            content=self._content,
            headers={"content-type": "application/yang-data+json"},
            request=request,
        )


@pytest.fixture
def patch_client():
    @contextlib.contextmanager
    def _patcher(nso_client: NsoClient, status: int, body: dict | None = None, container_status: int | None = None):
        transport = MockTransport(status, body, container_status)
        original = nso_client._client

        def _mock_client(timeout=None):
            return httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

        nso_client._client = _mock_client
        try:
            yield
        finally:
            nso_client._client = original

    return _patcher


async def test_get_lag_topology_returns_first_namespaced_entry(patch_client):
    client = _make_client()
    payload = {
        "network-state-export:device": [
            {
                "device-name": "sw03",
                "lag": [{"name": "Port-channel1", "lag-id": 1, "member": []}],
            }
        ]
    }
    with patch_client(client, 200, payload):
        result = await client.get_lag_topology("sw03")
    assert result == payload["network-state-export:device"][0]


async def test_get_lag_topology_returns_none_on_confirmed_404(patch_client):
    """Device 404 but the parent container is healthy → genuine per-device absence → None."""
    client = _make_client()
    with patch_client(client, 404, {"error": "not found"}, container_status=200):
        result = await client.get_lag_topology("sw03")
    assert result is None


async def test_get_lag_topology_raises_when_export_is_unavailable(patch_client):
    """Device AND parent container both 404 → the whole export is down (fleet-wide) → raise, so the
    caller's degraded path keeps the last-known rows instead of wiping the fleet's lag mirror."""
    client = _make_client()
    with patch_client(client, 404, {"error": "not found"}, container_status=404):
        with pytest.raises(NsoExportUnavailableError, match="not exported"):
            await client.get_lag_topology("sw03")


async def test_get_lag_topology_raises_on_non_404_error(patch_client):
    client = _make_client()
    with patch_client(client, 500, {"error": "boom"}):
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_lag_topology("sw03")
