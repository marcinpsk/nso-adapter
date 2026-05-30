# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
from __future__ import annotations

import contextlib
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


def _make_client() -> NsoClient:
    return NsoClient(_make_cfg(), "admin", "secret")


class MockTransport(httpx.AsyncBaseTransport):
    def __init__(self, status_code: int, body: dict | None = None):
        self.status_code = status_code
        self._content = json.dumps(body).encode() if body is not None else b""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            self.status_code,
            content=self._content,
            headers={"content-type": "application/yang-data+json"},
            request=request,
        )


@pytest.fixture
def patch_client():
    @contextlib.contextmanager
    def _patcher(nso_client: NsoClient, status: int, body: dict | None = None):
        transport = MockTransport(status, body)
        original = nso_client._client

        def _mock_client(timeout=None):
            return httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

        nso_client._client = _mock_client
        try:
            yield
        finally:
            nso_client._client = original

    return _patcher


_SAMPLE_PAYLOAD = {
    "network-state-export:device": [
        {
            "device-name": "sw03",
            "last-updated": "2026-06-01T12:00:00Z",
            "interface": [
                {"interface-name": "GigabitEthernet0/1", "description": "uplink", "enabled": True},
                {"interface-name": "GigabitEthernet0/2", "enabled": False},
            ],
        }
    ]
}


async def test_get_interface_attributes_returns_first_namespaced_entry(patch_client):
    client = _make_client()
    with patch_client(client, 200, _SAMPLE_PAYLOAD):
        result = await client.get_interface_attributes("sw03")
    assert result == _SAMPLE_PAYLOAD["network-state-export:device"][0]


async def test_get_interface_attributes_returns_none_on_404(patch_client):
    client = _make_client()
    with patch_client(client, 404, {"error": "not found"}):
        result = await client.get_interface_attributes("sw03")
    assert result is None


async def test_get_interface_attributes_raises_on_500(patch_client):
    client = _make_client()
    with patch_client(client, 500, {"error": "boom"}):
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_interface_attributes("sw03")


async def test_get_interface_attributes_returns_none_when_device_list_empty(patch_client):
    """Device list present but empty → return None (no data)."""
    client = _make_client()
    payload = {"network-state-export:device": []}
    with patch_client(client, 200, payload):
        result = await client.get_interface_attributes("sw03")
    assert result is None


async def test_get_interface_attributes_uses_correct_restconf_path(patch_client):
    """Verify the RESTCONF path contains the correct module and key."""
    client = _make_client()
    captured_requests = []

    class CapturingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            captured_requests.append(request)
            return httpx.Response(
                200,
                content=json.dumps(_SAMPLE_PAYLOAD).encode(),
                headers={"content-type": "application/yang-data+json"},
                request=request,
            )

    original = client._client

    def _mock_client(timeout=None):
        return httpx.AsyncClient(transport=CapturingTransport(), base_url="http://nso:8080")

    client._client = _mock_client
    try:
        await client.get_interface_attributes("sw03")
    finally:
        client._client = original

    assert len(captured_requests) == 1
    assert "network-state-export:interface-attributes/device=sw03" in str(captured_requests[0].url)
