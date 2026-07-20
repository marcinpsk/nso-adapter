# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S3 — the device-state envelope client methods.

Exercises ``get_device_state_section`` / ``get_device_state_doc`` /
``run_device_state_read`` through the real ``NsoClient`` request path against a
routing mock transport (device URL, container probe, and action URL answer
independently — the 404-disambiguation contract needs the probe to differ).
"""

from __future__ import annotations

import contextlib
import json

import httpx
import pytest

from nso_adapter.config import NsoInstanceConfig
from nso_adapter.nso.client import NsoClient, NsoExportUnavailableError


def _make_client() -> NsoClient:
    cfg = NsoInstanceConfig(
        name="nso-dev",
        base_url="http://nso:8080",
        ca_cert=None,
        username_ref="NSO_USERNAME",
        password_ref="NSO_PASSWORD",
        host_header=None,
    )
    return NsoClient(cfg, "admin", "secret")


class EnvelopeTransport(httpx.AsyncBaseTransport):
    """Routes the three URL shapes independently and records every request.

    * action POST (``/restconf/operations/``) → ``action_status``/``action_body``
    * keyed device GET (``/device=`` in the path) → ``device_status``/``device_body``
    * the bare ``device-state`` container probe → ``container_status``
    """

    def __init__(
        self,
        *,
        device_status: int = 200,
        device_body: dict | None = None,
        container_status: int = 200,
        action_status: int = 200,
        action_body: dict | None = None,
    ):
        self.device_status = device_status
        self.device_body = device_body
        self.container_status = container_status
        self.action_status = action_status
        self.action_body = action_body
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        url = str(request.url)
        if "/restconf/operations/" in url:
            status, body = self.action_status, self.action_body
        elif "/device=" in url:
            status, body = self.device_status, self.device_body
        else:
            status, body = self.container_status, {"network-state-export:device": []}
        return httpx.Response(
            status,
            content=json.dumps(body).encode() if body is not None else b"",
            headers={"content-type": "application/yang-data+json"},
            request=request,
        )


@pytest.fixture
def patch_client():
    @contextlib.contextmanager
    def _patcher(nso_client: NsoClient, transport: EnvelopeTransport):
        original = nso_client._client
        seen_timeouts: list[float | None] = []

        def _mock_client(timeout=None):
            seen_timeouts.append(timeout)
            return httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

        nso_client._client = _mock_client
        try:
            yield seen_timeouts
        finally:
            nso_client._client = original

    return _patcher


# ── get_device_state_section ─────────────────────────────────────────────────────────


async def test_section_get_returns_the_namespaced_section(patch_client):
    client = _make_client()
    body = {
        "network-state-export:ospf-config": {
            "status": "ok",
            "last-updated": "2026-07-20T12:00:00+00:00",
            "instance": [{"process-id": "1", "vrf": "", "area": [{"area-id": "0.0.0.0"}]}],
        }
    }
    with patch_client(client, EnvelopeTransport(device_body=body)):
        section = await client.get_device_state_section("sw01", "ospf-config")
    assert section["status"] == "ok"
    assert section["instance"][0]["area"] == [{"area-id": "0.0.0.0"}]


async def test_section_404_with_healthy_container_is_device_absent(patch_client):
    """A section on an existing device ALWAYS serves — 404 + live container = device unknown."""
    client = _make_client()
    transport = EnvelopeTransport(device_status=404, container_status=200)
    with patch_client(client, transport):
        assert await client.get_device_state_section("ghost", "ospf-config") is None
    # The probe must actually have run (second request, container URL, no /device=).
    assert len(transport.requests) == 2
    assert "/device=" not in str(transport.requests[1].url)


async def test_section_404_with_dead_container_raises_export_unavailable(patch_client):
    client = _make_client()
    with patch_client(client, EnvelopeTransport(device_status=404, container_status=404)):
        with pytest.raises(NsoExportUnavailableError):
            await client.get_device_state_section("sw01", "ospf-config")


async def test_section_5xx_raises(patch_client):
    client = _make_client()
    with patch_client(client, EnvelopeTransport(device_status=500)):
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_device_state_section("sw01", "ospf-config")


# ── get_device_state_doc ─────────────────────────────────────────────────────────────


async def test_doc_get_returns_the_first_device_entry(patch_client):
    client = _make_client()
    body = {
        "network-state-export:device": [
            {
                "device-name": "sw01",
                "ospf-config": {"status": "ok"},
                "logging-config": {"status": "ok"},
            }
        ]
    }
    with patch_client(client, EnvelopeTransport(device_body=body)):
        doc = await client.get_device_state_doc("sw01")
    assert doc["device-name"] == "sw01"
    assert doc["ospf-config"]["status"] == "ok"


async def test_doc_404_with_healthy_container_is_device_absent(patch_client):
    client = _make_client()
    with patch_client(client, EnvelopeTransport(device_status=404, container_status=200)):
        assert await client.get_device_state_doc("ghost") is None


async def test_doc_404_with_dead_container_raises_export_unavailable(patch_client):
    client = _make_client()
    with patch_client(client, EnvelopeTransport(device_status=404, container_status=404)):
        with pytest.raises(NsoExportUnavailableError):
            await client.get_device_state_doc("sw01")


# ── run_device_state_read ────────────────────────────────────────────────────────────


async def test_action_posts_module_qualified_input_and_returns_output(patch_client):
    client = _make_client()
    action_body = {
        "network-state-export:output": {
            "atomic": True,
            "last-updated": "2026-07-20T12:00:00+00:00",
            "ospf-config": {"status": "ok", "instance": []},
            "logging-config": {"status": "unsupported"},
        }
    }
    transport = EnvelopeTransport(action_body=action_body)
    with patch_client(client, transport):
        output = await client.run_device_state_read("sw01", ["ospf-config", "logging-config"])
    assert output["atomic"] is True
    assert output["logging-config"]["status"] == "unsupported"
    sent = json.loads(transport.requests[0].content)
    assert sent == {"network-state-export:input": {"device": "sw01", "family": ["ospf-config", "logging-config"]}}
    assert "/restconf/operations/network-state-export:device-state-read/run" in str(transport.requests[0].url)


async def test_action_error_raises_http_status_error(patch_client):
    """Bracket exhaustion / unknown device surface as an action ERROR — callers keep rows."""
    client = _make_client()
    with patch_client(client, EnvelopeTransport(action_status=500)):
        with pytest.raises(httpx.HTTPStatusError):
            await client.run_device_state_read("sw01", ["ospf-config"])


async def test_action_runs_on_the_device_state_read_timeout(patch_client):
    """The whale (rc1: all-18 in 75.6s) outlives the 120s action timeout — 180s is load-bearing."""
    client = _make_client()
    body = {"network-state-export:output": {"atomic": True}}
    with patch_client(client, EnvelopeTransport(action_body=body)) as seen_timeouts:
        await client.run_device_state_read("rc1", ["ospf-config"])
    assert seen_timeouts == [client._device_state_read_timeout]
    assert client._device_state_read_timeout == 180.0


async def test_section_and_doc_run_on_the_blanket_timeout(patch_client):
    """Record-served envelope GETs are warm reads — the default 30s timeout applies."""
    client = _make_client()
    body = {"network-state-export:ospf-config": {"status": "ok"}}
    with patch_client(client, EnvelopeTransport(device_body=body)) as seen_timeouts:
        await client.get_device_state_section("sw01", "ospf-config")
    assert seen_timeouts == [None]
