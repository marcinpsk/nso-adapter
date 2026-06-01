# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for SSESubscriber using mock httpx transports."""

from __future__ import annotations

import contextlib
import json

import httpx
import pytest

from nso_adapter.notifications.sse_subscriber import SSESubscriber

# ── helpers ──────────────────────────────────────────────────────────────────


class MockTransport(httpx.AsyncBaseTransport):
    """Returns a pre-canned JSON response."""

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


@contextlib.contextmanager
def patch_subscriber_client(sub: SSESubscriber, status: int, body: dict | None = None):
    """Context-manager that swaps _client() to use MockTransport."""
    transport = MockTransport(status, body)
    original = sub._client

    def _mock(timeout=None):
        return httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

    sub._client = _mock
    try:
        yield
    finally:
        sub._client = original


STREAMS_PAYLOAD = {
    "ietf-restconf-monitoring:streams": {
        "stream": [
            {
                "name": "NETCONF",
                "description": "default NETCONF event stream",
                "replay-support": False,
                "access": [
                    {
                        "encoding": "json",
                        "location": "http://nso:8080/restconf/streams/NETCONF/json",
                    }
                ],
            }
        ]
    }
}


# ── discover_streams ──────────────────────────────────────────────────────────


async def test_discover_streams_returns_list():
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    with patch_subscriber_client(sub, 200, STREAMS_PAYLOAD):
        streams = await sub.discover_streams()
    assert len(streams) == 1
    assert streams[0]["name"] == "NETCONF"
    assert streams[0]["access"][0]["encoding"] == "json"


async def test_discover_streams_empty_on_no_stream_key():
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    payload = {"ietf-restconf-monitoring:streams": {}}  # no "stream" key
    with patch_subscriber_client(sub, 200, payload):
        streams = await sub.discover_streams()
    assert streams == []


async def test_discover_streams_raises_on_401():
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    with patch_subscriber_client(sub, 401):
        with pytest.raises(httpx.HTTPStatusError):
            await sub.discover_streams()


# ── _client header behaviour ──────────────────────────────────────────────────


def test_client_sets_host_header_when_configured():
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"), host_header="nso.example.com")
    client = sub._client()
    assert client.headers.get("host") == "nso.example.com"


def test_client_omits_host_header_when_not_configured():
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    client = sub._client()
    assert client.headers.get("host") is None


# ── subscribe helpers ─────────────────────────────────────────────────────────


class MockSSETransport(httpx.AsyncBaseTransport):
    """Returns a finite SSE body containing pre-canned events."""

    def __init__(self, events: list[str], status_code: int = 200):
        self.status_code = status_code
        body = "".join(f"data: {e}\n\n" for e in events)
        self._content = body.encode()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            self.status_code,
            content=self._content,
            headers={"content-type": "text/event-stream"},
            request=request,
        )


@contextlib.contextmanager
def patch_subscriber_sse(sub: SSESubscriber, events: list[str], status: int = 200):
    transport = MockSSETransport(events, status)
    original = sub._client

    def _mock(timeout=None):
        return httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

    sub._client = _mock
    try:
        yield
    finally:
        sub._client = original


STREAM_URL = "http://nso:8080/restconf/streams/NETCONF/json"


# ── subscribe ─────────────────────────────────────────────────────────────────


async def test_subscribe_calls_on_event_for_each_sse_block():
    received: list[tuple[str, dict | None]] = []

    def on_event(raw: str, parsed: dict | None) -> None:
        received.append((raw, parsed))

    payload = json.dumps({"ietf-restconf:notification": {"eventTime": "2026-01-01T00:00:00Z"}})
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    with patch_subscriber_sse(sub, [payload]):
        await sub.subscribe(STREAM_URL, on_event, duration=5.0)

    assert len(received) == 1
    raw, parsed = received[0]
    assert parsed == {"ietf-restconf:notification": {"eventTime": "2026-01-01T00:00:00Z"}}


async def test_subscribe_delivers_multiple_events():
    received: list[tuple[str, dict | None]] = []

    def on_event(raw: str, parsed: dict | None) -> None:
        received.append((raw, parsed))

    events = [json.dumps({"ietf-restconf:notification": {"eventTime": f"2026-01-0{i}T00:00:00Z"}}) for i in range(1, 4)]
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    with patch_subscriber_sse(sub, events):
        await sub.subscribe(STREAM_URL, on_event, duration=5.0)

    assert len(received) == 3


async def test_subscribe_passes_none_parsed_on_invalid_json():
    received: list[tuple[str, dict | None]] = []

    def on_event(raw: str, parsed: dict | None) -> None:
        received.append((raw, parsed))

    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    with patch_subscriber_sse(sub, ["not-valid-{json"]):
        await sub.subscribe(STREAM_URL, on_event, duration=5.0)

    assert len(received) == 1
    raw, parsed = received[0]
    assert raw == "not-valid-{json"
    assert parsed is None


async def test_subscribe_raises_on_http_error():
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    with patch_subscriber_sse(sub, [], status=403):
        with pytest.raises(httpx.HTTPStatusError):
            await sub.subscribe(STREAM_URL, lambda *_: None, duration=5.0)


async def test_subscribe_empty_stream_calls_no_events():
    """A stream that closes immediately with no data produces zero on_event calls."""
    received: list = []
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    with patch_subscriber_sse(sub, []):
        await sub.subscribe(STREAM_URL, lambda raw, parsed: received.append(1), duration=5.0)
    assert received == []


async def test_subscribe_skips_event_type_and_comment_lines():
    """Lines starting with 'event:' or ':' are skipped; data still arrives."""
    received: list[tuple[str, dict | None]] = []

    class SSEWithCommentTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            body = b'event: notification\n: keep-alive\ndata: {"msg": "hello"}\n\n'
            return httpx.Response(
                200,
                content=body,
                headers={"content-type": "text/event-stream"},
                request=request,
            )

    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    original = sub._client

    def _mock(timeout=None):
        return httpx.AsyncClient(transport=SSEWithCommentTransport(), base_url="http://nso:8080")

    sub._client = _mock
    try:
        await sub.subscribe(STREAM_URL, lambda raw, parsed: received.append((raw, parsed)), duration=5.0)
    finally:
        sub._client = original

    assert len(received) == 1
    assert received[0][1] == {"msg": "hello"}


async def test_subscribe_completes_on_timeout():
    """asyncio.TimeoutError is caught internally; subscribe() returns normally."""
    import asyncio as _asyncio

    class BlockingSSETransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            await _asyncio.sleep(10)  # outlasts any short duration
            return httpx.Response(200, content=b"", headers={"content-type": "text/event-stream"}, request=request)

    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    original = sub._client

    def _mock(timeout=None):
        return httpx.AsyncClient(transport=BlockingSSETransport(), base_url="http://nso:8080")

    sub._client = _mock
    try:
        # duration=0.05 → wait_for times out; subscribe() should return without raising
        await sub.subscribe(STREAM_URL, lambda *_: None, duration=0.05)
    finally:
        sub._client = original
