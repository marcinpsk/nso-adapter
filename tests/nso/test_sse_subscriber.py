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


class _PostHeaderTimeoutTransport(httpx.AsyncBaseTransport):
    """200 + headers accepted, then the body stream raises ReadTimeout (the idle
    watchdog firing on an established-but-quiet stream)."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        async def _raising_body():
            raise httpx.ReadTimeout("idle watchdog", request=request)
            yield b""  # pragma: no cover - makes this an async generator

        return httpx.Response(
            200,
            stream=_PostHeaderTimeoutTransport._AsyncByteStream(_raising_body()),
            headers={"content-type": "text/event-stream"},
            request=request,
        )

    class _AsyncByteStream(httpx.AsyncByteStream):
        def __init__(self, agen):
            self._agen = agen

        async def __aiter__(self):
            async for chunk in self._agen:
                yield chunk


class _PreHeaderTimeoutTransport(httpx.AsyncBaseTransport):
    """The server never sends response headers (overloaded/hung) — ReadTimeout fires
    while ENTERING the stream, before any 200 was accepted."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("no response headers", request=request)


async def test_post_header_idle_raises_sse_idle_timeout():
    """S5a E (codex R1-F10/R2-F10): only a POST-header ReadTimeout is the healthy-but-quiet
    idle watchdog — it must surface as SseIdleTimeout so the reconnect loop can take the
    fast path."""
    from nso_adapter.notifications.sse_subscriber import SseIdleTimeout

    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    original = sub._client
    sub._client = lambda timeout=None: httpx.AsyncClient(
        transport=_PostHeaderTimeoutTransport(), base_url="http://nso:8080"
    )
    try:
        with pytest.raises(SseIdleTimeout):
            await sub.subscribe(STREAM_URL, lambda raw, parsed: None, duration=5.0, idle_read_timeout_s=0.5)
    finally:
        sub._client = original


async def test_pre_header_read_timeout_stays_a_transport_error():
    """A ReadTimeout with NO headers accepted is an overloaded/hung server, NOT a quiet
    stream — it must stay a plain transport error so the reconnect loop backs off."""
    from nso_adapter.notifications.sse_subscriber import SseIdleTimeout

    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    original = sub._client
    sub._client = lambda timeout=None: httpx.AsyncClient(
        transport=_PreHeaderTimeoutTransport(), base_url="http://nso:8080"
    )
    try:
        with pytest.raises(httpx.ReadTimeout):
            await sub.subscribe(STREAM_URL, lambda raw, parsed: None, duration=5.0, idle_read_timeout_s=0.5)
    except SseIdleTimeout:  # pragma: no cover - the failure mode under test
        raise AssertionError("pre-header ReadTimeout must not classify as idle")
    finally:
        sub._client = original


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


async def test_subscribe_does_not_log_raw_body_at_info():
    """s3-27: the INFO sse_event must not carry the raw body — it can contain sensitive leaf
    values (cleartext leak). Only path/size at INFO; the truncated body stays at DEBUG."""
    from structlog.testing import capture_logs

    payload = json.dumps({"secret-leaf": "hunter2"})
    sub = SSESubscriber("http://nso:8080", ("admin", "secret"))
    with capture_logs() as logs, patch_subscriber_sse(sub, [payload]):
        await sub.subscribe(STREAM_URL, lambda *_: None, duration=5.0)

    info_events = [e for e in logs if e.get("event") == "sse_event"]
    assert info_events, "expected an sse_event log at INFO"
    for e in info_events:
        assert "raw" not in e  # no cleartext body at INFO
        assert "hunter2" not in str(list(e.values()))


async def test_subscribe_idle_read_timeout_unwedges_half_open_connection():
    """s3-13: a half-open connection (200 headers sent, then no bytes — no event and no
    keep-alive) must not wedge aiter_lines forever. Exercised against a REAL socket that
    accepts the connection and then goes silent:

    * without the watchdog (idle_read_timeout_s=None, the old read=None) subscribe hangs
      until the outer wait_for trips;
    * with a finite idle read timeout httpx raises ReadTimeout (a RequestError) so the
      persistent subscriber's reconnect/backoff is reached.
    """
    import asyncio as _asyncio

    async def _handle(reader, writer):
        with contextlib.suppress(Exception):
            await reader.read(4096)  # consume the request line + headers
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
            await writer.drain()
            # Send NOTHING; block until the client disconnects so the handler doesn't
            # linger past the test (the half-open silence is what the watchdog catches).
            await reader.read()
        with contextlib.suppress(Exception):
            writer.close()

    server = await _asyncio.start_server(_handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{port}/stream"
    sub = SSESubscriber(f"http://127.0.0.1:{port}", ("admin", "secret"))
    try:
        # No watchdog → wedged: only the outer wait_for stops it (old read=None behaviour).
        with pytest.raises(_asyncio.TimeoutError):
            await _asyncio.wait_for(
                sub.subscribe(url, lambda *_: None, duration=float("inf"), idle_read_timeout_s=None),
                timeout=1.5,
            )
        # Watchdog → the POST-header idle fires well before the 5s bound. S5a E: an
        # established-then-silent stream is the HEALTHY quiet case → SseIdleTimeout
        # (fast reconnect), no longer a plain ReadTimeout (60s backoff) — this real
        # socket is the codex-requested headers-then-silence proof.
        from nso_adapter.notifications.sse_subscriber import SseIdleTimeout

        with pytest.raises(SseIdleTimeout):
            await _asyncio.wait_for(
                sub.subscribe(url, lambda *_: None, duration=float("inf"), idle_read_timeout_s=0.5),
                timeout=5,
            )
    finally:
        server.close()
        await server.wait_closed()


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
