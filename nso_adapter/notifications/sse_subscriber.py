# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""RESTCONF SSE subscriber for NSO notification streams."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable

import httpx
import structlog

logger = structlog.get_logger(__name__)


class SseIdleTimeout(Exception):
    """The idle watchdog fired on an ESTABLISHED stream (headers accepted, then quiet).

    NSO's NETCONF stream sends no keepalives, so a quiet watchdog window is HEALTHY —
    the reconnect loop takes the fast path instead of the transport-error backoff
    (S5a E, item 1335: the 90s-watchdog + 60s-backoff cycle was a ~40%% blind window).
    A PRE-header ReadTimeout (overloaded/hung server) stays a plain transport error.
    """


# Idle read-timeout watchdog: how long the stream may go without ANY bytes (an SSE
# event OR a keep-alive comment) before httpx raises ReadTimeout. Must exceed NSO's
# SSE keep-alive interval, or a healthy but quiet stream will reconnect needlessly.
_DEFAULT_IDLE_READ_TIMEOUT_S = 90.0


class SSESubscriber:
    """Discover and subscribe to NSO RESTCONF notification streams via SSE."""

    def __init__(
        self,
        base_url: str,
        auth: tuple[str, str],
        host_header: str | None = None,
        verify: str | bool = True,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._auth = auth
        self._host_header = host_header
        self._verify = verify

    def _client(self, timeout: httpx.Timeout | None = None) -> httpx.AsyncClient:
        headers: dict[str, str] = {}
        if self._host_header:
            headers["Host"] = self._host_header
        return httpx.AsyncClient(
            auth=self._auth,
            headers=headers,
            verify=self._verify,
            timeout=timeout or httpx.Timeout(30.0),
        )

    async def discover_streams(self) -> list[dict]:
        """Return all streams from ietf-restconf-monitoring:restconf-state/streams."""
        url = f"{self._base}/restconf/data/ietf-restconf-monitoring:restconf-state/streams"
        async with self._client() as c:
            resp = await c.get(url, headers={"Accept": "application/yang-data+json"})
            resp.raise_for_status()
            data = resp.json()
            outer = data.get("ietf-restconf-monitoring:streams", data)
            return outer.get("stream", [])

    async def subscribe(
        self,
        stream_url: str,
        on_event: Callable[[str, dict | None], None],
        duration: float = 30.0,
        *,
        idle_read_timeout_s: float | None = _DEFAULT_IDLE_READ_TIMEOUT_S,
    ) -> None:
        """Subscribe to *stream_url* for up to *duration* seconds.

        Calls *on_event(raw_data, parsed)* for each SSE event block.
        *parsed* is None when the data field is not valid JSON.
        Raises httpx.HTTPStatusError / httpx.RequestError on transport failure.

        *idle_read_timeout_s* is the idle watchdog: if no bytes (an SSE event OR a
        keep-alive comment) arrive within that window, httpx raises ReadTimeout so a
        silently half-open connection (NAT idle drop / NSO restart without RST) is
        surfaced as a transport error and the caller can reconnect, instead of
        ``aiter_lines`` blocking forever. ``None`` disables it (the old unbounded
        ``read=None`` behaviour — the stream can wedge indefinitely).
        """
        established = False

        async def _run() -> None:
            nonlocal established
            streaming_timeout = httpx.Timeout(connect=10.0, read=idle_read_timeout_s, write=10.0, pool=10.0)
            async with self._client(timeout=streaming_timeout) as c:
                async with c.stream("GET", stream_url, headers={"Accept": "text/event-stream"}) as response:
                    response.raise_for_status()
                    established = True
                    current_block: list[str] = []
                    async for line in response.aiter_lines():
                        if line.startswith("data:"):
                            current_block.append(line[5:].strip())
                        elif line.startswith("event:") or line.startswith(":"):
                            pass  # skip event-type lines and keep-alive comments
                        elif not line and current_block:
                            raw = "\n".join(current_block)
                            try:
                                parsed: dict | None = json.loads(raw)
                            except json.JSONDecodeError:
                                parsed = None
                            # Path/size only at INFO — the raw body can carry sensitive leaf
                            # values (cleartext leak); keep the (truncated) body at DEBUG.
                            logger.info("sse_event", stream=stream_url, bytes=len(raw))
                            logger.debug("sse_event.body", stream=stream_url, raw=raw[:200])
                            on_event(raw, parsed)
                            current_block = []

        try:
            await asyncio.wait_for(_run(), timeout=duration)
        except TimeoutError:
            logger.info("sse_subscribe_complete", stream=stream_url, duration=duration)
        except httpx.ReadTimeout as exc:
            # S5a E (item 1335): a POST-header ReadTimeout is the idle watchdog firing on
            # an ESTABLISHED-but-quiet stream (NSO sends no keepalives) — healthy, fast
            # reconnect. Pre-header (no 200 accepted: overloaded/hung server) stays a
            # transport error and backs off. str(ReadTimeout) is often EMPTY — repr
            # fallback so the error is never logged as ''.
            if established:
                logger.info("sse_idle_timeout", stream=stream_url, error=str(exc) or repr(exc))
                raise SseIdleTimeout(str(exc) or "idle watchdog") from exc
            logger.warning("sse_subscribe_error", stream=stream_url, error=str(exc) or repr(exc))
            raise
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.warning("sse_subscribe_error", stream=stream_url, error=str(exc) or repr(exc))
            raise
