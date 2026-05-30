# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Persistent SSE subscription with reconnect/backoff handling."""
from __future__ import annotations

import asyncio
from collections.abc import Callable

import httpx
import structlog

from .sse_subscriber import SSESubscriber

logger = structlog.get_logger(__name__)


async def persistent_subscriber(
    subscriber: SSESubscriber,
    stream_url: str,
    on_event: Callable[[str, dict | None], None],
    *,
    stop_event: asyncio.Event,
    initial_delay_s: float = 5.0,
    max_delay_s: float = 60.0,
) -> None:
    """Run subscribe() in a loop with exponential backoff on transport errors."""
    delay = initial_delay_s
    while not stop_event.is_set():
        try:
            await subscriber.subscribe(stream_url, on_event, duration=float("inf"))
            delay = initial_delay_s
        except asyncio.CancelledError:
            raise
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            logger.warning(
                "sse.reconnect_after_error",
                stream_url=stream_url,
                error=str(exc),
                next_delay_s=delay,
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:  # noqa: UP041 - explicit wait_for timeout handling
                pass
            delay = min(delay * 2, max_delay_s)
