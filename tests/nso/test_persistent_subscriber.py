# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
from __future__ import annotations

import asyncio
import importlib
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock

import httpx
import pytest

from nso_adapter.notifications.sse_subscriber import SSESubscriber

pytestmark = pytest.mark.asyncio

STREAM_URL = "http://nso:8080/restconf/streams/NETCONF/json"
OnEvent = Callable[[str, dict | None], None]


def load_persistent_subscriber() -> tuple[object, Callable[..., Awaitable[None]]]:
    try:
        module = importlib.import_module("nso_adapter.notifications.persistent_subscriber")
    except ModuleNotFoundError as exc:
        pytest.fail(f"persistent_subscriber module missing: {exc}")

    persistent_subscriber = getattr(module, "persistent_subscriber", None)
    if persistent_subscriber is None:
        pytest.fail("persistent_subscriber function missing from module")

    return module, persistent_subscriber


async def test_persistent_subscriber_reconnects_after_clean_eof(monkeypatch: pytest.MonkeyPatch):
    module, persistent_subscriber = load_persistent_subscriber()
    subscriber = SSESubscriber("http://nso:8080", ("admin", "secret"))
    stop_event = asyncio.Event()
    second_call_started = asyncio.Event()
    wait_for_calls: list[float] = []
    attempts = 0

    async def subscribe_side_effect(
        stream_url: str, on_event: OnEvent, duration: float, idle_read_timeout_s: float | None = 90.0
    ) -> None:
        nonlocal attempts
        attempts += 1
        assert stream_url == STREAM_URL
        assert duration == float("inf")
        if attempts == 1:
            return
        second_call_started.set()
        await stop_event.wait()

    async def fake_wait_for(awaitable: Awaitable[object], timeout: float) -> object:
        wait_for_calls.append(timeout)
        if hasattr(awaitable, "close"):
            awaitable.close()  # type: ignore[call-arg]
        raise asyncio.TimeoutError  # noqa: UP041 - explicit wait_for timeout behavior

    monkeypatch.setattr(module.asyncio, "wait_for", fake_wait_for)
    subscriber.subscribe = AsyncMock(side_effect=subscribe_side_effect)

    task = asyncio.create_task(persistent_subscriber(subscriber, STREAM_URL, lambda *_: None, stop_event=stop_event))
    await second_call_started.wait()
    stop_event.set()
    await task

    assert attempts == 2
    assert wait_for_calls == []


async def test_idle_timeout_reconnects_fast_and_resets_backoff(monkeypatch: pytest.MonkeyPatch):
    """S5a E (item 1335): the idle watchdog on a healthy-but-quiet stream must NOT eat the
    60s error backoff (the live ~40%% blind window) — short jitter, delay reset to initial."""
    module, persistent_subscriber = load_persistent_subscriber()
    from nso_adapter.notifications.sse_subscriber import SseIdleTimeout

    subscriber = SSESubscriber("http://nso:8080", ("admin", "secret"))
    stop_event = asyncio.Event()
    wait_for_calls: list[float] = []
    attempts = 0

    async def subscribe_side_effect(
        stream_url: str, on_event: OnEvent, duration: float, idle_read_timeout_s: float | None = 90.0
    ) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SseIdleTimeout("quiet stream")
        if attempts == 2:
            raise httpx.ConnectError("real failure")  # after the idle cycle, a REAL error
        stop_event.set()  # third attempt: clean return; the loop sees stop and exits

    async def fake_wait_for(awaitable, timeout: float):
        wait_for_calls.append(timeout)
        if hasattr(awaitable, "close"):
            awaitable.close()
        if stop_event.is_set():
            return
        raise asyncio.TimeoutError  # noqa: UP041 - explicit wait_for timeout behavior

    monkeypatch.setattr(module.asyncio, "wait_for", fake_wait_for)
    monkeypatch.setattr(subscriber, "subscribe", subscribe_side_effect)

    task = asyncio.create_task(
        persistent_subscriber(
            subscriber, STREAM_URL, lambda raw, parsed: None, stop_event=stop_event, initial_delay_s=5.0
        )
    )
    # plain await: asyncio.wait_for is monkeypatched module-wide above
    await task

    assert attempts == 3
    # idle cycle: SHORT jitter (~1s), not the 5s initial backoff; the following REAL
    # error then waits the RESET initial delay (5s), not a doubled one.
    assert wait_for_calls[0] <= 1.5, f"idle reconnect must be fast, got {wait_for_calls[0]}"
    assert wait_for_calls[1] == 5.0, f"backoff must reset after a healthy idle cycle, got {wait_for_calls[1]}"


async def test_persistent_subscriber_retries_after_transport_error(monkeypatch: pytest.MonkeyPatch):
    module, persistent_subscriber = load_persistent_subscriber()
    subscriber = SSESubscriber("http://nso:8080", ("admin", "secret"))
    stop_event = asyncio.Event()
    wait_for_calls: list[float] = []
    attempts = 0

    async def subscribe_side_effect(
        stream_url: str, on_event: OnEvent, duration: float, idle_read_timeout_s: float | None = 90.0
    ) -> None:
        nonlocal attempts
        attempts += 1
        assert stream_url == STREAM_URL
        assert duration == float("inf")
        # s3-13: the finite idle watchdog is threaded through to subscribe().
        assert idle_read_timeout_s == 90.0
        if attempts == 1:
            raise httpx.RequestError("boom")
        stop_event.set()

    async def fake_wait_for(awaitable: Awaitable[object], timeout: float) -> object:
        wait_for_calls.append(timeout)
        if hasattr(awaitable, "close"):
            awaitable.close()  # type: ignore[call-arg]
        raise asyncio.TimeoutError  # noqa: UP041 - explicit wait_for timeout behavior

    monkeypatch.setattr(module.asyncio, "wait_for", fake_wait_for)
    subscriber.subscribe = AsyncMock(side_effect=subscribe_side_effect)

    await persistent_subscriber(subscriber, STREAM_URL, lambda *_: None, stop_event=stop_event)

    assert attempts == 2
    assert wait_for_calls == [5.0]


async def test_persistent_subscriber_caps_exponential_backoff(monkeypatch: pytest.MonkeyPatch):
    module, persistent_subscriber = load_persistent_subscriber()
    subscriber = SSESubscriber("http://nso:8080", ("admin", "secret"))
    stop_event = asyncio.Event()
    wait_for_calls: list[float] = []

    async def subscribe_side_effect(
        stream_url: str, on_event: OnEvent, duration: float, idle_read_timeout_s: float | None = 90.0
    ) -> None:
        assert stream_url == STREAM_URL
        assert duration == float("inf")
        raise httpx.RequestError("boom")

    async def fake_wait_for(awaitable: Awaitable[object], timeout: float) -> object:
        wait_for_calls.append(timeout)
        if len(wait_for_calls) == 7:
            stop_event.set()
            return await awaitable
        if hasattr(awaitable, "close"):
            awaitable.close()  # type: ignore[call-arg]
        raise asyncio.TimeoutError  # noqa: UP041 - explicit wait_for timeout behavior

    monkeypatch.setattr(module.asyncio, "wait_for", fake_wait_for)
    subscriber.subscribe = AsyncMock(side_effect=subscribe_side_effect)

    await persistent_subscriber(subscriber, STREAM_URL, lambda *_: None, stop_event=stop_event)

    assert subscriber.subscribe.await_count == 7
    assert wait_for_calls == [5.0, 10.0, 20.0, 40.0, 60.0, 60.0, 60.0]


async def test_persistent_subscriber_returns_when_stopped_during_backoff(
    monkeypatch: pytest.MonkeyPatch,
):
    module, persistent_subscriber = load_persistent_subscriber()
    subscriber = SSESubscriber("http://nso:8080", ("admin", "secret"))
    stop_event = asyncio.Event()
    wait_for_calls: list[float] = []

    async def subscribe_side_effect(
        stream_url: str, on_event: OnEvent, duration: float, idle_read_timeout_s: float | None = 90.0
    ) -> None:
        assert stream_url == STREAM_URL
        assert duration == float("inf")
        raise httpx.RequestError("boom")

    async def fake_wait_for(awaitable: Awaitable[object], timeout: float) -> object:
        wait_for_calls.append(timeout)
        stop_event.set()
        return await awaitable

    monkeypatch.setattr(module.asyncio, "wait_for", fake_wait_for)
    subscriber.subscribe = AsyncMock(side_effect=subscribe_side_effect)

    await persistent_subscriber(subscriber, STREAM_URL, lambda *_: None, stop_event=stop_event)

    assert subscriber.subscribe.await_count == 1
    assert wait_for_calls == [5.0]


async def test_persistent_subscriber_returns_after_stop_event_during_active_stream():
    _, persistent_subscriber = load_persistent_subscriber()
    subscriber = SSESubscriber("http://nso:8080", ("admin", "secret"))
    stop_event = asyncio.Event()
    stream_started = asyncio.Event()

    async def subscribe_side_effect(
        stream_url: str, on_event: OnEvent, duration: float, idle_read_timeout_s: float | None = 90.0
    ) -> None:
        assert stream_url == STREAM_URL
        assert duration == float("inf")
        stream_started.set()
        await stop_event.wait()

    subscriber.subscribe = AsyncMock(side_effect=subscribe_side_effect)

    task = asyncio.create_task(persistent_subscriber(subscriber, STREAM_URL, lambda *_: None, stop_event=stop_event))
    await stream_started.wait()
    stop_event.set()
    await task

    assert subscriber.subscribe.await_count == 1
