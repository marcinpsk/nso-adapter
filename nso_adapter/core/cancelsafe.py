# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Cancellation-safe spans for two-phase mirror producers (READSEM S5a A3).

A materializer COMMITS mirror rows, then a separate await terminalizes the outcome
pointer. A cancellation (job budget timeout, shutdown) landing between the two leaves
NEW rows under the OLD outcome — the plugin then gates fresh data on a stale outcome
(codex S5a R1-F4). :func:`await_uncancellable` runs that span as a task while the
PARENT coroutine stays alive absorbing cancels — so the caller's AsyncSession and the
engine's family locks remain owned until the span completes (a detached task raced
the parent's rollback and ran outside released locks — codex R3-4) — then re-raises
the cancellation.

Semantics (codex-pinned):
- span exception with no cancel → propagates unchanged;
- after an absorbed cancel, CancelledError ALWAYS re-raises (a span failure during
  the drain is logged, never masks the cancellation);
- repeated cancels (the 5s shutdown ``wait_for`` wrappers) keep being absorbed;
- the FIRST absorbed cancel starts ``absorb_deadline_s`` — waiting past it cancels
  the span and observes it under ``asyncio.wait`` (non-cancelling; ``wait_for``
  would cancel-then-WAIT and hang on a cancel-suppressing span — codex R6-1) for
  ``drain_timeout_s``, then abandons it with the session logged as poisoned.
"""

from __future__ import annotations

import asyncio

import structlog

logger = structlog.get_logger(__name__)


async def await_uncancellable(coro, *, absorb_deadline_s: float = 5.0, drain_timeout_s: float = 2.0):
    """Run *coro* to completion, absorbing cancellation of the CALLING task meanwhile.

    Returns the span's result; re-raises the cancellation (after span completion) if
    any was absorbed. See the module docstring for the full contract.
    """
    loop = asyncio.get_running_loop()
    task: asyncio.Task = asyncio.ensure_future(coro)
    cancelled = False
    deadline: float | None = None
    expired = False

    while not task.done():
        if expired:
            # Deadline passed with the span still running: cancel it and OBSERVE under a
            # non-cancelling bounded wait; if it outlives even that, abandon it — the
            # session it holds must never be reused (the parent is unwinding anyway).
            task.cancel()
            try:
                _done, pending = await asyncio.wait({task}, timeout=drain_timeout_s)
            except asyncio.CancelledError:
                pending = {task}
            if pending:
                logger.error(
                    "cancelsafe.span_abandoned",
                    detail="terminalization span outlived cancel+drain; its session is poisoned",
                )
            break
        try:
            if deadline is None:
                await asyncio.shield(task)
            else:
                remaining = max(deadline - loop.time(), 0.0)
                _done, pending = await asyncio.wait({task}, timeout=remaining)
                if pending:
                    expired = True
                    logger.error(
                        "cancelsafe.absorb_deadline_expired",
                        absorb_deadline_s=absorb_deadline_s,
                        detail="span still running past the absorb deadline under cancellation",
                    )
        except asyncio.CancelledError:
            if task.cancelled():
                raise  # the span itself was cancelled from outside our control
            cancelled = True
            if deadline is None:
                deadline = loop.time() + absorb_deadline_s

    if task.done() and not task.cancelled() and task.exception() is not None:
        if cancelled:
            logger.warning("cancelsafe.span_failed_under_cancel", error=repr(task.exception()))
            raise asyncio.CancelledError()
        raise task.exception()

    if cancelled or expired:
        raise asyncio.CancelledError()
    return task.result()
