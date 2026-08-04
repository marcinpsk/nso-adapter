# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""M6.9c/d/e: the worker's two-phase cancellation, drain and disposition.

The runner is driven as an explicit task because the execution and drain phases have
different deadlines, and ``asyncio.wait_for`` cannot express that — at its timeout it
cancels the child and then waits for the cancellation to COMPLETE, which is unbounded
against a span that absorbs cancels. This repository deliberately contains such spans.

Every test asserts the job's terminal STATE, not merely that no exception escaped: the
failure these exist to catch looked exactly like clean completion.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from nso_adapter.core import worker as worker_mod
from nso_adapter.core.claim import acquire_claim
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio


async def _seed_running(device_id: int | None, job_type):
    from nso_adapter.store.models import Job, JobStatus

    async with session() as db:
        job = Job(job_type=job_type, device_id=device_id, status=JobStatus.running, context={})
        db.add(job)
        await db.commit()
        return job.id


async def _status(job_id: int):
    from nso_adapter.store.models import Job

    async with session() as db:
        return (await db.get(Job, job_id)).status


async def _claim(device_id: int):
    from nso_adapter.store.models import DeviceClaim

    async with session() as db:
        return await db.get(DeviceClaim, device_id)


class _CountingRunner:
    """A runner that records how many cancellations it is actually delivered.

    Counting deliveries to the RUNNER is the right scope: ``await_uncancellable`` legally
    cancels its own span child, and those must not count against an exactly-once assertion
    about the runner. The runner absorbs the first cancel and keeps going briefly, so a
    second ``cancel()`` from a second entry point would be observed rather than coalesced by
    the fact that the task was already finishing.
    """

    def __init__(self, *, absorb_first: bool = True, linger: float = 0.15) -> None:
        self.cancels = 0
        self.absorb_first = absorb_first
        self.linger = linger
        self.started = asyncio.Event()

    async def __call__(self, _job_id, _device_id, _reg=None):
        self.started.set()
        while True:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self.cancels += 1
                if self.absorb_first and self.cancels == 1:
                    # Absorb once, stay alive briefly: a second cancel would land here.
                    with contextlib.suppress(asyncio.CancelledError):
                        await asyncio.sleep(self.linger)
                    raise
                raise


async def _drive(job_id, device_id, job_type, runner, *, reg=None):
    task = asyncio.create_task(worker_mod._run_one_job(0, job_id, device_id, job_type, runner, reg))
    await asyncio.sleep(0.05)
    return task


# ── M6.9d: a runner that raises before terminalizing ────────────────────────


async def test_runner_exception_reaches_the_claim_bearing_terminal_writer(adapter_client):
    """M6.9d — the exception must not be swallowed by ``asyncio.wait``.

    Against a ``pending``-only implementation the worker released the claim, the job stayed
    ``running``, and the reaper could not recover it: the reaper scans claims, and the claim
    had just been released.
    """
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="w-raise", netbox_device_id=9850)
    job_id = await _seed_running(device_id, JobType.sync)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    async def _boom(_job_id, _device_id, _reg=None):
        raise RuntimeError("runner blew up before writing a status")

    task = await _drive(job_id, device_id, JobType.sync, _boom, reg=reg)
    await asyncio.wait_for(task, timeout=20)

    assert await _status(job_id) is JobStatus.failed, "the runner's exception was swallowed"
    assert await _claim(device_id) is None, "status and release were not one transaction"


async def test_runner_exception_on_the_claimless_lane_still_fails_the_job(adapter_client):
    """The unregistered lane keeps today's status-only behavior."""
    from nso_adapter.store.models import JobStatus, JobType

    job_id = await _seed_running(None, JobType.provision)

    async def _boom(_job_id, _device_id, _reg=None):
        raise RuntimeError("boom")

    task = await _drive(job_id, None, JobType.provision, _boom)
    await asyncio.wait_for(task, timeout=20)

    assert await _status(job_id) is JobStatus.failed


async def test_successful_run_releases_the_claim(adapter_client):
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="w-ok", netbox_device_id=9856)
    job_id = await _seed_running(device_id, JobType.sync)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    async def _fine(_job_id, _device_id, _reg=None):
        return None

    task = await _drive(job_id, device_id, JobType.sync, _fine, reg=reg)
    await asyncio.wait_for(task, timeout=20)

    assert await _claim(device_id) is None


# ── M6.9e: budget expiry dispositions per job type ──────────────────────────


@pytest.mark.parametrize(("type_name", "expected"), [("apply", "failed"), ("sync", "queued")])
async def test_budget_expiry_dispositions_the_job(adapter_client, monkeypatch, type_name, expected):
    """M6.9e — an expired budget must write a terminal state, not leave ``running``.

    ``apply`` ends failed: never silently re-push operator intent that may have changed. An
    idempotent type requeues.
    """
    from nso_adapter.store.models import JobType

    monkeypatch.setattr(worker_mod, "JOB_EXECUTION_BUDGET", 0.2)
    device_id = await seed_device(nso_device_name=f"w-budget-{type_name}", netbox_device_id=9851)
    job_type = getattr(JobType, type_name)
    job_id = await _seed_running(device_id, job_type)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    async def _slow(_job_id, _device_id, _reg=None):
        await asyncio.sleep(30)

    task = await _drive(job_id, device_id, job_type, _slow, reg=reg)
    await asyncio.wait_for(task, timeout=30)

    assert (await _status(job_id)).value == expected
    assert await _claim(device_id) is None


# ── M6.9c: cancellation propagates, exactly once, cleanup always runs ───────


async def test_worker_cancel_propagates_to_the_runner(adapter_client):
    """M6.9c(i) — cancelling the WORKER must cancel the runner.

    Cancelling a task awaiting ``asyncio.wait({runner})`` does NOT cancel the runner: the
    waiter raises while the runner keeps going. Without the except arm, shutdown bypasses
    every disposition branch and releases ownership while the runner is still writing.
    """
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="w-cancel1", netbox_device_id=9852)
    job_id = await _seed_running(device_id, JobType.sync)
    reg = await acquire_claim(device_id, "job", job_id=job_id)
    runner = _CountingRunner(absorb_first=False)

    task = await _drive(job_id, device_id, JobType.sync, runner, reg=reg)
    await runner.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=20)

    assert runner.cancels == 1, "the runner was left alive"
    # Cleanup still ran: dispositioned and released, not left running with a live claim.
    assert await _status(job_id) is JobStatus.queued
    assert await _claim(device_id) is None


async def test_double_cancel_does_not_unwind_the_drain(adapter_client):
    """M6.9c(ii) — ``stop_workers`` cancels twice: ``cancel()``, then a ``wait_for`` expiry.

    A single ``shield`` does not survive the second cancel, and a repeated cancel turns
    ``await_uncancellable``'s bounded drain into an abandoned live task. One shared drain
    handle plus the absorbing loop is what makes the second cancel harmless.
    """
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="w-cancel2", netbox_device_id=9853)
    job_id = await _seed_running(device_id, JobType.sync)
    reg = await acquire_claim(device_id, "job", job_id=job_id)
    runner = _CountingRunner()

    task = await _drive(job_id, device_id, JobType.sync, runner, reg=reg)
    await runner.started.wait()
    task.cancel()
    await asyncio.sleep(0.02)
    task.cancel()  # the second cancel stop_workers really issues
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=20)

    assert runner.cancels == 1, f"the runner was cancelled {runner.cancels} times, not once"
    assert await _status(job_id) is JobStatus.queued
    assert await _claim(device_id) is None


async def test_budget_expiry_colliding_with_shutdown_cancels_once(adapter_client, monkeypatch):
    """M6.9c(iii) — the budget fires, then shutdown cancels while the drain is running."""
    from nso_adapter.store.models import JobStatus, JobType

    monkeypatch.setattr(worker_mod, "JOB_EXECUTION_BUDGET", 0.2)
    device_id = await seed_device(nso_device_name="w-cancel3", netbox_device_id=9854)
    job_id = await _seed_running(device_id, JobType.sync)
    reg = await acquire_claim(device_id, "job", job_id=job_id)
    runner = _CountingRunner()

    task = await _drive(job_id, device_id, JobType.sync, runner, reg=reg)
    await runner.started.wait()
    await asyncio.sleep(0.3)  # let the budget expire and the drain begin
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=20)

    assert runner.cancels == 1
    assert await _status(job_id) is JobStatus.queued
    assert await _claim(device_id) is None


async def test_cancel_landing_during_cleanup_does_not_truncate_it(adapter_client):
    """M6.9c(v) — a cancel arriving while the disposition is committing.

    Every post-drain step runs under ``_absorb`` for this reason: a cancellation during
    cleanup must not skip the disposition, the release or the drain bookkeeping.
    """
    from nso_adapter.store.models import JobStatus, JobType

    device_id = await seed_device(nso_device_name="w-cancel5", netbox_device_id=9857)
    job_id = await _seed_running(device_id, JobType.sync)
    reg = await acquire_claim(device_id, "job", job_id=job_id)

    async def _boom(_job_id, _device_id, _reg=None):
        raise RuntimeError("boom")

    task = await _drive(job_id, device_id, JobType.sync, _boom, reg=reg)
    # Land cancels repeatedly while cleanup is in flight.
    for _ in range(5):
        task.cancel()
        await asyncio.sleep(0.01)
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=20)

    assert await _status(job_id) is JobStatus.failed, "cleanup was truncated by a late cancel"
    assert await _claim(device_id) is None


# ── the drain registry does not leak ────────────────────────────────────────


async def test_drain_registry_is_cleared_after_every_run(adapter_client):
    """A leaked entry keeps a finished runner task and its frames alive forever."""
    from nso_adapter.store.models import JobType

    device_id = await seed_device(nso_device_name="w-drains", netbox_device_id=9855)
    job_id = await _seed_running(device_id, JobType.sync)

    async def _quick(_job_id, _device_id, _reg=None):
        return None

    task = await _drive(job_id, device_id, JobType.sync, _quick)
    await asyncio.wait_for(task, timeout=20)

    assert worker_mod._drains == {}


async def test_drain_handle_is_idempotent_across_entry_points(adapter_client):
    """One drain per runner, whichever entry point asks — tested at the helper.

    Both entry points (budget expiry, worker cancellation) call ``_drain_handle``. In the
    assembled loop the second cancel is normally swallowed by ``_absorb`` before the outer
    handler can re-enter, so the loop-level tests cannot reach this path; the helper's
    contract is what guarantees it, and it is asserted here directly.

    Why it matters: a second ``task.cancel()`` turns ``await_uncancellable``'s bounded drain
    into an abandoned live task, because its drain phase treats a fresh cancel as "still
    pending" and abandons the child immediately.
    """
    runner = _CountingRunner(absorb_first=False)
    task = asyncio.create_task(runner(1, 1))
    await runner.started.wait()

    kwargs = {"job_id": 1, "device_id": 1, "job_type": "sync"}
    first = worker_mod._drain_handle(task, **kwargs)
    second = worker_mod._drain_handle(task, **kwargs)
    assert first is second, "a second entry point started a second drain"

    await asyncio.wait_for(first, timeout=20)
    assert runner.cancels == 1, f"the runner was cancelled {runner.cancels} times"
    worker_mod._drains.pop(task, None)


def test_shutdown_wait_outlasts_drain_plus_cleanup():
    """The relation, not the literal.

    The old 5s wait was below ``await_uncancellable``'s own absorb (5s) + drain (2s), so
    shutdown could return while a span was still draining — and returning releases ownership,
    which is precisely how a live child ends up behind a released claim.
    """
    assert worker_mod._SHUTDOWN_TASK_WAIT > worker_mod.JOB_CANCEL_DRAIN + worker_mod.JOB_CLEANUP_BOUND
