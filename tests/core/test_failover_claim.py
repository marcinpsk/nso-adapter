# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R1b chunk 3 / Q14: the failover tick acquires the device claim (M6.15, M6.18, M6.19).

Everything runs through the real scheduler tick against the NSO simulator from
``test_failover_e2e``: the address the simulator holds IS the wire, so "did the tick mutate
the device" is asserted on `sim.patches`, not on a spy.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select

from nso_adapter.core import scheduler as sched
from nso_adapter.core.claim import CLAIM_STALE_AFTER, acquire_claim, release_claim
from nso_adapter.core.failover import FAILOVER_TICK_BOUND_S
from nso_adapter.store.models import ActiveAddress, DeviceFailover
from tests.conftest import session
from tests.core.test_failover_e2e import _arm_and_load, _client_for, _NsoSim, _seed


async def _run_ticks(count: int, device_id: int) -> None:
    for _ in range(count):
        await _arm_and_load(device_id)
        await sched._scheduled_failover_probe()


async def _failover_row(device_id: int) -> DeviceFailover:
    async with session() as db:
        return (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one()


@pytest.mark.parametrize("purpose", ["job", "teardown", "intent_put", "sweep"])
async def test_a_claimed_device_is_never_switched(adapter_client, monkeypatch, purpose):
    """M6.15 / M6.19a — including the case the job-status gate cannot see.

    An apply commits its terminal status and then keeps its claim through the post-apply
    refresh, so `has_any_active_job` reads "idle" at exactly the moment the device is busy.
    The claim is the gate; the boolean is only a pre-filter.
    """
    from nso_adapter.config import get_config

    sim = _NsoSim(address="10.0.0.1")
    sim.reachable_addrs = {"192.0.2.5"}  # only OOB works → the fixture WOULD flip
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: _client_for(sim))
    device_id = await _seed()
    threshold = get_config().scheduler.failover_failure_threshold

    holder = await acquire_claim(device_id, purpose)
    try:
        await _run_ticks(threshold, device_id)
        assert sim.patches == [], "the tick rewrote the device address under a live claim"
        row = await _failover_row(device_id)
        assert row.active_address == ActiveAddress.primary.value
        assert row.consecutive_failures == 0, "the deferred tick still advanced hysteresis state"
    finally:
        await release_claim(holder)

    # The same fixture flips once the device is free — so the assertions above were not
    # passing for want of a reachable failure path.
    await _run_ticks(threshold, device_id)
    assert sim.address == "192.0.2.5"
    assert (await _failover_row(device_id)).active_address == ActiveAddress.oob.value


async def test_the_tick_releases_its_claim(adapter_client, monkeypatch):
    """Including on the error path: a device left claimed waits for the reaper to be probed."""
    from nso_adapter.store.models import DeviceClaim

    sim = _NsoSim(address="10.0.0.1")
    sim.always_reachable = True
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: _client_for(sim))
    device_id = await _seed()

    await _run_ticks(1, device_id)
    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None

    sim.raise_on_connect_addr = "10.0.0.1"  # the probe blows up mid-tick
    await _run_ticks(1, device_id)
    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None


async def test_the_tick_reselects_after_acquiring_the_claim(adapter_client, monkeypatch):
    """M6.19b — a transition committed between the caller's load and the claim is observed.

    The rival's transition is committed from inside the acquisition call, i.e. AFTER any
    preliminary load and BEFORE the tick's own read. An implementation that selects first
    and acquires afterwards keeps the stale row, believes it is still on the primary with
    `threshold - 1` failures banked, and performs the switch a second time — stamping
    `last_switch_at`, which the rival's own transition deliberately left NULL.
    """
    from nso_adapter.config import get_config

    sim = _NsoSim(address="10.0.0.1")
    sim.reachable_addrs = {"192.0.2.5"}
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: _client_for(sim))
    device_id = await _seed()
    threshold = get_config().scheduler.failover_failure_threshold

    await _run_ticks(threshold - 1, device_id)
    row = await _failover_row(device_id)
    assert row.consecutive_failures == threshold - 1
    assert row.last_switch_at is None

    from nso_adapter.core import claim as claim_mod

    original = claim_mod.acquire_claim

    async def _acquire_then_rival_transition(device, purpose, **kwargs):
        reg = await original(device, purpose, **kwargs)
        if reg is not None and purpose == "failover":
            # Another scheduler already completed the switch, over its own connection.
            async with session() as db:
                row = (
                    await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))
                ).scalar_one()
                row.active_address = ActiveAddress.oob.value
                row.consecutive_failures = 0
                row.consecutive_successes = 0
                await db.commit()
            sim.address = "192.0.2.5"
        return reg

    monkeypatch.setattr(claim_mod, "acquire_claim", _acquire_then_rival_transition)
    await _run_ticks(1, device_id)

    row = await _failover_row(device_id)
    assert row.active_address == ActiveAddress.oob.value
    assert row.last_switch_at is None, "the tick switched again from a snapshot the claim invalidated"


def test_the_tick_bound_is_shorter_than_the_claim_cutoff():
    """M6.18 — the bound is the ONLY thing keeping the tick's claim inside its lifetime.

    A heartbeat is impossible here: the tick holds its own claim row FOR UPDATE for the
    whole tick, so an independent heartbeat task would block on that lock.
    """
    assert FAILOVER_TICK_BOUND_S < CLAIM_STALE_AFTER


def test_the_tick_bound_clears_a_legitimate_worst_case_flip():
    """A real primary→OOB flip chains the active re-probe (45s default), an address read
    (30s client default) and up to three 120s NSO actions — set-address, re-connect,
    sync-from — ≈435s. A bound below that cancels a LEGITIMATE tick after NSO's address
    changed but before the DB commit: stored state rolls back while NSO stays flipped."""
    worst_case_flip = 45.0 + 30.0 + 3 * 120.0
    assert FAILOVER_TICK_BOUND_S > worst_case_flip


async def test_a_tick_that_overruns_is_cut_short(adapter_client, monkeypatch):
    """The bound is enforced, not documented."""
    from nso_adapter.store.models import DeviceClaim

    sim = _NsoSim(address="10.0.0.1")
    sim.always_reachable = True
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: _client_for(sim))
    device_id = await _seed()

    async def _never_returns(*args, **kwargs):
        await asyncio.sleep(3600)

    monkeypatch.setattr("nso_adapter.core.failover.run_failover_tick", _never_returns)
    monkeypatch.setattr("nso_adapter.core.failover.FAILOVER_TICK_BOUND_S", 0.3)

    await asyncio.wait_for(_run_ticks(1, device_id), timeout=20)

    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None
