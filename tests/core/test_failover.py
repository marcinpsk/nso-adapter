# SPDX-License-Identifier: Apache-2.0
"""Failover state machine: pure hysteresis primitives + tick orchestration.

The hysteresis primitives (step_failover/step_failback) are tested as a pure transition
table. The tick is tested against a hand-written recording fake of the NSO client (the NSO
RESTCONF API is the true external boundary) with ``probe_reachable`` stubbed so reachability
is deterministic — the real HTTP round-trip is covered separately in test_failover_e2e.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

import nso_adapter.core.failover as failover
from nso_adapter.config import SchedulerConfig
from nso_adapter.core.failover import FlipBudget, _next_due, run_failover_tick, step_failback, step_failover
from nso_adapter.store.models import ActiveAddress, Device, DeviceFailover

_BASE = datetime(2026, 6, 18, 12, 0, 0)


# ── Pure hysteresis primitives ───────────────────────────────────────────────


def test_step_failover_reachable_resets():
    assert step_failover(True, 2, has_oob=True, threshold=3) == failover.CounterStep(0, 0, False)


def test_step_failover_increments_below_threshold():
    assert step_failover(False, 1, has_oob=True, threshold=3) == failover.CounterStep(2, 0, False)


def test_step_failover_acts_at_threshold():
    step = step_failover(False, 2, has_oob=True, threshold=3)
    assert step.act is True and step.failures == 0  # reset on the transition


def test_step_failover_never_acts_without_oob():
    step = step_failover(False, 9, has_oob=False, threshold=3)
    assert step.act is False and step.failures == 10  # keeps counting for the UI


def test_step_failback_increments_below_threshold():
    assert step_failback(True, 2, threshold=5) == failover.CounterStep(0, 3, False)


def test_step_failback_acts_at_threshold():
    step = step_failback(True, 4, threshold=5)
    assert step.act is True and step.successes == 0


def test_step_failback_failed_probe_resets_streak():
    assert step_failback(False, 4, threshold=5) == failover.CounterStep(0, 0, False)


# ── Tick orchestration ───────────────────────────────────────────────────────


class FakeNso:
    """Records the address-changing calls the tick makes; serves a current address."""

    def __init__(self, address: str = "10.0.0.1"):
        self.address = address
        self.calls: list[tuple] = []

    async def set_address(self, name: str, address: str, port: int | None = None) -> None:
        self.calls.append(("set_address", address))
        self.address = address

    async def disconnect(self, name: str) -> dict:
        self.calls.append(("disconnect",))
        return {}

    async def sync_from(self, name: str) -> bool:
        self.calls.append(("sync_from",))
        return True

    async def get_address(self, name: str) -> str:
        return self.address


def _stub_probe(monkeypatch, reachable):
    """Patch failover.probe_reachable; *reachable* is a bool or a zero-arg callable."""
    calls = {"n": 0}

    async def _probe(client, name, timeout=None):
        calls["n"] += 1
        r = reachable() if callable(reachable) else reachable
        return r, "" if r else "unreachable", 0.01

    monkeypatch.setattr(failover, "probe_reachable", _probe)
    return calls


def _device() -> Device:
    return Device(nso_instance="nso-dev", nso_device_name="ra1")


def _failover_row(active="primary", oob="192.0.2.5", **kw) -> DeviceFailover:
    return DeviceFailover(device_id=1, primary_ip="10.0.0.1", oob_ip=oob, active_address=active, **kw)


async def _tick(device, fo, client, cfg, *, now, job_active=False, primary_due=True, oob_due=False):
    fo.next_primary_probe_at = None if primary_due else now + timedelta(days=1)
    fo.next_oob_probe_at = None if oob_due else now + timedelta(days=1)
    await run_failover_tick(device, fo, client, cfg, now=now, job_active=job_active)


async def test_failover_after_failure_threshold(monkeypatch):
    cfg = SchedulerConfig()  # failure_threshold=3
    _stub_probe(monkeypatch, reachable=False)
    dev, fo, client = _device(), _failover_row(), FakeNso()

    for i in range(cfg.failover_failure_threshold - 1):
        await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=3 * i))
        assert fo.active_address == ActiveAddress.primary.value  # not switched yet
        assert ("set_address", "192.0.2.5") not in client.calls

    await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=99))
    assert fo.active_address == ActiveAddress.oob.value
    assert ("set_address", "192.0.2.5") in client.calls
    assert fo.last_switch_at is not None
    assert fo.consecutive_failures == 0


async def test_no_switch_without_oob(monkeypatch):
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)
    dev, fo, client = _device(), _failover_row(oob=None), FakeNso()

    for i in range(cfg.failover_failure_threshold + 2):
        await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=3 * i))

    assert fo.active_address == ActiveAddress.primary.value
    assert client.calls == []  # never touched the address
    assert fo.consecutive_failures >= cfg.failover_failure_threshold  # still counting for the UI


async def test_job_active_defers_switch(monkeypatch):
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)
    dev, fo, client = _device(), _failover_row(), FakeNso()

    for i in range(cfg.failover_failure_threshold + 1):
        await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=3 * i), job_active=True)

    assert fo.active_address == ActiveAddress.primary.value  # never switched under a job
    assert client.calls == []
    # counter is re-armed at the threshold so the next job-free tick switches immediately
    assert fo.consecutive_failures == cfg.failover_failure_threshold


async def test_manual_override_skips_switch(monkeypatch):
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)
    dev, fo = _device(), _failover_row()
    client = FakeNso(address="172.16.9.9")  # operator set a third address

    for i in range(cfg.failover_failure_threshold + 1):
        await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=3 * i))

    assert fo.active_address == ActiveAddress.primary.value
    assert fo.manual_override is True
    assert all(c[0] != "set_address" for c in client.calls)


async def test_failback_after_success_threshold(monkeypatch):
    cfg = SchedulerConfig()  # success_threshold=5
    _stub_probe(monkeypatch, reachable=True)  # primary is back
    dev, fo = _device(), _failover_row(active=ActiveAddress.oob.value)
    client = FakeNso(address="192.0.2.5")

    for i in range(cfg.failover_success_threshold - 1):
        await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=3 * i))
        assert fo.active_address == ActiveAddress.oob.value  # still on OOB below threshold
        assert client.address == "192.0.2.5"  # reverted to OOB each sub-threshold tick

    await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=99))
    assert fo.active_address == ActiveAddress.primary.value
    assert client.address == "10.0.0.1"  # committed on primary
    assert fo.last_switch_at is not None


async def test_failback_revert_keeps_oob_when_primary_down(monkeypatch):
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)  # primary still dead
    dev, fo = _device(), _failover_row(active=ActiveAddress.oob.value)
    client = FakeNso(address="192.0.2.5")

    await _tick(dev, fo, client, cfg, now=_BASE)

    assert fo.active_address == ActiveAddress.oob.value
    assert client.address == "192.0.2.5"  # flipped to primary to test, reverted to OOB
    assert fo.consecutive_successes == 0


async def test_proactive_oob_health_flip(monkeypatch):
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)
    dev, fo, client = _device(), _failover_row(), FakeNso()

    await _tick(dev, fo, client, cfg, now=_BASE, primary_due=False, oob_due=True)

    assert fo.active_address == ActiveAddress.primary.value  # stayed on primary
    assert fo.oob_healthy is True
    assert fo.oob_health_checked_at is not None
    # flipped to OOB to test, then back to primary
    assert ("set_address", "192.0.2.5") in client.calls
    assert client.address == "10.0.0.1"


async def test_steady_state_reachable_is_idempotent(monkeypatch):
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)
    dev, fo, client = _device(), _failover_row(), FakeNso()

    for i in range(4):
        await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=3 * i))

    assert client.calls == []  # no address PATCH while healthy on primary
    assert fo.last_probe_result == "ok"


async def test_no_primary_ip_is_noop(monkeypatch):
    cfg = SchedulerConfig()
    calls = _stub_probe(monkeypatch, reachable=True)
    dev = _device()
    fo = DeviceFailover(device_id=1, primary_ip=None, oob_ip="192.0.2.5")
    await _tick(dev, fo, FakeNso(), cfg, now=_BASE)
    assert calls["n"] == 0  # never even probed


@pytest.mark.parametrize("active", [ActiveAddress.primary.value, ActiveAddress.oob.value])
async def test_probe_not_run_when_not_due(monkeypatch, active):
    cfg = SchedulerConfig()
    calls = _stub_probe(monkeypatch, reachable=True)
    dev, fo, client = _device(), _failover_row(active=active), FakeNso()
    await _tick(dev, fo, client, cfg, now=_BASE, primary_due=False, oob_due=False)
    assert calls["n"] == 0  # nothing due → no probe, no calls
    assert client.calls == []


async def test_oob_liveness_probe_records_health_on_oob(monkeypatch):
    """On OOB, an oob-due probe is a cheap liveness of the active address — no flip."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value)
    client = FakeNso(address="192.0.2.5")
    await _tick(dev, fo, client, cfg, now=_BASE, primary_due=False, oob_due=True)
    assert fo.oob_healthy is True
    assert fo.active_address == ActiveAddress.oob.value
    assert client.calls == []  # cheap liveness, no address change


async def test_oob_active_refreshes_at_primary_cadence(monkeypatch):
    """On OOB, the active address's liveness refreshes at the primary (active) cadence —
    NOT the slow proactive-fallback cadence, which left a device-on-OOB showing health
    'not checked' for up to a full OOB interval (the live sw01 observation)."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value)
    client = FakeNso(address="192.0.2.5")
    await _tick(dev, fo, client, cfg, now=_BASE, primary_due=False, oob_due=True)
    assert fo.oob_healthy is True
    assert fo.next_oob_probe_at == _BASE + timedelta(minutes=cfg.failover_primary_probe_interval)


async def test_switch_to_oob_probes_health_in_same_tick(monkeypatch):
    """Switching to OOB schedules its health probe immediately (next_oob_probe_at=now), so
    a device freshly moved onto OOB is probed in that tick instead of showing 'not checked'
    until the next scheduled OOB probe (up to a full OOB interval away)."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)  # primary down → switch to OOB
    dev, fo, client = _device(), _failover_row(), FakeNso()
    for i in range(cfg.failover_failure_threshold):
        await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=3 * i), oob_due=False)
    assert fo.active_address == ActiveAddress.oob.value
    assert fo.oob_healthy is not None  # probed in the switching tick, not left None for hours


async def test_proactive_oob_health_unreachable_reverts_to_primary(monkeypatch):
    """On primary, a proactive OOB flip-probe that fails records oob_healthy=False and
    still flips back to primary (the check must never strand the device on a dead OOB)."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)
    dev, fo, client = _device(), _failover_row(), FakeNso()
    await _tick(dev, fo, client, cfg, now=_BASE, primary_due=False, oob_due=True)
    assert fo.oob_healthy is False
    assert fo.active_address == ActiveAddress.primary.value
    assert client.address == "10.0.0.1"  # flipped back to primary


async def test_oob_health_flip_deferred_under_job(monkeypatch):
    cfg = SchedulerConfig()
    calls = _stub_probe(monkeypatch, reachable=True)
    dev, fo, client = _device(), _failover_row(), FakeNso()
    await _tick(dev, fo, client, cfg, now=_BASE, primary_due=False, oob_due=True, job_active=True)
    assert calls["n"] == 0 and client.calls == []  # no flip mid-job


async def test_failback_flip_deferred_under_job(monkeypatch):
    cfg = SchedulerConfig()
    calls = _stub_probe(monkeypatch, reachable=True)
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value)
    client = FakeNso(address="192.0.2.5")
    await _tick(dev, fo, client, cfg, now=_BASE, job_active=True)  # primary due → failback flip
    assert calls["n"] == 0 and client.calls == []
    assert fo.active_address == ActiveAddress.oob.value  # stays on OOB under a job


async def test_failback_flip_skipped_on_manual_override(monkeypatch):
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value)
    client = FakeNso(address="172.16.0.1")  # operator set a third address
    await _tick(dev, fo, client, cfg, now=_BASE)
    assert fo.manual_override is True
    assert all(c[0] != "set_address" for c in client.calls)


class _FlakyNso(FakeNso):
    """A client whose side calls (disconnect/sync-from/get-address) all raise — the switch
    must still complete (these are best-effort / non-blocking)."""

    async def disconnect(self, name):
        raise RuntimeError("no live session")

    async def sync_from(self, name):
        raise RuntimeError("sync boom")

    async def get_address(self, name):
        raise RuntimeError("get boom")


async def test_switch_tolerates_flaky_side_calls(monkeypatch):
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)
    dev, fo, client = _device(), _failover_row(), _FlakyNso()
    for i in range(cfg.failover_failure_threshold):
        await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=3 * i))
    # switched despite disconnect/sync-from/get-address all raising
    assert fo.active_address == ActiveAddress.oob.value
    assert client.address == "192.0.2.5"


# ── Reachability-aware onboarding (_bootstrap_address) ───────────────────────

import nso_adapter.core.onboarding as onboarding  # noqa: E402
import nso_adapter.nso.actions as actions  # noqa: E402
from nso_adapter.core.onboarding import _bootstrap_address  # noqa: E402


def _enable_failover(monkeypatch, enabled=True):
    from types import SimpleNamespace

    cfg = SchedulerConfig(enable_failover=enabled)
    monkeypatch.setattr(onboarding, "get_config", lambda: SimpleNamespace(scheduler=cfg))


def _stub_actions_probe(monkeypatch, reachable):
    async def _probe(client, name, timeout=None):
        return reachable, "" if reachable else "down", 0.0

    monkeypatch.setattr(actions, "probe_reachable", _probe)


async def test_bootstrap_disabled_returns_primary(monkeypatch):
    _enable_failover(monkeypatch, enabled=False)
    client = FakeNso()
    active, step = await _bootstrap_address(client, "ra1", "10.0.0.1", "192.0.2.5")
    assert active == ActiveAddress.primary.value and step is None
    assert client.calls == []


async def test_bootstrap_no_oob_returns_primary(monkeypatch):
    _enable_failover(monkeypatch, enabled=True)
    active, step = await _bootstrap_address(FakeNso(), "ra1", "10.0.0.1", None)
    assert active == ActiveAddress.primary.value and step is None


async def test_bootstrap_primary_reachable_stays_primary(monkeypatch):
    _enable_failover(monkeypatch, enabled=True)
    _stub_actions_probe(monkeypatch, reachable=True)
    client = FakeNso()
    active, step = await _bootstrap_address(client, "ra1", "10.0.0.1", "192.0.2.5")
    assert active == ActiveAddress.primary.value
    assert client.calls == []  # never changed the address
    assert step["status"] == "primary"


async def test_bootstrap_primary_unreachable_switches_to_oob(monkeypatch):
    _enable_failover(monkeypatch, enabled=True)
    _stub_actions_probe(monkeypatch, reachable=False)
    client = FakeNso()
    active, step = await _bootstrap_address(client, "ra1", "10.0.0.1", "192.0.2.5")
    assert active == ActiveAddress.oob.value
    assert ("set_address", "192.0.2.5") in client.calls
    assert client.address == "192.0.2.5"
    assert step["status"] == "oob"


# ── Phase-1: flip budget + jitter (staggering) ────────────────────────────────


def test_flip_budget_take():
    """FlipBudget hands out exactly *limit* permits then refuses; 0 refuses immediately."""
    b = FlipBudget(2)
    assert (b.take(), b.take(), b.take()) == (True, True, False)
    assert FlipBudget(0).take() is False


def test_next_due_no_jitter_is_exact():
    assert _next_due(_BASE, 15, 0.0) == _BASE + timedelta(minutes=15)


def test_next_due_jitter_within_bounds():
    """Jittered due-time lands in [now+interval, now+interval*(1+fraction)]."""
    lo = _BASE + timedelta(minutes=10)
    hi = _BASE + timedelta(minutes=10, seconds=10 * 60 * 0.2)
    for _ in range(50):
        assert lo <= _next_due(_BASE, 10, 0.2) <= hi


async def test_flip_budget_blocks_switch_and_leaves_due(monkeypatch):
    """An exhausted flip budget defers the primary→OOB switch and keeps the device due."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)
    dev = _device()
    fo = _failover_row(consecutive_failures=cfg.failover_failure_threshold - 1)
    fo.next_primary_probe_at = None  # due
    client = FakeNso()

    await run_failover_tick(dev, fo, client, cfg, now=_BASE, flip_budget=FlipBudget(0))

    assert fo.active_address == ActiveAddress.primary.value  # NOT switched
    assert all(c[0] != "set_address" for c in client.calls)
    assert fo.consecutive_failures == cfg.failover_failure_threshold  # re-armed
    assert fo.next_primary_probe_at is None  # left due → retry next tick (prompt failover)


async def test_flip_budget_allows_switch_when_available(monkeypatch):
    """With budget, the switch proceeds and the budget is consumed."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)
    dev = _device()
    fo = _failover_row(consecutive_failures=cfg.failover_failure_threshold - 1)
    fo.next_primary_probe_at = None
    budget = FlipBudget(1)
    client = FakeNso()

    await run_failover_tick(dev, fo, client, cfg, now=_BASE, flip_budget=budget)

    assert fo.active_address == ActiveAddress.oob.value  # switched
    assert budget.remaining == 0  # consumed
    assert fo.next_primary_probe_at is not None  # advanced (probe ran)


async def test_flip_budget_blocks_failback_probe_and_leaves_due(monkeypatch):
    """An exhausted budget skips the disruptive failback flip-probe and keeps it due."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)  # primary would be reachable
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value)
    fo.next_primary_probe_at = None
    client = FakeNso(address="192.0.2.5")

    await run_failover_tick(dev, fo, client, cfg, now=_BASE, flip_budget=FlipBudget(0))

    assert fo.active_address == ActiveAddress.oob.value  # no failback
    assert client.calls == []  # never flipped
    assert fo.next_primary_probe_at is None  # left due


async def test_jitter_advances_primary_due_within_window(monkeypatch):
    """A probe that runs advances the due-time by interval + bounded forward jitter."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)
    dev, fo, client = _device(), _failover_row(), FakeNso()
    fo.next_primary_probe_at = None

    await run_failover_tick(dev, fo, client, cfg, now=_BASE, jitter_fraction=0.5)

    lo = _BASE + timedelta(minutes=cfg.failover_primary_probe_interval)
    hi = lo + timedelta(seconds=cfg.failover_primary_probe_interval * 60 * 0.5)
    assert lo <= fo.next_primary_probe_at <= hi
