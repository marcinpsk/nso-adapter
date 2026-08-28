# SPDX-License-Identifier: Apache-2.0
"""Failover state machine: pure hysteresis primitives + tick orchestration.

The hysteresis primitives (step_failover/step_failback) are tested as a pure transition
table. The tick is tested against a hand-written recording fake of the NSO client (the NSO
RESTCONF API is the true external boundary) with ``probe_reachable`` stubbed so reachability
is deterministic — the real HTTP round-trip is covered separately in test_failover_e2e.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import nso_adapter.core.failover as failover
from nso_adapter.config import SchedulerConfig
from nso_adapter.core.failover import FlipBudget, _next_due, run_failover_tick, step_failback, step_failover
from nso_adapter.store.models import ActiveAddress, Device, DeviceFailover

_BASE = datetime(2026, 6, 18, 12, 0, 0, tzinfo=UTC)


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


def _on_oob_without_primary() -> DeviceFailover:
    return DeviceFailover(device_id=1, primary_ip=None, oob_ip="192.0.2.5", active_address=ActiveAddress.oob.value)


async def test_active_oob_liveness_runs_without_a_primary_ip(monkeypatch):
    """A device sitting on OOB keeps its liveness after the plugin clears the primary IP.

    That leg needs no primary address, and skipping it froze the health of the very address
    the operator is connecting through ("not checked" for as long as the primary stays gone)."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)
    dev, fo = _device(), _on_oob_without_primary()
    client = FakeNso(address="192.0.2.5")

    await _tick(dev, fo, client, cfg, now=_BASE, primary_due=False, oob_due=True)

    assert fo.oob_healthy is True
    assert fo.oob_health_checked_at == _BASE
    assert fo.next_oob_probe_at == _BASE + timedelta(minutes=cfg.failover_primary_probe_interval)
    assert client.calls == []  # cheap liveness, no address change


async def test_no_failback_attempted_without_a_primary_ip(monkeypatch):
    """The primary probe is due but there is no primary address to flip to — do nothing."""
    cfg = SchedulerConfig()
    calls = _stub_probe(monkeypatch, reachable=True)
    dev, fo = _device(), _on_oob_without_primary()
    client = FakeNso(address="192.0.2.5")

    await _tick(dev, fo, client, cfg, now=_BASE, primary_due=True, oob_due=False)

    assert calls["n"] == 0
    assert client.calls == []
    assert fo.active_address == ActiveAddress.oob.value


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


async def test_active_oob_timeout_is_recorded_with_detail_and_uses_longer_timeout(monkeypatch):
    """A cold active-OOB connect gets the active-address window and retains a timeout
    verdict/detail; it must not collapse into the misleading binary 'unreachable'."""
    cfg = SchedulerConfig()
    object.__setattr__(cfg, "failover_active_probe_timeout", 45.0)
    seen = {}

    async def _probe(client, name, timeout=None):
        seen["timeout"] = timeout
        return failover.ReachabilityProbe(
            failover.ProbeStatus.timeout,
            "cold connect exceeded 10 seconds",
            10.0,
        )

    monkeypatch.setattr(failover, "probe_reachable", _probe)
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value)

    await _tick(dev, fo, FakeNso(address="192.0.2.5"), cfg, now=_BASE, primary_due=False, oob_due=True)

    assert seen["timeout"] == 45.0
    assert fo.last_probe_result == "timeout"
    assert fo.last_probe_target == "oob"
    assert fo.last_probe_detail == "cold connect exceeded 10 seconds"
    assert fo.oob_health_result == "timeout"
    assert fo.oob_health_detail == "cold connect exceeded 10 seconds"
    assert fo.oob_healthy is None  # legacy boolean must not turn a timeout into "unreachable"


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


async def test_switch_to_oob_defers_health_probe_to_next_tick(monkeypatch):
    """Switching to OOB leaves the health probe DUE but does not run it in the switching tick
    (whose session may still be pinned to the old primary). It fires on the very next tick,
    against a freshly redialed session — deferred by one tick, not left 'not checked' for a
    full OOB interval (s3-18/s3-19)."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)  # primary down → switch to OOB
    dev, fo, client = _device(), _failover_row(), FakeNso()
    for i in range(cfg.failover_failure_threshold):
        await _tick(dev, fo, client, cfg, now=_BASE + timedelta(minutes=3 * i), oob_due=False)
    switch_now = _BASE + timedelta(minutes=3 * (cfg.failover_failure_threshold - 1))
    assert fo.active_address == ActiveAddress.oob.value
    # not probed in the switching tick, but left promptly due
    assert fo.oob_healthy is None
    assert fo.next_oob_probe_at is not None and fo.next_oob_probe_at <= switch_now
    # the very next tick (fresh session) records the OOB health — not left "not checked" for hours
    await run_failover_tick(dev, fo, client, cfg, now=switch_now + timedelta(minutes=1))
    assert fo.oob_healthy is not None


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


def _set_address_calls(client) -> list[tuple]:
    return [c for c in client.calls if c[0] == "set_address"]


class _UnreadableAddressNso(FakeNso):
    """NSO won't say which address it holds — the way back from a flip is unknown."""

    async def get_address(self, name):
        raise RuntimeError("get boom")


async def test_failback_flip_reverts_to_the_pre_flip_address_when_oob_equals_primary(monkeypatch):
    """``oob_ip == primary_ip`` normalizes to "no OOB" while the row still reads active=oob.

    The revert must target the address NSO actually had: reverting to the normalized-away OOB
    PATCHed a null address (error swallowed) and left NSO on the dead primary."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)  # primary still down → the flip must revert
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value, oob="10.0.0.1")
    client = FakeNso(address="10.0.0.1")

    await _tick(dev, fo, client, cfg, now=_BASE)

    assert all(c[1] is not None for c in _set_address_calls(client))  # never PATCH a null address
    assert _set_address_calls(client)[-1] == ("set_address", "10.0.0.1")  # back to the pre-flip address
    assert fo.active_address == ActiveAddress.oob.value


async def test_failback_flip_reverts_to_the_pre_flip_address_when_oob_ip_cleared(monkeypatch):
    """The plugin can clear ``oob_ip`` while the row is still active on OOB (the upsert never
    touches the active address), so the stored OOB is no revert target at all."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value, oob=None)
    client = FakeNso(address="10.0.0.1")  # NSO drifted onto the primary while the row says OOB

    await _tick(dev, fo, client, cfg, now=_BASE)

    assert all(c[1] is not None for c in _set_address_calls(client))
    assert _set_address_calls(client)[-1] == ("set_address", "10.0.0.1")
    assert client.address == "10.0.0.1"


async def test_failback_flip_refused_when_the_current_address_is_unreadable(monkeypatch):
    """A disruptive flip whose way back is unknown must not start. It still counts as run, so
    the device re-arms on the normal interval instead of retrying the read every tick."""
    cfg = SchedulerConfig()
    calls = _stub_probe(monkeypatch, reachable=True)  # primary would be reachable
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value)
    client = _UnreadableAddressNso(address="192.0.2.5")

    await _tick(dev, fo, client, cfg, now=_BASE)

    assert client.calls == []  # never flipped
    assert calls["n"] == 0  # never probed
    assert fo.active_address == ActiveAddress.oob.value
    assert fo.next_primary_probe_at == _BASE + timedelta(minutes=cfg.failover_primary_probe_interval)


async def test_failback_flip_flags_manual_override_when_oob_ip_cleared(monkeypatch):
    """With ``oob_ip`` cleared, an address that is neither managed IP is still an operator's
    (or a stale OOB): flag it and leave NSO alone rather than flipping to primary."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value, oob=None)
    client = FakeNso(address="172.16.9.9")  # the OOB the plugin no longer reports

    await _tick(dev, fo, client, cfg, now=_BASE)

    assert fo.manual_override is True
    assert client.calls == []


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


# ── s3-18 / s3-19: separate a switch/failback from same-tick verification ──────


async def test_failback_does_not_reflip_oob_same_tick(monkeypatch):
    """s3-19: a failback (OOB→primary) must NOT trigger a proactive OOB health flip in the
    same tick — that re-flips primary→OOB→primary (3 address changes, double flip budget)."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=True)  # primary is back
    dev = _device()
    fo = _failover_row(active=ActiveAddress.oob.value, consecutive_successes=cfg.failover_success_threshold - 1)
    fo.next_primary_probe_at = None  # due → failback flip-probe
    fo.next_oob_probe_at = None  # also due → would fire the proactive OOB flip same tick
    client = FakeNso(address="192.0.2.5")

    await run_failover_tick(dev, fo, client, cfg, now=_BASE)

    assert fo.active_address == ActiveAddress.primary.value  # failed back
    # the OOB proactive flip must NOT have run this tick (no set_address back to the OOB IP)
    assert ("set_address", "192.0.2.5") not in client.calls


async def test_switch_defers_oob_liveness_to_next_tick(monkeypatch):
    """s3-18/s3-19: after switching primary→OOB the OOB liveness probe must not run in the
    SAME tick (NSO's session may still be pinned to the old primary) — it is deferred to the
    next tick (which redials) and stays promptly due."""
    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)  # primary down → switch
    dev = _device()
    fo = _failover_row(consecutive_failures=cfg.failover_failure_threshold - 1)
    fo.next_primary_probe_at = None  # due
    fo.next_oob_probe_at = None  # would fire OOB liveness same tick
    client = FakeNso()

    await run_failover_tick(dev, fo, client, cfg, now=_BASE)

    assert fo.active_address == ActiveAddress.oob.value  # switched
    # OOB liveness did NOT run this tick (no health recorded) — deferred to a fresh-session tick
    assert fo.oob_health_checked_at is None
    # still due so it fires the very next tick
    assert fo.next_oob_probe_at is not None and fo.next_oob_probe_at <= _BASE


async def test_switch_surfaces_failed_session_drop(monkeypatch):
    """s3-18: when the post-set_address disconnect fails (the session stays pinned to the old
    address), the switch surfaces it (warning) rather than swallowing it silently."""
    from structlog.testing import capture_logs

    class DropFailsNso(FakeNso):
        async def disconnect(self, name):
            self.calls.append(("disconnect",))
            raise RuntimeError("disconnect RPC failed")

    cfg = SchedulerConfig()
    _stub_probe(monkeypatch, reachable=False)
    dev = _device()
    fo = _failover_row(consecutive_failures=cfg.failover_failure_threshold - 1)
    fo.next_primary_probe_at = None  # due
    fo.next_oob_probe_at = _BASE + timedelta(days=1)  # not due — isolate the switch
    client = DropFailsNso()

    with capture_logs() as logs:
        await run_failover_tick(dev, fo, client, cfg, now=_BASE)

    assert fo.active_address == ActiveAddress.oob.value  # switch still recorded (config changed)
    events = [e.get("event") for e in logs]
    assert "failover.switch.session_drop_failed" in events


class FlakyNso(FakeNso):
    """A FakeNso whose get_address raises — the on-OOB device with a flaky session."""

    async def get_address(self, name: str) -> str:
        raise TimeoutError("session read timed out")


async def test_failback_flip_refused_when_current_address_is_unreadable(monkeypatch):
    """#1630: a flip whose way back is unknown must not start.

    The old code treated a failed manual-override read as "can't tell -> proceed",
    flipped, and then reverted to the stored oob_ip - None when the operator had
    cleared it - stranding the device on the down primary.
    """
    client = FlakyNso()
    fo = _failover_row(active="oob", oob=None)
    _stub_probe(monkeypatch, False)

    await _tick(_device(), fo, client, SchedulerConfig(), now=_BASE)

    assert [c for c in client.calls if c[0] == "set_address"] == []
    assert fo.failback_blocked_reason == "address_unreadable"
    assert fo.next_primary_probe_at is not None  # ran -> re-armed, retried next interval


async def test_failback_flip_reverts_to_the_address_nso_actually_had(monkeypatch):
    """#1630: the revert target is the pre-flip read, never the stored oob_ip."""
    client = FakeNso(address="10.0.0.1")  # NSO found on the primary (a managed slot)
    fo = _failover_row(active="oob", oob="192.0.2.5")
    _stub_probe(monkeypatch, False)

    await _tick(_device(), fo, client, SchedulerConfig(), now=_BASE)

    sets = [c for c in client.calls if c[0] == "set_address"]
    assert sets[-1] == ("set_address", "10.0.0.1"), "revert must restore what NSO had, not invent a move"
    assert fo.failback_blocked_reason is None


async def test_a_recovered_address_read_clears_the_stale_unreadable_reason(monkeypatch):
    """A successful get_address invalidates address_unreadable on every branch,
    including the manual-override exit that records no probe."""
    client = FakeNso(address="203.0.113.7")  # readable, but foreign
    fo = _failover_row(active="oob", failback_blocked_reason="address_unreadable")
    _stub_probe(monkeypatch, False)

    await _tick(_device(), fo, client, SchedulerConfig(), now=_BASE)

    assert fo.manual_override is True
    assert fo.failback_blocked_reason is None
