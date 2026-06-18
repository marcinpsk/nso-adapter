# SPDX-License-Identifier: Apache-2.0
"""Management-IP failover — keep NSO pointed at an address it can actually manage.

NSO probes reachability (only NSO has management-plane reach); the adapter switches the
device's management address between the NetBox **primary** IP and the **OOB** IP with
hysteresis. Cadence is keyed to *which address* is probed: the active address is a cheap
``connect``; the inactive address requires a disruptive flip (set address → connect → flip
back). See the mgmt-IP-failover plan.

This module holds (1) the pure hysteresis primitives ``step_failover`` / ``step_failback``
(transition-table testable, no I/O) and (2) the I/O tick ``run_failover_tick`` that wraps
them. ``upsert_failover_ips`` ingests the plugin-sourced IPs without touching failover state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import select

from nso_adapter.nso.actions import probe_reachable
from nso_adapter.store.models import ActiveAddress, DeviceFailover

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from nso_adapter.config import SchedulerConfig
    from nso_adapter.nso.client import NsoClient
    from nso_adapter.store.models import Device

logger = structlog.get_logger(__name__)

_PRIMARY = ActiveAddress.primary.value
_OOB = ActiveAddress.oob.value


def _utcnow() -> datetime:
    """Naive-UTC now — the DeviceFailover DateTime columns are timezone-naive."""
    return datetime.now(UTC).replace(tzinfo=None)


# ── Pure hysteresis primitives (no I/O — fully unit-testable) ─────────────────


@dataclass(frozen=True)
class CounterStep:
    """Result of one primary-reachability evaluation: new counters + whether to act."""

    failures: int
    successes: int
    act: bool  # failover (when on primary) or failback (when on OOB) should happen now


def step_failover(reachable: bool, failures: int, has_oob: bool, threshold: int) -> CounterStep:
    """On-primary: a probe of the active primary IP. Fail over after *threshold* failures.

    Reachable resets the failure count. With no OOB to fall to, keep counting (so the UI
    still shows the device unreachable) but never act.
    """
    if reachable:
        return CounterStep(0, 0, False)
    failures += 1
    if has_oob and failures >= threshold:
        return CounterStep(0, 0, True)  # counters reset on the transition
    return CounterStep(failures, 0, False)


def step_failback(reachable: bool, successes: int, threshold: int) -> CounterStep:
    """On-OOB: a flip-probe of the primary IP. Fail back after *threshold* successes.

    A single failed primary probe resets the success streak (hysteresis damps flapping).
    """
    if reachable:
        successes += 1
        if successes >= threshold:
            return CounterStep(0, 0, True)  # counters reset on the transition
        return CounterStep(0, successes, False)
    return CounterStep(0, 0, False)


# ── I/O helpers ───────────────────────────────────────────────────────────────


def _due(next_at: datetime | None, now: datetime) -> bool:
    return next_at is None or next_at <= now


async def _safe_disconnect(client: NsoClient, name: str) -> None:
    """Drop NSO's cached session so the new address is dialed. Absent session is fine."""
    try:
        await client.disconnect(name)
    except Exception as exc:  # no live session / already disconnected — not an error
        logger.debug("failover.disconnect_ignored", device=name, error=repr(exc))


async def _safe_sync_from(client: NsoClient, name: str) -> None:
    """Best-effort sync-from after a switch — a failure must not fail the switch."""
    try:
        await client.sync_from(name)
    except Exception as exc:
        logger.warning("failover.sync_from_failed", device=name, error=repr(exc))


async def _set_address(client: NsoClient, name: str, address: str) -> None:
    await client.set_address(name, address)
    await _safe_disconnect(client, name)


async def _flip_and_probe(client: NsoClient, name: str, address: str, timeout: float):
    """Point NSO at *address*, drop the session, and probe — returns probe_reachable()."""
    await _set_address(client, name, address)
    return await probe_reachable(client, name, timeout)


async def _is_manual_override(client: NsoClient, fo: DeviceFailover, name: str) -> bool:
    """Return True when NSO's current address is neither the primary nor the OOB IP.

    An operator (or another system) set it by hand — record it and stop fighting them.
    """
    try:
        current = await client.get_address(name)
    except Exception:
        return False  # can't tell → don't block the loop
    if current is None:
        return False
    return current not in (fo.primary_ip, fo.oob_ip)


def _record_probe(fo: DeviceFailover, now: datetime, reachable: bool) -> None:
    fo.last_probe_at = now
    fo.last_probe_result = "ok" if reachable else "fail"


# ── State transitions (the switch + failback commit) ──────────────────────────


async def _switch_to_oob(client: NsoClient, fo: DeviceFailover, name: str, cfg: SchedulerConfig, now: datetime) -> None:
    await _set_address(client, name, fo.oob_ip)
    fo.active_address = _OOB
    fo.consecutive_failures = 0
    fo.consecutive_successes = 0
    fo.last_switch_at = now
    if cfg.failover_sync_from_after_switch:
        await _safe_sync_from(client, name)
    logger.info("failover.switch", device=name, to=_OOB, address=fo.oob_ip)


async def _commit_failback(
    client: NsoClient, fo: DeviceFailover, name: str, cfg: SchedulerConfig, now: datetime
) -> None:
    # The flip already physically set the address to primary — just commit the state.
    fo.active_address = _PRIMARY
    fo.consecutive_failures = 0
    fo.consecutive_successes = 0
    fo.last_switch_at = now
    if cfg.failover_sync_from_after_switch:
        await _safe_sync_from(client, name)
    logger.info("failover.failback", device=name, address=fo.primary_ip)


# ── Per-address probe handlers ────────────────────────────────────────────────


async def _probe_primary(
    client: NsoClient,
    fo: DeviceFailover,
    name: str,
    cfg: SchedulerConfig,
    now: datetime,
    has_oob: bool,
    job_active: bool,
) -> None:
    """Probe the primary IP and drive the failover/failback decision."""
    if fo.active_address == _PRIMARY:
        # Cheap read-only liveness of the active primary.
        reachable, detail, elapsed = await probe_reachable(client, name, cfg.failover_probe_timeout)
        logger.debug("failover.probe", device=name, target=_PRIMARY, active=True, reachable=reachable, elapsed=elapsed)
        _record_probe(fo, now, reachable)
        step = step_failover(reachable, fo.consecutive_failures, has_oob, cfg.failover_failure_threshold)
        fo.consecutive_failures, fo.consecutive_successes = step.failures, step.successes
        if step.act:
            if job_active or await _is_manual_override(client, fo, name):
                # Don't switch mid-apply or over an operator's manual address — re-arm and retry.
                fo.manual_override = not job_active
                fo.consecutive_failures = cfg.failover_failure_threshold
                return
            fo.manual_override = False
            await _switch_to_oob(client, fo, name, cfg, now)
        return

    # On OOB → failback flip-probe of primary. Mutates the address, so defer under a job.
    if job_active:
        return
    if await _is_manual_override(client, fo, name):
        fo.manual_override = True
        return
    fo.manual_override = False
    reachable, detail, elapsed = await _flip_and_probe(client, name, fo.primary_ip, cfg.failover_probe_timeout)
    logger.debug("failover.flip_probe", device=name, target=_PRIMARY, reachable=reachable, elapsed=elapsed)
    _record_probe(fo, now, reachable)
    step = step_failback(reachable, fo.consecutive_successes, cfg.failover_success_threshold)
    fo.consecutive_failures, fo.consecutive_successes = step.failures, step.successes
    if step.act:
        await _commit_failback(client, fo, name, cfg, now)
    else:
        # Threshold not met (or primary still down) → revert to OOB to keep the device reachable.
        await _set_address(client, name, fo.oob_ip)


async def _probe_oob(
    client: NsoClient, fo: DeviceFailover, name: str, cfg: SchedulerConfig, now: datetime, job_active: bool
) -> None:
    """Probe the OOB IP — liveness when on OOB, proactive fallback-health flip when on primary."""
    if fo.active_address == _OOB:
        # Cheap liveness of the active OOB (surfaces the both-down case; no state change).
        reachable, _detail, elapsed = await probe_reachable(client, name, cfg.failover_probe_timeout)
        logger.debug("failover.probe", device=name, target=_OOB, active=True, reachable=reachable, elapsed=elapsed)
        _record_probe(fo, now, reachable)
        fo.oob_healthy = reachable
        fo.oob_health_checked_at = now
        return

    # On primary → proactive fallback-health flip-probe of OOB. Mutates address; defer under a job.
    if job_active:
        return
    if await _is_manual_override(client, fo, name):
        fo.manual_override = True
        return
    fo.manual_override = False
    reachable, _detail, elapsed = await _flip_and_probe(client, name, fo.oob_ip, cfg.failover_probe_timeout)
    logger.debug("failover.flip_probe", device=name, target=_OOB, reachable=reachable, elapsed=elapsed)
    fo.oob_healthy = reachable
    fo.oob_health_checked_at = now
    # Always flip back to primary — this was only a fallback-health check.
    await _set_address(client, name, fo.primary_ip)


# ── Tick orchestrator ─────────────────────────────────────────────────────────


async def run_failover_tick(
    device: Device,
    fo: DeviceFailover,
    client: NsoClient,
    cfg: SchedulerConfig,
    *,
    now: datetime | None = None,
    job_active: bool = False,
) -> None:
    """Process one failover tick for *device*, mutating *fo* in place (caller commits).

    Runs only the per-address probes that are *due*; switches/flips are deferred when a
    sync/apply job holds the device's one-per-device lane (*job_active*).
    """
    now = now or _utcnow()
    name = device.nso_device_name
    if not fo.primary_ip:
        return  # nothing to manage without a primary address
    # Normalize a freshly-created (not-yet-flushed) row whose column defaults haven't
    # materialized — the scheduler's loaded rows already carry these.
    fo.active_address = fo.active_address or _PRIMARY
    fo.consecutive_failures = fo.consecutive_failures or 0
    fo.consecutive_successes = fo.consecutive_successes or 0
    has_oob = bool(fo.oob_ip) and fo.oob_ip != fo.primary_ip

    if _due(fo.next_primary_probe_at, now):
        await _probe_primary(client, fo, name, cfg, now, has_oob, job_active)
        fo.next_primary_probe_at = now + timedelta(minutes=cfg.failover_primary_probe_interval)

    if has_oob and _due(fo.next_oob_probe_at, now):
        await _probe_oob(client, fo, name, cfg, now, job_active)
        fo.next_oob_probe_at = now + timedelta(minutes=cfg.failover_oob_probe_interval)


# ── IP ingestion (plugin → adapter) ───────────────────────────────────────────


async def _get_or_create_failover(db: AsyncSession, device_id: int) -> DeviceFailover:
    row = (await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))).scalar_one_or_none()
    if row is None:
        row = DeviceFailover(device_id=device_id)
        db.add(row)
    return row


async def set_initial_failover_state(
    db: AsyncSession, device_id: int, primary_ip: str | None, oob_ip: str | None, active_address: str
) -> DeviceFailover:
    """Seed a device's failover row at onboarding: the IPs + which address it bootstrapped on.

    Used by reachability-aware provisioning so a fresh device that came up over OOB starts in
    the ``on_oob`` state (and fails back to primary once the in-band address is up).
    """
    fo = await _get_or_create_failover(db, device_id)
    fo.primary_ip = primary_ip
    fo.oob_ip = oob_ip
    fo.active_address = active_address
    return fo


async def upsert_failover_ips(db: AsyncSession, device: Device, primary_ip: str | None, oob_ip: str | None) -> bool:
    """Persist the plugin-sourced primary/OOB IPs onto the device's failover row.

    Touches ONLY the IPs — never the active address, counters, or probe state. Does NOT create
    a row when there is nothing to store (both IPs None and no existing row) — so the per-device
    scope reconcile doesn't litter empty rows for devices an older plugin reports without IPs.
    Returns True if anything changed.
    """
    existing = (
        await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device.id))
    ).scalar_one_or_none()
    if existing is None and primary_ip is None and oob_ip is None:
        return False
    fo = existing
    if fo is None:
        fo = DeviceFailover(device_id=device.id)
        db.add(fo)
    changed = False
    if fo.primary_ip != primary_ip:
        fo.primary_ip = primary_ip
        changed = True
    if fo.oob_ip != oob_ip:
        fo.oob_ip = oob_ip
        changed = True
    return changed
