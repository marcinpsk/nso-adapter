# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""The shared read-mirror refresh engine (READSEM S1).

One executor replaces the ~18 copy-pasted ``refresh_<family>_for_device`` bodies. Each family
declares a :class:`FamilySpec` (getter + empty-policy + extractor + materializer); the engine
does the invariant part uniformly — skip unmapped devices, read once, classify the result into
the :data:`~nso_adapter.nso.read_outcome.ReadOutcome` vocabulary, and drive the mirror action:

* **Present** → materialize the extracted rows (a present-but-empty read replaces → clears).
* **AbsentAuthoritative** → materialize an empty row set (clear; the device genuinely has none).
* **Unavailable** → keep the last-known rows and return ``False`` (a degraded surface).

The family's *writes* stay family-owned in its materializer (bgp's multi-table flush,
vlan/switchport diff-by-key, etc.); only the empty/error *semantics* are centralized here. The
returned ``bool`` preserves the legacy ``refresh_*_for_device`` contract (``True`` = read
succeeded or nothing-to-read; ``False`` = read failed, rows untouched) so existing callers,
``_run_surfaces``, and monkeypatching tests keep working unchanged.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.client import NsoClient, NsoExportUnavailableError
from nso_adapter.nso.read_outcome import (
    AbsentAuthoritative,
    EmptyPolicy,
    Present,
    ReadOutcome,
    Unavailable,
    UnavailableReason,
    classify_envelope_section,
    classify_read,
)
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device

logger = structlog.get_logger(__name__)


async def _record_read(
    db: AsyncSession, device: Device, spec: FamilySpec, outcome: ReadOutcome, refresh_source: str
) -> int | None:
    """Best-effort phase-1 outcome record; a store failure must never break the mirror refresh."""
    try:
        return await outcome_store.record_read_outcome(db, device.id, spec.name, outcome, refresh_source=refresh_source)
    except Exception as exc:  # noqa: BLE001 — telemetry write; the mirror is the source of truth
        logger.warning(f"{spec.name}.outcome.read_record_failed", device_id=device.id, error=repr(exc))
        return None


async def _record_result(
    db: AsyncSession, spec: FamilySpec, attempt_id: int | None, *, result: str, succeeded: bool, row_count: int | None
) -> None:
    """Best-effort phase-2 outcome record + pointer advance."""
    if attempt_id is None:
        return
    try:
        await outcome_store.record_result(db, attempt_id, result=result, succeeded=succeeded, row_count=row_count)
    except Exception as exc:  # noqa: BLE001 — telemetry write; never fail the refresh over it
        logger.warning(f"{spec.name}.outcome.result_record_failed", attempt_id=attempt_id, error=repr(exc))


@dataclass(frozen=True)
class FamilySpec:
    """Declarative description of one read-mirror family (the policy-table row).

    * ``name`` — the surface name used in logs and degraded-surface lists (e.g. ``"static_route"``).
    * ``empty_policy`` — how a ``None`` read is interpreted (see :class:`EmptyPolicy`).
    * ``getter`` — ``(client, device_name) -> Awaitable[dict | None]``; the existing
      ``NsoClient.get_*`` coroutine (so mocks that patch the getter keep working).
    * ``extract`` — ``data -> payload``; pull the family's payload out of the read entry. A
      single-table family returns its row ``list``; a multi-table family (bgp, isis, snmp, …)
      can return the whole entry ``dict`` (or any structure) for its materializer to destructure.
    * ``materialize`` — ``(db, device, payload, refresh_source) -> Awaitable[None]``; the
      family-owned full-replace/upsert that also commits.
    * ``wire_name`` — the device-state envelope section name (e.g. ``"static-route"``).
      **This is the READSEM S3 fetch-source flip**: ``None`` keeps the legacy per-family
      getter path byte-for-byte; set, the engine reads the family's envelope section
      (status-declared) and ``getter`` goes unused. Flips per family, one line each.

    An authoritative clear (``AbsentAuthoritative``) is expressed by feeding the extractor an
    empty entry ``{}`` — so ``extract({})`` yields the family's "nothing" payload (``[]`` for a
    list family, ``{}`` for a dict family) and the SAME materialize path performs the clear. No
    separate clear hook, and no per-family "empty value" to keep in sync.
    """

    name: str
    empty_policy: EmptyPolicy
    getter: Callable[[NsoClient, str], Awaitable[dict | None]]
    extract: Callable[[dict], object]
    materialize: Callable[[AsyncSession, Device, object, str], Awaitable[None]]
    wire_name: str | None = None


async def _escalate_not_ready(device: Device, nso_client: NsoClient, spec: FamilySpec) -> ReadOutcome:
    """``not-ready`` → ONE ``device-state-read run`` for this family (READSEM S3).

    The envelope never extracts and its records are in-process — after a `packages reload`
    (or a NED remount) every section is ``not-ready`` until an extraction runs. The action
    extracts on demand and CAS-updates the records, so this single escalation both answers
    THIS read and re-warms the envelope for the next poll. Action output sections are
    terminal (``ok|unsupported|error``) — a ``not-ready`` here is a contract violation and
    is refused rather than looped on.
    """
    assert spec.wire_name is not None
    try:
        output = await nso_client.run_device_state_read(device.nso_device_name, [spec.wire_name])
    except Exception as exc:  # noqa: BLE001 — action error (bracket exhaustion, unknown device) → keep rows
        return Unavailable(UnavailableReason.read_error, detail=repr(exc))
    section = output.get(spec.wire_name)
    if section is None:
        return Unavailable(UnavailableReason.read_error, detail="action output missing the requested section")
    outcome = classify_envelope_section(section, spec.empty_policy)
    if isinstance(outcome, Unavailable) and outcome.reason is UnavailableReason.not_ready:
        return Unavailable(UnavailableReason.read_error, detail="action returned not-ready (contract violation)")
    return outcome


async def _classify_family_read(device: Device, nso_client: NsoClient, spec: FamilySpec) -> ReadOutcome:
    """Read + classify one family for one device, honoring the spec's fetch source."""
    if spec.wire_name is None:
        return await classify_read(
            lambda: spec.getter(nso_client, device.nso_device_name),
            spec.empty_policy,
        )
    try:
        section = await nso_client.get_device_state_section(device.nso_device_name, spec.wire_name)
    except NsoExportUnavailableError as exc:
        return Unavailable(UnavailableReason.export_down, detail=repr(exc))
    except Exception as exc:  # noqa: BLE001 — any read failure is Unavailable; the mirror is kept
        return Unavailable(UnavailableReason.read_error, detail=repr(exc))
    outcome = classify_envelope_section(section, spec.empty_policy)
    if isinstance(outcome, Unavailable) and outcome.reason is UnavailableReason.not_ready:
        logger.info(
            f"{spec.name}.refresh.not_ready_escalating",
            device_id=device.id,
            device_name=device.nso_device_name,
        )
        outcome = await _escalate_not_ready(device, nso_client, spec)
    return outcome


async def run_family_refresh(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    spec: FamilySpec,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Refresh one family's mirror for one device via the shared engine.

    Behaviour-equivalent to the hand-written ``refresh_<family>_for_device`` it replaces:
    returns ``True`` on a successful read (including an authoritative clear or an intentional
    skip) and ``False`` when the read was unavailable and the last-known rows were left intact.
    """
    if not device.nso_device_name:
        logger.debug(f"{spec.name}.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return True

    outcome = await _classify_family_read(device, nso_client, spec)
    return await _apply_outcome(db, device, spec, outcome, refresh_source)


async def run_family_refresh_from_section(
    db: AsyncSession,
    device: Device,
    spec: FamilySpec,
    section: dict | None,
    *,
    refresh_source: str = "sync",
) -> bool:
    """Refresh one family from a PRE-FETCHED envelope section (READSEM fetch grains b/c).

    The multi-family call classes — the periodic ``sync_device`` projection (one whole-device
    GET) and the atomic Sync-Now/onboarding action — fetch once and feed each family's section
    here. No escalation: the doc supplier is responsible for healing ``not-ready`` sections
    (a still-not-ready section records honestly and keeps rows, a degraded surface).
    """
    if not device.nso_device_name:
        logger.debug(f"{spec.name}.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return True

    outcome = classify_envelope_section(section, spec.empty_policy)
    return await _apply_outcome(db, device, spec, outcome, refresh_source)


async def _apply_outcome(
    db: AsyncSession,
    device: Device,
    spec: FamilySpec,
    outcome: ReadOutcome,
    refresh_source: str,
) -> bool:
    """Drive the mirror action + two-phase outcome record for a classified read."""
    # Phase 1: record the read outcome before the materializer runs (independent session).
    attempt_id = await _record_read(db, device, spec, outcome, refresh_source)

    if isinstance(outcome, Present):
        payload = spec.extract(outcome.data)
        await spec.materialize(db, device, payload, refresh_source)
        row_count = len(payload) if isinstance(payload, (list, tuple)) else None
        logger.info(
            f"{spec.name}.refresh.done",
            device_id=device.id,
            device_name=device.nso_device_name,
            row_count=row_count,
            freshness=outcome.freshness.value,
            refresh_source=refresh_source,
        )
        await _record_result(db, spec, attempt_id, result="replaced", succeeded=True, row_count=row_count)
        return True

    if isinstance(outcome, AbsentAuthoritative):
        # Clear by materializing the "nothing" payload for this family (extract of an empty entry).
        await spec.materialize(db, device, spec.extract({}), refresh_source)
        logger.info(
            f"{spec.name}.refresh.cleared",
            device_id=device.id,
            device_name=device.nso_device_name,
            refresh_source=refresh_source,
        )
        await _record_result(db, spec, attempt_id, result="cleared", succeeded=True, row_count=0)
        return True

    # Unavailable — keep the last-known rows in every case.
    assert isinstance(outcome, Unavailable)
    if outcome.reason in (UnavailableReason.not_authoritative, UnavailableReason.unsupported):
        # Declared/expected absence of authority: a keep-on-None inventory family's 404
        # (legacy path) or the envelope's declared `unsupported` (this NED has no reader for
        # the family). Not a read failure — keep the rows and report success (NOT a degraded
        # surface), so it never flips the device to `partial` on every poll.
        logger.info(
            f"{spec.name}.refresh.not_authoritative",
            device_id=device.id,
            device_name=device.nso_device_name,
            reason=outcome.reason.value,
            refresh_source=refresh_source,
        )
        await _record_result(db, spec, attempt_id, result="kept", succeeded=True, row_count=None)
        return True

    logger.warning(
        f"{spec.name}.refresh.unavailable",
        device_id=device.id,
        device_name=device.nso_device_name,
        reason=outcome.reason.value,
        detail=outcome.detail,
    )
    await _record_result(db, spec, attempt_id, result="kept", succeeded=False, row_count=None)
    return False
