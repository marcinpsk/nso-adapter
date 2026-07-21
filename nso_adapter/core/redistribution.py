# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Refresh device_redistribution rows from OSPF/ISIS/BGP redistribute oper-data.

Reads redistribute statements already cached by the NSO package and upserts
device_redistribution rows.

Entry points:
- refresh_redistribution_for_device() — called on-demand after each OSPF/ISIS/BGP
  refresh to pick up any redistribution changes in the same NSO response cycle.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import classify_envelope_family_read
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import (
    AbsentAuthoritative,
    EmptyPolicy,
    Freshness,
    Present,
    ReadOutcome,
    Unavailable,
    UnavailableReason,
)
from nso_adapter.nso.shape import as_list
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, DeviceRedistribution

logger = structlog.get_logger(__name__)


def _ospf_dest_ref(instance: dict) -> str:
    """Stable dest_ref for OSPF: '<process_id>'."""
    return str(instance.get("process-id", ""))


def _isis_dest_ref(process: dict) -> str:
    """Stable dest_ref for ISIS: area-tag (empty string for untagged process)."""
    return str(process.get("process-tag", ""))


def _bgp_dest_ref(asn: str, scope: dict) -> str:
    """Stable dest_ref for BGP AF: '<asn>/<vrf>/<afi>'.

    One redistribute list lives per (asn, vrf, afi) address-family block.
    """
    vrf = scope.get("vrf", "") or ""
    return f"{asn}/{vrf}"


def _build_rows(
    device_id: int,
    dest_protocol: str,
    dest_ref: str,
    redist_list: list[dict],
    now: datetime,
    refresh_source: str,
) -> list[DeviceRedistribution]:
    rows = []
    for entry in as_list(redist_list):
        src_proto = str(entry.get("source-protocol", "")).strip()
        src_ref = str(entry.get("source-ref", "")).strip()
        if not src_proto:
            continue
        rows.append(
            DeviceRedistribution(
                device_id=device_id,
                dest_protocol=dest_protocol,
                dest_ref=dest_ref,
                source_protocol=src_proto,
                source_ref=src_ref,
                route_map=entry.get("route-map") or None,
                metric=entry.get("metric"),
                metric_type=entry.get("metric-type") or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )
    return rows


def _ospf_redistribution_rows(device_id: int, entry: dict, now: datetime, refresh_source: str) -> list:
    rows: list[DeviceRedistribution] = []
    for inst in as_list(entry.get("instance")):
        rows.extend(_build_rows(device_id, "ospf", _ospf_dest_ref(inst), inst.get("redistribute"), now, refresh_source))
    return rows


def _isis_redistribution_rows(device_id: int, entry: dict, now: datetime, refresh_source: str) -> list:
    rows: list[DeviceRedistribution] = []
    for proc in as_list(entry.get("process")):
        rows.extend(_build_rows(device_id, "isis", _isis_dest_ref(proc), proc.get("redistribute"), now, refresh_source))
    return rows


def _bgp_redistribution_rows(device_id: int, entry: dict, now: datetime, refresh_source: str) -> list:
    rows: list[DeviceRedistribution] = []
    for router in as_list(entry.get("router")):
        asn = str(router.get("asn", ""))
        for scope in as_list(router.get("scope")):
            scope_dest_ref = _bgp_dest_ref(asn, scope)
            for af in as_list(scope.get("address-family")):
                afi = str(af.get("afi", ""))
                dest_ref = f"{scope_dest_ref}/{afi}" if afi else scope_dest_ref
                rows.extend(_build_rows(device_id, "bgp", dest_ref, af.get("redistribute"), now, refresh_source))
    return rows


# Each source protocol: its envelope section (READSEM S3 B5) + the row builder that
# partitions on this dest_protocol. The redistribute lists ride inside the protocol
# families' own sections — redistribution reads the same wire the family specs read.
_REDIST_COMPONENTS = (
    ("ospf", "ospf-config", _ospf_redistribution_rows),
    ("isis", "isis-interface", _isis_redistribution_rows),
    ("bgp", "bgp-config", _bgp_redistribution_rows),
)


async def refresh_redistribution_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read OSPF/ISIS/BGP oper-data for *device* and refresh redistribution rows.

    Composite family (READSEM §2.6): each of the three source-protocol reads is classified
    independently into the :data:`~nso_adapter.nso.read_outcome.ReadOutcome` vocabulary, then a
    declared merge policy applies:

    * Any read that is a **confirmed fleet-wide export outage** (``NsoExportUnavailableError`` →
      ``export_down``) → **keep everything** untouched and return ``False``. Every protocol would
      report empty; full-replacing then would wipe the mirror over a transient blip.
    * Otherwise **per-component retention** (operator decision): a protocol whose read is
      authoritative (Present / confirmed-absent) full-replaces its ``dest_protocol`` partition; a
      protocol whose read failed with a non-outage error KEEPS its last-known rows. Returns
      ``True`` only when all three reads were authoritative, ``False`` if any was kept-stale.
    """
    if not device.nso_device_name:
        logger.debug("redistribution.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return True

    # Whole-span family lock (codex S3-R4): classify + materialize under "redistribution",
    # so the projected batch (which holds the same key) and this poll cannot interleave an
    # older snapshot over a newer one. Lock-before-semaphore ordering is preserved (the
    # escalation inside classify takes the shared action semaphore).
    from nso_adapter.core import refresh_engine as _engine

    async with _engine._family_lock(device.id, "redistribution"):
        outcomes: dict[str, ReadOutcome] = {}
        for proto, wire_name, _builder in _REDIST_COMPONENTS:
            outcomes[proto] = await classify_envelope_family_read(
                device,
                nso_client,
                wire_name=wire_name,
                empty_policy=EmptyPolicy.pop,
                family_name=f"redistribution.{proto}",
            )
        return await refresh_redistribution_from_outcomes(
            db, device, outcomes, refresh_source=refresh_source, own_lock=False
        )


async def refresh_redistribution_from_outcomes(
    db: AsyncSession,
    device: Device,
    outcomes: dict[str, ReadOutcome],
    *,
    refresh_source: str = "sync",
    own_lock: bool = True,
) -> bool:
    """Aggregate + materialize from PRE-CLASSIFIED component outcomes.

    Shared by the wrapper above (own section reads, grain a) and the projected fan-out
    (codex S3-R3 F2: grains b/c classify redistribution's three components from the SAME
    fetched doc/action snapshot — no extra NSO reads, one source of truth per sweep).

    Record discipline (codex S3-R3 F4): phase 1 is recorded BEFORE materialization; the
    mutation runs under the engine's two-mode failure guard (savepoint alive → roll it
    back + terminalize the same attempt; commit-time death → recover the session + a
    fresh terminal row); phase 2 terminalizes on success.
    """
    from nso_adapter.core import refresh_engine as _engine
    from nso_adapter.core.refresh_engine import _recover_session

    if own_lock:
        async with _engine._family_lock(device.id, "redistribution"):
            return await refresh_redistribution_from_outcomes(
                db, device, outcomes, refresh_source=refresh_source, own_lock=False
            )

    name = device.nso_device_name
    device_id = device.id
    now = datetime.now(UTC).replace(tzinfo=None)

    # Tier 1 — any confirmed export outage aborts the whole refresh, rows untouched.
    if any(isinstance(o, Unavailable) and o.reason is UnavailableReason.export_down for o in outcomes.values()):
        logger.warning("redistribution.refresh.degraded", device_id=device_id, device_name=name)
        await _record_composite(
            db,
            device,
            Unavailable(UnavailableReason.export_down),
            refresh_source,
            result="kept",
            succeeded=False,
            row_count=None,
        )
        return False

    # Merged phase-1 outcome, recorded BEFORE any mutation — the COMPLETE terminal
    # contract (READSEM S4 D7). Buckets: replaced = authoritative (Present rebuilds /
    # AbsentAuthoritative clears its partition), errors = retained by a real failure,
    # unsupported = declared no-reader (deliberately non-failing, the ArcOS asymmetry).
    assert _REDIST_COMPONENTS, "empty _REDIST_COMPONENTS is a programming error"
    replaced = [p for p, o in outcomes.items() if not isinstance(o, Unavailable)]
    errors = [
        p for p, o in outcomes.items() if isinstance(o, Unavailable) and o.reason is not UnavailableReason.unsupported
    ]
    unsupported_only = [
        p for p, o in outcomes.items() if isinstance(o, Unavailable) and o.reason is UnavailableReason.unsupported
    ]
    # Worst freshness among the REPLACED authoritative components: fresh < aged < stale
    # (SA-1 — a lone stale check silently demoted aged to fresh).
    _FRESHNESS_RANK = {Freshness.fresh: 0, Freshness.aged: 1, Freshness.stale: 2}
    worst_freshness = max(
        (o.freshness for o in outcomes.values() if isinstance(o, Present)),
        key=lambda f: _FRESHNESS_RANK[f],
        default=Freshness.fresh,
    )
    if replaced:
        # ≥1 partition authoritatively rebuilt → the mirror IS serve-worthy (present/
        # replaced/succeeded). A retained-by-error partition degrades freshness to stale
        # AND keeps the device partial (fn returns False); retained-only-unsupported
        # stays non-failing with the worst freshness among the replaced components.
        merged: ReadOutcome = Present({}, Freshness.stale if errors else worst_freshness)
        terminal_result, terminal_succeeded = "replaced", True
        composite_ok = not errors
    elif errors:
        # Nothing replaced, at least one real failure → unavailable with the WORST reason.
        merged = Unavailable(
            _worst_reason([o for o in outcomes.values() if isinstance(o, Unavailable)]),
            detail=f"components kept: {sorted(errors + unsupported_only)}",
        )
        terminal_result, terminal_succeeded, composite_ok = "kept", False, False
    else:
        # All components declared unsupported: nothing was read — never claim
        # fresh-present/replaced. Still a non-failure (no partial).
        merged = Unavailable(UnavailableReason.unsupported)
        terminal_result, terminal_succeeded, composite_ok = "kept", True, True
    attempt_id = None
    try:
        attempt_id = await outcome_store.record_read_outcome(
            db, device_id, "redistribution", merged, refresh_source=refresh_source
        )
    except Exception as exc:  # noqa: BLE001 — telemetry write; the mirror is the source of truth
        logger.warning("redistribution.outcome.read_record_failed", device_id=device_id, error=repr(exc))
        await _recover_session(db, device, "redistribution", device_id)

    # Tier 2 — per-component aggregation under the two-mode materialization guard.
    rebuilt: list[DeviceRedistribution] = []
    savepoint = await db.begin_nested()
    try:
        rebuilt = await _rebuild_partitions(db, device_id, name, outcomes, now, refresh_source)
        # First-wins in-refresh dedup: a duplicate identity tuple in the export would otherwise
        # IntegrityError on commit (uq_deviceredistribution_identity).
        seen: set[tuple[str, str, str, str]] = set()
        for row in rebuilt:
            key = (row.dest_protocol, row.dest_ref, row.source_protocol, row.source_ref)
            if key in seen:
                continue
            seen.add(key)
            db.add(row)
        await db.commit()
    except Exception:
        try:
            if savepoint.is_active:
                await savepoint.rollback()
                if attempt_id is not None:
                    await outcome_store.record_result(db, attempt_id, result="error", succeeded=False, row_count=None)
            else:
                await _recover_session(db, device, "redistribution", device_id)
                failed_id = await outcome_store.record_read_outcome(
                    db, device_id, "redistribution", merged, refresh_source=refresh_source
                )
                await outcome_store.record_result(db, failed_id, result="error", succeeded=False, row_count=None)
        except Exception as store_exc:  # noqa: BLE001 — telemetry; the materialization error is the story
            logger.warning("redistribution.outcome.terminalize_failed", device_id=device_id, error=repr(store_exc))
        raise

    logger.info(
        "redistribution.refresh.done",
        device_id=device_id,
        device_name=name,
        row_count=len(rebuilt),
        refresh_source=refresh_source,
    )
    try:
        if attempt_id is not None:
            await outcome_store.record_result(
                db,
                attempt_id,
                result=terminal_result,
                succeeded=terminal_succeeded,
                row_count=len(rebuilt),
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("redistribution.outcome.result_record_failed", device_id=device_id, error=repr(exc))
        await _recover_session(db, device, "redistribution", device_id)
    return composite_ok


# Reason severity for the no-authoritative-component merge (D7): a real failure always
# outranks a declared capability gap.
_REASON_SEVERITY = (
    UnavailableReason.export_down,
    UnavailableReason.read_error,
    UnavailableReason.not_ready,
    UnavailableReason.not_authoritative,
    UnavailableReason.unsupported,
)


def _worst_reason(unavailables: list[Unavailable]) -> UnavailableReason:
    """Pick the most severe reason among *unavailables* per :data:`_REASON_SEVERITY`."""
    reasons = {o.reason for o in unavailables}
    for reason in _REASON_SEVERITY:
        if reason in reasons:
            return reason
    return UnavailableReason.read_error  # unreachable with a non-empty input


async def _record_composite(
    db: AsyncSession,
    device: Device,
    outcome: ReadOutcome,
    refresh_source: str,
    *,
    result: str,
    succeeded: bool,
    row_count: int | None,
) -> None:
    """Best-effort two-phase record for paths that never materialize (tier-1 outage)."""
    from nso_adapter.core.refresh_engine import _recover_session
    from nso_adapter.store import outcome_store

    device_id = device.id
    try:
        attempt_id = await outcome_store.record_read_outcome(
            db, device_id, "redistribution", outcome, refresh_source=refresh_source
        )
        await outcome_store.record_result(db, attempt_id, result=result, succeeded=succeeded, row_count=row_count)
    except Exception as exc:  # noqa: BLE001 — telemetry write; the mirror is the source of truth
        logger.warning("redistribution.outcome.record_failed", device_id=device_id, error=repr(exc))
        await _recover_session(db, device, "redistribution", device_id)


async def _rebuild_partitions(
    db: AsyncSession,
    device_id: int,
    name: str,
    outcomes: dict[str, ReadOutcome],
    now: datetime,
    refresh_source: str,
) -> list[DeviceRedistribution]:
    """Apply the per-component aggregation (R1-F7): replace / keep-unsupported / keep-failed."""
    rebuilt: list[DeviceRedistribution] = []
    for proto, _wire_name, builder in _REDIST_COMPONENTS:
        outcome = outcomes[proto]
        if isinstance(outcome, (Present, AbsentAuthoritative)):
            entry = outcome.data if isinstance(outcome, Present) else {}
            await db.execute(
                delete(DeviceRedistribution).where(
                    DeviceRedistribution.device_id == device_id,
                    DeviceRedistribution.dest_protocol == proto,
                )
            )
            rebuilt.extend(builder(device_id, entry, now, refresh_source))
        elif isinstance(outcome, Unavailable) and outcome.reason is UnavailableReason.unsupported:
            logger.info(
                "redistribution.refresh.component_unsupported",
                device_id=device_id,
                device_name=name,
                protocol=proto,
            )
        else:
            logger.warning(
                "redistribution.refresh.component_kept",
                device_id=device_id,
                device_name=name,
                protocol=proto,
                reason=outcome.reason.value,
            )
    return rebuilt
