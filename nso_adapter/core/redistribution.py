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

    name = device.nso_device_name
    now = datetime.now(UTC).replace(tzinfo=None)

    # Classify each component's envelope section independently (pop policy: a confirmed
    # DEVICE absence is an authoritative clear for every partition).
    outcomes: dict[str, ReadOutcome] = {}
    for proto, wire_name, _builder in _REDIST_COMPONENTS:
        outcomes[proto] = await classify_envelope_family_read(
            device,
            nso_client,
            wire_name=wire_name,
            empty_policy=EmptyPolicy.pop,
            family_name=f"redistribution.{proto}",
        )

    # Tier 1 — any confirmed export outage aborts the whole refresh, rows untouched.
    if any(isinstance(o, Unavailable) and o.reason is UnavailableReason.export_down for o in outcomes.values()):
        logger.warning("redistribution.refresh.degraded", device_id=device.id, device_name=name)
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

    # Tier 2 — per-component aggregation (codex S3-R1 F7, explicit):
    # * Present (fresh OR stale=degraded-success) / AbsentAuthoritative → full-replace the
    #   partition (stale carries the degraded marker into the composite outcome record);
    # * declared `unsupported` → keep the partition AND count as success — an ArcOS device
    #   (OSPF unsupported, ISIS/BGP supported) must not sit permanently `partial`;
    # * anything else (read_error, failed escalation) → keep the partition + failure.
    all_authoritative = True
    any_stale = False
    rebuilt: list[DeviceRedistribution] = []
    for proto, _wire_name, builder in _REDIST_COMPONENTS:
        outcome = outcomes[proto]
        if isinstance(outcome, (Present, AbsentAuthoritative)):
            if isinstance(outcome, Present) and outcome.freshness is Freshness.stale:
                any_stale = True
            entry = outcome.data if isinstance(outcome, Present) else {}
            await db.execute(
                delete(DeviceRedistribution).where(
                    DeviceRedistribution.device_id == device.id,
                    DeviceRedistribution.dest_protocol == proto,
                )
            )
            rebuilt.extend(builder(device.id, entry, now, refresh_source))
        elif isinstance(outcome, Unavailable) and outcome.reason is UnavailableReason.unsupported:
            logger.info(
                "redistribution.refresh.component_unsupported",
                device_id=device.id,
                device_name=name,
                protocol=proto,
            )
        else:
            all_authoritative = False
            logger.warning(
                "redistribution.refresh.component_kept",
                device_id=device.id,
                device_name=name,
                protocol=proto,
                reason=outcome.reason.value,
            )

    # First-wins in-refresh dedup: a duplicate identity tuple in the export would otherwise
    # IntegrityError on commit (uq_deviceredistribution_identity). dest_protocol is part of the
    # identity, so this only collides within a single rebuilt protocol's rows.
    seen: set[tuple[str, str, str, str]] = set()
    for row in rebuilt:
        key = (row.dest_protocol, row.dest_ref, row.source_protocol, row.source_ref)
        if key in seen:
            continue
        seen.add(key)
        db.add(row)
    await db.commit()

    logger.info(
        "redistribution.refresh.done",
        device_id=device.id,
        device_name=name,
        row_count=len(rebuilt),
        refresh_source=refresh_source,
    )
    # Composite outcome record (codex S3-R1 F7 — redistribution never recorded): the merged
    # worst-of view. Success (incl. unsupported-skips) → Present, stale if ANY component was;
    # a kept component → Unavailable(read_error) with succeeded=False.
    if all_authoritative:
        merged: ReadOutcome = Present({}, Freshness.stale if any_stale else Freshness.fresh)
        await _record_composite(
            db, device, merged, refresh_source, result="replaced", succeeded=True, row_count=len(rebuilt)
        )
    else:
        kept = [
            p
            for p, o in outcomes.items()
            if isinstance(o, Unavailable) and o.reason is not UnavailableReason.unsupported
        ]
        merged = Unavailable(UnavailableReason.read_error, detail=f"components kept: {kept}")
        await _record_composite(
            db, device, merged, refresh_source, result="kept", succeeded=False, row_count=len(rebuilt)
        )
    return all_authoritative


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
    """Best-effort two-phase outcome record for the composite family (never breaks the refresh).

    Same session-recovery contract as the engine's phase helpers (codex S3-R2 F2 class): a
    store write that dies at the DB level dooms the caller's transaction and expires the
    ORM instances — recover so the caller's next work stays usable.
    """
    from nso_adapter.core.refresh_engine import _recover_session

    device_id = device.id
    try:
        attempt_id = await outcome_store.record_read_outcome(
            db, device_id, "redistribution", outcome, refresh_source=refresh_source
        )
        await outcome_store.record_result(db, attempt_id, result=result, succeeded=succeeded, row_count=row_count)
    except Exception as exc:  # noqa: BLE001 — telemetry write; the mirror is the source of truth
        logger.warning("redistribution.outcome.record_failed", device_id=device_id, error=repr(exc))
        await _recover_session(db, device, "redistribution", device_id)
