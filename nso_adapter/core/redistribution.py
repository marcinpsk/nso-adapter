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

from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import (
    AbsentAuthoritative,
    EmptyPolicy,
    Present,
    Unavailable,
    UnavailableReason,
    classify_read,
)
from nso_adapter.nso.shape import as_list
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


# Each source protocol: its getter + the row builder that partitions on this dest_protocol.
_REDIST_COMPONENTS = (
    ("ospf", lambda c, n: c.get_ospf(n), _ospf_redistribution_rows),
    ("isis", lambda c, n: c.get_isis_interfaces(n), _isis_redistribution_rows),
    ("bgp", lambda c, n: c.get_bgp_config(n), _bgp_redistribution_rows),
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

    # Classify each component read independently. Each getter is a pop-policy config family: a
    # container-confirmed 404 → AbsentAuthoritative (that protocol has no redistribution).
    outcomes: dict[str, object] = {}
    for proto, getter, _builder in _REDIST_COMPONENTS:
        outcomes[proto] = await classify_read(lambda g=getter: g(nso_client, name), EmptyPolicy.pop)

    # Tier 1 — any confirmed export outage aborts the whole refresh, rows untouched.
    if any(isinstance(o, Unavailable) and o.reason is UnavailableReason.export_down for o in outcomes.values()):
        logger.warning("redistribution.refresh.degraded", device_id=device.id, device_name=name)
        return False

    # Tier 2 — per-component retention. Build the fresh rows for the authoritative protocols and
    # full-replace only those partitions; a read_error protocol keeps its rows (never deleted).
    all_authoritative = True
    rebuilt: list[DeviceRedistribution] = []
    for proto, _getter, builder in _REDIST_COMPONENTS:
        outcome = outcomes[proto]
        if isinstance(outcome, (Present, AbsentAuthoritative)):
            entry = outcome.data if isinstance(outcome, Present) else {}
            await db.execute(
                delete(DeviceRedistribution).where(
                    DeviceRedistribution.device_id == device.id,
                    DeviceRedistribution.dest_protocol == proto,
                )
            )
            rebuilt.extend(builder(device.id, entry, now, refresh_source))
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
    return all_authoritative
