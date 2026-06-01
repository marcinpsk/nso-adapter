# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Redistribution refresh — reads redistribute statements from OSPF/ISIS/BGP
oper-data already cached by the NSO package and upserts device_redistribution rows.

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
    for entry in redist_list:
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


async def refresh_redistribution_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read OSPF/ISIS/BGP oper-data for *device* and upsert redistribution rows.

    Performs a full-replace: deletes all existing DeviceRedistribution rows for the
    device, then re-inserts from fresh oper-data.  Three NSO requests are made —
    get_ospf, get_isis_interfaces, get_bgp_config.  Individual failures are logged
    and skipped; the remaining protocols still get upserted.
    """
    if not device.nso_device_name:
        logger.debug("redistribution.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return

    now = datetime.now(UTC).replace(tzinfo=None)
    rows: list[DeviceRedistribution] = []

    # ── OSPF ─────────────────────────────────────────────────────────────────
    try:
        ospf_entry = await nso_client.get_ospf(device.nso_device_name)
        for inst in (ospf_entry or {}).get("instance", []):
            dest_ref = _ospf_dest_ref(inst)
            rows.extend(_build_rows(device.id, "ospf", dest_ref, inst.get("redistribute", []), now, refresh_source))
    except Exception as exc:
        logger.warning("redistribution.refresh.ospf_error", device_id=device.id, error=repr(exc))

    # ── ISIS ─────────────────────────────────────────────────────────────────
    try:
        isis_entry = await nso_client.get_isis_interfaces(device.nso_device_name)
        for proc in (isis_entry or {}).get("process", []):
            dest_ref = _isis_dest_ref(proc)
            rows.extend(_build_rows(device.id, "isis", dest_ref, proc.get("redistribute", []), now, refresh_source))
    except Exception as exc:
        logger.warning("redistribution.refresh.isis_error", device_id=device.id, error=repr(exc))

    # ── BGP ──────────────────────────────────────────────────────────────────
    try:
        bgp_entry = await nso_client.get_bgp_config(device.nso_device_name)
        for router in (bgp_entry or {}).get("router", []):
            asn = str(router.get("asn", ""))
            for scope in router.get("scope", []):
                scope_dest_ref = _bgp_dest_ref(asn, scope)
                for af in scope.get("address-family", []):
                    afi = str(af.get("afi", ""))
                    dest_ref = f"{scope_dest_ref}/{afi}" if afi else scope_dest_ref
                    rows.extend(
                        _build_rows(device.id, "bgp", dest_ref, af.get("redistribute", []), now, refresh_source)
                    )
    except Exception as exc:
        logger.warning("redistribution.refresh.bgp_error", device_id=device.id, error=repr(exc))

    # ── Upsert ───────────────────────────────────────────────────────────────
    await db.execute(delete(DeviceRedistribution).where(DeviceRedistribution.device_id == device.id))
    for row in rows:
        db.add(row)
    await db.commit()

    logger.info(
        "redistribution.refresh.done",
        device_id=device.id,
        device_name=device.nso_device_name,
        row_count=len(rows),
        refresh_source=refresh_source,
    )
