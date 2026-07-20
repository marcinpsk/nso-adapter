# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""SNMP config refresh — reads NSO oper-data and full-replaces the DB cache.

Entry points:
- refresh_snmp_config_for_device() — on-demand refresh (scheduler / SSE handler)
- handle_snmp_config_change()      — SSE on_event handler (config-change notification)
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.lag_topology import parse_changed_nso_devices
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, SnmpCommunity, SnmpHost, SnmpSystemInfo, SnmpV3User

logger = structlog.get_logger(__name__)


async def _delete_snmp_rows(db: AsyncSession, device: Device) -> None:
    """Delete every SNMP mirror row for *device* (community / v3-user / host / system-info).

    Not committed here — the caller owns the transaction boundary.
    """
    await db.execute(delete(SnmpCommunity).where(SnmpCommunity.device_id == device.id))
    await db.execute(delete(SnmpV3User).where(SnmpV3User.device_id == device.id))
    await db.execute(delete(SnmpHost).where(SnmpHost.device_id == device.id))
    await db.execute(delete(SnmpSystemInfo).where(SnmpSystemInfo.device_id == device.id))


async def _upsert_snmp_config(
    db: AsyncSession,
    device: Device,
    entry: dict,
    refresh_source: str,
) -> None:
    """Full-replace all SNMP rows for *device* from *entry*."""
    now = datetime.now(UTC).replace(tzinfo=None)

    await _delete_snmp_rows(db, device)

    for comm in entry.get("community", []):
        db.add(
            SnmpCommunity(
                device_id=device.id,
                community_hash=comm.get("name", ""),
                access=comm.get("access", "RO"),
                acl=comm.get("acl") or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    for user in entry.get("v3-user", []):
        db.add(
            SnmpV3User(
                device_id=device.id,
                username=user.get("username", ""),
                has_auth_secret=bool(user.get("has-auth-secret", False)),
                has_priv_secret=bool(user.get("has-priv-secret", False)),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    for host in entry.get("host", []):
        db.add(
            SnmpHost(
                device_id=device.id,
                address=host.get("address", ""),
                version=host.get("version") or None,
                notify_type=host.get("notify-type") or None,
                port=host.get("port") or None,
                # v3 hosts only — the export gates it on version, precisely so a v1/v2c host's
                # community string (the same NED field) can never arrive here (CR-P16).
                username=host.get("user") or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    location = entry.get("location") or None
    contact = entry.get("contact") or None
    if location or contact:
        db.add(
            SnmpSystemInfo(
                device_id=device.id,
                location=location,
                contact=contact,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    await db.commit()


async def refresh_snmp_config_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read SNMP oper-data for *device* from NSO and upsert DB rows.

    Returns True on a successful read (or nothing to read); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    if not device.nso_device_name:
        logger.debug("snmp.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return True

    try:
        entry = await nso_client.get_snmp_config(device.nso_device_name)
    except Exception as exc:
        logger.warning("snmp.refresh.nso_error", device_id=device.id, error=repr(exc))
        return False

    if entry is None:
        # Read-mirror empty-semantics contract (see interface_ip.refresh's B2 note for the
        # other side). snmp-config is a POP-ON-EMPTY export family: a genuinely SNMP-less but
        # synced device 404s. Per the export's deliberate design (network_state_export/snmp.py
        # `_refresh_device`: "the operator really did remove it"), that None is AUTHORITATIVE —
        # the mirror must CLEAR to match. This is the opposite of interface_ip, a present-empty
        # "inventory" family whose 404 means only unsupported-NED, so it KEEPS.
        #
        # Crucially, get_snmp_config confirms a bare 404 against the parent container before
        # returning None (mirroring get_route_policy): a fleet-wide export outage — package not
        # loaded / mid-`packages reload` / callpoint erroring — 404s EVERY device at once, and is
        # raised as NsoExportUnavailableError → caught by the `except` above → return False, rows
        # untouched. So None here is only ever a confirmed per-device absence, never a wipe over a
        # degraded read.
        await _delete_snmp_rows(db, device)
        await db.commit()
        logger.info(
            "snmp.refresh.cleared",
            device_id=device.id,
            nso_device_name=device.nso_device_name,
            reason="nso_returned_none",
        )
        return True

    await _upsert_snmp_config(db, device, entry, refresh_source)
    logger.info(
        "snmp.refresh.done",
        device_id=device.id,
        nso_device_name=device.nso_device_name,
        communities=len(entry.get("community", [])),
        v3_users=len(entry.get("v3-user", [])),
        hosts=len(entry.get("host", [])),
        source=refresh_source,
    )
    return True


async def handle_snmp_config_change(
    event_data: dict,
    db: AsyncSession,
    nso_clients: dict[str, NsoClient],
) -> None:
    """Process a NETCONF config-change event and refresh SNMP config rows."""
    changed = parse_changed_nso_devices(event_data)
    if not changed:
        return

    result = await db.execute(select(Device).where(Device.nso_device_name.in_(changed)))
    devices = result.scalars().all()

    for device in devices:
        nso_client = nso_clients.get(device.nso_instance)
        if nso_client is None:
            logger.debug(
                "snmp.event.no_client",
                device_id=device.id,
                instance=device.nso_instance,
            )
            continue
        await refresh_snmp_config_for_device(db, device, nso_client, refresh_source="notification")
