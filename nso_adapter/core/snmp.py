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
from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import EmptyPolicy
from nso_adapter.nso.shape import as_list
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

    # as_list guards the RESTCONF singleton-rendered-as-bare-dict case for each child list.
    for comm in as_list(entry.get("community")):
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

    for user in as_list(entry.get("v3-user")):
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

    for host in as_list(entry.get("host")):
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


SNMP_SPEC = FamilySpec(
    name="snmp",
    # snmp-config is a POP-ON-EMPTY export family: a genuinely SNMP-less but synced device 404s,
    # and get_snmp_config confirms that bare 404 against the parent container before returning
    # None (a fleet-wide outage raises NsoExportUnavailableError → Unavailable → keep). So a None
    # here is a container-confirmed per-device absence → AbsentAuthoritative → clear. This is the
    # opposite of interface_ip, a present-empty inventory family whose 404 means only
    # unsupported-NED, so it KEEPS.
    empty_policy=EmptyPolicy.pop,
    getter=lambda client, name: client.get_snmp_config(name),
    # The materializer takes the whole entry dict; extract({}) → the clear (delete all + add none).
    extract=lambda data: data,
    materialize=_upsert_snmp_config,
)


async def refresh_snmp_config_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read SNMP oper-data for *device* from NSO and upsert DB rows (via the shared refresh engine).

    Returns True on a successful read (or nothing to read); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, SNMP_SPEC, refresh_source=refresh_source)


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
