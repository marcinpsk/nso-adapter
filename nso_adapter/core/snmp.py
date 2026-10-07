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
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.domain.service_observation import project_snmp
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.shape import as_list
from nso_adapter.secrets.refs import require_secret_fingerprint
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
    now = datetime.now(UTC)
    for comm in as_list(entry.get("community")):
        require_secret_fingerprint(comm.get("name") if isinstance(comm, dict) else None)
    document = project_snmp(entry)

    await _delete_snmp_rows(db, device)

    for comm in document.communities or []:
        db.add(
            SnmpCommunity(
                device_id=device.id,
                community_hash=comm.name,
                access=comm.access if "access" in comm.present else "RO",
                acl=comm.acl or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    for user in document.users or []:
        db.add(
            SnmpV3User(
                device_id=device.id,
                username=user.username,
                has_auth_secret=bool(user.has_auth_secret),
                has_priv_secret=bool(user.has_priv_secret),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    for host in document.hosts or []:
        db.add(
            SnmpHost(
                device_id=device.id,
                address=host.address,
                version=host.version or None,
                notify_type=host.notify_type or None,
                port=host.port or None,
                username=host.user or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )

    location = document.system.location or None
    contact = document.system.contact or None
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


SNMP_SPEC = FamilySpec(
    name="snmp",
    # snmp-config is a POP-ON-EMPTY export family: a genuinely SNMP-less but synced device 404s,
    # and get_snmp_config confirms that bare 404 against the parent container before returning
    # None (a fleet-wide outage raises NsoExportUnavailableError → Unavailable → keep). So a None
    # here is a container-confirmed per-device absence → AbsentAuthoritative → clear. This is the
    # opposite of interface_ip, a present-empty inventory family whose 404 means only
    # unsupported-NED, so it KEEPS.
    # The materializer takes the whole entry dict; extract({}) → the clear (delete all + add none).
    extract=lambda data: data,
    materialize=_upsert_snmp_config,
    wire_name="snmp-config",  # READSEM S3: fetch from the device-state envelope
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
