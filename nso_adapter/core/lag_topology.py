# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""LAG topology refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_lag_topology_for_device() — called on-demand by SSE handler or scheduler
- handle_netconf_config_change() — called by the SSE on_event handler
- parse_changed_nso_devices() — pure function; extracted for testability
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import Device, LagInterface, LagMember

logger = structlog.get_logger(__name__)

_DEVICE_RE = re.compile(r"devices/device\[name='([^']+)'\]")


def parse_changed_nso_devices(event_data: dict) -> set[str]:
    """Extract NSO device names from a netconf-config-change event payload."""
    notification = event_data.get("ietf-restconf:notification", event_data)
    change = notification.get("netconf-config-change") or notification.get(
        "ietf-netconf-notifications:netconf-config-change"
    )
    if not change:
        return set()

    devices: set[str] = set()
    for edit in change.get("edit", []):
        target = edit.get("target", "")
        match = _DEVICE_RE.search(target)
        if match:
            devices.add(match.group(1))
    return devices


async def _upsert_lags(
    db: AsyncSession,
    device: Device,
    lags_data: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing rows, then insert fresh ones."""
    existing = await db.execute(select(LagInterface.id).where(LagInterface.device_id == device.id))
    lag_ids = existing.scalars().all()
    if lag_ids:
        await db.execute(delete(LagMember).where(LagMember.lag_interface_id.in_(lag_ids)))
    await db.execute(delete(LagInterface).where(LagInterface.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)
    for lag in lags_data:
        li = LagInterface(
            device_id=device.id,
            name=lag["name"],
            lag_id=int(lag["lag-id"]),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(li)
        await db.flush()
        for member in as_list(lag.get("member")):
            db.add(
                LagMember(
                    lag_interface_id=li.id,
                    interface_name=member["interface-name"],
                    mode=member.get("mode", "unknown"),
                )
            )
    await db.commit()


async def refresh_lag_topology_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read oper-data for *device* from NSO and upsert DB rows.

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    if not device.nso_device_name:
        logger.debug("lag.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return True

    try:
        entry = await nso_client.get_lag_topology(device.nso_device_name)
    except Exception as exc:
        logger.warning("lag.refresh.nso_error", device_id=device.id, error=repr(exc))
        return False

    lags_data = as_list(entry.get("lag")) if entry else []
    await _upsert_lags(db, device, lags_data, refresh_source)
    logger.info(
        "lag.refresh.done",
        device_id=device.id,
        nso_device_name=device.nso_device_name,
        lag_count=len(lags_data),
        source=refresh_source,
    )
    return True


async def handle_netconf_config_change(
    event_data: dict,
    db: AsyncSession,
    nso_clients: dict[str, NsoClient],
) -> None:
    """Process a NETCONF config-change event and refresh LAG topology."""
    changed = parse_changed_nso_devices(event_data)
    if not changed:
        return

    result = await db.execute(select(Device).where(Device.nso_device_name.in_(changed)))
    devices = result.scalars().all()

    for device in devices:
        nso_client = nso_clients.get(device.nso_instance)
        if nso_client is None:
            logger.debug("lag.event.no_client", device_id=device.id, instance=device.nso_instance)
            continue
        await refresh_lag_topology_for_device(db, device, nso_client, refresh_source="notification")
