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

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
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
        if "lag-id" not in lag:
            # Live ra1: a Nokia lag named without digits ("lag-aa") serves no lag-id, and
            # the column is NOT NULL — skip the entry (bgp's asn-less-router convention)
            # instead of KeyError'ing the whole refresh and losing every lag.
            logger.warning(
                "lag_topology.entry_skipped",
                device_id=device.id,
                lag_name=lag.get("name"),
                reason="no lag-id",
            )
            continue
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


LAG_TOPOLOGY_SPEC = FamilySpec(
    name="lag",
    # as_list guards the singleton-rendered-as-bare-dict case; extract({}) → [] → clear.
    extract=lambda data: as_list(data.get("lag")),
    materialize=_upsert_lags,
    wire_name="lag-topology",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_lag_topology_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read oper-data for *device* from NSO and upsert DB rows (via the shared refresh engine).

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, LAG_TOPOLOGY_SPEC, refresh_source=refresh_source)
