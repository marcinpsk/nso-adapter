# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Per-interface BFD refresh — reads NSO bfd-config oper-data and upserts the DB."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import EmptyPolicy
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import Device, DeviceBfdInterface

logger = structlog.get_logger(__name__)


async def _upsert_bfd_data(
    db: AsyncSession,
    device: Device,
    interfaces: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing BFD-interface rows for *device*, then insert fresh."""
    await db.execute(delete(DeviceBfdInterface).where(DeviceBfdInterface.device_id == device.id))
    now = datetime.now(UTC).replace(tzinfo=None)
    seen: set[str] = set()
    for iface in interfaces:
        name = iface.get("interface-name", "")
        if not name or name in seen:
            continue
        seen.add(name)
        db.add(
            DeviceBfdInterface(
                device_id=device.id,
                interface_name=name,
                bound_port=iface.get("bound-port") or None,
                min_tx=iface.get("min-tx"),
                min_rx=iface.get("min-rx"),
                multiplier=iface.get("multiplier"),
                micro_bfd=bool(iface.get("micro-bfd", False)),
                enabled=bool(iface.get("enabled", True)),
                last_refreshed_at=now,
                refresh_source=refresh_source,
            )
        )
    await db.commit()


BFD_SPEC = FamilySpec(
    name="bfd",
    empty_policy=EmptyPolicy.pop,
    getter=lambda client, name: client.get_bfd_config(name),
    # as_list guards the singleton-rendered-as-bare-dict case (was a raw .get → crash).
    extract=lambda data: as_list(data.get("interface")),
    materialize=_upsert_bfd_data,
    wire_name="bfd-config",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_bfd_interfaces_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read BFD oper-data for *device* from NSO and upsert DB rows (via the shared engine).

    Returns True on a successful read (or nothing to read); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, BFD_SPEC, refresh_source=refresh_source)
