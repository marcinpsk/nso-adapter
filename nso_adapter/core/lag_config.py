# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""LAG config refresh — reads NSO lag-config oper-data and upserts the DB."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import Device, LagBundleConfig, LagMemberConfig

logger = structlog.get_logger(__name__)


async def _upsert_lag_configs(
    db: AsyncSession,
    device: Device,
    bundles_data: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing rows, then insert fresh ones."""
    existing = await db.execute(select(LagBundleConfig.id).where(LagBundleConfig.device_id == device.id))
    bundle_ids = existing.scalars().all()
    if bundle_ids:
        await db.execute(delete(LagMemberConfig).where(LagMemberConfig.lag_bundle_id.in_(bundle_ids)))
    await db.execute(delete(LagBundleConfig).where(LagBundleConfig.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)
    for bundle in bundles_data:
        # name + lag-id are the NOT-NULL identity; a bundle missing either (or a non-numeric
        # lag-id) is malformed — skip it rather than KeyError/ValueError-abort the upsert
        # (which runs outside the fetch try/except) and freeze the whole LAG mirror.
        name = bundle.get("name")
        lag_id_raw = bundle.get("lag-id")
        if not name or lag_id_raw is None:
            continue
        try:
            lag_id = int(lag_id_raw)
        except (TypeError, ValueError):
            continue
        b = LagBundleConfig(
            device_id=device.id,
            name=name,
            lag_id=lag_id,
            min_links=bundle.get("min-links"),
            system_priority=bundle.get("system-priority"),
            system_id=bundle.get("system-id"),
            timer=bundle.get("timer"),
            admin_key=bundle.get("admin-key"),
            # NX-P2: the reader emits `vpc-sensitive` only for a vPC-protected bundle (absent =
            # ordinary). Carry it so the plugin can gate/badge it — a vPC bundle is refused
            # zero-write by the lag-reconciler, so it must never be offered for accept.
            vpc_sensitive=bool(bundle.get("vpc-sensitive")),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(b)
        await db.flush()
        # as_list guards the singleton-rendered-as-bare-dict case for the nested member list.
        for member in as_list(bundle.get("member")):
            member_name = member.get("interface-name")
            if not member_name:
                continue  # NOT-NULL member key missing → skip this member
            db.add(
                LagMemberConfig(
                    lag_bundle_id=b.id,
                    interface_name=member_name,
                    mode=member.get("mode"),
                    port_priority=member.get("port-priority"),
                )
            )


LAG_CONFIG_SPEC = FamilySpec(
    name="lag_config",
    # as_list guards the singleton-rendered-as-bare-dict case; extract({}) → [] → clear.
    extract=lambda data: as_list(data.get("lag")),
    materialize=_upsert_lag_configs,
    wire_name="lag-config",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_lag_config_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read lag-config oper-data for *device* from NSO and upsert DB rows (via the shared refresh engine).

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, LAG_CONFIG_SPEC, refresh_source=refresh_source)
