# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""LAG config refresh — reads NSO lag-config oper-data and upserts the DB."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
from nso_adapter.domain.switching_observation import project_lag_config
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

    now = datetime.now(UTC)
    projection = project_lag_config({"lag": bundles_data})
    for bundle in projection.bundles or []:
        b = LagBundleConfig(
            device_id=device.id,
            name=bundle.name,
            lag_id=bundle.lag_id,
            min_links=bundle.min_links,
            system_priority=bundle.system_priority,
            system_id=bundle.system_id,
            timer=bundle.timer,
            admin_key=bundle.admin_key,
            vpc_sensitive=bool(bundle.vpc_sensitive),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(b)
        await db.flush()
        for member in bundle.member or []:
            db.add(
                LagMemberConfig(
                    lag_bundle_id=b.id,
                    interface_name=member.interface_name,
                    mode=member.mode,
                    port_priority=member.port_priority,
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
