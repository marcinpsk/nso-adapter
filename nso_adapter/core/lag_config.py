# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""LAG config refresh — reads NSO lag-config oper-data and upserts the DB."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.client import NsoClient
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
        b = LagBundleConfig(
            device_id=device.id,
            name=bundle["name"],
            lag_id=int(bundle["lag-id"]),
            min_links=bundle.get("min-links"),
            system_priority=bundle.get("system-priority"),
            system_id=bundle.get("system-id"),
            timer=bundle.get("timer"),
            admin_key=bundle.get("admin-key"),
            last_refreshed_at=now,
            refresh_source=refresh_source,
        )
        db.add(b)
        await db.flush()
        for member in bundle.get("member", []):
            db.add(
                LagMemberConfig(
                    lag_bundle_id=b.id,
                    interface_name=member["interface-name"],
                    mode=member.get("mode"),
                    port_priority=member.get("port-priority"),
                )
            )
    await db.commit()


async def refresh_lag_config_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> None:
    """Read lag-config oper-data for *device* from NSO and upsert DB rows."""
    if not device.nso_device_name:
        logger.debug("lag_config.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return

    try:
        entry = await nso_client.get_lag_config(device.nso_device_name)
    except Exception as exc:
        logger.warning("lag_config.refresh.nso_error", device_id=device.id, error=repr(exc))
        return

    bundles_data = entry.get("lag", []) if entry else []
    await _upsert_lag_configs(db, device, bundles_data, refresh_source)
    logger.info(
        "lag_config.refresh.done",
        device_id=device.id,
        bundle_count=len(bundles_data),
        source=refresh_source,
    )
