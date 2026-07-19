# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Nokia L2-service (epipe/vpls + SAPs) refresh — reads NSO oper-data, upserts the DB.

Entry points:
- refresh_l2_services_for_device() — on-demand refresh (scheduler)
- handle_l2_service_change()        — SSE config-change hook (make_handler)

Read-only mirror: one flat DeviceL2Sap row per SAP, carrying its parent service.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.lag_topology import parse_changed_nso_devices
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import Device, DeviceL2Sap

logger = structlog.get_logger(__name__)


async def _upsert_l2_saps(
    db: AsyncSession,
    device: Device,
    services_data: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing SAP rows for *device*, then insert the fresh set."""
    await db.execute(delete(DeviceL2Sap).where(DeviceL2Sap.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)
    for service in services_data:
        service_name = service.get("service-name", "")
        if not service_name:
            continue
        service_type = service.get("service-type", "")
        service_id = service.get("service-id")
        for sap in as_list(service.get("sap")):
            sap_id = sap.get("sap-id", "")
            if not sap_id:
                continue
            db.add(
                DeviceL2Sap(
                    device_id=device.id,
                    service_name=service_name,
                    service_type=service_type,
                    service_id=service_id,
                    sap_id=sap_id,
                    port=sap.get("port", ""),
                    outer_tag=sap.get("outer-tag"),
                    inner_tag=sap.get("inner-tag"),
                    last_refreshed_at=now,
                    refresh_source=refresh_source,
                )
            )
    await db.commit()


async def refresh_l2_services_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read L2-service oper-data for *device* from NSO and upsert DB rows.

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    if not device.nso_device_name:
        logger.debug("l2_service.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return True

    try:
        entry = await nso_client.get_l2_services(device.nso_device_name)
    except Exception as exc:
        logger.warning("l2_service.refresh.nso_error", device_id=device.id, error=repr(exc))
        return False

    services_data = as_list(entry.get("service")) if entry else []
    await _upsert_l2_saps(db, device, services_data, refresh_source)
    logger.info(
        "l2_service.refresh.done",
        device_id=device.id,
        device_name=device.nso_device_name,
        service_count=len(services_data),
        refresh_source=refresh_source,
    )
    return True


async def handle_l2_service_change(
    event_data: dict,
    db: AsyncSession,
    nso_clients: dict[str, NsoClient],
) -> None:
    """Process a NETCONF config-change event and refresh L2-service rows."""
    changed = parse_changed_nso_devices(event_data)
    if not changed:
        return

    result = await db.execute(select(Device).where(Device.nso_device_name.in_(changed)))
    devices = result.scalars().all()

    for device in devices:
        nso_client = nso_clients.get(device.nso_instance)
        if nso_client is None:
            logger.debug("l2_service.event.no_client", device_id=device.id, instance=device.nso_instance)
            continue
        await refresh_l2_services_for_device(db, device, nso_client, refresh_source="notification")
