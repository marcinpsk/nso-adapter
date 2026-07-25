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
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh
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


L2_SERVICE_SPEC = FamilySpec(
    name="l2_service",
    extract=lambda data: as_list(data.get("service")),
    materialize=_upsert_l2_saps,
    wire_name="l2-service",  # READSEM S3: fetch from the device-state envelope
)


async def refresh_l2_services_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read L2-service oper-data for *device* from NSO and upsert DB rows (via the shared engine).

    Returns True on a successful read (or an intentional skip); False when the NSO read
    failed and the last-known rows were left untouched (a degraded surface).
    """
    return await run_family_refresh(db, device, nso_client, L2_SERVICE_SPEC, refresh_source=refresh_source)
