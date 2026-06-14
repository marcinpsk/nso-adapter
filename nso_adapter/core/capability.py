# SPDX-License-Identifier: Apache-2.0
"""Route-policy capability matrix — populate + query the device_capability cache.

The cache (keyed by ``(ned_id, sw_version)``) lets the plugin flag, at attach time,
which parts of a route-map / community-list won't apply on a device — instead of the
operator finding out only when it silently didn't land. Two halves feed each verdict:

  - representable (``source='probe'``) — from the NSO ``capability-probe`` action (what
    the reconciler can model/send for this NED);
  - accepted (``source='apply'``) — from a real device-parser rejection at commit (what
    the box actually takes). Apply wins over probe.
"""

from __future__ import annotations

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso import actions
from nso_adapter.nso.client import NsoClient
from nso_adapter.store import models as m

logger = structlog.get_logger(__name__)


async def _upsert(
    db: AsyncSession, ned_id: str, sw_version: str, scope: str, name: str, status: str, detail: str, source: str
) -> None:
    row = (
        await db.execute(
            select(m.DeviceCapability).where(
                m.DeviceCapability.ned_id == ned_id,
                m.DeviceCapability.sw_version == sw_version,
                m.DeviceCapability.scope == scope,
                m.DeviceCapability.name == name,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        db.add(
            m.DeviceCapability(
                ned_id=ned_id,
                sw_version=sw_version,
                scope=scope,
                name=name,
                status=status,
                detail=detail,
                source=source,
            )
        )
        return
    # An apply-sourced 'unsupported' is authoritative (a real device rejection) — a later
    # representable probe must NOT downgrade it back to native.
    if row.source == "apply" and row.status == "unsupported" and source == "probe":
        return
    row.status, row.detail, row.source = status, detail, source


async def record_probe_capability(db: AsyncSession, ned_id: str, sw_version: str, elements) -> int:
    """Store the representable-half verdict (list of ``{scope,name,status,detail}``)."""
    count = 0
    for el in elements:
        await _upsert(
            db,
            ned_id,
            sw_version,
            str(el.get("scope", "")),
            str(el.get("name", "")),
            str(el.get("status", "")),
            str(el.get("detail", ""))[:256],
            "probe",
        )
        count += 1
    await db.commit()
    return count


async def record_capability_rejection(
    db: AsyncSession, ned_id: str, sw_version: str, scope: str, name: str, detail: str
) -> None:
    """Record an accepted-half rejection (device parser refused it at commit). Apply wins."""
    if not ned_id:
        return
    await _upsert(db, ned_id, sw_version, scope, name, "unsupported", str(detail)[:256], "apply")
    await db.commit()


async def get_device_capability(db: AsyncSession, ned_id: str, sw_version: str) -> list[m.DeviceCapability]:
    """All cached capability rows for a ``(ned_id, sw_version)`` key."""
    return list(
        (
            await db.execute(
                select(m.DeviceCapability).where(
                    m.DeviceCapability.ned_id == ned_id,
                    m.DeviceCapability.sw_version == sw_version,
                )
            )
        )
        .scalars()
        .all()
    )


async def refresh_device_capability(db: AsyncSession, nso_client: NsoClient, device_name: str) -> dict:
    """Invoke the NSO capability-probe action for a device and store the representable half.

    Returns ``{ned_id, sw_version, count}`` (or ``{}`` when the probe reports no NED).
    """
    out = await actions.capability_probe(nso_client, device_name)
    ned_id = str(out.get("ned-id", "") or "")
    sw_version = str(out.get("sw-version", "") or "")
    if not ned_id:
        logger.debug("capability.refresh.no_ned", device=device_name)
        return {}
    elements = out.get("element", []) or []
    count = await record_probe_capability(db, ned_id, sw_version, elements)
    logger.info("capability.refresh.done", device=device_name, ned_id=ned_id, sw_version=sw_version, elements=count)
    return {"ned_id": ned_id, "sw_version": sw_version, "count": count}
