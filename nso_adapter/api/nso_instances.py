# SPDX-License-Identifier: Apache-2.0
"""NSO instances discovery endpoints — GET /api/v1/nso-instances."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.api.errors import api_error
from nso_adapter.config import get_config
from nso_adapter.core.importer import get_nso_client
from nso_adapter.nso.neds import extract_ned_id_from_device_dict, ned_family
from nso_adapter.store.models import Device

router = APIRouter(prefix="/api/v1/nso-instances", tags=["nso-instances"])


@router.get("", dependencies=[Depends(verify_token)])
async def list_nso_instances():
    cfg = get_config()
    out = []
    for inst in cfg.nso_instances:
        reachable = False
        try:
            client = get_nso_client(inst.name)
            await client.list_devices()
            reachable = True
        except Exception:
            pass
        out.append({"id": inst.name, "name": inst.name, "base_url": inst.base_url, "reachable": reachable})
    return out


@router.get("/{instance_id}/devices", dependencies=[Depends(verify_token)])
async def list_instance_devices(instance_id: str, db: AsyncSession = Depends(get_db)):
    """Return enriched device list for *instance_id*.

    Each item includes NED family, address, auth group, admin state, and an
    onboarded cross-reference against the adapter's Device table.  All nullable
    fields are always present (null rather than omitted) so the plugin can stay
    branchless.

    Returns 404 if the instance ID is unknown, 502 on NSO connectivity error.
    """
    cfg = get_config()
    if not any(inst.name == instance_id for inst in cfg.nso_instances):
        raise api_error(404, "not_found", f"NSO instance '{instance_id}' not found")

    try:
        client = get_nso_client(instance_id)
        device_list = await client.list_devices()
    except Exception as exc:
        raise api_error(502, "nso_unreachable", str(exc)) from exc

    # Build onboarded cross-reference in one DB query (not per item)
    rows = await db.execute(
        select(Device.nso_device_name, Device.id, Device.netbox_device_id).where(Device.nso_instance == instance_id)
    )
    by_name: dict[str, tuple[int, int | None]] = {}
    for r in rows:
        if r.nso_device_name not in by_name:
            by_name[r.nso_device_name] = (r.id, r.netbox_device_id)

    out = []
    for d in device_list:
        if not isinstance(d, dict) or not d.get("name"):
            continue
        name = d["name"]
        raw_ned_id = extract_ned_id_from_device_dict(d)
        onboarded_row = by_name.get(name)
        out.append(
            {
                "name": name,
                "address": d.get("address") or None,
                "ned_id": raw_ned_id,
                "platform": ned_family(raw_ned_id) if raw_ned_id else None,
                "auth_group": d.get("authgroup") or None,
                "admin_state": (d.get("state") or {}).get("admin-state") or None,  # "" → None
                "onboarded": onboarded_row is not None,
                "onboarded_device_id": onboarded_row[0] if onboarded_row else None,
                "onboarded_netbox_device_id": onboarded_row[1] if onboarded_row else None,
            }
        )

    out.sort(key=lambda x: x["name"])
    return out


@router.get("/{instance_id}/neds", dependencies=[Depends(verify_token)])
async def list_instance_neds(instance_id: str):
    """Return the NED packages installed on *instance_id* (the available NEDs).

    Each entry: {ned_id, package, version, oper_status, vendor, operating_systems,
    product_families, platform}. ``platform`` is the short family label
    (:func:`ned_family`) when recognised, for matching NetBox platforms.

    404 if the instance is unknown, 502 on NSO connectivity error.
    """
    cfg = get_config()
    if not any(inst.name == instance_id for inst in cfg.nso_instances):
        raise api_error(404, "not_found", f"NSO instance '{instance_id}' not found")
    try:
        client = get_nso_client(instance_id)
        neds = await client.list_ned_packages()
    except Exception as exc:
        raise api_error(502, "nso_unreachable", str(exc)) from exc
    for n in neds:
        n["platform"] = ned_family(n["ned_id"]) if n.get("ned_id") else None
    return neds
