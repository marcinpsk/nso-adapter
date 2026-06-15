# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""LAG config apply — writes accepted LACP/LAG intent to the lag-reconciler service (M33)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from nso_adapter.nso.apply import NsoApplyError
from nso_adapter.nso.apply import apply_lag_config as _nso_apply_lag_config

if TYPE_CHECKING:
    from nso_adapter.nso.client import NsoClient
    from nso_adapter.store.models import Device

logger = structlog.get_logger(__name__)


def _build_bundle_payload(bundles) -> list[dict]:
    """Translate the apply-request bundle list into YANG-style service dicts."""
    out: list[dict] = []
    for b in bundles:
        entry: dict = {"name": b.name, "lag-id": b.lag_id}
        if b.min_links is not None:
            entry["min-links"] = b.min_links
        if b.system_priority is not None:
            entry["system-priority"] = b.system_priority
        if b.system_id is not None:
            entry["system-id"] = b.system_id
        if b.timer is not None:
            entry["timer"] = b.timer
        if b.admin_key is not None:
            entry["admin-key"] = b.admin_key
        members: list[dict] = []
        for m in b.members:
            member: dict = {"interface-name": m.interface_name}
            if m.mode is not None:
                member["mode"] = m.mode
            if m.port_priority is not None:
                member["port-priority"] = m.port_priority
            members.append(member)
        if members:
            entry["member"] = members
        out.append(entry)
    return out


async def apply_lag_config(device: Device, payload, nso_client: NsoClient) -> dict:
    """Full-replace the lag-reconciler service for *device* with the request bundles.

    Returns a result envelope: ``{"status": "deployed", "device": <name>,
    "bundle_count": N}`` on success, or ``{"status": "error", ...}`` on a write
    failure.
    """
    if not device.nso_device_name:
        return {
            "status": "error",
            "error": "no_nso_device_name",
            "message": f"Device {device.id} has no nso_device_name",
        }

    bundles = _build_bundle_payload(payload.bundles)

    try:
        # Full-replace: the plugin always pushes the full owned bundle snapshot, so
        # PUT-replace drops bundles removed in NetBox (merge-PATCH would not).
        await _nso_apply_lag_config(nso_client, device.nso_device_name, bundles, replace=True)
    except NsoApplyError as exc:
        logger.warning(
            "lag_intent.apply.failed",
            device_id=device.id,
            nso_device_name=device.nso_device_name,
            error=exc.code,
        )
        return {
            "status": "error",
            "error": exc.code,
            "message": str(exc),
            "detail": exc.detail,
        }

    logger.info(
        "lag_intent.apply.deployed",
        device_id=device.id,
        nso_device_name=device.nso_device_name,
        bundle_count=len(bundles),
    )
    return {
        "status": "deployed",
        "device": device.nso_device_name,
        "bundle_count": len(bundles),
    }
