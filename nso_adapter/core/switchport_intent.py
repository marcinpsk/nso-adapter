# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Switchport apply — writes accepted L2 switchport intent to the switchport-reconciler (M34)."""
from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from nso_adapter.nso.apply import NsoApplyError
from nso_adapter.nso.apply import apply_switchport_config as _nso_apply_switchport_config

if TYPE_CHECKING:
    from nso_adapter.nso.client import NsoClient
    from nso_adapter.store.models import Device

logger = structlog.get_logger(__name__)


def _build_interface_payload(interfaces) -> list[dict]:
    """Translate the apply-request interface list into YANG-style switchport dicts."""
    out: list[dict] = []
    for itf in interfaces:
        entry: dict = {"interface-name": itf.interface_name}
        if itf.mode:
            entry["mode"] = itf.mode
        if itf.untagged_vlan is not None:
            entry["untagged-vlan"] = itf.untagged_vlan
        if itf.tagged_vlans:
            entry["tagged-vlan"] = list(itf.tagged_vlans)
        out.append(entry)
    return out


async def apply_switchport_config(device: "Device", payload, nso_client: "NsoClient") -> dict:
    """Full-replace the switchport-reconciler service for *device* with the request interfaces."""
    if not device.nso_device_name:
        return {
            "status": "error",
            "error": "no_nso_device_name",
            "message": f"Device {device.id} has no nso_device_name",
        }

    interfaces = _build_interface_payload(payload.interfaces)

    try:
        await _nso_apply_switchport_config(nso_client, device.nso_device_name, interfaces)
    except NsoApplyError as exc:
        logger.warning(
            "switchport_intent.apply.failed",
            device_id=device.id,
            nso_device_name=device.nso_device_name,
            error=exc.code,
        )
        return {"status": "error", "error": exc.code, "message": str(exc), "detail": exc.detail}

    logger.info(
        "switchport_intent.apply.deployed",
        device_id=device.id,
        nso_device_name=device.nso_device_name,
        interface_count=len(interfaces),
    )
    return {"status": "deployed", "device": device.nso_device_name, "interface_count": len(interfaces)}
