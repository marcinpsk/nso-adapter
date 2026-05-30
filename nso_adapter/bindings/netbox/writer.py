# SPDX-License-Identifier: Apache-2.0
"""NetBox binding — write description/enabled onto dcim.Interface.

Implements the NetBox write step of the sync flow (docs/nso-adapter.md §7 step 4).
"""
from __future__ import annotations

from dataclasses import dataclass

import structlog

from nso_adapter.bindings.netbox.client import NetboxClient
from nso_adapter.bindings.netbox.mapper import resolve_or_create_interface
from nso_adapter.domain.models import Interface as DomainInterface

logger = structlog.get_logger(__name__)


@dataclass
class WriteResult:
    interfaces_written: int = 0
    interfaces_created: int = 0
    interfaces_skipped: int = 0


async def write_interfaces(
    client: NetboxClient,
    netbox_device_id: int,
    interfaces: list[DomainInterface],
    scope_attrs: list[str],
) -> WriteResult:
    """Write *scope_attrs* values from NSO onto NetBox dcim.Interface objects.

    Returns a WriteResult with counts for the job result summary.
    """
    result = WriteResult()

    for iface in interfaces:
        nb_id = await resolve_or_create_interface(client, netbox_device_id, iface)
        if nb_id is None:
            result.interfaces_skipped += 1
            continue

        payload: dict = {}
        if "description" in scope_attrs and iface.nso.description is not None:
            payload["description"] = iface.nso.description
        if "enabled" in scope_attrs and iface.nso.enabled is not None:
            payload["enabled"] = iface.nso.enabled

        if not payload:
            continue

        try:
            nb_obj = await client.get_interface(netbox_device_id, iface.name)
            is_new = nb_obj is None
            await client.patch_interface(nb_id, payload)
            if is_new:
                result.interfaces_created += 1
            else:
                result.interfaces_written += 1
        except Exception as exc:
            logger.warning("netbox.write_failed", interface=iface.name, error=str(exc))
            result.interfaces_skipped += 1

    return result
