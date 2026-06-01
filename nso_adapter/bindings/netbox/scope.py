# SPDX-License-Identifier: Apache-2.0
"""NetBox binding — read managed scope from the netbox-nso-plugin model.

This is the self-healing / reconcile path (docs/nso-adapter.md §9).
The fast path is PUT /devices/{id}/scope called directly by the plugin.

The reconcile call targets:
  GET /api/plugins/nso/device-management/
Returns a list of records each containing netbox_device_id + managed attributes.

On a NetBox outage (non-200) this function raises so the scheduler can abort
wholesale — never interpret an outage as "everything deleted".
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog

from nso_adapter.bindings.netbox.client import NetboxClient

logger = structlog.get_logger(__name__)

_PLUGIN_SCOPE_PATH = "/api/plugins/nso/device-management/"


@dataclass
class PluginScopeRecord:
    netbox_device_id: int
    attributes: list[str]


async def fetch_all_scope(client: NetboxClient) -> list[PluginScopeRecord]:
    """Fetch the full scope list from the NetBox plugin.

    Raises on any error — callers must not interpret errors as empty scope.
    """
    url = f"{client._base}{_PLUGIN_SCOPE_PATH}"
    async with client._client() as c:
        resp = await c.get(url)
        resp.raise_for_status()
        data = resp.json()

    records: list[PluginScopeRecord] = []
    results = data.get("results", data) if isinstance(data, dict) else data
    for item in results:
        if not isinstance(item, dict):
            logger.warning("netbox.scope.unexpected_item", item=repr(item))
            continue
        device_field = item.get("device")
        if isinstance(device_field, dict):
            nb_id = device_field.get("id") or item.get("netbox_device_id")
        elif isinstance(device_field, (int, str)):
            # NetBox returns the FK as a bare integer when no full serializer depth
            nb_id = int(device_field)
        else:
            nb_id = item.get("netbox_device_id")
        attrs = item.get("managed_attributes") or item.get("attributes", [])
        if nb_id is not None:
            records.append(PluginScopeRecord(netbox_device_id=int(nb_id), attributes=list(attrs)))

    logger.debug("netbox.scope.fetched", count=len(records))
    return records
