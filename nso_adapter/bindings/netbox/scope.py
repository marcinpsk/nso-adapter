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
    # Plugin-sourced management addresses (NetBox primary_ip / oob_ip, host only) — feed
    # the mgmt-IP failover loop. None when the device has no such IP in NetBox.
    primary_ip: str | None = None
    oob_ip: str | None = None


def _parse_scope_item(item: object) -> PluginScopeRecord | None:
    """Map one plugin device-management row to a PluginScopeRecord (None to skip)."""
    if not isinstance(item, dict):
        logger.warning("netbox.scope.unexpected_item", item=repr(item))
        return None
    device_field = item.get("device")
    if isinstance(device_field, dict):
        nb_id = device_field.get("id") or item.get("netbox_device_id")
    elif isinstance(device_field, (int, str)):
        # NetBox returns the FK as a bare integer when no full serializer depth
        nb_id = int(device_field)
    else:
        nb_id = item.get("netbox_device_id")
    if nb_id is None:
        return None
    attrs = item.get("managed_attributes") or item.get("attributes", [])
    return PluginScopeRecord(
        netbox_device_id=int(nb_id),
        attributes=list(attrs),
        primary_ip=item.get("primary_ip"),
        oob_ip=item.get("oob_ip"),
    )


async def fetch_all_scope(client: NetboxClient) -> list[PluginScopeRecord]:
    """Fetch the FULL scope list from the NetBox plugin, following pagination.

    Raises on any error — callers must not interpret errors as empty scope. Follows the
    DRF ``next`` links to the end: a single-page read UNDER-reports the managed fleet, and
    the scope reconcile deletes ("offboards") every adapter device absent from this list —
    so a truncated read silently offboards every device beyond page 1.
    """
    # Use the shared pooled client directly — do NOT `async with` it. It is a long-lived
    # singleton reused across all calls and already opened by other requests, so entering
    # it as a context manager raises "Cannot open a client instance more than once" (and
    # on exit would close the pool for everyone). Lifecycle is owned by NetboxClient.aclose().
    c = client._client()
    url: str | None = f"{client._base}{_PLUGIN_SCOPE_PATH}"
    records: list[PluginScopeRecord] = []
    while url:
        resp = await c.get(url)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            results = data.get("results", [])
            url = data.get("next")  # a page fetch that fails mid-sweep raises → caller aborts
        else:
            results = data  # a bare list (unpaginated) — done after one pass
            url = None
        for item in results:
            record = _parse_scope_item(item)
            if record is not None:
                records.append(record)

    logger.debug("netbox.scope.fetched", count=len(records))
    return records
