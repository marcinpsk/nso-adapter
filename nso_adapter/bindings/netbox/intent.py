# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""NetBox binding — read accepted intent from the netbox-nso-plugin model.

This is the self-healing / reconcile path for intent (docs/nso-adapter.md §9,
decision L).  The fast path is PUT /devices/{id}/intent called directly by the
plugin on every Accept action.

The reconcile call targets:
  GET /api/plugins/nso/interface-state/
Returns a list of NSOInterfaceState records each containing
  interface (dict with device, name), attribute, status, accepted_at.

On a NetBox outage (non-200) this function raises so the scheduler can abort
wholesale — never interpret an outage as "no intent".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import structlog

from nso_adapter.bindings.netbox.client import NetboxClient

logger = structlog.get_logger(__name__)

_PLUGIN_INTENT_PATH = "/api/plugins/nso/interface-state/"


@dataclass
class PluginIntentRecord:
    netbox_device_id: int
    interface_name: str
    attribute: str
    intent_value: str | None
    accepted_at: datetime | None


async def fetch_all_intent(client: NetboxClient) -> list[PluginIntentRecord]:
    """Fetch all accepted interface-state records from the NetBox plugin.

    Only records with status='accepted' carry valid intent; others are skipped.
    Raises on any error — callers must not interpret errors as empty intent.
    """
    url = f"{client._base}{_PLUGIN_INTENT_PATH}"
    results: list[dict] = []

    async with client._client() as c:
        # Handle DRF pagination
        next_url: str | None = url
        while next_url:
            resp = await c.get(next_url)
            resp.raise_for_status()
            data = resp.json()
            page = data.get("results", data) if isinstance(data, dict) else data
            results.extend(page)
            next_url = data.get("next") if isinstance(data, dict) else None

    records: list[PluginIntentRecord] = []
    for item in results:
        if item.get("status") != "accepted":
            continue

        # interface is a nested dict: {id, url, display, device, name, ...}
        iface_data = item.get("interface", {})
        if not isinstance(iface_data, dict):
            continue

        device_data = iface_data.get("device", {})
        nb_device_id = device_data.get("id") if isinstance(device_data, dict) else None
        iface_name = iface_data.get("name")

        if nb_device_id is None or not iface_name:
            continue

        attribute = item.get("attribute")
        if not attribute:
            continue

        # intent_value lives on the corresponding dcim.Interface field (description/enabled).
        # The plugin serializer exposes the value separately (or we derive it from the
        # current interface state).  Use nso_value as a fallback if not explicitly included.
        intent_value = item.get("intent_value") or item.get("nso_value")

        accepted_at_raw = item.get("accepted_at")
        accepted_at: datetime | None = None
        if accepted_at_raw:
            try:
                accepted_at = datetime.fromisoformat(accepted_at_raw.replace("Z", "+00:00")).replace(tzinfo=None)
            except (ValueError, AttributeError):
                pass

        records.append(
            PluginIntentRecord(
                netbox_device_id=int(nb_device_id),
                interface_name=iface_name,
                attribute=attribute,
                intent_value=intent_value,
                accepted_at=accepted_at,
            )
        )

    logger.debug("netbox.intent.fetched", count=len(records))
    return records
