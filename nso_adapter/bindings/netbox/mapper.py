# SPDX-License-Identifier: Apache-2.0
"""NetBox binding — domain ↔ NetBox objects + interface identity mapping.

Handles name resolution (NSO interface name → NetBox interface) and auto-creates
missing interfaces when decision I from docs/00-plan.md applies.
"""
from __future__ import annotations

import structlog

from nso_adapter.bindings.netbox.client import NetboxClient
from nso_adapter.domain.models import Interface as DomainInterface

logger = structlog.get_logger(__name__)

# Mapping of common NSO/NED interface type prefixes to NetBox dcim.Interface type slugs.
# This is a best-effort heuristic used during auto-creation (decision I).
_IFACE_TYPE_MAP: dict[str, str] = {
    "GigabitEthernet": "1000base-t",
    "TenGigE": "10gbase-x-sfpp",
    "TenGigabitEthernet": "10gbase-x-sfpp",
    "HundredGigE": "100gbase-x-cfp",
    "FortyGigabitEthernet": "40gbase-x-qsfpp",
    "Loopback": "virtual",
    "loopback": "virtual",
    "Bundle-Ether": "lag",
    "Port-channel": "lag",
    "Vlan": "virtual",
    "Tunnel": "virtual",
    "Management": "other",
    "MgmtEth": "other",
    "Serial": "other",
}


def _guess_netbox_type(interface_name: str) -> str:
    """Best-effort: map an interface name prefix to a NetBox interface type slug."""
    for prefix, nb_type in _IFACE_TYPE_MAP.items():
        if interface_name.startswith(prefix):
            return nb_type
    return "other"


async def resolve_or_create_interface(
    client: NetboxClient,
    netbox_device_id: int,
    iface: DomainInterface,
) -> int | None:
    """Return the NetBox interface ID for *iface*, creating it if missing.

    Returns the NetBox interface id, or None if auto-creation fails.
    """
    nb_iface = await client.get_interface(netbox_device_id, iface.name)
    if nb_iface:
        return nb_iface["id"]

    # Auto-create (decision I)
    nb_type = _guess_netbox_type(iface.name)
    payload = {
        "device": netbox_device_id,
        "name": iface.name,
        "type": nb_type,
    }
    try:
        created = await client.create_interface(payload)
        nb_id = created["id"]
        logger.info("netbox.interface.created", name=iface.name, netbox_id=nb_id, type=nb_type)
        return nb_id
    except Exception as exc:
        logger.warning("netbox.interface.create_failed", name=iface.name, error=str(exc))
        return None
