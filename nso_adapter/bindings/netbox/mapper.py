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


def _split_dotted_unit(interface_name: str) -> tuple[str, str] | None:
    """Return (base, unit) for a dotted logical-unit name, else None.

    Junos/Cisco subinterfaces are ``<base>.<unit>`` — e.g. ``ae98.100``,
    ``GigabitEthernet0/0.10``. Returns None for non-dotted names and for names
    whose dot is not a clean unit separator (multiple dots, empty side). Nokia
    port-ids use ``/`` and LAG members use ``:`` — neither has a ``.`` so they
    are safe and resolve as plain interfaces.
    """
    if interface_name.count(".") != 1:
        return None
    base, unit = interface_name.split(".", 1)
    if not base or not unit:
        return None
    return base, unit


async def bulk_ensure_interfaces(
    client: NetboxClient,
    netbox_device_id: int,
    names: list[str],
) -> dict[str, int]:
    """Ensure every name in *names* exists in NetBox, return a name→id map.

    Bulk, two-pass, and idempotent — the performant replacement for calling
    resolve_or_create_interface per interface (plan Layer A):

    1. One bulk GET of existing interfaces (name→object map).
    2. Bulk-create missing BASE interfaces (a base is either a plain name or the
       left side of a ``<base>.<unit>``). Merge their ids into the map.
    3. Bulk-create missing UNIT interfaces as ``type=virtual`` with ``parent``
       resolved by BASE NAME from the map (never positionally).
    4. Bulk-reparent pre-existing flat units that lack a parent.

    Returns {name: netbox_interface_id} for every requested name that now
    resolves (rows NetBox rejected outright are omitted and logged).
    """
    wanted = list(dict.fromkeys(names))  # de-dup, preserve order
    existing = {i["name"]: i for i in await client.list_interfaces(netbox_device_id)}
    name_to_id: dict[str, int] = {n: o["id"] for n, o in existing.items()}

    # Partition wanted names into bases and units, and collect all base names.
    units: list[tuple[str, str]] = []  # (full_name, base_name)
    all_base_names: set[str] = set()
    for name in wanted:
        split = _split_dotted_unit(name)
        if split is None:
            all_base_names.add(name)
        else:
            base_name, _unit = split
            all_base_names.add(base_name)
            units.append((name, base_name))

    # ── Pass 1: create missing bases ──
    missing_bases = [b for b in all_base_names if b not in name_to_id]
    if missing_bases:
        created = await client.bulk_create_interfaces(
            [{"device": netbox_device_id, "name": b, "type": _guess_netbox_type(b)} for b in missing_bases]
        )
        for obj in created:
            name_to_id[obj["name"]] = obj["id"]

    # ── Pass 2: create missing units (parent resolved by base NAME) ──
    missing_units = [(full, base) for full, base in units if full not in name_to_id]
    unit_payloads = []
    for full, base in missing_units:
        payload = {"device": netbox_device_id, "name": full, "type": "virtual"}
        parent_id = name_to_id.get(base)
        if parent_id is not None:
            payload["parent"] = parent_id
        else:
            logger.warning("netbox.bulk_ensure.base_unresolved", unit=full, base=base)
        unit_payloads.append(payload)
    if unit_payloads:
        created = await client.bulk_create_interfaces(unit_payloads)
        for obj in created:
            name_to_id[obj["name"]] = obj["id"]

    # ── Pass 3: reparent pre-existing flat units lacking a parent ──
    reparent = []
    for full, base in units:
        obj = existing.get(full)
        parent_id = name_to_id.get(base)
        if obj is not None and parent_id is not None and not obj.get("parent"):
            reparent.append({"id": obj["id"], "parent": parent_id})
    if reparent:
        await client.bulk_patch_interfaces(reparent)

    return name_to_id


async def _resolve_or_create_simple(
    client: NetboxClient,
    netbox_device_id: int,
    name: str,
    *,
    nb_type: str | None = None,
    parent_id: int | None = None,
) -> int | None:
    """Resolve a NetBox interface by name, creating it if missing.

    If it already exists but lacks a ``parent`` we expect, patch the parent in
    (re-parents flat units created before subinterface modeling — plan decision 2).
    """
    nb_iface = await client.get_interface(netbox_device_id, name)
    if nb_iface:
        nb_id = nb_iface["id"]
        if parent_id is not None and not nb_iface.get("parent"):
            try:
                await client.patch_interface(nb_id, {"parent": parent_id})
                logger.info("netbox.interface.reparented", name=name, netbox_id=nb_id, parent=parent_id)
            except Exception as exc:
                logger.warning("netbox.interface.reparent_failed", name=name, error=str(exc))
        return nb_id

    payload: dict = {
        "device": netbox_device_id,
        "name": name,
        "type": nb_type or _guess_netbox_type(name),
    }
    if parent_id is not None:
        payload["parent"] = parent_id
    try:
        created = await client.create_interface(payload)
        nb_id = created["id"]
        logger.info("netbox.interface.created", name=name, netbox_id=nb_id, type=payload["type"], parent=parent_id)
        return nb_id
    except Exception as exc:
        logger.warning("netbox.interface.create_failed", name=name, error=str(exc))
        return None


async def resolve_or_create_interface(
    client: NetboxClient,
    netbox_device_id: int,
    iface: DomainInterface,
) -> int | None:
    """Return the NetBox interface ID for *iface*, creating it if missing.

    Logical units (``<base>.<unit>``, e.g. Junos ``ae98.100``) are modelled as
    NetBox subinterfaces: the base is ensured first, then the unit is
    created/looked-up as ``type='virtual'`` with ``parent`` set to the base.
    Pre-existing flat units get their ``parent`` patched in. Returns the
    interface id, or None if creation fails. (Decision I + subinterface plan.)
    """
    split = _split_dotted_unit(iface.name)
    if split is None:
        return await _resolve_or_create_simple(client, netbox_device_id, iface.name)

    base_name, _unit = split
    base_id = await _resolve_or_create_simple(client, netbox_device_id, base_name)
    if base_id is None:
        logger.warning("netbox.interface.base_unresolved", unit=iface.name, base=base_name)
        # Don't drop the unit — create it parentless rather than lose it.
        return await _resolve_or_create_simple(client, netbox_device_id, iface.name, nb_type="virtual")
    return await _resolve_or_create_simple(client, netbox_device_id, iface.name, nb_type="virtual", parent_id=base_id)
