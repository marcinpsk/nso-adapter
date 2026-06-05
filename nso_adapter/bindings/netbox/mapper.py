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
    "Management-lo": "virtual",  # Nokia mgmt loopback — must out-rank "Management"
    "Management": "other",
    "MgmtEth": "other",
    "Serial": "other",
    # Nokia SR OS (timos): LAGs and logical loopback/system interfaces.
    "lag-": "lag",
    "system": "virtual",
    "lo": "virtual",
}


def _guess_netbox_type(interface_name: str) -> str:
    """Best-effort: map an interface name prefix to a NetBox interface type slug.

    Longest matching prefix wins, so more-specific entries (``Management-lo``)
    out-rank their shorter relatives (``Management``) regardless of dict order.
    """
    for prefix in sorted(_IFACE_TYPE_MAP, key=len, reverse=True):
        if interface_name.startswith(prefix):
            return _IFACE_TYPE_MAP[prefix]
    return "other"


def _split_unit(interface_name: str) -> tuple[str, str] | None:
    """Return (base, unit) for a logical-unit name, else None.

    Handles two separator styles, exactly one of which may be present:
    - ``.`` — Junos/Cisco subinterfaces (``ae98.100``, ``GigabitEthernet0/0.10``).
    - ``:`` — Nokia SR OS LAG channels and SAPs (``lag-99:10``, ``1/1/c22/1:4090``).

    Returns None unless there is exactly one separator with non-empty sides:
    plain names (``1/1/c8/1``, ``system``), names with multiple/mixed separators,
    and names with an empty side all resolve as plain interfaces (logged upstream).
    Nokia port-ids use ``/``, which is never treated as a separator.
    """
    n_dot = interface_name.count(".")
    n_colon = interface_name.count(":")
    if n_dot == 1 and n_colon == 0:
        sep = "."
    elif n_colon == 1 and n_dot == 0:
        sep = ":"
    else:
        return None
    base, unit = interface_name.split(sep, 1)
    if not base or not unit:
        return None
    return base, unit


def _base_type_for(kind: str | None, name: str) -> str:
    """NetBox type for a BASE (parent-less) interface, from its M27R kind or name.

    Nokia loopback/system + unbound logical interfaces are virtual; LAGs are lag;
    physical ports and other vendors fall back to the name-prefix guess.
    """
    if kind in ("loopback", "logical"):
        return "virtual"
    if kind == "lag":
        return "lag"
    return _guess_netbox_type(name)


async def bulk_ensure_interfaces(
    client: NetboxClient,
    netbox_device_id: int,
    interfaces: list[str | dict],
) -> dict[str, int]:
    """Ensure every requested interface exists in NetBox, return a name→id map.

    Accepts either plain names (back-compat) or dicts carrying the M27R
    ``parent_binding``/``kind`` (Nokia logical interfaces). Bulk, idempotent:

    1. Bulk GET existing interfaces.
    2. Resolve each interface's PARENT — explicit ``parent_binding`` (Nokia
       logical, e.g. ``LAG99:10`` → ``lag-99``) wins; else the ``<base>.<unit>`` /
       ``<base>:<unit>`` split (Junos/IOS units). The interface keeps its faithful
       (logical) NAME — we no longer rename Nokia logical interfaces to their bound
       port, so IS-IS/OSPF/IP correlation matches them by name directly.
    3. Bulk-create missing bases (incl. implicit LAG parents like ``lag-99``).
    4. Bulk-create missing children as ``type=virtual`` with their resolved parent.
    5. Bulk-reparent pre-existing flat children lacking a parent.

    Returns {name: netbox_interface_id} for every requested name that now resolves.
    """
    # Normalize input → list of {name, parent_binding, kind}, de-duped by name.
    norm: list[dict] = []
    seen: set[str] = set()
    for it in interfaces:
        if isinstance(it, str):
            name, pb, kind = it, None, None
        else:
            name, pb, kind = it["name"], (it.get("parent_binding") or None), it.get("kind")
        if not name or name in seen:
            continue
        seen.add(name)
        norm.append({"name": name, "parent_binding": pb, "kind": kind})

    existing = {i["name"]: i for i in await client.list_interfaces(netbox_device_id)}
    name_to_id: dict[str, int] = {n: o["id"] for n, o in existing.items()}
    kind_by_name = {it["name"]: it["kind"] for it in norm}

    # Resolve parent per interface: explicit parent_binding > name-split base > none.
    children: list[tuple[str, str]] = []  # (name, parent_name)
    base_names: set[str] = set()
    for it in norm:
        name, pb = it["name"], it["parent_binding"]
        if pb:
            children.append((name, pb))
            base_names.add(pb)
            continue
        split = _split_unit(name)
        if split is not None:
            base, _unit = split
            children.append((name, base))
            base_names.add(base)
        else:
            base_names.add(name)

    # ── Pass 1: create missing bases (kind-typed; implicit LAG parents → lag) ──
    missing_bases = [b for b in base_names if b not in name_to_id]
    if missing_bases:
        created = await client.bulk_create_interfaces(
            [{"device": netbox_device_id, "name": b, "type": _base_type_for(kind_by_name.get(b), b)} for b in missing_bases]
        )
        for obj in created:
            name_to_id[obj["name"]] = obj["id"]

    # ── Pass 2: create missing children (virtual) with resolved parent ──
    child_payloads = []
    for name, parent in children:
        if name in name_to_id:
            continue
        payload = {"device": netbox_device_id, "name": name, "type": "virtual"}
        parent_id = name_to_id.get(parent)
        if parent_id is not None:
            payload["parent"] = parent_id
        else:
            logger.warning("netbox.bulk_ensure.parent_unresolved", child=name, parent=parent)
        child_payloads.append(payload)
    if child_payloads:
        created = await client.bulk_create_interfaces(child_payloads)
        for obj in created:
            name_to_id[obj["name"]] = obj["id"]

    # ── Pass 3: fix pre-existing children whose parent is missing OR wrong ──
    # Wrong-parent correction matters during the M27R transition: an interface a
    # prior (name-split) sync parented to a guessed base (e.g. LAG99:10 → LAG99)
    # is re-pointed to the real bound port/LAG (lag-99) from parent_binding.
    reparent = []
    for name, parent in children:
        obj = existing.get(name)
        parent_id = name_to_id.get(parent)
        if obj is None or parent_id is None:
            continue
        cur = obj.get("parent")
        cur_id = cur.get("id") if isinstance(cur, dict) else cur
        cur_type = obj.get("type")
        cur_type_val = cur_type.get("value") if isinstance(cur_type, dict) else cur_type
        patch: dict = {}
        if cur_id != parent_id:
            patch["parent"] = parent_id
        # A child is a sub-interface → must be virtual; NetBox forbids a parent on a
        # non-virtual interface, so retype any child a prior sync created as a guessed
        # physical type (e.g. IXIA_CRPD created type=other by the old name-split path).
        if cur_type_val is not None and cur_type_val != "virtual":
            patch["type"] = "virtual"
        if patch:
            patch["id"] = obj["id"]
            reparent.append(patch)

    # Retype pre-existing parent-less logical/loopback bases (e.g. an unbound
    # loopback ``IXIA`` a prior name-split sync created as ``other``) to virtual.
    for b in base_names:
        if kind_by_name.get(b) not in ("loopback", "logical"):
            continue
        obj = existing.get(b)
        if obj is None:
            continue
        cur_type = obj.get("type")
        cur_type_val = cur_type.get("value") if isinstance(cur_type, dict) else cur_type
        if cur_type_val is not None and cur_type_val != "virtual":
            reparent.append({"id": obj["id"], "type": "virtual"})

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
    split = _split_unit(iface.name)
    if split is None:
        return await _resolve_or_create_simple(client, netbox_device_id, iface.name)

    base_name, _unit = split
    base_id = await _resolve_or_create_simple(client, netbox_device_id, base_name)
    if base_id is None:
        logger.warning("netbox.interface.base_unresolved", unit=iface.name, base=base_name)
        # Don't drop the unit — create it parentless rather than lose it.
        return await _resolve_or_create_simple(client, netbox_device_id, iface.name, nb_type="virtual")
    return await _resolve_or_create_simple(client, netbox_device_id, iface.name, nb_type="virtual", parent_id=base_id)
