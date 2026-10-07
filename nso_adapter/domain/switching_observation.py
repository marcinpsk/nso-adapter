# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Device projections shared by switching observations and read mirrors."""

from __future__ import annotations

import json
from typing import TypeVar

from pydantic import BaseModel, ConfigDict, Field

from nso_adapter.domain.read_projection import DeviceEntry, UnprojectableEntry, project_entries
from nso_adapter.nso.shape import VLAN_ID_MAX, VLAN_ID_MIN, as_list, wire_int

_INVALID_TAGGED_VLAN_RANGE = "tagged-vlans contains an invalid VLAN range"


def parse_vlan_string(raw: object) -> list[int]:
    """Expand the wire VLAN range string into a sorted VLAN ID set."""
    if raw is None or raw == "":
        return []
    if not isinstance(raw, str):
        raise ValueError(f"tagged-vlans must be a string (type {type(raw).__name__})")
    vlans: set[int] = set()
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            raise ValueError(_INVALID_TAGGED_VLAN_RANGE)
        unusable = None
        try:
            if "-" in chunk:
                start, end = (wire_int(value) for value in chunk.split("-", 1))
            else:
                start = end = wire_int(chunk)
        except ValueError:
            unusable = ValueError(_INVALID_TAGGED_VLAN_RANGE)
        if unusable is not None:
            raise unusable
        if not (VLAN_ID_MIN <= start <= end <= VLAN_ID_MAX):
            raise ValueError(_INVALID_TAGGED_VLAN_RANGE)
        vlans.update(range(start, end + 1))
    return sorted(vlans)


class LagMemberEntry(DeviceEntry):
    identity = ("interface_name",)

    interface_name: str
    mode: str | None = None


class LagConfigMemberEntry(LagMemberEntry):
    integers = ("port_priority",)

    port_priority: int | None = None


class LagTopologyEntry(DeviceEntry):
    identity = ("name",)
    integers = ("lag_id",)
    children = {"member": LagMemberEntry}

    name: str = Field(min_length=1)
    lag_id: int
    member: list[LagMemberEntry] | None = None


class LagConfigEntry(DeviceEntry):
    identity = ("lag_id",)
    integers = ("lag_id", "min_links", "system_priority", "admin_key")
    children = {"member": LagConfigMemberEntry}

    name: str = Field(min_length=1)
    lag_id: int
    min_links: int | None = None
    system_priority: int | None = None
    system_id: str | None = None
    timer: str | None = None
    admin_key: int | None = None
    vpc_sensitive: bool | None = None
    member: list[LagConfigMemberEntry] | None = None


class LagConfigDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    present: list[str]
    bundles: list[LagConfigEntry] | None
    unprojectable: list[UnprojectableEntry]


class LagTopologyDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    present: list[str]
    bundles: list[LagTopologyEntry] | None
    unprojectable: list[UnprojectableEntry]


class InterfaceMtuEntry(DeviceEntry):
    identity = ("interface_name",)

    interface_name: str
    mtu: int | None = None
    ip_mtu: int | None = None
    mpls_mtu: int | None = None
    bound_port: str | None = None


class InterfaceMtuDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    present: list[str]
    interfaces: list[InterfaceMtuEntry] | None
    unprojectable: list[UnprojectableEntry]


class SviEntry(DeviceEntry):
    identity = ("interface_name",)
    vlans = ("vlan_id",)

    interface_name: str
    vlan_id: int
    type: str | None = None
    vrf: str | None = None


class SviDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    present: list[str]
    interfaces: list[SviEntry] | None
    unprojectable: list[UnprojectableEntry]


class SubinterfaceEntry(DeviceEntry):
    identity = ("interface_name",)
    vlans = ("dot1q_vlan",)

    interface_name: str
    parent_interface: str | None = None
    dot1q_vlan: int
    type: str | None = None
    vrf: str | None = None


class SubinterfaceDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    present: list[str]
    interfaces: list[SubinterfaceEntry] | None
    unprojectable: list[UnprojectableEntry]


class SwitchportEntry(DeviceEntry):
    identity = ("interface_name",)
    vlans = ("untagged_vlan",)
    converters = {"tagged_vlans": parse_vlan_string}
    wire_aliases = {
        "interface_name": ("interface-name", "interface_name"),
        "untagged_vlan": ("untagged-vlan", "untagged_vlan"),
        "tagged_vlans": ("tagged-vlans", "tagged_vlans"),
    }

    interface_name: str
    mode: str | None = None
    untagged_vlan: int | None = None
    tagged_vlans: list[int] | None = None


class SwitchportDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    present: list[str]
    interfaces: list[SwitchportEntry] | None
    unprojectable: list[UnprojectableEntry]


def _collection_value(
    data: dict | None, keys: tuple[str, ...], field: str
) -> tuple[object, list[str], list[UnprojectableEntry]]:
    data = data or {}
    keys_present = [key for key in keys if key in data]
    if not keys_present:
        return [], [], []
    value = data[keys_present[0]]
    if any(json.dumps(data[key], sort_keys=True) != json.dumps(value, sort_keys=True) for key in keys_present[1:]):
        return [], [field], [UnprojectableEntry(index=0, reason=f"{keys[0]}: conflicting collection aliases")]
    return value, [field], []


EntryT = TypeVar("EntryT", bound=DeviceEntry)


def _project_collection(  # noqa: UP047
    data: dict | None, keys: tuple[str, ...], field: str, model: type[EntryT]
) -> tuple[list[EntryT] | None, list[UnprojectableEntry], list[str]]:
    raw, present, invalid = _collection_value(data, keys, field)
    if invalid or raw is None:
        return None if raw is None else [], invalid, present
    entries, invalid = project_entries(raw, model, keys[0])
    return entries, invalid, present


def project_lag_config(data: dict | None) -> LagConfigDocument:
    entries, invalid, present = _project_collection(data, ("lag",), "bundles", LagConfigEntry)
    return LagConfigDocument(present=present, bundles=entries, unprojectable=invalid)


def project_lag_topology(data: dict | None) -> LagTopologyDocument:
    entries, invalid, present = _project_collection(data, ("lag",), "bundles", LagTopologyEntry)
    return LagTopologyDocument(present=present, bundles=entries, unprojectable=invalid)


def project_interface_mtu(data: dict | None) -> InterfaceMtuDocument:
    raw_rows, present, invalid_aliases = _collection_value(data, ("interface",), "interfaces")
    if raw_rows is None:
        return InterfaceMtuDocument(present=present, interfaces=None, unprojectable=invalid_aliases)
    rows = []
    invalid_fields = []
    for index, raw in enumerate(as_list(raw_rows)):
        if not isinstance(raw, dict):
            rows.append(raw)
            continue
        row = raw.copy()
        for field in ("mtu", "ip-mtu", "mpls-mtu"):
            if field not in row or row[field] is None:
                continue
            try:
                row[field] = wire_int(row[field])
            except (TypeError, ValueError):
                row[field] = None
                invalid_fields.append(
                    UnprojectableEntry(index=index, reason=f"interface[{index}].{field}: invalid integer")
                )
        rows.append(row)
    entries, invalid = project_entries(rows, InterfaceMtuEntry, "interface")
    invalid.extend(invalid_fields)
    invalid.sort(key=lambda item: (item.index, item.reason))
    return InterfaceMtuDocument(present=present, interfaces=entries, unprojectable=invalid)


def project_svis(data: dict | None) -> SviDocument:
    entries, invalid, present = _project_collection(data, ("interface",), "interfaces", SviEntry)
    return SviDocument(present=present, interfaces=entries, unprojectable=invalid)


def project_subinterfaces(data: dict | None) -> SubinterfaceDocument:
    entries, invalid, present = _project_collection(data, ("interface",), "interfaces", SubinterfaceEntry)
    return SubinterfaceDocument(present=present, interfaces=entries, unprojectable=invalid)


def project_switchports(data: dict | None) -> SwitchportDocument:
    entries, invalid, present = _project_collection(data, ("interface", "interfaces"), "interfaces", SwitchportEntry)
    return SwitchportDocument(present=present, interfaces=entries, unprojectable=invalid)
