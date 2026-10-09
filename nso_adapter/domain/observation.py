# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Canonical device projections shared by read mirrors and observations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, ValidationError

from nso_adapter.domain.read_projection import (
    ObservationCoverage,
    UnprojectableEntry,
    VlanDocument,
    observation_payload,
    project_vlans,
)
from nso_adapter.domain.service_observation import (
    BfdDocument,
    L2ServiceDocument,
    LoggingDocument,
    SnmpDocument,
    StaticRouteDocument,
    project_bfd,
    project_l2_services,
    project_logging,
    project_snmp,
    project_static_routes,
)
from nso_adapter.domain.switching_observation import (
    InterfaceMtuDocument,
    LagConfigDocument,
    LagTopologyDocument,
    SubinterfaceDocument,
    SviDocument,
    SwitchportDocument,
    project_interface_mtu,
    project_lag_config,
    project_lag_topology,
    project_subinterfaces,
    project_svis,
    project_switchports,
)
from nso_adapter.nso.shape import as_list


class InterfaceAttributesEntry(BaseModel):
    model_config = ConfigDict(strict=True)

    name: str
    description: str | None
    enabled: bool | None
    kind: str | None
    parent_binding: str | None
    encap_tag: str | None
    vrf: str | None
    service: str | None


class InterfaceAttributesDocument(BaseModel):
    interfaces: list[InterfaceAttributesEntry]
    unprojectable: list[UnprojectableEntry]


class InterfaceIpAddressEntry(BaseModel):
    model_config = ConfigDict(strict=True)

    address: str
    prefix_length: int | None
    family: str
    secondary: bool
    vrf: str | None


class InterfaceIpEntry(BaseModel):
    model_config = ConfigDict(strict=True)

    interface: str
    bound_port: str | None
    addresses: list[InterfaceIpAddressEntry]


class InterfaceIpDocument(BaseModel):
    interfaces: list[InterfaceIpEntry]
    unprojectable: list[UnprojectableEntry]


ObservationPayload = (
    InterfaceAttributesDocument
    | InterfaceIpDocument
    | VlanDocument
    | LagConfigDocument
    | LagTopologyDocument
    | SwitchportDocument
    | InterfaceMtuDocument
    | SviDocument
    | SubinterfaceDocument
    | BfdDocument
    | L2ServiceDocument
    | LoggingDocument
    | SnmpDocument
    | StaticRouteDocument
)


@dataclass(frozen=True)
class ObservationDocument:
    family: str
    document: ObservationPayload
    coverage: ObservationCoverage


def digest_document(document: object) -> str:
    """Hash compact canonical JSON without changing device values."""
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _invalid_entry(index: int, error: ValidationError) -> UnprojectableEntry:
    fields = sorted({str(item["loc"][0]) for item in error.errors(include_input=False)})
    return UnprojectableEntry(index=index, reason="invalid " + ", ".join(fields))


def project_interface_attributes(data: dict | None) -> InterfaceAttributesDocument:
    interfaces = []
    unprojectable = []
    for index, entry in enumerate(as_list((data or {}).get("interface"))):
        name = entry.get("interface-name") if isinstance(entry, dict) else None
        if not isinstance(name, str) or not name:
            unprojectable.append(UnprojectableEntry(index=index, reason="missing or invalid interface-name"))
            continue
        try:
            interfaces.append(
                InterfaceAttributesEntry(
                    name=name,
                    description=entry.get("description"),
                    enabled=entry.get("enabled"),
                    kind=entry.get("kind"),
                    parent_binding=entry.get("parent-binding"),
                    encap_tag=entry.get("encap-tag"),
                    vrf=entry.get("vrf"),
                    service=entry.get("service"),
                )
            )
        except ValidationError as exc:
            unprojectable.append(_invalid_entry(index, exc))
    return InterfaceAttributesDocument(interfaces=interfaces, unprojectable=unprojectable)


def extract_prefix_length(address: str) -> int | None:
    if "/" in address:
        try:
            return int(address.split("/", 1)[1])
        except ValueError:
            pass
    return None


def _project_addresses(entries: object, index: int) -> tuple[list[InterfaceIpAddressEntry], list[UnprojectableEntry]]:
    addresses = []
    unprojectable = []
    for position, entry in enumerate(as_list(entries)):
        address = entry.get("address") if isinstance(entry, dict) else None
        if not isinstance(address, str) or not address:
            unprojectable.append(
                UnprojectableEntry(index=index, reason=f"address[{position}]: missing or invalid address")
            )
            continue
        try:
            addresses.append(
                InterfaceIpAddressEntry(
                    address=address,
                    prefix_length=extract_prefix_length(address),
                    family="ipv4" if entry.get("family") in (None, "") else entry.get("family"),
                    secondary=False if entry.get("secondary") is None else entry.get("secondary"),
                    vrf="" if entry.get("vrf") is None else entry.get("vrf"),
                )
            )
        except ValidationError as exc:
            invalid = _invalid_entry(index, exc)
            unprojectable.append(UnprojectableEntry(index=index, reason=f"address[{position}]: {invalid.reason}"))
    return addresses, unprojectable


def project_interface_ips(interfaces_data: list) -> InterfaceIpDocument:
    interfaces = []
    unprojectable = []
    for index, entry in enumerate(interfaces_data):
        name = entry.get("interface-name") if isinstance(entry, dict) else None
        if not isinstance(name, str) or not name:
            unprojectable.append(UnprojectableEntry(index=index, reason="missing or invalid interface-name"))
            continue
        addresses, invalid_addresses = _project_addresses(entry.get("address"), index)
        unprojectable.extend(invalid_addresses)
        try:
            interfaces.append(
                InterfaceIpEntry(
                    interface=name,
                    bound_port=None if entry.get("bound-port") in (None, "") else entry.get("bound-port"),
                    addresses=addresses,
                )
            )
        except ValidationError as exc:
            unprojectable.append(_invalid_entry(index, exc))
    return InterfaceIpDocument(interfaces=interfaces, unprojectable=unprojectable)


def observe_interface_attributes(data: dict | None) -> ObservationDocument:
    document = project_interface_attributes(data)
    document.interfaces.sort(key=lambda item: item.name)
    return ObservationDocument(
        family="interface_attributes",
        document=document,
        coverage=ObservationCoverage(attributes=["description", "enabled"]),
    )


def observe_interface_ips(data: dict | None) -> ObservationDocument:
    document = project_interface_ips(as_list((data or {}).get("interface")))
    document.interfaces.sort(key=lambda item: item.interface)
    for interface in document.interfaces:
        interface.addresses.sort(key=lambda item: (item.family, item.address, item.vrf or ""))
    return ObservationDocument(
        family="interface_ip",
        document=document,
        coverage=ObservationCoverage(attributes=["address", "prefix_length", "secondary", "vrf"]),
    )


def _not_comparable(family: str, document: ObservationPayload) -> list[str]:
    return ["auth_secret", "priv_secret"] if family == "snmp" else []


def _observer(
    family: str, project: Callable[[dict | None], ObservationPayload], attributes: list[str]
) -> Callable[[dict | None], ObservationDocument]:
    def observe(data: dict | None) -> ObservationDocument:
        projected = project(data)
        document = observation_payload(projected)
        return ObservationDocument(
            family=family,
            document=document,
            coverage=ObservationCoverage(
                attributes=sorted(attributes), not_comparable=_not_comparable(family, document)
            ),
        )

    return observe


OBSERVERS: dict[str, Callable[[dict | None], ObservationDocument]] = {
    "interface_attributes": observe_interface_attributes,
    "interface_ip": observe_interface_ips,
    "vlan": _observer("vlan", project_vlans, ["name", "vlan_id"]),
    "lag_config": _observer(
        "lag_config",
        project_lag_config,
        [
            "name",
            "lag_id",
            "min_links",
            "system_priority",
            "system_id",
            "timer",
            "admin_key",
            "vpc_sensitive",
            "member",
            "member.interface_name",
            "member.mode",
            "member.port_priority",
        ],
    ),
    "lag": _observer("lag", project_lag_topology, ["name", "lag_id", "member", "member.interface_name", "member.mode"]),
    "switchport": _observer(
        "switchport", project_switchports, ["interface_name", "mode", "untagged_vlan", "tagged_vlans"]
    ),
    "interface_mtu": _observer(
        "interface_mtu", project_interface_mtu, ["interface_name", "mtu", "ip_mtu", "mpls_mtu", "bound_port"]
    ),
    "svi": _observer("svi", project_svis, ["interface_name", "vlan_id", "type", "vrf"]),
    "subinterface": _observer(
        "subinterface", project_subinterfaces, ["interface_name", "parent_interface", "dot1q_vlan", "type", "vrf"]
    ),
    "bfd": _observer(
        "bfd", project_bfd, ["bound_port", "enabled", "interface_name", "micro_bfd", "min_rx", "min_tx", "multiplier"]
    ),
    "l2_service": _observer(
        "l2_service",
        project_l2_services,
        ["service_name", "service_type", "service_id", "sap_id", "port", "outer_tag", "inner_tag"],
    ),
    "logging": _observer(
        "logging",
        project_logging,
        [
            "address",
            "port",
            "severity",
            "facility",
            "transport",
            "vrf",
            "source",
            "console_severity",
            "monitor_severity",
            "module_severity",
        ],
    ),
    "snmp": _observer(
        "snmp",
        project_snmp,
        [
            "name",
            "access",
            "acl",
            "has_secret",
            "username",
            "has_auth_secret",
            "has_priv_secret",
            "address",
            "version",
            "notify_type",
            "port",
            "user",
            "location",
            "contact",
        ],
    ),
    "static_route": _observer(
        "static_route",
        project_static_routes,
        ["vrf", "prefix", "next_hop", "interface_next_hop", "next_hop_vrf", "metric", "permanent", "tag", "name"],
    ),
}


def observe_family(family: str, data: dict | None, *, ned_id: str | None = None) -> ObservationDocument | None:
    observer = OBSERVERS.get(family)
    return observer(data) if observer is not None else None
