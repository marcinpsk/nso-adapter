# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Canonical device projections shared by read mirrors and observations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from nso_adapter.nso.shape import as_list


class UnprojectableEntry(BaseModel):
    index: int
    reason: str


class ObservationCoverage(BaseModel):
    attributes: list[str]


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


@dataclass(frozen=True)
class ObservationDocument:
    family: Literal["interface_attributes", "interface_ip"]
    document: InterfaceAttributesDocument | InterfaceIpDocument
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


OBSERVERS: dict[str, Callable[[dict | None], ObservationDocument]] = {
    "interface_attributes": observe_interface_attributes,
    "interface_ip": observe_interface_ips,
}


def observe_family(family: str, data: dict | None) -> ObservationDocument | None:
    observer = OBSERVERS.get(family)
    return observer(data) if observer is not None else None
