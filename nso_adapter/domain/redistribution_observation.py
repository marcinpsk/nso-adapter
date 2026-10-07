# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Redistribution projection and coverage of authoritative protocol components."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

from nso_adapter.domain.asn import parse_asn, redistribution_source_protocol
from nso_adapter.domain.read_projection import DeviceEntry, ObservationCoverage, UnprojectableEntry, project_entries


class RedistributionSourceEntry(DeviceEntry):
    identity = ("source_protocol", "source_ref")

    source_protocol: str
    source_ref: str = ""
    route_map: str | None = None
    metric: int | None = None
    metric_type: str | None = None

    @field_validator("source_protocol", mode="before")
    @classmethod
    def protocol_name(cls, value):
        return redistribution_source_protocol(value)

    @field_validator("source_ref", mode="before")
    @classmethod
    def source_reference(cls, value, info):
        if info.data.get("source_protocol") == "bgp" and value != "":
            return str(parse_asn(value))
        return value.strip() if isinstance(value, str) else value


class RedistributionEntry(RedistributionSourceEntry):
    dest_protocol: Literal["bgp", "isis", "ospf"]
    dest_ref: str
    dest_vrf: str | None = None


class RedistributionSourceCoverage(BaseModel):
    model_config = ConfigDict(strict=True)

    protocol: str
    reference: str


class RedistributionComponentCoverage(BaseModel):
    model_config = ConfigDict(strict=True)

    protocol: Literal["bgp", "isis", "ospf"]
    destinations: list[str]
    sources: list[RedistributionSourceCoverage]


class RedistributionCoverage(ObservationCoverage):
    components: list[RedistributionComponentCoverage]


class OspfDestination(DeviceEntry):
    identity = ("process_id", "vrf")
    children = {"redistribute": RedistributionSourceEntry}

    process_id: str
    vrf: str = ""
    redistribute: list[RedistributionSourceEntry] | None = None

    @field_validator("process_id", mode="before")
    @classmethod
    def process_identifier(cls, value):
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ValueError("invalid process-id")
        return str(value)


class IsisDestination(DeviceEntry):
    identity = ("process_tag",)
    children = {"redistribute": RedistributionSourceEntry}

    process_tag: str = ""
    redistribute: list[RedistributionSourceEntry] | None = None


class BgpAfDestination(DeviceEntry):
    identity = ("afi",)
    allow_empty_identity = (*DeviceEntry.allow_empty_identity, "afi")
    children = {"redistribute": RedistributionSourceEntry}

    afi: str = ""
    redistribute: list[RedistributionSourceEntry] | None = None


class BgpScopeDestination(DeviceEntry):
    identity = ("vrf",)
    children = {"address_family": BgpAfDestination}

    vrf: str = ""
    address_family: list[BgpAfDestination] | None = None


class BgpDestination(DeviceEntry):
    identity = ("asn",)
    children = {"scope": BgpScopeDestination}

    asn: str
    scope: list[BgpScopeDestination] | None = None

    @field_validator("asn", mode="before")
    @classmethod
    def valid_asn(cls, value):
        parse_asn(value)
        return str(value)


class OspfRedistributionComponent(BaseModel):
    model_config = ConfigDict(strict=True)

    protocol: Literal["ospf"] = "ospf"
    inventory: list[OspfDestination] | None
    present: list[str]


class IsisRedistributionComponent(BaseModel):
    model_config = ConfigDict(strict=True)

    protocol: Literal["isis"] = "isis"
    inventory: list[IsisDestination] | None
    present: list[str]


class BgpRedistributionComponent(BaseModel):
    model_config = ConfigDict(strict=True)

    protocol: Literal["bgp"] = "bgp"
    inventory: list[BgpDestination] | None
    present: list[str]


RedistributionComponent = OspfRedistributionComponent | IsisRedistributionComponent | BgpRedistributionComponent


class RedistributionDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    entries: list[RedistributionEntry]
    components: list[RedistributionComponent]
    unprojectable: list[UnprojectableEntry]


def project_redistribution_component(
    protocol: str, data: dict
) -> tuple[RedistributionComponent, list[UnprojectableEntry]]:
    present = []
    if protocol == "ospf":
        key = "instance"
        inventory, invalid = project_entries(data.get(key), OspfDestination, "ospf.instance")
        if key in data:
            present.append("inventory")
        return OspfRedistributionComponent(
            inventory=None if key in data and data[key] is None else inventory, present=present
        ), invalid
    if protocol == "isis":
        key = "process"
        inventory_isis, invalid = project_entries(data.get(key), IsisDestination, "isis.process")
        if key in data:
            present.append("inventory")
        return IsisRedistributionComponent(
            inventory=None if key in data and data[key] is None else inventory_isis, present=present
        ), invalid
    if protocol != "bgp":
        raise ValueError("unknown redistribution protocol")
    key = "router"
    inventory_bgp, invalid = project_entries(data.get(key), BgpDestination, "bgp.router")
    if key in data:
        present.append("inventory")
    return BgpRedistributionComponent(
        inventory=None if key in data and data[key] is None else inventory_bgp, present=present
    ), invalid


def _projected_destinations(
    component: RedistributionComponent,
) -> list[tuple[str, list[RedistributionSourceEntry], str, str | None]]:
    destinations: list[tuple[str, list[RedistributionSourceEntry], str, str | None]] = []
    if isinstance(component, OspfRedistributionComponent):
        return [
            (entry.process_id, entry.redistribute or [], f"instance[{entry._source_index}]", entry.vrf)
            for entry in component.inventory or []
        ]
    if isinstance(component, IsisRedistributionComponent):
        return [
            (entry.process_tag, entry.redistribute or [], f"process[{entry._source_index}]", None)
            for entry in component.inventory or []
        ]
    for router in component.inventory or []:
        for scope in router.scope or []:
            for family in scope.address_family or []:
                destinations.append(
                    (
                        f"{router.asn}/{scope.vrf}" + (f"/{family.afi}" if family.afi else ""),
                        family.redistribute or [],
                        f"router[{router._source_index}].scope[{scope._source_index}].address-family[{family._source_index}]",
                        None,
                    )
                )
    return destinations


def project_redistribution_sources(
    raw: object, path: str
) -> tuple[list[RedistributionSourceEntry], list[UnprojectableEntry]]:
    return project_entries(raw, RedistributionSourceEntry, path)


def project_redistribution(data: dict | None) -> tuple[RedistributionDocument, RedistributionCoverage]:
    data = data or {}
    entries: list[RedistributionEntry] = []
    invalid: list[UnprojectableEntry] = []
    coverage_components = []
    document_components = []
    seen = set()
    protocols: tuple[Literal["bgp", "isis", "ospf"], ...] = ("bgp", "isis", "ospf")
    for protocol in protocols:
        if protocol not in data:
            continue
        component, bad_destinations = project_redistribution_component(protocol, data[protocol])
        document_components.append(component)
        invalid.extend(bad_destinations)
        destinations = _projected_destinations(component)
        sources: set[tuple[str, str]] = set()
        for reference, projected, location, vrf in destinations:
            for source in projected:
                identity = (protocol, reference, vrf, source.source_protocol, source.source_ref)
                if identity in seen:
                    invalid.append(
                        UnprojectableEntry(
                            index=source._source_index,
                            reason=f"{protocol}.{location}: duplicate redistribution identity",
                        )
                    )
                    continue
                seen.add(identity)
                sources.add((source.source_protocol, source.source_ref))
                entries.append(
                    RedistributionEntry(**source.model_dump(), dest_protocol=protocol, dest_ref=reference, dest_vrf=vrf)
                )
        coverage_components.append(
            RedistributionComponentCoverage(
                protocol=protocol,
                destinations=sorted({ref for ref, _, _, _ in destinations}),
                sources=[RedistributionSourceCoverage(protocol=proto, reference=ref) for proto, ref in sorted(sources)],
            )
        )
    entries.sort(
        key=lambda entry: (
            entry.dest_protocol,
            entry.dest_ref,
            entry.dest_vrf or "",
            entry.source_protocol,
            entry.source_ref,
        )
    )
    return RedistributionDocument(
        entries=entries, components=document_components, unprojectable=invalid
    ), RedistributionCoverage(
        attributes=["dest_protocol", "dest_ref", "metric", "metric_type", "route_map", "source_protocol", "source_ref"],
        components=coverage_components,
    )
