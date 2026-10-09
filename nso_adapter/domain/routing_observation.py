# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Routing projections shared by immutable observations and read mirrors."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.core.isis_canon import isis_level
from nso_adapter.domain.asn import parse_asn
from nso_adapter.domain.read_projection import DeviceEntry, UnprojectableEntry, project_entries


class CredentialEntry(DeviceEntry):
    @model_validator(mode="after")
    def credential_metadata(self):
        for name in self.credentials:
            if name not in self.model_fields_set:
                continue
            value = getattr(self, name)
            setattr(self, f"{name}_present", None if value is None else bool(value))
            self.present = sorted((set(self.present) - {name}) | {f"{name}_present"})
        return self


class BgpPolicyEntry(DeviceEntry):
    identity = ("afi",)
    wire_aliases = {"routemap_in": ("routemap-in", "policy-in"), "routemap_out": ("routemap-out", "policy-out")}

    afi: str
    enabled: bool | None = None
    routemap_in: str | None = None
    routemap_out: str | None = None
    prefixlist_in: str | None = None
    prefixlist_out: str | None = None


class BgpPeerEntry(CredentialEntry):
    credentials = ("password",)
    identity = ("peer_address", "peer_group")
    children = {"peer_address_family": BgpPolicyEntry}

    peer_address: str
    peer_group: str | None = None
    remote_as: str | None = None
    local_as: str | None = None
    enabled: bool | None = None
    ttl: int | None = None
    password: str | None = Field(default=None, exclude=True)
    password_present: bool | None = None
    source: str | None = None
    description: str | None = None
    bfd_enabled: bool | None = None
    peer_address_family: list[BgpPolicyEntry] | None = None

    @field_validator("remote_as", "local_as", mode="before")
    @classmethod
    def valid_asn(cls, value):
        if value is not None:
            parse_asn(value)
            return str(value)
        return value


class BgpPeerGroupEntry(DeviceEntry):
    identity = ("name",)
    children = {"peer_group_address_family": BgpPolicyEntry}

    name: str
    remote_as: str | None = None
    source: str | None = None
    peer_group_address_family: list[BgpPolicyEntry] | None = None

    @field_validator("remote_as", mode="before")
    @classmethod
    def valid_asn(cls, value):
        return BgpPeerEntry.valid_asn(value)


class BgpAddressFamilyEntry(DeviceEntry):
    identity = ("afi",)

    afi: str


class BgpScopeEntry(DeviceEntry):
    identity = ("vrf",)
    children = {"address_family": BgpAddressFamilyEntry, "peer": BgpPeerEntry, "peer_group": BgpPeerGroupEntry}

    vrf: str = ""
    address_family: list[BgpAddressFamilyEntry] | None = None
    peer: list[BgpPeerEntry] | None = None
    peer_group: list[BgpPeerGroupEntry] | None = None


class BgpRouterEntry(DeviceEntry):
    identity = ("asn",)
    children = {"scope": BgpScopeEntry}

    asn: str
    router_id: str | None = None
    scope: list[BgpScopeEntry] | None = None

    @field_validator("asn", mode="before")
    @classmethod
    def valid_asn(cls, value):
        parse_asn(value)
        return str(value)


class BgpDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    routers: list[BgpRouterEntry] | None
    present: list[str]
    unprojectable: list[UnprojectableEntry]


def project_bgp(data: dict | None) -> BgpDocument:
    data = data or {}
    entries, invalid = project_entries(data.get("router"), BgpRouterEntry, "router")
    return BgpDocument(
        routers=None if "router" in data and data["router"] is None else entries,
        present=["routers"] if "router" in data else [],
        unprojectable=invalid,
    )


class IsisSettingEntry(DeviceEntry):
    identity = ("key",)

    key: str
    value: str | None = None


class IsisLevelEntry(CredentialEntry):
    credentials = ("auth_key",)
    identity = ("level",)

    level: int
    default_metric: int | None = None
    wide_metrics_only: bool | None = None
    preference: int | None = None
    labeled_preference: int | None = None
    disabled: bool | None = None
    auth_type: str | None = None
    auth_present: bool | None = None
    auth_key: str | None = Field(default=None, exclude=True)
    auth_key_present: bool | None = None
    metric: int | None = None
    hello_interval: int | None = None
    hello_multiplier: int | None = None
    priority: int | None = None
    passive: bool | None = None


class IsisSegmentRoutingEntry(DeviceEntry):
    enabled: bool | None = None
    srv6_enabled: bool | None = None
    prefix_sid_range: str | None = None
    srgb_start: int | None = None
    srgb_range: int | None = None
    srlb_start: int | None = None
    srlb_range: int | None = None
    node_sid_index: int | None = None
    node_sid_label: int | None = None
    node_sid_v6_index: int | None = None
    node_sid_v6_label: int | None = None
    maximum_sid_depth: int | None = None
    tunnel_table_pref: int | None = None


class IsisFlexAlgoEntry(DeviceEntry):
    identity = ("algo_id",)

    algo_id: int
    metric_type: str | None = None
    priority: int | None = None
    admin_group_exclude: str | None = None
    admin_group_include_any: str | None = None
    admin_group_include_all: str | None = None


class IsisLocatorEntry(DeviceEntry):
    identity = ("name",)

    name: str
    prefix: str | None = None
    algorithm: int | None = None
    is_anycast: bool | None = None
    is_micro_segment: bool | None = None
    flavor: str | None = None
    block_length: int | None = None
    node_length: int | None = None
    function_length: int | None = None
    argument_length: int | None = None
    isis_level: int | None = None
    enabled: bool | None = None


class IsisPrefixSidEntry(DeviceEntry):
    identity = ("algorithm",)

    algorithm: int
    sid_index: int | None = None
    sid_label: int | None = None
    n_flag: bool | None = None
    no_php: bool | None = None
    explicit_null: bool | None = None
    readvertise: bool | None = None


class IsisProcessEntry(CredentialEntry):
    integers = ("reference_bandwidth",)
    credentials = ("area_auth_key", "domain_auth_key")
    identity = ("process_tag",)
    children = {
        "setting": IsisSettingEntry,
        "level": IsisLevelEntry,
        "flex_algo": IsisFlexAlgoEntry,
        "srv6_locator": IsisLocatorEntry,
    }
    containers = {"segment_routing": IsisSegmentRoutingEntry}

    process_tag: str = ""
    net: str | None = None
    is_type: str | None = None
    metric_style: str | None = None
    overload_bit: bool | None = None
    area_auth_type: str | None = None
    area_auth_present: bool | None = None
    area_auth_key: str | None = Field(default=None, exclude=True)
    area_auth_key_present: bool | None = None
    domain_auth_type: str | None = None
    domain_auth_present: bool | None = None
    domain_auth_key: str | None = Field(default=None, exclude=True)
    domain_auth_key_present: bool | None = None
    spf_initial_wait: int | None = None
    spf_max_wait: int | None = None
    lsp_initial_wait: int | None = None
    lsp_max_wait: int | None = None
    lsp_lifetime: int | None = None
    lsp_refresh_interval: int | None = None
    lsp_mtu: int | None = None
    overload_on_startup: bool | None = None
    overload_timeout: int | None = None
    te_enabled: bool | None = None
    suppress_attached_bit: bool | None = None
    ignore_attached_bit: bool | None = None
    fast_reroute: str | None = None
    microloop_avoidance: bool | None = None
    distance: int | None = None
    maximum_paths: int | None = None
    reference_bandwidth: int | None = None
    segment_routing_reported: bool | None = None
    segment_routing_configured: bool | None = None
    setting: list[IsisSettingEntry] | None = None
    level: list[IsisLevelEntry] | None = None
    segment_routing: IsisSegmentRoutingEntry | None = None
    flex_algo: list[IsisFlexAlgoEntry] | None = None
    srv6_locator: list[IsisLocatorEntry] | None = None

    @field_validator("is_type", mode="before")
    @classmethod
    def canonical_level(cls, value):
        return isis_level(value)


class IsisInterfaceEntry(CredentialEntry):
    credentials = ("hello_auth_key",)
    identity = ("interface_name", "af")
    children = {"setting": IsisSettingEntry, "level": IsisLevelEntry, "prefix_sid": IsisPrefixSidEntry}

    interface_name: str
    af: str
    process_tag: str = ""
    circuit_type: str | None = None
    network_type: str | None = None
    metric: int | None = None
    passive: bool | None = None
    bound_port: str | None = None
    hello_auth_type: str | None = None
    hello_auth_present: bool | None = None
    hello_auth_key: str | None = Field(default=None, exclude=True)
    hello_auth_key_present: bool | None = None
    bfd_enabled: bool | None = None
    frr_enabled: bool | None = None
    frr_protection: str | None = None
    csnp_interval: int | None = None
    retransmit_interval: int | None = None
    lsp_interval: int | None = None
    mesh_group: str | None = None
    setting: list[IsisSettingEntry] | None = None
    level: list[IsisLevelEntry] | None = None
    prefix_sid: list[IsisPrefixSidEntry] | None = None

    @field_validator("circuit_type", mode="before")
    @classmethod
    def canonical_level(cls, value):
        return isis_level(value)


class IsisDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    processes: list[IsisProcessEntry] | None
    interfaces: list[IsisInterfaceEntry] | None
    present: list[str]
    unprojectable: list[UnprojectableEntry]


def project_isis(data: dict | None) -> IsisDocument:
    data = data or {}
    processes, invalid = project_entries(data.get("process"), IsisProcessEntry, "process")
    interfaces, bad_interfaces = project_entries(data.get("interface"), IsisInterfaceEntry, "interface")
    return IsisDocument(
        processes=None if "process" in data and data["process"] is None else processes,
        interfaces=None if "interface" in data and data["interface"] is None else interfaces,
        present=sorted(name for wire, name in (("process", "processes"), ("interface", "interfaces")) if wire in data),
        unprojectable=invalid + bad_interfaces,
    )


class OspfAreaEntry(DeviceEntry):
    identity = ("area_id",)

    area_id: str
    area_type: str | None = None


class OspfInstanceEntry(DeviceEntry):
    identity = ("process_id", "vrf")
    children = {"area": OspfAreaEntry}

    process_id: str
    router_id: str | None = None
    vrf: str = ""
    enabled: bool | None = None
    area: list[OspfAreaEntry] | None = None


class OspfInterfaceEntry(CredentialEntry):
    credentials = ("auth_key",)
    identity = ("interface_name", "process_id")

    interface_name: str
    process_id: str | None = None
    area_id: str | None = None
    passive: bool | None = None
    priority: int | None = None
    cost: int | None = None
    network_type: str | None = None
    auth_type: str | None = None
    auth_present: bool | None = None
    auth_key: str | None = Field(default=None, exclude=True)
    auth_key_present: bool | None = None
    bfd_enabled: bool | None = None


class OspfDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    instances: list[OspfInstanceEntry] | None
    interfaces: list[OspfInterfaceEntry] | None
    present: list[str]
    unprojectable: list[UnprojectableEntry]


def project_ospf(data: dict | None) -> OspfDocument:
    data = data or {}
    instances, invalid = project_entries(data.get("instance"), OspfInstanceEntry, "instance")
    interfaces, bad_interfaces = project_entries(data.get("interface"), OspfInterfaceEntry, "interface")
    return OspfDocument(
        instances=None if "instance" in data and data["instance"] is None else instances,
        interfaces=None if "interface" in data and data["interface"] is None else interfaces,
        present=sorted(name for wire, name in (("instance", "instances"), ("interface", "interfaces")) if wire in data),
        unprojectable=invalid + bad_interfaces,
    )


class PolicyPrefixEntry(DeviceEntry):
    identity = ("sequence",)

    sequence: int
    action: str
    prefix: str
    ge: int | None = None
    le: int | None = None


class PolicyCommunityEntry(DeviceEntry):
    identity = ("sequence",)

    sequence: int
    action: str
    community: str


class PolicyAsPathEntry(DeviceEntry):
    identity = ("sequence",)

    sequence: int
    action: str
    pattern: str


class PolicyRouteMapEntry(DeviceEntry):
    identity = ("sequence",)

    sequence: int
    action: str
    match_prefix_lists: list[str] | None = None
    match_community_lists: list[str] | None = None
    match_as_paths: list[str] | None = None
    match_json: str | None = None
    set_json: str | None = None

    @field_validator("match_prefix_lists", "match_community_lists", "match_as_paths")
    @classmethod
    def sorted_names(cls, value):
        return sorted(value) if value is not None else None


class PolicyPrefixList(DeviceEntry):
    identity = ("name",)
    children = {"entry": PolicyPrefixEntry}

    name: str
    family: int | None = None
    entry: list[PolicyPrefixEntry] | None = None


class PolicyCommunityList(DeviceEntry):
    identity = ("name",)
    children = {"entry": PolicyCommunityEntry}

    name: str
    invert_match: bool | None = None
    entry: list[PolicyCommunityEntry] | None = None


class PolicyAsPath(DeviceEntry):
    identity = ("name",)
    children = {"entry": PolicyAsPathEntry}

    name: str
    entry: list[PolicyAsPathEntry] | None = None


class PolicyRouteMap(DeviceEntry):
    identity = ("name",)
    children = {"entry": PolicyRouteMapEntry}

    name: str
    entry: list[PolicyRouteMapEntry] | None = None


class RoutePolicyDocument(BaseModel):
    model_config = ConfigDict(strict=True)

    prefix_lists: list[PolicyPrefixList] | None
    community_lists: list[PolicyCommunityList] | None
    as_paths: list[PolicyAsPath] | None
    route_maps: list[PolicyRouteMap] | None
    present: list[str]
    unprojectable: list[UnprojectableEntry]


def project_route_policy(data: dict | None, *, ned_id: str | None = None) -> RoutePolicyDocument:
    data = data or {}
    prefix_lists, invalid_prefixes = project_entries(data.get("prefix-list"), PolicyPrefixList, "prefix-list")
    community_lists, invalid_communities = project_entries(
        data.get("community-list"), PolicyCommunityList, "community-list"
    )
    as_paths, invalid_paths = project_entries(data.get("as-path"), PolicyAsPath, "as-path")
    route_maps, invalid_maps = project_entries(data.get("route-map"), PolicyRouteMap, "route-map")
    dialect = community_dialect_for(ned_id)
    for community_list in community_lists:
        for entry in community_list.entry or []:
            entry.community = dialect.to_canonical(entry.community)
    return RoutePolicyDocument(
        prefix_lists=None if "prefix-list" in data and data["prefix-list"] is None else prefix_lists,
        community_lists=None if "community-list" in data and data["community-list"] is None else community_lists,
        as_paths=None if "as-path" in data and data["as-path"] is None else as_paths,
        route_maps=None if "route-map" in data and data["route-map"] is None else route_maps,
        present=sorted(
            name
            for wire, name in (
                ("prefix-list", "prefix_lists"),
                ("community-list", "community_lists"),
                ("as-path", "as_paths"),
                ("route-map", "route_maps"),
            )
            if wire in data
        ),
        unprojectable=invalid_prefixes + invalid_communities + invalid_paths + invalid_maps,
    )
