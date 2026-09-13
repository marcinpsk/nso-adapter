# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""One representative row set per document section, shared by the encoder pins (#1522 C9).

Real transient ORM instances, never stand-ins: the encoders read the same attributes the
legacy per-service body builders read, so a column renamed under a fake would still pass.
Every optional leaf that changes the emitted body is exercised at least once, in both its
set and its unset form, because the golden bodies are the behaviour-preservation evidence
for the extraction and an unexercised leaf proves nothing.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from nso_adapter.store.models import (
    BfdIntent,
    BgpAfIntent,
    BgpPeerAfIntent,
    BgpPeerIntent,
    BgpRouterIntent,
    BgpScopeIntent,
    DbInterface,
    InterfaceIntent,
    InterfaceIpIntent,
    InterfaceMtuIntent,
    IsisFlexAlgoIntent,
    IsisInterfaceIntent,
    IsisLevelIntent,
    IsisProcessIntent,
    L2SapIntent,
    LagBundleIntent,
    LagMemberIntent,
    LoggingHostIntent,
    LoggingLevelsIntent,
    OspfInstanceIntent,
    OspfInterfaceIntent,
    RedistributionIntent,
    RoutePolicyObjectIntent,
    SnmpCommunityIntent,
    SnmpHostIntent,
    SnmpSystemInfoIntent,
    SnmpV3UserIntent,
    StaticRouteIntent,
    SubinterfaceIntent,
    SviIntent,
    SwitchportIntent,
    SwitchportTaggedVlanIntent,
    VlanIntent,
)

ACCEPTED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

#: The NED whose dialect the golden route-policy body was captured under.
NOKIA_NED = "timos-nc-23.10"

DEVICE_NAME = "lab-device-01"


def _row(model, **fields):
    row = model(**fields)
    if hasattr(model, "accepted_at"):
        row.accepted_at = ACCEPTED
    return row


def interfaces() -> dict[int, DbInterface]:
    """The interface writer context the interface section's proof carries."""
    return {
        1: DbInterface(id=1, device_id=1, name="GigabitEthernet0/0", kind="physical"),
        2: DbInterface(
            id=2,
            device_id=1,
            name="1/1/1:100",
            kind="logical",
            parent_binding="1/1/1",
            encap_tag="100",
            vrf="VPRN-A",
            service="VPRN-A",
        ),
    }


def section_rows() -> dict[str, dict[type, list[Any]]]:
    """Every section's hydrated document rows, keyed the way ``hydrate_section`` keys them."""
    bundle = _row(LagBundleIntent, id=1, device_id=1, name="Port-channel1", lag_id=1, min_links=2, timer="fast")
    bundle.members = [
        LagMemberIntent(id=1, lag_bundle_id=1, interface_name="GigabitEthernet0/1", mode="active", port_priority=32),
        LagMemberIntent(id=2, lag_bundle_id=1, interface_name="GigabitEthernet0/2"),
    ]
    bare_bundle = _row(LagBundleIntent, id=2, device_id=1, name="Port-channel2")
    bare_bundle.members = []

    trunk = _row(SwitchportIntent, id=1, device_id=1, interface_name="GigabitEthernet0/3", mode="trunk")
    trunk.tagged_vlans = [
        SwitchportTaggedVlanIntent(id=1, switchport_id=1, vlan_id=20),
        SwitchportTaggedVlanIntent(id=2, switchport_id=1, vlan_id=10),
    ]
    access = _row(
        SwitchportIntent, id=2, device_id=1, interface_name="GigabitEthernet0/4", mode="access", untagged_vlan=10
    )
    access.tagged_vlans = []

    peer_af = BgpPeerAfIntent(
        id=1, peer_id=1, af="ipv4-unicast", enabled=True, routemap_in="RM-IN", prefixlist_out="PL-OUT"
    )
    peer = BgpPeerIntent(
        id=1,
        scope_id=1,
        peer_address="192.0.2.2",
        enabled=True,
        peer_group="SPINE",
        remote_as="65200",
        ttl=2,
        source="Loopback0",
    )
    peer.peer_address_families = [peer_af]
    scope = BgpScopeIntent(id=1, router_id=1, vrf="")
    scope.address_families = [BgpAfIntent(id=1, scope_id=1, af="ipv4-unicast")]
    scope.peers = [peer]
    router = _row(BgpRouterIntent, id=1, device_id=1, asn="65100", router_id="192.0.2.1")
    router.scopes = [scope]

    return {
        "snmp": {
            SnmpCommunityIntent: [
                _row(
                    SnmpCommunityIntent,
                    id=1,
                    device_id=1,
                    label="ro-community",
                    vault_ref="secret/nso/snmp#ro",
                    access="RO",
                    acl="SNMP-ACL",
                ),
                _row(
                    SnmpCommunityIntent,
                    id=2,
                    device_id=1,
                    label="rw-community",
                    vault_ref="secret/nso/snmp#rw",
                    access="rw",
                    acl=None,
                ),
            ],
            SnmpV3UserIntent: [
                _row(
                    SnmpV3UserIntent,
                    id=1,
                    device_id=1,
                    username="ops",
                    group_name="ops-group",
                    auth_protocol="sha",
                    priv_protocol="aes",
                    auth_vault_ref="secret/nso/snmp#auth",
                    priv_vault_ref="secret/nso/snmp#priv",
                ),
                _row(SnmpV3UserIntent, id=2, device_id=1, username="viewer"),
            ],
            SnmpHostIntent: [
                _row(
                    SnmpHostIntent,
                    id=1,
                    device_id=1,
                    address="198.51.100.10",
                    version="2c",
                    notify_type="trap",
                    community_or_user="ro-community",
                    port=1162,
                ),
                _row(
                    SnmpHostIntent,
                    id=2,
                    device_id=1,
                    address="198.51.100.11",
                    version="3",
                    notify_type="inform",
                    community_or_user="",
                    port=None,
                ),
            ],
            SnmpSystemInfoIntent: [
                _row(SnmpSystemInfoIntent, id=1, device_id=1, location="rack-7", contact="noc@example.net")
            ],
        },
        "static_route": {
            StaticRouteIntent: [
                _row(
                    StaticRouteIntent,
                    id=1,
                    device_id=1,
                    vrf="",
                    prefix="198.18.0.0/24",
                    next_hop="198.18.1.1",
                    metric=10,
                    tag=101,
                    permanent=True,
                    name="to-core",
                ),
                _row(
                    StaticRouteIntent,
                    id=2,
                    device_id=1,
                    vrf="RED",
                    prefix="198.18.2.0/24",
                    next_hop="",
                    interface_next_hop="GigabitEthernet0/0",
                    next_hop_vrf="BLUE",
                ),
            ]
        },
        "logging": {
            LoggingHostIntent: [
                _row(
                    LoggingHostIntent,
                    id=1,
                    device_id=1,
                    address="198.51.100.20",
                    port=1514,
                    severity="informational",
                    facility="local7",
                    transport="tcp",
                    vrf="MGMT",
                    source="Loopback0",
                ),
                _row(
                    LoggingHostIntent,
                    id=2,
                    device_id=1,
                    address="198.51.100.21",
                    port=None,
                    severity="",
                    facility="",
                    transport="",
                    vrf="",
                    source="",
                ),
            ],
            LoggingLevelsIntent: [
                _row(
                    LoggingLevelsIntent,
                    id=1,
                    device_id=1,
                    console_severity="warnings",
                    monitor_severity=None,
                    module_severity="errors",
                )
            ],
        },
        "svi": {
            SviIntent: [
                _row(SviIntent, id=1, device_id=1, interface_name="Vlan10", vlan_id=10, svi_type="irb", vrf="RED"),
                _row(SviIntent, id=2, device_id=1, interface_name="Vlan20", vlan_id=20, svi_type="svi", vrf=None),
            ]
        },
        "subinterface": {
            SubinterfaceIntent: [
                _row(
                    SubinterfaceIntent,
                    id=1,
                    device_id=1,
                    interface_name="GigabitEthernet0/0.100",
                    parent_interface="GigabitEthernet0/0",
                    dot1q_vlan=100,
                    sub_type="dot1q",
                    vrf="RED",
                ),
                _row(
                    SubinterfaceIntent,
                    id=2,
                    device_id=1,
                    interface_name="GigabitEthernet0/0.200",
                    parent_interface="GigabitEthernet0/0",
                    dot1q_vlan=200,
                    sub_type="dot1q",
                    vrf=None,
                ),
            ]
        },
        "vlan": {
            VlanIntent: [
                _row(VlanIntent, id=1, device_id=1, vlan_id=10, name="users"),
                _row(VlanIntent, id=2, device_id=1, vlan_id=20, name=None),
            ]
        },
        "bfd": {
            BfdIntent: [
                _row(
                    BfdIntent,
                    id=1,
                    device_id=1,
                    interface_name="GigabitEthernet0/0",
                    min_tx=300,
                    min_rx=300,
                    multiplier=3,
                    micro_bfd=True,
                ),
                _row(BfdIntent, id=2, device_id=1, interface_name="GigabitEthernet0/1", micro_bfd=False),
            ]
        },
        "interface_mtu": {
            InterfaceMtuIntent: [
                _row(
                    InterfaceMtuIntent,
                    id=1,
                    device_id=1,
                    interface_name="GigabitEthernet0/0",
                    mtu=9000,
                    ip_mtu=8986,
                    mpls_mtu=9000,
                ),
                _row(InterfaceMtuIntent, id=2, device_id=1, interface_name="GigabitEthernet0/1"),
            ]
        },
        "l2_sap": {
            L2SapIntent: [
                _row(
                    L2SapIntent,
                    id=1,
                    device_id=1,
                    service_name="epipe-1",
                    service_type="epipe",
                    sap_id="1/1/1:100",
                    port="1/1/1",
                    outer_tag=100,
                    inner_tag=200,
                ),
                _row(
                    L2SapIntent,
                    id=2,
                    device_id=1,
                    service_name="vpls-1",
                    service_type="vpls",
                    sap_id="1/1/2",
                    port="",
                ),
            ]
        },
        "isis": {
            IsisProcessIntent: [
                _row(
                    IsisProcessIntent,
                    id=1,
                    device_id=1,
                    process_tag="CORE",
                    net="49.0001.0000.0000.0001.00",
                    is_type="level-2",
                    metric_style="wide",
                    overload_bit=False,
                    area_auth_type="md5",
                    area_auth_key="area-key",
                    fast_reroute="ti-lfa",
                    microloop_avoidance=True,
                ),
                _row(IsisProcessIntent, id=2, device_id=1, process_tag="EDGE"),
            ],
            IsisInterfaceIntent: [
                _row(
                    IsisInterfaceIntent,
                    id=1,
                    device_id=1,
                    interface_name="GigabitEthernet0/0",
                    af="ipv4-unicast",
                    process_tag="CORE",
                    circuit_type="level-2",
                    network_type="point-to-point",
                    metric=100,
                    passive=False,
                    bfd_enabled=True,
                    frr_enabled=True,
                    frr_protection="node",
                ),
                _row(
                    IsisInterfaceIntent,
                    id=2,
                    device_id=1,
                    interface_name="Loopback0",
                    af="ipv6-unicast",
                    process_tag="CORE",
                    passive=True,
                ),
            ],
            IsisLevelIntent: [
                _row(
                    IsisLevelIntent,
                    id=1,
                    device_id=1,
                    process_tag="CORE",
                    level=2,
                    wide_metrics_only=True,
                    labeled_preference=100,
                    disabled=False,
                )
            ],
            IsisFlexAlgoIntent: [
                _row(
                    IsisFlexAlgoIntent,
                    id=1,
                    device_id=1,
                    process_tag="ORPHAN",
                    algo_id=128,
                    metric_type="delay",
                    priority=100,
                    admin_group_exclude="RED",
                )
            ],
            RedistributionIntent: [
                _row(
                    RedistributionIntent,
                    id=1,
                    device_id=1,
                    dest_protocol="isis",
                    dest_ref="CORE",
                    source_protocol="connected",
                    source_ref="",
                    route_map="RM-CONN",
                    metric=20,
                    metric_type="external",
                )
            ],
        },
        "bgp": {
            BgpRouterIntent: [router],
            RedistributionIntent: [
                _row(
                    RedistributionIntent,
                    id=2,
                    device_id=1,
                    dest_protocol="bgp",
                    dest_ref="65100::ipv4-unicast",
                    source_protocol="static",
                    source_ref="",
                    route_map="RM-STATIC",
                    metric=None,
                )
            ],
        },
        "route_policy": {
            RoutePolicyObjectIntent: [
                _row(
                    RoutePolicyObjectIntent,
                    id=1,
                    device_id=1,
                    family="prefix_list",
                    name="PL-OUT",
                    entries=[{"sequence": 10, "action": "permit", "prefix": "198.18.0.0/24"}],
                    invert_match=False,
                ),
                _row(
                    RoutePolicyObjectIntent,
                    id=2,
                    device_id=1,
                    family="community_list",
                    name="CL-LARGE",
                    entries=[{"community": "large:64512:1:2"}, {"community": "64512:100"}],
                    invert_match=True,
                ),
                _row(
                    RoutePolicyObjectIntent,
                    id=3,
                    device_id=1,
                    family="community_list",
                    name="CL-BANDWIDTH",
                    entries=[{"community": "bandwidth:64512:100"}],
                    invert_match=False,
                ),
                _row(
                    RoutePolicyObjectIntent,
                    id=4,
                    device_id=1,
                    family="as_path",
                    name="AP-1",
                    entries=[{"sequence": 10, "action": "permit", "regex": "^65200_"}],
                    invert_match=False,
                ),
                _row(
                    RoutePolicyObjectIntent,
                    id=5,
                    device_id=1,
                    family="route_map",
                    name="RM-IN",
                    entries=[{"sequence": 10, "action": "permit", "match_prefix_lists": ["PL-OUT"]}],
                    invert_match=False,
                ),
            ]
        },
        "ospf": {
            OspfInstanceIntent: [
                _row(
                    OspfInstanceIntent,
                    id=1,
                    device_id=1,
                    process_id="1",
                    router_id="192.0.2.1",
                    vrf="",
                    areas=None,
                    enabled=True,
                ),
                _row(OspfInstanceIntent, id=2, device_id=1, process_id="2", vrf="RED", enabled=False),
            ],
            OspfInterfaceIntent: [
                _row(
                    OspfInterfaceIntent,
                    id=1,
                    device_id=1,
                    interface_name="GigabitEthernet0/0",
                    process_id="1",
                    area_id="0",
                    passive=False,
                    priority=10,
                    cost=100,
                    network_type="point-to-point",
                    auth_type="md5",
                    auth_key="ospf-key",
                ),
                _row(
                    OspfInterfaceIntent,
                    id=2,
                    device_id=1,
                    interface_name="Loopback0",
                    process_id="1",
                    area_id="0",
                    passive=True,
                ),
            ],
            RedistributionIntent: [
                _row(
                    RedistributionIntent,
                    id=3,
                    device_id=1,
                    dest_protocol="ospf",
                    dest_ref="9",
                    source_protocol="connected",
                    source_ref="",
                    metric=5,
                )
            ],
        },
        "switchport": {
            SwitchportIntent: [trunk, access],
            SwitchportTaggedVlanIntent: [*trunk.tagged_vlans],
        },
        "lag": {
            LagBundleIntent: [bundle, bare_bundle],
            LagMemberIntent: [*bundle.members],
        },
        "interface_config": {
            InterfaceIntent: [
                _row(InterfaceIntent, id=1, interface_id=1, attribute="description", intent_value="uplink"),
                _row(InterfaceIntent, id=2, interface_id=1, attribute="enabled", intent_value="true"),
                _row(InterfaceIntent, id=3, interface_id=2, attribute="description", intent_value=None),
                _row(InterfaceIntent, id=4, interface_id=2, attribute="enabled", intent_value="false"),
            ],
            InterfaceIpIntent: [
                _row(
                    InterfaceIpIntent,
                    id=1,
                    interface_id=2,
                    address="198.18.3.1/30",
                    vrf="VPRN-A",
                    family="ipv4",
                    secondary=False,
                ),
                _row(
                    InterfaceIpIntent,
                    id=2,
                    interface_id=2,
                    address="2001:db8::1/64",
                    vrf="VPRN-A",
                    family="ipv6",
                    secondary=False,
                ),
            ],
        },
    }


#: Attribute rows the interface proof records as ELIGIBLE. Row 4 is deliberately absent:
#: an ineligible attribute must not reach the wire.
ELIGIBLE_ATTRIBUTES: frozenset[tuple[int, str]] = frozenset({(1, "description"), (1, "enabled"), (2, "description")})
