# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""The canonical read-outcome family registry (READSEM S4 D8).

``nso_adapter.core.families`` is the single source for the outcome-store family key
vocabulary the S4 API exposes: the 17 engine ``FamilySpec.name``s plus the two
non-engine writers (``redistribution``, ``interface_attributes``). The plugin vendors a
copy of this list; ``FAMILIES_VERSION`` is served on the aggregate endpoint so a drift
is visible at runtime, and these tests pin the adapter side of that contract.
"""

from __future__ import annotations

from nso_adapter.core.families import ALL_FAMILY_KEYS, ENGINE_FAMILY_KEYS, FAMILIES_VERSION


def test_registry_is_the_full_19_key_vocabulary():
    assert set(ALL_FAMILY_KEYS) == {
        "lag",
        "logging",
        "snmp",
        "bgp",
        "svi",
        "subinterface",
        "interface_ip",
        "isis",
        "vlan",
        "switchport",
        "bfd",
        "l2_service",
        "static_route",
        "interface_mtu",
        "lag_config",
        "ospf",
        "route_policy",
        "redistribution",
        "interface_attributes",
    }
    assert len(ALL_FAMILY_KEYS) == 19
    # Keys are the outcome store's underscore vocabulary — never the wire's hyphen names.
    assert all("-" not in key for key in ALL_FAMILY_KEYS)


def test_engine_subset_matches_the_family_specs():
    """The 17 engine spec names must equal ENGINE_FAMILY_KEYS — a new/renamed FamilySpec
    that skips the registry breaks the S4 API vocabulary silently otherwise."""
    from nso_adapter.core.bfd import BFD_SPEC
    from nso_adapter.core.bgp import BGP_SPEC
    from nso_adapter.core.interface_ip import INTERFACE_IP_SPEC
    from nso_adapter.core.interface_mtu import INTERFACE_MTU_SPEC
    from nso_adapter.core.isis import ISIS_SPEC
    from nso_adapter.core.l2_service import L2_SERVICE_SPEC
    from nso_adapter.core.lag_config import LAG_CONFIG_SPEC
    from nso_adapter.core.lag_topology import LAG_TOPOLOGY_SPEC
    from nso_adapter.core.logging_config import LOGGING_CONFIG_SPEC
    from nso_adapter.core.ospf import OSPF_SPEC
    from nso_adapter.core.route_policy import ROUTE_POLICY_SPEC
    from nso_adapter.core.snmp import SNMP_SPEC
    from nso_adapter.core.static_route import STATIC_ROUTE_SPEC
    from nso_adapter.core.subinterface import SUBINTERFACE_SPEC
    from nso_adapter.core.svi import SVI_SPEC
    from nso_adapter.core.vlan import SWITCHPORT_SPEC, VLAN_DATABASE_SPEC

    spec_names = {
        spec.name
        for spec in (
            BFD_SPEC,
            BGP_SPEC,
            INTERFACE_IP_SPEC,
            INTERFACE_MTU_SPEC,
            ISIS_SPEC,
            L2_SERVICE_SPEC,
            LAG_CONFIG_SPEC,
            LAG_TOPOLOGY_SPEC,
            LOGGING_CONFIG_SPEC,
            OSPF_SPEC,
            ROUTE_POLICY_SPEC,
            SNMP_SPEC,
            STATIC_ROUTE_SPEC,
            SUBINTERFACE_SPEC,
            SVI_SPEC,
            SWITCHPORT_SPEC,
            VLAN_DATABASE_SPEC,
        )
    }
    assert spec_names == set(ENGINE_FAMILY_KEYS)
    assert set(ENGINE_FAMILY_KEYS) | {"redistribution", "interface_attributes"} == set(ALL_FAMILY_KEYS)


def test_families_version_is_a_positive_int():
    assert isinstance(FAMILIES_VERSION, int)
    assert FAMILIES_VERSION >= 1
