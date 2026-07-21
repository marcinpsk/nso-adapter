# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""The canonical read-outcome family vocabulary (READSEM S4 D8).

The single source for the family keys the outcome store records and the S4 API exposes:
the 17 engine ``FamilySpec.name``s plus the two non-engine outcome writers. Keys use the
store's underscore vocabulary, never the wire's hyphenated names. The plugin vendors a
copy of this list; ``FAMILIES_VERSION`` is served by the aggregate read-state endpoint so
a cross-repo drift surfaces as a visible runtime warning, not silent misrendering.

Deliberately dependency-free (a literal, not derived from the FamilySpec objects) so
``api/`` modules can import it without dragging in every family module;
``tests/core/test_families.py`` pins it against the real specs.
"""

from __future__ import annotations

# Bump when keys are added/removed/renamed; the plugin warns on mismatch.
FAMILIES_VERSION = 1

# The 17 engine FamilySpec.name values (see the flip manifest in test_refresh_contract.py).
ENGINE_FAMILY_KEYS: tuple[str, ...] = (
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
)

# Engine families + the two non-engine outcome writers (core/redistribution.py,
# core/importer.py) — every `family` value the outcome store can contain.
ALL_FAMILY_KEYS: tuple[str, ...] = (*ENGINE_FAMILY_KEYS, "redistribution", "interface_attributes")
