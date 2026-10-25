# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Census: every export collection has a consumer that states what it needs (#1833 R2-1, M8).

A new collection in the export YANG reaches the vendored manifest; these checks fail until an
adapter consumer requires or carries it, and until every verifier path resolves to it.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from nso_adapter.core.families import ENGINE_FAMILY_KEYS
from nso_adapter.core.importer import INTERFACE_ATTRIBUTES_REQUIRES, INTERFACE_ATTRIBUTES_WIRE, projectable_spec
from nso_adapter.core.projection import NoComparison, section_registry
from nso_adapter.core.redistribution import _REDIST_COMPONENTS
from nso_adapter.core.refresh_engine import FamilySpec
from nso_adapter.core.removal import _reader_path, residue_wire_name, section_guard_lists
from nso_adapter.nso import collection_coverage
from nso_adapter.nso.collection_coverage import FAMILIES, collection_paths
from tests.collection_coverage_helpers import REDIST_WIRES


def _engine_specs() -> list[FamilySpec]:
    names = ["lag_topology" if key == "lag" else key for key in ENGINE_FAMILY_KEYS]
    specs = [projectable_spec(name) for name in names]
    assert all(spec is not None for spec in specs)
    return specs  # type: ignore[return-value]


def _claims() -> dict[str, set[str]]:
    """Return {family: collection paths some consumer requires}."""
    claims: dict[str, set[str]] = {}
    for spec in _engine_specs():
        claims.setdefault(spec.wire_name, set()).update(spec.requires)
    for component in _REDIST_COMPONENTS:
        claims.setdefault(component.wire_name, set()).update(component.requires)
    claims.setdefault(INTERFACE_ATTRIBUTES_WIRE, set()).update(INTERFACE_ATTRIBUTES_REQUIRES)
    return claims


def _unclaimed(claims: dict[str, set[str]]) -> dict[str, list[str]]:
    gaps = {family: sorted(set(collection_paths(family)) - claims.get(family, set())) for family in FAMILIES}
    return {family: paths for family, paths in gaps.items() if paths}


def test_every_family_the_adapter_reads_is_a_manifest_family_and_back():
    assert set(_claims()) == set(FAMILIES)
    assert {component.protocol: component.wire_name for component in _REDIST_COMPONENTS} == REDIST_WIRES


def test_every_engine_family_requires_collections():
    for spec in _engine_specs():
        assert spec.requires, spec.name


def test_every_manifest_collection_has_a_consumer():
    assert _unclaimed(_claims()) == {}


def test_the_census_names_a_collection_no_consumer_states(monkeypatch):
    families = deepcopy(FAMILIES)
    families["svi"]["interface"]["collections"]["ipv6-address"] = {
        "kind": "list",
        "keys": ("address",),
        "collections": {},
    }
    monkeypatch.setattr(collection_coverage, "FAMILIES", families)
    assert _unclaimed(_claims()) == {"svi": ["interface/ipv6-address"]}


@pytest.mark.parametrize(("requires", "message"), [((), "non-empty"), (("interfaces",), "unknown")])
def test_a_family_spec_states_manifest_paths(requires, message):
    with pytest.raises(ValueError, match=message):
        FamilySpec(
            name="svi",
            extract=lambda data: data,
            materialize=lambda *_args: None,  # type: ignore[arg-type,return-value]
            wire_name="svi",
            requires=requires,
        )


def test_every_export_verifier_path_is_a_manifest_collection():
    checked = 0
    for section, entry in section_registry().items():
        wire = residue_wire_name(section)
        if wire is None or section == "interface_config":
            continue  # interface_config compares addresses on interface/address, below
        for guard_list in section_guard_lists(section):
            assert "/".join(_reader_path(section, guard_list)) in collection_paths(wire), (section, guard_list.label)
            checked += 1
        assert entry.guard_lists or isinstance(entry.verify, NoComparison), section
    assert checked >= 20
    assert "interface/address" in collection_paths(residue_wire_name("interface_config") or "")
