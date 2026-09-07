# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The write-side section registry and its per-section wire encoders (#1522 C9, memo A8).

``device_intent_golden.json`` holds the body every section encodes for the shared row set.
The fourteen sections that had a per-service builder were captured FROM those builders
before the extraction, so an equal body is behaviour preservation and not a restatement of
the new code. ``switchport`` and ``lag`` never had a writer: their goldens are read off
``switchport-intent.yang`` and ``lag-intent.yang`` and are the pin from here on.
"""

from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path

import pytest

from nso_adapter.core.community_dialect import community_dialect_by_name, community_dialect_for
from nso_adapter.core.projection import (
    InterfaceExecution,
    projection_sections,
    section_registry,
    section_rows_by_table,
)
from nso_adapter.nso import apply as nso_apply
from nso_adapter.nso.apply import (
    NsoApplyError,
    SectionExecution,
    refuse_gated_local_levels,
    unrenderable_route_policy_members,
)
from tests.nso.device_intent_rows import ELIGIBLE_ATTRIBUTES, NOKIA_NED, interfaces, section_rows

_GOLDEN = json.loads((Path(__file__).parent / "device_intent_golden.json").read_text())


def _execution(section: str) -> SectionExecution:
    """The frozen facts each section's golden body was captured under."""
    if section == "route_policy":
        return SectionExecution(NOKIA_NED, community_dialect_for(NOKIA_NED))
    if section == "interface_config":
        return SectionExecution(
            None, community_dialect_for(None), InterfaceExecution(interfaces(), ELIGIBLE_ATTRIBUTES)
        )
    return SectionExecution(None, community_dialect_for(None))


def _rows(section: str) -> dict[str, list]:
    """The section's fixture rows, keyed by table name and complete for every declared table."""
    by_model = section_rows()[section]
    return {spec.model.__tablename__: by_model.get(spec.model, []) for spec in section_registry()[section].tables}


@pytest.mark.parametrize("section", sorted(_GOLDEN))
def test_each_section_encodes_its_golden_body(section):
    entry = section_registry()[section]
    assert entry.encode(_rows(section), _execution(section)) == _GOLDEN[section]


def test_the_golden_covers_every_registered_section():
    assert set(_GOLDEN) == projection_sections()


def test_an_ineligible_attribute_never_reaches_the_wire():
    rows = _rows("interface_config")
    body = section_registry()["interface_config"].encode(rows, _execution("interface_config"))
    nokia = next(entry for entry in body["interface"] if entry["interface-name"] == "1/1/1:100")
    # Interface 2's `enabled` row is authorized but ineligible; its description is eligible.
    assert "enabled" not in nokia
    assert nokia["description"] == ""


def test_route_policy_encodes_through_the_frozen_dialect_not_a_device_row():
    rows = _rows("route_policy")
    nokia = section_registry()["route_policy"].encode(
        rows, SectionExecution(NOKIA_NED, community_dialect_for(NOKIA_NED))
    )
    identity = section_registry()["route_policy"].encode(
        rows, SectionExecution(NOKIA_NED, community_dialect_by_name("identity"))
    )
    large = next(entry for entry in nokia["community-list"] if entry["name"] == "CL-LARGE")
    assert large["entry"][0]["community"] == "64512:1:2"
    identity_large = next(entry for entry in identity["community-list"] if entry["name"] == "CL-LARGE")
    assert identity_large["entry"][0]["community"] == "large:64512:1:2"


def test_unrenderable_members_are_reported_not_silently_dropped():
    dialect = community_dialect_for(NOKIA_NED)
    assert unrenderable_route_policy_members(_rows("route_policy"), dialect) == [
        ("CL-BANDWIDTH", "bandwidth:64512:100")
    ]
    assert unrenderable_route_policy_members(_rows("route_policy"), community_dialect_by_name("identity")) == []


@pytest.mark.parametrize("gate", ["0", "1"])
def test_flipping_the_local_levels_gate_cannot_change_an_encoded_body(monkeypatch, gate):
    """An encoder is a pure function of its rows and its context, never of the environment.

    A body that depended on a process's environment would let two adapters encode one
    document differently, and a retry of a frozen generation send different bytes.
    """
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", gate)
    body = section_registry()["logging"].encode(_rows("logging"), _execution("logging"))
    assert body == _GOLDEN["logging"]
    assert body["local-levels"] == {"console-severity": "warnings", "module-severity": "errors"}


def test_the_send_boundary_refuses_gated_local_levels(monkeypatch):
    """The refusal moved out of the encoder; it did not go away."""
    rows = _rows("logging")
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "0")
    with pytest.raises(NsoApplyError) as excinfo:
        refuse_gated_local_levels(rows)
    assert excinfo.value.code == "local_levels_gated"
    assert excinfo.value.detail == {"levels": {"console-severity": "warnings", "module-severity": "errors"}}
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "1")
    assert refuse_gated_local_levels(rows) is None
    # A hosts-only device has no severities to gate, so a closed gate never refuses it.
    monkeypatch.setenv("NSO_ADAPTER_LOGGING_LOCAL_LEVELS_WRITE", "0")
    assert refuse_gated_local_levels({**rows, "logging_levels_intent": []}) is None


def test_an_encoder_refuses_a_row_set_missing_one_of_its_declared_tables():
    rows = _rows("snmp")
    del rows["snmp_host_intent"]
    with pytest.raises(KeyError):
        section_registry()["snmp"].encode(rows, _execution("snmp"))


def test_section_rows_by_table_fills_every_declared_table():
    document = {"vlan": {"vlan_intent": [], "_execution": {"context": {"ned_id": None, "dialect": "identity"}}}}
    assert section_rows_by_table(document, "vlan") == {"vlan_intent": []}
    document = {"snmp": {"_execution": {"context": {"ned_id": None, "dialect": "identity"}}}}
    assert sorted(section_rows_by_table(document, "snmp")) == [
        "snmp_community_intent",
        "snmp_host_intent",
        "snmp_system_info_intent",
        "snmp_v3_user_intent",
    ]


def test_the_registry_containers_are_the_aggregate_yang_containers():
    # Committed snapshot of `tools/device_intent_containers.py` in nso-packages, never a
    # read from a sibling checkout: the two repos move together, so a rename lands in both.
    snapshot = json.loads((Path(__file__).parent / "device_intent_containers.json").read_text())
    containers = [entry.container for entry in section_registry().values()]
    assert len(containers) == len(set(containers))
    assert set(containers) == set(snapshot["containers"])
    assert len(snapshot["containers"]) == len(set(snapshot["containers"]))

    from nso_adapter.core import apply, removal

    for module in (apply, removal):
        for node in ast.walk(ast.parse(Path(inspect.getfile(module)).read_text())):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id.endswith("_CONTAINER"):
                        assert node.value.value not in containers, f"{module.__name__}.{target.id} repeats a container"


def test_every_section_declares_a_capability_scope_and_a_result_counter():
    for section, entry in section_registry().items():
        assert entry.capability_scopes, section
        assert entry.result_keys, section
    assert section_registry()["interface_config"].capability_scopes == ("interface_attribute", "interface_ip")
    assert section_registry()["interface_config"].result_keys == ("attribute", "ip")


def test_a_registry_whose_containers_collide_fails_at_startup(monkeypatch):
    from nso_adapter.core import projection

    broken = {section: entry for section, entry in projection.section_registry().items()}
    broken["vlan"] = broken["vlan"]._replace(container="snmp")
    monkeypatch.setattr(projection, "section_registry", lambda: broken)
    with pytest.raises(RuntimeError, match="both claim container"):
        projection._validate_section_registry(frozenset({"switchport", "lag"}))


def test_a_registry_naming_an_unregistered_read_family_fails_at_startup(monkeypatch):
    from nso_adapter.core import projection

    broken = dict(projection.section_registry())
    broken["vlan"] = broken["vlan"]._replace(read_family="not_a_family")
    monkeypatch.setattr(projection, "section_registry", lambda: broken)
    with pytest.raises(RuntimeError, match="unregistered family"):
        projection._validate_section_registry(frozenset({"switchport", "lag"}))


# ── the derived consumers: every one of them READS the registry (#1522 memo A8) ──
#
# Each pin used to guard a hand-kept table beside the registry. The tables are gone with the
# per-family senders, so the pins now hold the consumers to the registry instead.


def test_the_job_result_counters_are_the_registry_result_keys_in_registry_order():
    from nso_adapter.core.apply import _result_keys

    expected = tuple(key for entry in section_registry().values() for key in entry.result_keys)
    assert _result_keys() == expected
    # The counter names themselves are a plugin contract and did not change with the sender.
    assert set(_result_keys()) >= {"attribute", "ip", "snmp", "static_route", "logging", "switchport", "lag"}
    assert _result_keys()[-2:] == ("attribute", "ip"), "the two interface counters follow the batch counters"


def test_capability_scopes_resolve_through_the_registry_container_map():
    from nso_adapter.core.apply import _capability_scopes_for

    for section, entry in section_registry().items():
        assert tuple(_capability_scopes_for(entry.container)) == entry.capability_scopes, section
    assert _capability_scopes_for("no-such-family") == []


def test_every_read_family_resolves_to_the_residue_wire_name():
    from nso_adapter.core.importer import projectable_spec
    from nso_adapter.core.removal import residue_wire_name

    for section, entry in section_registry().items():
        spec = projectable_spec(entry.read_family)
        assert spec is not None, section
        assert residue_wire_name(section) == spec.wire_name, section


# ── the purity rule the amendment states: encode(rows, frozen context) ───────────────


def _encoder_call_graph() -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    """Every module-level function in ``nso/apply.py`` an encoder can reach."""
    module = ast.parse(Path(inspect.getfile(nso_apply)).read_text())
    defined = {node.name: node for node in module.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    reached: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
    pending = [entry.encode.__name__ for entry in section_registry().values()]
    while pending:
        name = pending.pop()
        node = defined.get(name)
        if node is None or name in reached:
            continue
        reached[name] = node
        pending.extend(
            call.func.id
            for call in ast.walk(node)
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id in defined
        )
    return reached


#: Reading any of these makes a body a function of the process rather than of the document.
_IMPURE_READS = {
    ("os", "environ"),
    ("os", "getenv"),
    ("datetime", "now"),
    ("datetime", "utcnow"),
    ("date", "today"),
    ("time", "time"),
    ("time", "monotonic"),
}


def test_no_encoder_reads_the_environment_the_clock_or_the_network():
    """An encoder is a pure function of its rows and its frozen context (#1522 memo A8).

    A body that depended on a process's environment, on the clock, or on a device read
    would let two adapters encode one document differently, and a retry of a frozen
    generation send different bytes than the attempt it retries.
    """
    graph = _encoder_call_graph()
    assert {entry.encode.__name__ for entry in section_registry().values()} <= set(graph)
    impure: list[str] = []
    for name, node in sorted(graph.items()):
        if isinstance(node, ast.AsyncFunctionDef):
            impure.append(f"{name}: an encoder path may not be a coroutine")
        for child in ast.walk(node):
            if isinstance(child, ast.Await):
                impure.append(f"{name}: awaits, so it can reach the network")
            if (
                isinstance(child, ast.Attribute)
                and isinstance(child.value, ast.Name)
                and (child.value.id, child.attr) in _IMPURE_READS
            ):
                impure.append(f"{name}: reads {child.value.id}.{child.attr}")
    assert not impure, impure


def test_static_route_legacy_body_paths_are_deleted():
    from nso_adapter.core import apply, removal

    definitions = {
        node.name
        for module in (apply, removal)
        for node in ast.walk(ast.parse(Path(inspect.getfile(module)).read_text()))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert not definitions & {"_static_route_snapshot", "_replace_static_route"}
