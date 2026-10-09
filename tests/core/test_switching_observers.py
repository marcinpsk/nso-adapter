# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Device projections for the switching read families."""

from __future__ import annotations

from copy import deepcopy
from importlib import import_module

import pytest
from pydantic import ValidationError

from nso_adapter.domain.observation import digest_document, observe_family
from nso_adapter.domain.read_projection import entry_payload
from tests.fixtures.switching_read_payloads import SWITCHING_READ_PAYLOADS

SWITCHING_READS = [
    (
        "lag_config",
        "lag",
        "bundles",
        "project_lag_config",
        deepcopy(SWITCHING_READ_PAYLOADS["lag_config"]["lag"][0]),
        {"member", "lag_id", "name", "timer", "system_priority", "min_links"},
    ),
    (
        "lag",
        "lag",
        "bundles",
        "project_lag_topology",
        deepcopy(SWITCHING_READ_PAYLOADS["lag"]["lag"][0]),
        {"lag_id", "member", "name"},
    ),
    (
        "switchport",
        "interface",
        "interfaces",
        "project_switchports",
        deepcopy(SWITCHING_READ_PAYLOADS["switchport"]["interface"][0]),
        {"untagged_vlan", "mode", "interface_name", "tagged_vlans"},
    ),
    (
        "interface_mtu",
        "interface",
        "interfaces",
        "project_interface_mtu",
        deepcopy(SWITCHING_READ_PAYLOADS["interface_mtu"]["interface"][2]),
        {"bound_port", "ip_mtu", "interface_name"},
    ),
    (
        "svi",
        "interface",
        "interfaces",
        "project_svis",
        deepcopy(SWITCHING_READ_PAYLOADS["svi"]["interface"][0]),
        {"type", "interface_name", "vrf", "vlan_id"},
    ),
    (
        "subinterface",
        "interface",
        "interfaces",
        "project_subinterfaces",
        deepcopy(SWITCHING_READ_PAYLOADS["subinterface"]["interface"][1]),
        {"dot1q_vlan", "type", "parent_interface", "interface_name"},
    ),
    (
        "vlan",
        "vlan",
        "vlans",
        "project_vlans",
        deepcopy(SWITCHING_READ_PAYLOADS["vlan"]["vlan"][0]),
        {"name", "vlan_id"},
    ),
]


@pytest.mark.parametrize(("family", "wire_key", "document_key", "projector", "row", "present"), SWITCHING_READS)
def test_switching_observer_projects_existing_read_shape(family, wire_key, document_key, projector, row, present):
    payload = deepcopy(SWITCHING_READ_PAYLOADS[family])
    observed = observe_family(family, {"status": "ok", **payload})
    assert observed is not None
    entries = getattr(observed.document, document_key)
    assert len(entries) == len(payload[wire_key])
    assert observed.document.model_dump()["unprojectable"] == []
    assert set(observed.coverage.attributes) >= present
    identity = "vlan-id" if family == "vlan" else "name" if wire_key == "lag" else "interface-name"
    for entry, expected in zip(entries, sorted(payload[wire_key], key=lambda item: item[identity]), strict=True):
        assert entry.present == sorted(key.replace("-", "_") for key in expected)
        if "tagged-vlans" in expected:
            expected["tagged-vlans"] = [10, 20]
        assert entry_payload(entry) == expected
    assert (
        type(observed.document).model_validate(observed.document.model_dump()).model_dump()
        == observed.document.model_dump()
    )


@pytest.mark.parametrize(("family", "wire_key", "document_key", "projector", "row", "present"), SWITCHING_READS)
def test_switching_observer_authoritative_empty(family, wire_key, document_key, projector, row, present):
    observed = observe_family(family, {"status": "ok"})
    assert observed is not None
    assert observed.document.model_dump() == {"present": [], document_key: [], "unprojectable": []}
    assert set(observed.coverage.attributes) >= present


@pytest.mark.parametrize(("family", "wire_key", "document_key", "projector", "row", "present"), SWITCHING_READS)
def test_switching_observer_reports_each_unprojectable_row(family, wire_key, document_key, projector, row, present):
    observed = observe_family(family, {wire_key: [row, {}, "invalid"]})
    assert observed is not None
    assert len(observed.document.model_dump()[document_key]) == 1
    assert [entry["index"] for entry in observed.document.model_dump()["unprojectable"]] == [1, 2]
    assert all(entry["reason"] for entry in observed.document.model_dump()["unprojectable"])


@pytest.mark.parametrize(("family", "wire_key", "document_key", "projector", "row", "present"), SWITCHING_READS)
def test_switching_document_and_digest_are_order_independent(family, wire_key, document_key, projector, row, present):
    other = row.copy()
    if wire_key == "lag":
        other.update({"name": "Port-channel2", "lag-id": 2})
    elif wire_key == "vlan":
        other["vlan-id"] = 20
    else:
        other["interface-name"] = "xe-0/0/1.200"
    first = observe_family(family, {wire_key: [row, other]})
    second = observe_family(family, {wire_key: [other, row]})
    assert first is not None and second is not None
    assert first.document.model_dump() == second.document.model_dump()
    assert digest_document(first.document.model_dump()) == digest_document(second.document.model_dump())


@pytest.mark.parametrize("family", ["lag_config", "lag"])
def test_lag_observer_reports_invalid_and_duplicate_nested_members(family):
    observed = observe_family(
        family,
        {
            "lag": [
                {
                    "name": "Port-channel1",
                    "lag-id": 1,
                    "member": [
                        {"interface-name": "Gi0/1", "mode": "active"},
                        {"mode": "active"},
                        {"interface-name": "Gi0/1", "mode": "passive"},
                    ],
                }
            ]
        },
    )
    assert observed is not None
    assert [row["interface_name"] for row in observed.document.model_dump()["bundles"][0]["member"]] == ["Gi0/1"]
    assert [row["index"] for row in observed.document.model_dump()["unprojectable"]] == [1, 2]
    assert "member[1]" in observed.document.model_dump()["unprojectable"][0]["reason"]
    assert "duplicate" in observed.document.model_dump()["unprojectable"][1]["reason"]


def test_mtu_observer_preserves_missing_null_zero_and_empty():
    observed = observe_family(
        "interface_mtu",
        {
            "interface": [
                {"interface-name": "ae1"},
                {"interface-name": "ae2", "mtu": None},
                {"interface-name": "ae3", "mtu": 0, "bound-port": ""},
            ]
        },
    )
    assert observed is not None
    rows = observed.document.model_dump()["interfaces"]
    assert rows[0]["mtu"] is None and "mtu" not in rows[0]["present"]
    assert rows[1]["mtu"] is None and "mtu" in rows[1]["present"]
    assert rows[2]["mtu"] == 0 and rows[2]["bound_port"] == ""


def test_mtu_observer_reports_invalid_leaf_and_keeps_valid_fields():
    observed = observe_family(
        "interface_mtu", {"interface": [{"interface-name": "Gi0/1", "mtu": True, "ip-mtu": "9000"}]}
    )
    assert observed is not None
    row = observed.document.interfaces[0]
    assert row.mtu is None
    assert row.ip_mtu == 9000
    assert "mtu" in row.present
    assert [(item.index, item.reason) for item in observed.document.unprojectable] == [
        (0, "interface[0].mtu: invalid integer")
    ]


def test_mtu_document_refuses_coercion_when_reconstructed():
    module = import_module("nso_adapter.domain.switching_observation")
    document = module.project_interface_mtu({"interface": [{"interface-name": "Gi0/1", "mtu": 9000}]})
    payload = document.model_dump()
    payload["interfaces"][0]["mtu"] = "9000"
    with pytest.raises(ValidationError):
        type(document).model_validate(payload)


def test_lag_config_observer_preserves_false_vpc_flag():
    observed = observe_family("lag_config", {"lag": [{"name": "Port-channel1", "lag-id": 1, "vpc-sensitive": False}]})
    assert observed is not None
    row = observed.document.model_dump()["bundles"][0]
    assert row["vpc_sensitive"] is False
    assert "vpc_sensitive" in row["present"]


def test_switchport_projection_uses_canonical_tagged_vlan_set():
    module = import_module("nso_adapter.domain.switching_observation")
    projected = module.project_switchports({"interface": [{"interface-name": "Gi0/1", "tagged-vlans": "20,10,10"}]})
    assert projected.interfaces[0].tagged_vlans == [10, 20]
    assert projected.unprojectable == []


@pytest.mark.parametrize("raw", [False, 1, [], "0", "4095", "20-10", "10,"])
def test_switchport_observer_reports_invalid_tagged_vlan_value(raw):
    observed = observe_family("switchport", {"interface": [{"interface-name": "Gi0/1", "tagged-vlans": raw}]})
    assert observed is not None
    assert observed.document.model_dump()["interfaces"] == []
    assert observed.document.model_dump()["unprojectable"][0]["index"] == 0


def test_switchport_observer_preserves_missing_null_and_empty_tagged_vlans():
    observed = observe_family(
        "switchport",
        {
            "interface": [
                {"interface-name": "Gi0/1"},
                {"interface-name": "Gi0/2", "tagged-vlans": None},
                {"interface-name": "Gi0/3", "tagged-vlans": ""},
            ]
        },
    )
    assert observed is not None
    rows = observed.document.model_dump()["interfaces"]
    assert rows[0]["tagged_vlans"] is None and "tagged_vlans" not in rows[0]["present"]
    assert rows[1]["tagged_vlans"] is None and "tagged_vlans" in rows[1]["present"]
    assert rows[2]["tagged_vlans"] == [] and "tagged_vlans" in rows[2]["present"]


@pytest.mark.parametrize(("family", "wire_key", "document_key", "projector", "row", "present"), SWITCHING_READS)
def test_switching_observer_reports_duplicate_identity(family, wire_key, document_key, projector, row, present):
    observed = observe_family(family, {wire_key: [row, row.copy()]})
    assert observed is not None
    assert len(observed.document.model_dump()[document_key]) == 1
    assert observed.document.unprojectable[0].index == 1
    assert "duplicate identity" in observed.document.unprojectable[0].reason


@pytest.mark.parametrize(("family", "wire_key", "document_key", "projector", "row", "present"), SWITCHING_READS)
def test_switching_observer_preserves_missing_null_and_empty_collection(
    family, wire_key, document_key, projector, row, present
):
    missing = observe_family(family, {})
    null = observe_family(family, {wire_key: None})
    empty = observe_family(family, {wire_key: []})
    assert missing is not None and null is not None and empty is not None
    assert missing.document.model_dump()[document_key] == []
    assert missing.document.present == []
    assert null.document.model_dump()[document_key] is None
    assert null.document.present == [document_key]
    assert empty.document.model_dump()[document_key] == []
    assert empty.document.present == [document_key]


@pytest.mark.parametrize("family", ["switchport", "vlan"])
def test_switching_observer_reports_conflicting_collection_aliases(family):
    keys = ("interface", "interfaces") if family == "switchport" else ("vlan", "vlans")
    observed = observe_family(family, {keys[0]: [], keys[1]: [{"vlan-id": 10, "interface-name": "Gi0/1"}]})
    assert observed is not None
    assert observed.document.unprojectable[0].index == 0
    assert "conflicting" in observed.document.unprojectable[0].reason


@pytest.mark.parametrize("family", ["switchport", "vlan"])
def test_switching_collection_aliases_distinguish_nested_false_and_zero(family):
    keys = ("interface", "interfaces") if family == "switchport" else ("vlan", "vlans")
    row = {"interface-name": "Gi0/1", "vlan-id": 10, "vendor-marker": False}
    other = {**row, "vendor-marker": 0}
    observed = observe_family(family, {keys[0]: [row], keys[1]: [other]})
    assert observed is not None
    assert observed.document.unprojectable[0].index == 0
    assert "conflicting" in observed.document.unprojectable[0].reason
