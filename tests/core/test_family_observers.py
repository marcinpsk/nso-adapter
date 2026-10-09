# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Canonical family observations from device read payloads."""

from nso_adapter.domain.observation import observe_family
from tests._secret_discipline import assert_text_free_of


def test_vlan_observation_keeps_device_values_and_reports_invalid_entries():
    payload = {
        "status": "ok",
        "vlan": [{"vlan-id": 20, "name": "DATA"}, {"vlan-id": 10, "name": ""}, {"name": "NO-ID"}],
    }
    observation = observe_family("vlan", payload)
    assert observation is not None
    assert observation.document.model_dump() == {
        "present": ["vlans"],
        "vlans": [
            {"vlan_id": 10, "name": "", "present": ["name", "vlan_id"]},
            {"vlan_id": 20, "name": "DATA", "present": ["name", "vlan_id"]},
        ],
        "unprojectable": [{"index": 2, "reason": "vlan[2]: invalid vlan_id"}],
    }
    assert observation.coverage.attributes == ["name", "vlan_id"]


def test_unknown_vlan_fields_are_reported_without_their_values():
    observed = observe_family("vlan", {"vlan": [{"vlan-id": 10, "credential": "placeholder-secret"}]})
    assert_text_free_of(observed.document.model_dump_json(), ["placeholder-secret"])
    assert observed.document.vlans[0].vlan_id == 10
    assert observed.document.unprojectable[0].model_dump() == {
        "index": 0,
        "reason": "vlan[0]: unsupported fields: credential",
    }
