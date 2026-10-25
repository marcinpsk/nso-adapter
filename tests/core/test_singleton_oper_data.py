# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""A one-entry export list rendered as a bare object is refused at the client boundary (#1833).

The P0 wire captures show the action and the facade render a one-entry list as a JSON array of
one object (RFC 7951). The collection decoder refuses every other shape, so the refresh keeps
the last-known rows and records a read error instead of guessing at a singleton.
"""

from __future__ import annotations

import pytest

from nso_adapter.core.bgp import refresh_bgp_config_for_device
from nso_adapter.core.interface_ip import refresh_interface_ips_for_device
from nso_adapter.core.lag_topology import refresh_lag_topology_for_device
from nso_adapter.core.route_policy import refresh_route_policy_for_device
from nso_adapter.nso.collection_coverage import MARKER
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device
from tests.conftest import seed_device, session
from tests.core.test_collection_coverage_e2e import CoverageNso

CASES = [
    ("lag-topology", "lag", refresh_lag_topology_for_device, {MARKER: ["lag"], "lag": {"name": "Port-channel1"}}),
    (
        "interface-ip",
        "interface_ip",
        refresh_interface_ips_for_device,
        {
            MARKER: ["interface"],
            "interface": [{"interface-name": "port0", MARKER: ["address"], "address": {"address": "198.18.0.1/24"}}],
        },
    ),
    (
        "bgp-config",
        "bgp",
        refresh_bgp_config_for_device,
        {MARKER: ["router"], "router": [{"asn": "65001", MARKER: ["scope"], "scope": {"vrf": ""}}]},
    ),
    (
        "route-policy",
        "route_policy",
        refresh_route_policy_for_device,
        {
            MARKER: ["prefix-list"],
            "prefix-list": [{"name": "PL-1", MARKER: ["entry"], "entry": {"sequence": 10, "action": "permit"}}],
        },
    ),
]


@pytest.mark.parametrize(("wire", "family", "refresh", "payload"), CASES, ids=[case[0] for case in CASES])
async def test_a_bare_object_collection_is_a_contract_error_and_keeps_rows(
    adapter_client, wire, family, refresh, payload
):
    device_id = await seed_device(nso_device_name="PLACEHOLDER-RTR-01")
    nso = CoverageNso({wire: {"status": "ok", **payload}})
    async with session() as db:
        device = await db.get(Device, device_id)
        assert await refresh(db, device, nso) is False
        row = await outcome_store.get_current_outcome(db, device_id, family)
    assert row is not None
    assert (row.read_outcome, row.read_reason, row.result) == ("unavailable", "read_error", "kept")
    assert row.read_failures is not None
    assert row.read_failures[0]["error_type"] == "CollectionCoverageError"
