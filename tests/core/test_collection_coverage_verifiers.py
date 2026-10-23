# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Export verifiers treat an uncovered collection as unproven, never clean or missing (#1833 R2-1).

The device-state-read action is answered at the RESTCONF boundary and decoded by the real client.
"""

from __future__ import annotations

from nso_adapter.core.apply import _reader_compare_walk, _static_route_device_state
from nso_adapter.core.removal import _interface_config_residue, _residue_after_removal, section_guard_lists
from nso_adapter.nso.collection_coverage import MARKER
from nso_adapter.store.models import BgpRouterIntent, Device
from tests.core.test_collection_coverage_e2e import CoverageNso

DEVICE = Device(id=7, nso_device_name="PLACEHOLDER-RTR-01")


def _bgp(scope_marker: list[str]) -> dict:
    scope = {"vrf": "", MARKER: scope_marker}
    return {"status": "ok", MARKER: ["router"], "router": [{"asn": "64512", MARKER: ["scope"], "scope": [scope]}]}


async def test_static_route_proof_is_unproven_when_routes_are_not_covered():
    assert await _static_route_device_state(CoverageNso({"static-route": {"status": "ok", MARKER: []}}), DEVICE) == (
        "unsupported",
        {},
    )
    covered = CoverageNso({"static-route": {"status": "ok", MARKER: ["route"]}})
    assert await _static_route_device_state(covered, DEVICE) == ("ok", {})


async def test_removal_residue_is_unproven_when_the_removed_list_is_not_covered():
    context = {"removed": {"peer": [["198.18.0.2"]]}}
    unread = CoverageNso({"bgp-config": _bgp(["address-family", "peer-group"])})
    assert await _residue_after_removal(unread, DEVICE, "bgp", context) == (None, ["peer"])
    read = CoverageNso({"bgp-config": _bgp(["address-family", "peer", "peer-group"])})
    assert await _residue_after_removal(read, DEVICE, "bgp", context) == ({}, [])


async def test_interface_config_residue_is_unproven_when_addresses_are_not_covered():
    context = {"removed": {"address": [["port0", "198.18.0.1/24", ""]]}}
    interface = {"interface-name": "port0", MARKER: []}
    unread = CoverageNso({"interface-ip": {"status": "ok", MARKER: ["interface"], "interface": [interface]}})
    assert await _interface_config_residue(unread, DEVICE, context) is None
    interface[MARKER] = ["address"]
    read = CoverageNso({"interface-ip": {"status": "ok", MARKER: ["interface"], "interface": [interface]}})
    assert await _interface_config_residue(read, DEVICE, context) == {}


async def test_reader_compare_reports_an_uncovered_key_as_unproven_not_missing():
    row = BgpRouterIntent(id=11, asn="64512")
    lists = section_guard_lists("bgp")
    sent = [(row, "router", ("64512",)), (row, "peer", ("198.18.0.2",))]
    unread = (
        await CoverageNso({"bgp-config": _bgp(["address-family"])}).run_device_state_read(
            DEVICE.nso_device_name, ["bgp-config"]
        )
    )["bgp-config"]
    ok, failed, fails, status, evidence = _reader_compare_walk("bgp", sent, [], unread, lists, 1, job_id=1, device_id=7)
    assert (ok, failed, fails, status) == (1, 0, [], "partial")
    assert evidence == {(BgpRouterIntent.__tablename__, 11): "unproven"}
    assert row.last_apply_error is None

    read = (
        await CoverageNso({"bgp-config": _bgp(["address-family", "peer"])}).run_device_state_read(
            DEVICE.nso_device_name, ["bgp-config"]
        )
    )["bgp-config"]
    ok, failed, _fails, status, evidence = _reader_compare_walk("bgp", sent, [], read, lists, 1, job_id=1, device_id=7)
    assert (ok, failed, status) == (0, 1, "missing")
    assert evidence == {(BgpRouterIntent.__tablename__, 11): "missing"}
