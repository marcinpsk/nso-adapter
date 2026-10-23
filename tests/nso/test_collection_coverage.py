# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The covered-collections decoder and the vendored collection manifest (#1833)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from nso_adapter.nso import collection_coverage
from nso_adapter.nso.collection_coverage import (
    MANIFEST_PATH,
    MARKER,
    CollectionCoverageError,
    admit_coverage,
    collection_paths,
    decode_section,
    decode_sections,
    load_manifest,
    uncovered_paths,
    validate_paths,
)
from nso_adapter.nso.read_outcome import Present, Unavailable, UnavailableReason
from tests._secret_discipline import assert_text_free_of
from tests.collection_coverage_helpers import exported

ROOT = Path(__file__).resolve().parents[2]


def _scope(**fields) -> dict:
    return {"vrf": "", **fields}


def _bgp(scope: dict) -> dict:
    return {
        "status": "ok",
        MARKER: ["router"],
        "router": [{"asn": "64512", MARKER: ["scope"], "scope": [scope]}],
    }


def test_a_covered_omitted_collection_decodes_as_empty_and_the_marker_never_leaves():
    section = _bgp(_scope(**{MARKER: ["address-family", "peer", "peer-group"]}))
    decoded = decode_section("bgp-config", section)
    scope = decoded["router"][0]["scope"][0]
    assert scope == {"vrf": "", "address-family": [], "peer": [], "peer-group": []}
    assert_text_free_of(json.dumps(decoded), [MARKER])
    assert MARKER in json.dumps(section)  # the served body is not mutated


def test_an_uncovered_omitted_collection_stays_absent():
    decoded = decode_section("bgp-config", _bgp(_scope(**{MARKER: ["peer"]})))
    assert decoded["router"][0]["scope"][0] == {"vrf": "", "peer": []}


def test_a_parent_without_a_marker_covers_nothing():
    decoded = decode_section("bgp-config", _bgp(_scope()))
    assert decoded["router"][0]["scope"][0] == {"vrf": ""}


def test_two_parents_carry_their_own_coverage():
    section = {
        "status": "ok",
        MARKER: ["router"],
        "router": [
            {
                "asn": "64512",
                MARKER: ["scope"],
                "scope": [_scope(**{MARKER: ["peer-group"]}), {"vrf": "CUST-A", MARKER: []}],
            }
        ],
    }
    scopes = decode_section("bgp-config", section)["router"][0]["scope"]
    assert scopes == [{"vrf": "", "peer-group": []}, {"vrf": "CUST-A"}]
    assert uncovered_paths("bgp-config", decode_section("bgp-config", section), ["router/scope/peer-group"]) == [
        "router/scope/peer-group"
    ]


def test_a_populated_collection_is_unchanged():
    peer = {"peer-address": "198.18.0.2", MARKER: ["peer-address-family"], "peer-address-family": [{"afi": "ipv4"}]}
    section = _bgp(_scope(**{MARKER: ["peer"], "peer": [peer]}))
    decoded = decode_section("bgp-config", section)
    assert decoded["router"][0]["scope"][0]["peer"] == [
        {"peer-address": "198.18.0.2", "peer-address-family": [{"afi": "ipv4"}]}
    ]


def test_a_covered_leaf_list_decodes_as_empty_and_keeps_scalars():
    entry = {"sequence": 10, "action": "permit", MARKER: ["match-prefix-lists", "match-as-paths"]}
    entry["match-prefix-lists"] = ["PL-A"]
    section = {
        "status": "ok",
        MARKER: ["route-map"],
        "route-map": [{"name": "RM", MARKER: ["entry"], "entry": [entry]}],
    }
    decoded = decode_section("route-policy", section)["route-map"][0]["entry"][0]
    assert decoded == {"sequence": 10, "action": "permit", "match-prefix-lists": ["PL-A"], "match-as-paths": []}


@pytest.mark.parametrize(
    ("section", "message"),
    [
        (_bgp(_scope(**{MARKER: ["neighbor"]})), "names a collection this parent does not bear"),
        (_bgp(_scope(**{MARKER: ["peer", "peer"]})), "repeats a name"),
        (_bgp(_scope(**{MARKER: "peer"})), "must be an array of names"),
        (_bgp(_scope(**{MARKER: [7]})), "must be an array of names"),
        (_bgp(_scope(peer=[])), "a collection without a coverage statement"),
        (_bgp(_scope(**{MARKER: ["peer"], "peer": {"peer-address": "198.18.0.2"}})), "must be an array"),
        (_bgp(_scope(**{MARKER: ["peer"], "peer": [None]})), "a list entry must be an object"),
        (_bgp(_scope(**{MARKER: ["peer"], "peer": None})), "must be an array"),
        (
            _bgp(
                _scope(
                    **{
                        MARKER: ["peer"],
                        "peer": [
                            {
                                "peer-address": "198.18.0.2",
                                MARKER: ["peer-address-family"],
                                "peer-address-family": [{"afi": "ipv4", MARKER: []}],
                            }
                        ],
                    }
                )
            ),
            "sits on a parent with no collection",
        ),
    ],
    ids=[
        "unknown",
        "duplicate",
        "scalar-marker",
        "non-string",
        "uncovered-array",
        "bare-object",
        "null-entry",
        "null",
        "non-bearing",
    ],
)
def test_a_section_that_breaks_the_contract_is_refused(section, message):
    with pytest.raises(CollectionCoverageError, match=message):
        decode_section("bgp-config", section)


@pytest.mark.parametrize(
    "section",
    [
        {"status": "ok", MARKER: ["host"], "local-levels": {"console-severity": "warning", MARKER: []}},
        {"status": "ok", MARKER: ["host"], "local-levels": {"nested": [{MARKER: ["host"]}]}},
    ],
    ids=["container", "nested-list"],
)
def test_a_marker_below_a_value_that_is_not_a_collection_is_refused(section):
    with pytest.raises(CollectionCoverageError, match="sits on a parent with no collection"):
        decode_section("logging-config", section)


def test_a_leaf_list_value_must_be_a_scalar():
    entry = {"sequence": 10, MARKER: ["match-as-paths"], "match-as-paths": [{"name": "AP"}]}
    section = {
        "status": "ok",
        MARKER: ["route-map"],
        "route-map": [{"name": "RM", MARKER: ["entry"], "entry": [entry]}],
    }
    with pytest.raises(CollectionCoverageError, match="leaf-list value must be a scalar"):
        decode_section("route-policy", section)


def test_an_unknown_family_and_a_non_object_section_are_refused():
    with pytest.raises(CollectionCoverageError, match="not a family"):
        decode_section("eigrp-config", {"status": "ok"})
    with pytest.raises(CollectionCoverageError, match="must be an object"):
        decode_section("bgp-config", ["router"])


def test_decode_sections_decodes_object_sections_and_keeps_others_for_the_classifier():
    served = {"device-name": "PLACEHOLDER-DEVICE", "svi": exported("svi", {"status": "ok"}), "vlan-database": 3}
    decoded = decode_sections(["svi", "vlan-database", "lag-config"], served)
    assert decoded == {
        "device-name": "PLACEHOLDER-DEVICE",
        "svi": {"status": "ok", "interface": []},
        "vlan-database": 3,
    }


def test_a_section_without_status_data_decodes_to_itself():
    assert decode_section("snmp-config", {"status": "unsupported"}) == {"status": "unsupported"}


def test_collection_paths_and_validation_read_the_manifest():
    assert collection_paths("l2-service") == {"service": "list", "service/sap": "list"}
    assert validate_paths("l2-service", ["service/sap"]) == ("service/sap",)
    with pytest.raises(ValueError, match="unknown"):
        validate_paths("l2-service", ["service/saps"])
    with pytest.raises(ValueError, match="non-empty"):
        validate_paths("l2-service", [])
    with pytest.raises(ValueError, match="not a manifest collection path"):
        uncovered_paths("l2-service", {}, ["sap"])


def test_an_empty_parent_list_needs_no_statement_below_it():
    decoded = decode_section("l2-service", {"status": "ok", MARKER: ["service"]})
    assert uncovered_paths("l2-service", decoded, ["service", "service/sap"]) == []
    assert uncovered_paths("l2-service", {"status": "ok"}, ["service", "service/sap"]) == ["service", "service/sap"]


def test_admission_turns_an_uncovered_present_read_into_not_covered():
    decoded = decode_section(
        "l2-service", exported("l2-service", {"service": [{"service-name": "S1"}]}, cover_all=False)
    )
    outcome = admit_coverage(Present(decoded), "l2-service", ["service", "service/sap"], consumer="l2_service")
    assert outcome == Unavailable(UnavailableReason.not_covered)
    covered = Present(decode_section("l2-service", exported("l2-service", {"service": [{"service-name": "S1"}]})))
    assert admit_coverage(covered, "l2-service", ["service", "service/sap"], consumer="l2_service") is covered
    kept = Unavailable(UnavailableReason.unsupported)
    assert admit_coverage(kept, "l2-service", ["service"], consumer="l2_service") is kept


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("[]", "exactly the marker and families"),
        ('{"marker": "", "families": {"x": {"collections": {}}}}', "marker must be a name"),
        ('{"marker": "m", "families": {"x": {}}}', "exactly the collections key"),
        ('{"marker": "m", "families": {"x": {"collections": {"a": {"kind": "set"}}}}}', "list or leaf-list"),
        ('{"marker": "m", "families": {"x": {"collections": {"a": {"kind": "list", "keys": []}}}}}', "kind, keys"),
        ('{"marker": "m", "families": {"x": {"collections": {"a": {"kind": "leaf-list", "keys": ["k"]}}}}}', "only"),
        ('{"marker": "m", "families": {"x": {"collections": []}}}', "must be an object"),
    ],
)
def test_a_malformed_manifest_is_refused(text, message):
    with pytest.raises(CollectionCoverageError, match=message):
        load_manifest(text)


def test_the_vendored_manifest_is_the_one_the_decoder_loads():
    marker, families = load_manifest(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert (marker, families) == (collection_coverage.MARKER, collection_coverage.FAMILIES)
    assert marker == "covered-collections"
    assert MANIFEST_PATH.read_bytes().endswith(b"}\n")


def _packages_checkout() -> Path | None:
    configured = os.environ.get("NSO_PACKAGES_ROOT")
    root = Path(configured) if configured else ROOT.parent / "nso-packages"
    return root if (root / "tools" / "gen_collection_manifest.py").is_file() else None


PACKAGES = _packages_checkout()

# Collected only where a packages checkout with the generator exists; CI vendors the copy above.
if PACKAGES is not None:

    def test_the_vendored_manifest_matches_the_packages_generator():
        assert PACKAGES is not None
        result = subprocess.run(
            [sys.executable, "tools/gen_collection_manifest.py", "--check", "--adapter-out", str(MANIFEST_PATH.parent)],
            cwd=PACKAGES,
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
