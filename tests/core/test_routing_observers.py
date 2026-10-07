# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Device observations for routing families."""

import pytest

from nso_adapter.core.families import ALL_FAMILY_KEYS
from nso_adapter.domain.observation import OBSERVERS, observe_family
from tests._secret_discipline import assert_text_free_of
from tests.fixtures.routing_read_payloads import (
    BGP_ROUTERS_READ,
    ISIS_LEVEL_READ,
    OSPF_INSTANCES_READ,
    OSPF_INTERFACES_READ,
    ROUTE_POLICY_COMMUNITIES_READ,
)


@pytest.mark.parametrize("family", ALL_FAMILY_KEYS)
def test_every_compared_family_has_an_observer(family):
    assert family in OBSERVERS


@pytest.mark.parametrize(
    "family,payload,coverage",
    [
        ("bgp", {"router": BGP_ROUTERS_READ}, {"afi", "routemap_in", "prefixlist_out"}),
        ("isis", ISIS_LEVEL_READ, {"process_tag", "is_type", "circuit_type"}),
        ("ospf", {"instance": OSPF_INSTANCES_READ, "interface": OSPF_INTERFACES_READ}, {"process_id", "area_id"}),
        ("route_policy", ROUTE_POLICY_COMMUNITIES_READ, {"invert_match", "community", "sequence"}),
    ],
)
def test_routing_observers_reuse_existing_ned_read_fixtures(family, payload, coverage):
    observed = observe_family(family, payload, ned_id="timos-nc-23.10" if family == "route_policy" else None)
    document = observed.document.model_dump()
    assert document["unprojectable"] == []
    assert set(observed.coverage.attributes) >= coverage
    if family == "bgp":
        policies = [peer["peer_address_family"][0] for peer in document["routers"][0]["scope"][0]["peer"]]
        assert policies[0]["routemap_in"] == "PIN"
        assert policies[1]["prefixlist_out"] == "PLO"
    elif family == "isis":
        assert document["processes"][0]["is_type"] == "level-2-only"
        assert document["interfaces"][0]["circuit_type"] == "level-2-only"
    elif family == "ospf":
        assert [instance["process_id"] for instance in document["instances"]] == ["1", "2"]
        assert [interface["process_id"] for interface in document["interfaces"]] == ["1", "2"]
    else:
        community_list = next(entry for entry in document["community_lists"] if entry["name"] == "SCRUBBER")
        assert community_list["invert_match"] is True
        assert community_list["entry"][1]["community"] == "large:64500:.*:[0-4]"


def test_redistribution_observer_projects_existing_afi_less_bgp_and_trimmed_protocol():
    payload = {
        "bgp": {
            "router": [
                {
                    "asn": "64512",
                    "scope": [
                        {
                            "vrf": "BLUE",
                            "address-family": [
                                {"redistribute": [{"source-protocol": " connected ", "source-ref": " "}]}
                            ],
                        }
                    ],
                }
            ]
        }
    }
    observed = observe_family("redistribution", payload)
    document = observed.document.model_dump()
    assert document["unprojectable"] == []
    assert document["entries"][0]["dest_ref"] == "64512/BLUE"
    assert document["entries"][0]["source_protocol"] == "connected"
    assert document["entries"][0]["source_ref"] == ""
    family = document["components"][0]["inventory"][0]["scope"][0]["address_family"][0]
    assert family["afi"] == "" and "afi" not in family["present"]
    assert observed.coverage.components[0].destinations == ["64512/BLUE"]


def test_bgp_observer_projects_nested_device_values():
    payload = {
        "router": [
            {
                "asn": "64512",
                "router-id": "198.18.0.1",
                "scope": [
                    {
                        "vrf": "",
                        "address-family": [{"afi": "ipv4-unicast"}],
                        "peer": [
                            {
                                "peer-address": "198.18.0.2",
                                "remote-as": "64513",
                                "enabled": False,
                                "peer-address-family": [{"afi": "ipv4-unicast", "policy-in": "IMPORT"}, {}],
                            },
                            {},
                        ],
                    }
                ],
            }
        ]
    }
    observation = observe_family("bgp", payload)
    assert observation is not None
    document = observation.document.model_dump()
    router = document["routers"][0]
    assert router["asn"] == "64512"
    assert router["router_id"] == "198.18.0.1"
    peer = router["scope"][0]["peer"][0]
    assert peer["remote_as"] == "64513" and peer["enabled"] is False
    assert peer["peer_address_family"][0]["routemap_in"] == "IMPORT"
    assert len(document["unprojectable"]) == 2
    assert all(item["reason"] for item in document["unprojectable"])
    assert "remote_as" in observation.coverage.attributes


@pytest.mark.parametrize(
    "family,key", [("bgp", "router"), ("isis", "process"), ("ospf", "instance"), ("route_policy", "prefix-list")]
)
def test_routing_empty_and_unprojectable(family, key):
    empty = observe_family(family, {})
    assert empty is not None and empty.document.unprojectable == []
    invalid = observe_family(family, {key: [False]})
    assert invalid is not None
    assert invalid.document.unprojectable[0].index == 0
    assert "expected object" in invalid.document.unprojectable[0].reason


def test_isis_observer_covers_process_interface_and_flex_algorithm():
    payload = {
        "process": [
            {
                "process-tag": "0",
                "is-type": "level-2",
                "overload-bit": False,
                "level": [{"level": 2, "default-metric": 0}],
                "segment-routing": {"enabled": False},
                "flex-algo": [{"algo-id": 128, "priority": 0}, {"priority": 10}],
            }
        ],
        "interface": [
            {
                "interface-name": "Gi0/1",
                "af": "ipv4",
                "circuit-type": "level-2",
                "passive": False,
                "prefix-sid": [{"algorithm": 0, "sid-index": 0}],
            }
        ],
    }
    observation = observe_family("isis", payload)
    assert observation is not None
    document = observation.document.model_dump()
    process = document["processes"][0]
    assert process["is_type"] == "level-2-only"
    assert process["overload_bit"] is False
    assert process["level"][0]["default_metric"] == 0
    assert process["flex_algo"][0]["algo_id"] == 128 and process["flex_algo"][0]["priority"] == 0
    assert document["interfaces"][0]["prefix_sid"][0]["sid_index"] == 0
    assert "algo_id" in observation.coverage.attributes
    assert "flex_algo" in observation.coverage.attributes
    assert len(document["unprojectable"]) == 1
    assert "flex_algo[1]" in document["unprojectable"][0]["reason"]


def test_ospf_observer_projects_processes_across_vrfs_and_interface_values():
    observation = observe_family(
        "ospf",
        {
            "instance": [
                {
                    "process-id": "1",
                    "vrf": "BLUE",
                    "enabled": False,
                    "area": [{"area-id": "0", "area-type": "stub"}, {}],
                },
                {"process-id": "1", "vrf": ""},
            ],
            "interface": [{"interface-name": "Gi0/0", "process-id": "1", "area-id": "0", "cost": 0, "passive": False}],
        },
    )
    assert observation is not None
    document = observation.document.model_dump()
    assert [instance["vrf"] for instance in document["instances"]] == ["", "BLUE"]
    assert document["instances"][1]["enabled"] is False
    assert document["interfaces"][0]["cost"] == 0
    assert document["interfaces"][0]["passive"] is False
    assert document["instances"][1]["area"][0]["area_type"] == "stub"
    assert "area_type" in observation.coverage.attributes and "cost" in observation.coverage.attributes
    assert len(document["unprojectable"]) == 1 and "area[1]" in document["unprojectable"][0]["reason"]


def test_route_policy_observer_uses_the_materializer_community_dialect():
    observation = observe_family(
        "route_policy",
        {
            "prefix-list": [
                {"name": "PL", "family": 6, "entry": [{"sequence": 0, "action": "permit", "prefix": "2001:db8::/32"}]}
            ],
            "community-list": [
                {
                    "name": "SCRUBBER",
                    "invert-match": False,
                    "entry": [{"sequence": 10, "action": "permit", "community": "64512&.*&[0-4]"}, {}],
                }
            ],
            "as-path": [{"name": "AS", "entry": [{"sequence": 10, "action": "permit", "pattern": "^64512$"}]}],
            "route-map": [
                {
                    "name": "IMPORT",
                    "entry": [{"sequence": 10, "action": "permit", "match-prefix-lists": ["PL"], "match-json": "{}"}],
                }
            ],
        },
        ned_id="timos-nc-23.10",
    )
    assert observation is not None
    document = observation.document.model_dump()
    assert document["community_lists"][0]["entry"][0]["community"] == "large:64512:.*:[0-4]"
    assert document["community_lists"][0]["invert_match"] is False
    assert document["prefix_lists"][0]["entry"][0]["sequence"] == 0
    assert document["as_paths"][0]["entry"][0]["pattern"] == "^64512$"
    assert document["route_maps"][0]["entry"][0]["match_prefix_lists"] == ["PL"]
    assert "community" in observation.coverage.attributes and "match_json" in observation.coverage.attributes
    assert len(document["unprojectable"]) == 1 and "entry[1]" in document["unprojectable"][0]["reason"]


def test_redistribution_observer_reports_component_coverage_and_invalid_rows():
    observation = observe_family(
        "redistribution",
        {
            "ospf": {
                "instance": [
                    {
                        "process-id": "1",
                        "redistribute": [
                            {"source-protocol": "connected", "metric": 0, "route-map": ""},
                            {},
                        ],
                    }
                ]
            },
            "bgp": {
                "router": [
                    {
                        "asn": "64512",
                        "scope": [
                            {
                                "vrf": "BLUE",
                                "address-family": [
                                    {
                                        "afi": "ipv4-unicast",
                                        "redistribute": [{"source-protocol": "static", "source-ref": ""}],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
        },
    )
    assert observation is not None
    document = observation.document.model_dump()
    assert [(row["dest_protocol"], row["dest_ref"]) for row in document["entries"]] == [
        ("bgp", "64512/BLUE/ipv4-unicast"),
        ("ospf", "1"),
    ]
    assert document["entries"][1]["metric"] == 0 and document["entries"][1]["route_map"] == ""
    assert observation.coverage.model_dump()["components"] == [
        {
            "protocol": "bgp",
            "destinations": ["64512/BLUE/ipv4-unicast"],
            "sources": [{"protocol": "static", "reference": ""}],
        },
        {"protocol": "ospf", "destinations": ["1"], "sources": [{"protocol": "connected", "reference": ""}]},
    ]
    assert len(document["unprojectable"]) == 1
    assert "redistribute[1]" in document["unprojectable"][0]["reason"]
    empty = observe_family("redistribution", {"bgp": {}, "isis": {}, "ospf": {}})
    assert empty.document.entries == [] and empty.document.unprojectable == []
    assert [component.protocol for component in empty.document.components] == ["bgp", "isis", "ospf"]
    assert empty.coverage.model_dump()["components"] == [
        {"protocol": proto, "destinations": [], "sources": []} for proto in ["bgp", "isis", "ospf"]
    ]


@pytest.mark.parametrize(
    "family,wire,key",
    [
        ("bgp", "router", "routers"),
        ("isis", "process", "processes"),
        ("ospf", "instance", "instances"),
        ("route_policy", "prefix-list", "prefix_lists"),
    ],
)
def test_routing_top_level_presence_stays_distinct(family, wire, key):
    documents = [observe_family(family, payload).document.model_dump() for payload in [{}, {wire: None}, {wire: []}]]
    assert documents[0][key] == [] and key not in documents[0]["present"]
    assert documents[1][key] is None and key in documents[1]["present"]
    assert documents[2][key] == [] and key in documents[2]["present"]


def test_bgp_conflicting_policy_aliases_are_unprojectable():
    observation = observe_family(
        "bgp",
        {
            "router": [
                {
                    "asn": "64512",
                    "scope": [
                        {
                            "vrf": "",
                            "peer": [
                                {
                                    "peer-address": "198.18.0.2",
                                    "peer-address-family": [
                                        {"afi": "ipv4-unicast", "routemap-in": "", "policy-in": "IMPORT"}
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        },
    )
    assert observation is not None
    assert observation.document.routers[0].scope[0].peer[0].peer_address_family == []
    assert len(observation.document.unprojectable) == 1
    assert "conflicting aliases for routemap_in" in observation.document.unprojectable[0].reason


@pytest.mark.parametrize(
    "family,payloads",
    [
        ("bgp", [{"router": [{"asn": "64513"}, {"asn": "64512"}]}, {"router": [{"asn": "64512"}, {"asn": "64513"}]}]),
        (
            "isis",
            [
                {"process": [{"process-tag": "B"}, {"process-tag": "A"}]},
                {"process": [{"process-tag": "A"}, {"process-tag": "B"}]},
            ],
        ),
        (
            "ospf",
            [
                {"instance": [{"process-id": "2"}, {"process-id": "1"}]},
                {"instance": [{"process-id": "1"}, {"process-id": "2"}]},
            ],
        ),
        (
            "route_policy",
            [{"route-map": [{"name": "B"}, {"name": "A"}]}, {"route-map": [{"name": "A"}, {"name": "B"}]}],
        ),
        (
            "redistribution",
            [
                {"ospf": {"instance": [{"process-id": "2"}, {"process-id": "1"}]}},
                {"ospf": {"instance": [{"process-id": "1"}, {"process-id": "2"}]}},
            ],
        ),
    ],
)
def test_routing_documents_are_independent_of_inventory_order(family, payloads):
    documents = [observe_family(family, payload).document.model_dump(mode="json") for payload in payloads]
    assert documents[0] == documents[1]


async def test_bgp_mirror_keeps_device_group_precedence_while_observation_sorts(adapter_client):
    from nso_adapter.core.bgp import BGP_SPEC
    from nso_adapter.core.refresh_engine import run_family_refresh_from_outcome
    from nso_adapter.nso.read_outcome import Freshness, Present
    from nso_adapter.store.models import Device
    from tests.conftest import AUTH, seed_device, session

    payload = {
        "router": [
            {
                "asn": "64512",
                "scope": [
                    {
                        "vrf": "",
                        "peer": [
                            {
                                "peer-address": "198.18.0.2",
                                "peer-group": "Z",
                                "remote-as": "64513",
                                "peer-address-family": [{"afi": "ipv4-unicast", "routemap-in": "IMPORT-Z"}],
                            },
                            {
                                "peer-address": "198.18.0.2",
                                "peer-group": "A",
                                "remote-as": "64514",
                                "peer-address-family": [{"afi": "ipv4-unicast", "routemap-in": "IMPORT-A"}],
                            },
                        ],
                    }
                ],
            }
        ]
    }
    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        await run_family_refresh_from_outcome(db, device, BGP_SPEC, Present(payload, Freshness.fresh))
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/bgp-config", headers=AUTH)
    assert response.status_code == 200, response.text
    body = response.json()
    peer = body["routers"][0]["scopes"][0]["peers"][0]
    assert peer["peer_group"] == "Z" and peer["remote_as"] == "64513"
    assert peer["address_families"][0]["routemap_in"] == "IMPORT-Z"
    observed_peers = body["observation"]["document"]["routers"][0]["scope"][0]["peer"]
    assert [entry["peer_group"] for entry in observed_peers] == ["A", "Z"]


@pytest.mark.parametrize("protocol,key", [("ospf", "instance"), ("isis", "process"), ("bgp", "router")])
def test_redistribution_component_inventory_presence_stays_distinct(protocol, key):
    documents = [
        observe_family("redistribution", {protocol: payload}).document.model_dump()
        for payload in [{}, {key: None}, {key: []}]
    ]
    assert len({str(document) for document in documents}) == 3


@pytest.mark.parametrize(
    "protocol,key,identity", [("ospf", "instance", {"process-id": "1"}), ("isis", "process", {"process-tag": "CORE"})]
)
def test_redistribution_destination_source_list_presence_stays_distinct(protocol, key, identity):
    payloads = [
        {protocol: {key: [{**identity, **source}]}} for source in [{}, {"redistribute": None}, {"redistribute": []}]
    ]
    documents = [observe_family("redistribution", payload).document.model_dump() for payload in payloads]
    assert len({str(document) for document in documents}) == 3


def test_bgp_redistribution_nested_source_list_presence_stays_distinct():
    payloads = [
        {
            "bgp": {
                "router": [
                    {"asn": "64512", "scope": [{"vrf": "", "address-family": [{"afi": "ipv4-unicast", **source}]}]}
                ]
            }
        }
        for source in [{}, {"redistribute": None}, {"redistribute": []}]
    ]
    documents = [observe_family("redistribution", payload).document.model_dump() for payload in payloads]
    assert len({str(document) for document in documents}) == 3


def test_wire_alias_conflicts_distinguish_boolean_from_integer():
    observation = observe_family(
        "switchport", {"interface": [{"interface-name": "Gi0/1", "untagged-vlan": 1, "untagged_vlan": True}]}
    )
    assert observation is not None
    assert observation.document.interfaces == []
    assert "conflicting aliases" in observation.document.unprojectable[0].reason


def test_isis_unknown_nested_srgb_has_value_free_indexed_diagnostic():
    observed = observe_family(
        "isis",
        {
            "process": [
                {
                    "process-tag": "CORE",
                    "segment-routing": {
                        "enabled": True,
                        "srgb": {"lower-bound": 16000, "secret": "placeholder-unsupported-secret"},
                    },
                }
            ]
        },
    )
    document = observed.document.model_dump()
    assert_text_free_of(str(document), ["placeholder-unsupported-secret"])
    assert document["processes"][0]["segment_routing"]["enabled"] is True
    assert document["unprojectable"] == [
        {
            "index": 0,
            "reason": "process[0].segment_routing[0]: unsupported fields: srgb",
        }
    ]


@pytest.mark.parametrize(
    "family,collection,identity,secret",
    [
        ("bgp", "router", {"peer-address": "198.18.0.2"}, "password"),
        ("isis", "process", {"process-tag": "CORE"}, "area-auth-key"),
        ("isis", "process", {"process-tag": "CORE"}, "domain-auth-key"),
    ],
)
def test_observed_credentials_keep_missing_null_empty_and_key_distinct(family, collection, identity, secret):
    results = []
    for fields in ({}, {secret: None}, {secret: ""}, {secret: "placeholder-key"}):
        entry = identity | fields
        payload = {collection: [entry]}
        if family == "bgp":
            payload = {"router": [{"asn": "64512", "scope": [{"vrf": "", "peer": [entry]}]}]}
        observation = observe_family(family, payload)
        if family == "bgp":
            observed = observation.document.routers[0].scope[0].peer[0]
        else:
            observed = observation.document.processes[0]
        key = secret.replace("-", "_")
        assert getattr(observed, key) is None
        assert key not in observation.coverage.attributes
        results.append(observed.model_dump())
    assert len({str(result) for result in results}) == 4
    key = secret.replace("-", "_")
    assert results[0][f"{key}_present"] is None and f"{key}_present" not in results[0]["present"]
    assert results[1][f"{key}_present"] is None and f"{key}_present" in results[1]["present"]
    assert results[2][f"{key}_present"] is False
    assert all(f"{key}_fingerprint" not in result for result in results)
    assert results[3][f"{key}_present"] is True
    assert key in observation.coverage.not_comparable


def test_presence_only_exports_do_not_claim_key_comparison():
    bgp = observe_family(
        "bgp",
        {
            "router": [
                {
                    "asn": "64512",
                    "scope": [
                        {
                            "vrf": "",
                            "peer": [
                                {"peer-address": "198.18.0.2"},
                            ],
                        }
                    ],
                }
            ]
        },
    )
    assert "password" in bgp.coverage.not_comparable
    isis = observe_family(
        "isis", {"process": [{"process-tag": "CORE", "area-auth-present": True, "domain-auth-present": True}]}
    )
    assert {"area_auth_key", "domain_auth_key", "hello_auth_key", "level.auth_key"} <= set(isis.coverage.not_comparable)
    assert "auth_key" in observe_family("ospf", {}).coverage.not_comparable
    assert {"auth_secret", "priv_secret"} <= set(observe_family("snmp", {}).coverage.not_comparable)


@pytest.mark.parametrize(
    "family,key,metadata",
    [
        ("bgp", "password", "password-fingerprint"),
        ("isis", "area-auth-key", "area-auth-key-fingerprint"),
        ("isis", "domain-auth-key", "domain-auth-key-fingerprint"),
    ],
)
def test_credential_metadata_is_derived_and_never_trusted_from_export(family, key, metadata):
    for supplied_key in ({}, {key: "placeholder-key"}):
        entry = {metadata: "placeholder-injected-secret", **supplied_key}
        if family == "bgp":
            payload = {
                "router": [
                    {
                        "asn": "64512",
                        "scope": [
                            {
                                "vrf": "",
                                "peer": [
                                    {"peer-address": "198.18.0.2", **entry},
                                ],
                            }
                        ],
                    }
                ]
            }
        else:
            payload = {"process": [{"process-tag": "CORE", **entry}]}
        observed = observe_family(family, payload)
        assert_text_free_of(observed.document.model_dump_json(), ["placeholder-injected-secret"])
        assert observed.document.unprojectable
        assert metadata in observed.document.unprojectable[0].reason
        row = observed.document.routers[0].scope[0].peer[0] if family == "bgp" else observed.document.processes[0]
        assert metadata.replace("-", "_") not in row.model_dump()
        assert getattr(row, f"{key.replace('-', '_')}_present") is (True if supplied_key else None)


@pytest.mark.parametrize(
    "family,location,key",
    [
        ("isis", "interface", "hello-auth-key"),
        ("isis", "process-level", "auth-key"),
        ("isis", "interface-level", "auth-key"),
        ("ospf", "interface", "auth-key"),
    ],
)
def test_auth_keys_are_presence_only_and_keep_wire_states(family, location, key):
    from tests._secret_discipline import assert_text_free_of

    results = []
    for fields in ({}, {key: None}, {key: ""}, {key: "$8$secret=="}):
        interface = {"interface-name": "Gi0/1", **({"af": "ipv4"} if family == "isis" else {"process-id": "1"})}
        if location == "process-level":
            payload = {"process": [{"process-tag": "CORE", "level": [{"level": 2, **fields}]}]}
        elif location == "interface-level":
            payload = {"interface": [{**interface, "level": [{"level": 2, **fields}]}]}
        else:
            payload = {"interface": [{**interface, **fields}]}
        observed = observe_family(family, payload)
        assert observed.document.unprojectable == []
        rows = observed.document.processes if location == "process-level" else observed.document.interfaces
        row = rows[0].level[0] if location.endswith("level") else rows[0]
        document = row.model_dump()
        assert key.replace("-", "_") not in document
        assert not any("fingerprint" in name for name in document)
        assert_text_free_of(observed.document.model_dump_json(), ["$8$secret=="])
        coverage_key = "level.auth_key" if location.endswith("level") else key.replace("-", "_")
        assert coverage_key in observed.coverage.not_comparable
        results.append(document)
    field = f"{key.replace('-', '_')}_present"
    assert results[0][field] is None and field not in results[0]["present"]
    assert results[1][field] is None and field in results[1]["present"]
    assert results[2][field] is False
    assert results[3][field] is True
    assert len({str(result) for result in results}) == 4
