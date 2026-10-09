# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Device observations for the service read families."""

from __future__ import annotations

from copy import deepcopy

import pytest
from sqlalchemy import select

from nso_adapter.core.static_route import _upsert_static_routes
from nso_adapter.domain.observation import digest_document, observe_family
from nso_adapter.domain.service_observation import project_static_routes
from nso_adapter.store.models import Device, DeviceStaticRoute
from tests.conftest import seed_device, session
from tests.core.test_l2_service import _NSO_ENTRY
from tests.fixtures.family_read_payloads import (
    BFD_INTERFACE_READ_PAYLOAD,
    LOGGING_LEVELS_READ_PAYLOAD,
    SNMP_COMMUNITIES_READ_PAYLOAD,
    SNMP_RECEIVERS_READ_PAYLOAD,
    SNMP_SYSTEM_READ_PAYLOAD,
    SNMP_USERS_READ_PAYLOAD,
    SNMP_V3_RECEIVERS_READ_PAYLOAD,
    STATIC_ROUTES_READ_PAYLOAD,
)

# Payloads use the exporter shapes covered by the existing family refresh tests.
READ_PAYLOADS = {
    "bfd": BFD_INTERFACE_READ_PAYLOAD,
    "l2_service": _NSO_ENTRY,
    "logging": LOGGING_LEVELS_READ_PAYLOAD,
    "snmp": {
        **SNMP_COMMUNITIES_READ_PAYLOAD,
        **SNMP_USERS_READ_PAYLOAD,
        **SNMP_SYSTEM_READ_PAYLOAD,
        "host": SNMP_RECEIVERS_READ_PAYLOAD["host"] + SNMP_V3_RECEIVERS_READ_PAYLOAD["host"],
    },
    "static_route": STATIC_ROUTES_READ_PAYLOAD,
}

EXPECTED_COVERAGE = {
    "bfd": ["bound_port", "enabled", "interface_name", "micro_bfd", "min_rx", "min_tx", "multiplier"],
    "l2_service": ["inner_tag", "outer_tag", "port", "sap_id", "service_id", "service_name", "service_type"],
    "logging": [
        "address",
        "console_severity",
        "facility",
        "module_severity",
        "monitor_severity",
        "port",
        "severity",
        "source",
        "transport",
        "vrf",
    ],
    "snmp": [
        "access",
        "acl",
        "address",
        "contact",
        "has_auth_secret",
        "has_priv_secret",
        "has_secret",
        "location",
        "name",
        "notify_type",
        "port",
        "user",
        "username",
        "version",
    ],
    "static_route": [
        "interface_next_hop",
        "metric",
        "name",
        "next_hop",
        "next_hop_vrf",
        "permanent",
        "prefix",
        "tag",
        "vrf",
    ],
}


@pytest.mark.parametrize("family", READ_PAYLOADS)
def test_service_observer_projects_realistic_read_payload(family):
    observation = observe_family(family, deepcopy(READ_PAYLOADS[family]))
    assert observation is not None
    document = observation.document.model_dump()
    assert document["unprojectable"] == []
    assert sorted(observation.coverage.attributes) == EXPECTED_COVERAGE[family]
    if family == "bfd":
        first, second = document["interfaces"]
        assert first == {
            "present": ["enabled", "interface_name", "micro_bfd", "min_rx", "min_tx", "multiplier"],
            "interface_name": "ae10",
            "bound_port": None,
            "min_tx": 300,
            "min_rx": 300,
            "multiplier": 3,
            "micro_bfd": True,
            "enabled": True,
        }
        assert second["bound_port"] == "lag-99"
        assert second["micro_bfd"] is False
    elif family == "l2_service":
        first, second = document["services"]
        assert first["service_name"] == "701"
        assert first["saps"][0] == {
            "present": ["inner_tag", "outer_tag", "port", "sap_id"],
            "sap_id": "1/1/c28/1:100.10",
            "port": "1/1/c28/1",
            "outer_tag": 100,
            "inner_tag": 10,
        }
        assert second["service_id"] == 4022
        assert [sap["outer_tag"] for sap in second["saps"]] == [3999, 4022]
    elif family == "logging":
        assert document["hosts"][0] == {
            "present": ["address"],
            "address": "10.0.0.1",
            "port": None,
            "severity": None,
            "facility": None,
            "transport": None,
            "vrf": None,
            "source": None,
        }
        assert document["local_levels"] == {
            "present": ["console_severity", "module_severity", "monitor_severity"],
            "console_severity": "CRITICAL",
            "monitor_severity": "NOTICE",
            "module_severity": "NOTICE",
        }
    elif family == "snmp":
        assert document["communities"][0] == {
            "present": ["access", "acl", "has_secret", "name"],
            "name": "abc123def456abcd",
            "access": "RO",
            "acl": "20",
            "has_secret": True,
        }
        assert len(document["communities"]) == 2
        assert document["users"][0] == {
            "present": ["has_auth_secret", "has_priv_secret", "username"],
            "username": "monitor",
            "has_auth_secret": True,
            "has_priv_secret": False,
        }
        assert len(document["users"]) == 2
        assert document["hosts"][0] == {
            "present": ["address", "notify_type", "port", "version"],
            "address": "10.0.1.100",
            "version": "2c",
            "notify_type": "trap",
            "port": 162,
            "user": None,
        }
        assert document["hosts"][1] == {
            "present": ["address", "notify_type", "user", "version"],
            "address": "10.0.1.101",
            "version": "3",
            "notify_type": "inform",
            "port": None,
            "user": "netmon-v3",
        }
        assert document["system"] == {
            "present": ["contact", "location"],
            "location": "ITC-Lab",
            "contact": "noc@example.com",
        }
    else:
        first, second = document["routes"]
        assert first["present"] == ["next_hop", "prefix", "vrf"]
        assert first["vrf"] == ""
        assert first["prefix"] == "10.0.0.0/8"
        assert first["next_hop"] == "192.168.1.1"
        assert first["metric"] is None
        assert second["present"] == ["metric", "next_hop", "prefix", "vrf"]
        assert second["vrf"] == "MGMT"
        assert second["metric"] == 1


def test_logging_document_keeps_all_remote_host_fields():
    observation = observe_family(
        "logging",
        {
            "host": [
                {
                    "address": "198.18.0.10",
                    "port": 514,
                    "severity": "WARNING",
                    "facility": "LOCAL7",
                    "transport": "udp",
                    "vrf": "MGMT",
                    "source": "loopback0",
                }
            ],
        },
    )
    assert observation is not None
    (host,) = observation.document.hosts
    assert host.model_dump() == {
        "present": ["address", "facility", "port", "severity", "source", "transport", "vrf"],
        "address": "198.18.0.10",
        "port": 514,
        "severity": "WARNING",
        "facility": "LOCAL7",
        "transport": "udp",
        "vrf": "MGMT",
        "source": "loopback0",
    }


def test_static_route_document_keeps_zero_false_and_interface_next_hop_fields():
    observation = observe_family(
        "static_route",
        {
            "route": [
                {
                    "vrf": "",
                    "prefix": "198.18.0.0/24",
                    "next-hop": "198.18.1.1",
                    "metric": 0,
                    "permanent": False,
                    "tag": 0,
                    "name": "example route",
                },
                {
                    "vrf": "MGMT",
                    "prefix": "198.19.0.0/24",
                    "next-hop": "",
                    "interface-next-hop": "loopback0",
                    "next-hop-vrf": "DEFAULT",
                },
            ]
        },
    )
    assert observation is not None
    first, second = observation.document.routes
    assert first.metric == first.tag == 0
    assert first.permanent is False
    assert first.name == "example route"
    assert second.next_hop == ""
    assert second.interface_next_hop == "loopback0"
    assert second.next_hop_vrf == "DEFAULT"


@pytest.mark.parametrize("family", ["bfd", "l2_service", "logging", "snmp", "static_route"])
def test_service_family_has_an_authoritative_empty_observation(family):
    observation = observe_family(family, {})
    assert observation is not None
    assert observation.family == family
    assert observation.document.unprojectable == []
    assert observation.coverage.attributes


@pytest.mark.parametrize(
    ("family", "payload", "reason"),
    [
        ("bfd", {"interface": [{"interface-name": "ae10", "min-tx": False}]}, "interface[0]: invalid integer"),
        ("l2_service", {"service": [{"service-name": "epipe", "sap": [7]}]}, "service[0].saps[0]: expected object"),
        ("logging", {"host": [{"address": "198.18.0.10", "port": False}]}, "host[0]: invalid integer"),
        ("snmp", {"community": [{"name": "placeholder-plaintext-community"}]}, "community[0]: invalid name"),
        ("static_route", {"route": [{"prefix": "198.18.0.0/24", "metric": False}]}, "route[0]: invalid integer"),
    ],
)
def test_service_observer_reports_unprojectable_entries(family, payload, reason):
    observation = observe_family(family, payload)
    assert observation is not None
    (invalid,) = observation.document.unprojectable
    assert invalid.index == 0
    assert invalid.reason.startswith(reason)
    assert "placeholder-plaintext-community" not in observation.document.model_dump_json()


@pytest.mark.parametrize(
    ("family", "wire", "field"),
    [
        ("bfd", "interface", "interfaces"),
        ("l2_service", "service", "services"),
        ("logging", "host", "hosts"),
        ("snmp", "host", "hosts"),
        ("static_route", "route", "routes"),
    ],
)
def test_service_documents_keep_absent_null_and_empty_lists_distinct(family, wire, field):
    documents = [observe_family(family, payload).document.model_dump() for payload in ({}, {wire: None}, {wire: []})]
    assert documents[0][field] == []
    assert documents[0]["present"] == []
    assert documents[1][field] is None
    assert documents[1]["present"] == [field]
    assert documents[2][field] == []
    assert documents[2]["present"] == [field]
    assert len({digest_document(document) for document in documents}) == 3


@pytest.mark.parametrize("family", READ_PAYLOADS)
def test_service_observers_sort_read_entries(family):
    payload = deepcopy(READ_PAYLOADS[family])
    reordered = deepcopy(payload)
    for value in reordered.values():
        if isinstance(value, list):
            value.reverse()
            for row in value:
                if isinstance(row, dict) and isinstance(row.get("sap"), list):
                    row["sap"].reverse()
    first = observe_family(family, payload)
    second = observe_family(family, reordered)
    assert first is not None and second is not None
    first_document = first.document.model_dump(mode="json")
    second_document = second.document.model_dump(mode="json")
    assert first_document == second_document
    assert digest_document(first_document) == digest_document(second_document)


@pytest.mark.parametrize(
    "payload",
    [
        {"host": [{"address": "198.18.0.11", "version": ["3"]}]},
        {"host": [{"address": "198.18.0.11", "notify-type": ["trap", "inform"]}]},
        {"host": [{"address": "198.18.0.11", "version": "2c", "user": "placeholder-plaintext-community"}]},
    ],
)
def test_snmp_invalid_host_values_do_not_enter_the_document(payload):
    observation = observe_family("snmp", payload)
    assert observation is not None
    assert observation.document.hosts == []
    (invalid,) = observation.document.unprojectable
    assert invalid.index == 0
    assert invalid.reason.startswith("host[0]: invalid")
    assert "placeholder-plaintext-community" not in observation.document.model_dump_json()


@pytest.mark.parametrize(
    ("family", "wire"),
    [("bfd", "interface"), ("l2_service", "service"), ("logging", "host"), ("snmp", "host"), ("static_route", "route")],
)
def test_service_observers_report_duplicate_device_identities(family, wire):
    payload = deepcopy(READ_PAYLOADS[family])
    payload[wire].append(deepcopy(payload[wire][0]))
    observation = observe_family(family, payload)
    assert observation is not None
    (invalid,) = observation.document.unprojectable
    assert invalid.index == len(payload[wire]) - 1
    assert invalid.reason.endswith("duplicate identity")


def test_bfd_document_keeps_missing_null_false_and_zero_fields_distinct():
    payload = {
        "interface": [
            {"interface-name": "ae1"},
            {"interface-name": "ae2", "enabled": None, "min-tx": None},
            {"interface-name": "ae3", "enabled": False, "min-tx": 0, "bound-port": ""},
        ]
    }
    observation = observe_family("bfd", payload)
    assert observation is not None
    missing, null, explicit = observation.document.interfaces
    assert missing.present == ["interface_name"]
    assert missing.enabled is None and missing.min_tx is None
    assert null.present == ["enabled", "interface_name", "min_tx"]
    assert null.enabled is None and null.min_tx is None
    assert explicit.enabled is False
    assert explicit.min_tx == 0
    assert explicit.bound_port == ""


def test_l2_service_document_reports_invalid_saps_without_losing_valid_saps():
    payload = {
        "service": [
            {"service-name": "example", "sap": [{"sap-id": "lag-1:0", "outer-tag": 0}, {"sap-id": "lag-1:0"}, False]}
        ]
    }
    observation = observe_family("l2_service", payload)
    assert observation is not None
    (service,) = observation.document.services
    (sap,) = service.saps
    assert sap.sap_id == "lag-1:0"
    assert sap.outer_tag == 0
    duplicate, malformed = observation.document.unprojectable
    assert duplicate.index == 1 and duplicate.reason.endswith("duplicate identity")
    assert malformed.index == 2 and malformed.reason.endswith("expected object")


@pytest.mark.parametrize("levels", [[], [{"console-severity": "ERROR"}], "ERROR", False])
def test_logging_document_refuses_malformed_local_levels_container(levels):
    observation = observe_family("logging", {"local-levels": levels})
    assert observation is not None
    assert observation.document.local_levels is None
    (invalid,) = observation.document.unprojectable
    assert invalid.index == 0
    assert invalid.reason == "local-levels[0]: expected object"


def test_snmp_document_keeps_system_null_empty_and_missing_distinct():
    observations = [observe_family("snmp", payload) for payload in ({}, {"location": None}, {"location": ""})]
    missing, null, empty = [observation.document.system for observation in observations]
    assert missing.location is None and missing.present == []
    assert null.location is None and null.present == ["location"]
    assert empty.location == "" and empty.present == ["location"]


async def test_static_route_default_identity_does_not_insert_duplicate_mirror_rows(adapter_client):
    device_id = await seed_device(nso_device_name="example-static-observation", netbox_device_id=179001)
    routes = [
        {"prefix": "198.18.0.0/24", "interface-next-hop": "loopback0"},
        {"vrf": "", "prefix": "198.18.0.0/24", "next-hop": "", "interface-next-hop": "loopback0"},
    ]
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        await _upsert_static_routes(db, device, routes, "poll")
        await db.flush()
        mirrored = (await db.scalars(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id))).all()
        (row,) = mirrored
        assert row.vrf == ""
        assert row.next_hop == ""
        assert row.interface_next_hop == "loopback0"
    document = project_static_routes({"route": routes})
    (route,) = document.routes
    assert route.vrf is None
    assert route.next_hop is None
    assert route.present == ["interface_next_hop", "prefix"]
    (invalid,) = document.unprojectable
    assert invalid.index == 1
    assert invalid.reason == "route[1]: duplicate identity"
