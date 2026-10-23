# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Empty versus unavailable collections end to end: wire, refresh, store and family GETs (#1833).

The fake NSO answers at its RESTCONF boundary only. Everything after it is the real client
decoder, refresh engine, PostgreSQL store and FastAPI app.
"""

from __future__ import annotations

import json
from copy import deepcopy
from urllib.parse import unquote

import httpx
import pytest
from sqlalchemy import select

from nso_adapter.config import NsoInstanceConfig
from nso_adapter.core import importer
from nso_adapter.core.bgp import refresh_bgp_config_for_device
from nso_adapter.core.interface_ip import refresh_interface_ips_for_device
from nso_adapter.core.isis import refresh_isis_interfaces_for_device
from nso_adapter.core.l2_service import refresh_l2_services_for_device
from nso_adapter.core.logging_config import refresh_logging_config_for_device
from nso_adapter.core.redistribution import refresh_redistribution_for_device
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.collection_coverage import FAMILIES, MARKER
from nso_adapter.store.models import (
    Device,
    DeviceBgpPeerGroup,
    DeviceIsisProcess,
    DeviceLoggingHost,
    DeviceRedistribution,
    InterfaceIpAddress,
    MappingStatus,
)
from tests.collection_coverage_helpers import exported
from tests.conftest import AUTH, seed_device, session

DEVICE = "PLACEHOLDER-RTR-01"


class CoverageNso(NsoClient):
    """Serve wire sections from one table on the section, document and action surfaces."""

    def __init__(self, sections: dict[str, dict]):
        super().__init__(
            NsoInstanceConfig(name="nso-dev", base_url="http://nso.example", username_ref="USER", password_ref="PASS"),
            "placeholder-user",
            "placeholder-password",
        )
        self.sections = sections

    def _client(self, timeout=None):
        return httpx.AsyncClient(transport=httpx.MockTransport(self.respond))

    def _section(self, wire: str) -> dict:
        return deepcopy(self.sections.get(wire, {"status": "unsupported"}))

    def respond(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith(":device-state-read/run"):
            asked = json.loads(request.content)["network-state-export:input"]
            output = {"device-name": asked["device"], "atomic": True}
            output |= {wire: self._section(wire) for wire in asked["family"]}
            return httpx.Response(200, json={"network-state-export:output": output})
        if "device-state" in path:
            tail = unquote(path.rsplit("/", 1)[-1])
            if tail.startswith("device="):
                entry = {"device-name": tail.removeprefix("device=")}
                entry |= {wire: self._section(wire) for wire in FAMILIES}
                return httpx.Response(200, json={"network-state-export:device": [entry]})
            return httpx.Response(200, json={f"network-state-export:{tail}": self._section(tail)})
        if request.method == "POST":
            return httpx.Response(200, json={"tailf-ncs:output": {"result": True}})
        return httpx.Response(200, json={"tailf-ncs:device": {"device-type": {"cli": {"ned-id": "test-ned"}}}})


def _peer(address: str) -> dict:
    return {"peer-address": address, "enabled": True, "peer-address-family": [{"afi": "ipv4-unicast"}]}


def _bgp_wire(*scopes: dict) -> dict:
    """A bgp-config section whose scopes state their own coverage."""
    return {"status": "ok", MARKER: ["router"], "router": [{"asn": "64512", MARKER: ["scope"], "scope": list(scopes)}]}


def _groupless_scope(vrf: str) -> dict:
    """A scope that read address families, peers and groups and found no group."""
    return {
        "vrf": vrf,
        MARKER: ["address-family", "peer", "peer-group"],
        "address-family": [{"afi": "ipv4-unicast", MARKER: ["redistribute"]}],
        "peer": [{**_peer("198.18.0.2"), MARKER: ["peer-address-family"]}],
    }


async def _refresh(device_id: int, refresh, nso: NsoClient) -> bool:
    async with session() as db:
        device = await db.get(Device, device_id)
        return await refresh(db, device, nso)


async def _family(client, device_id: int, path: str) -> dict:
    response = await client.get(f"/api/v1/devices/{device_id}/{path}", headers=AUTH)
    assert response.status_code == 200, response.text
    return response.json()


def _scope_by_vrf(body: dict) -> dict[str, dict]:
    return {scope["vrf"]: scope for scope in body["observation"]["document"]["routers"][0]["scope"]}


async def test_groupless_scopes_reach_the_observation_as_covered_empty_groups(adapter_client):
    device_id = await seed_device(nso_device_name=DEVICE)
    nso = CoverageNso({"bgp-config": _bgp_wire(_groupless_scope(""), _groupless_scope("CUST-A"))})
    assert await _refresh(device_id, refresh_bgp_config_for_device, nso) is True
    body = await _family(adapter_client, device_id, "bgp-config")
    assert body["read_state"]["outcome"] == "present" and body["read_state"]["result"] == "replaced"
    assert body["observation"]["document"]["unprojectable"] == []
    for scope in _scope_by_vrf(body).values():
        assert scope["peer_group"] == []
        assert {"address_family", "peer", "peer_group"} <= set(scope["present"])
        assert scope["address_family"][0]["afi"] == "ipv4-unicast"
        assert scope["peer"][0]["peer_address"] == "198.18.0.2"


async def test_a_scope_that_does_not_cover_groups_keeps_rows_and_publishes_nothing(adapter_client):
    device_id = await seed_device(nso_device_name=DEVICE)
    grouped = _groupless_scope("")
    grouped["peer-group"] = [{"name": "G1", MARKER: ["peer-group-address-family"], "peer-group-address-family": []}]
    nso = CoverageNso({"bgp-config": _bgp_wire(grouped)})
    assert await _refresh(device_id, refresh_bgp_config_for_device, nso) is True
    first = await _family(adapter_client, device_id, "bgp-config")

    unread = _groupless_scope("")
    unread[MARKER] = ["address-family", "peer"]
    nso.sections["bgp-config"] = _bgp_wire(unread)
    assert await _refresh(device_id, refresh_bgp_config_for_device, nso) is True
    second = await _family(adapter_client, device_id, "bgp-config")
    assert second["read_state"]["outcome"] == "unavailable"
    assert second["read_state"]["reason"] == "not_covered"
    assert second["read_state"]["result"] == "kept"
    assert second["read_state"]["payload_revision"] == first["read_state"]["payload_revision"]
    assert second["observation"] == first["observation"]
    async with session() as db:
        groups = (await db.execute(select(DeviceBgpPeerGroup.name))).scalars().all()
    assert groups == ["G1"]


@pytest.mark.parametrize(
    "scope_marker",
    [["address-family", "peer", "peer-group", "neighbor"], ["peer", "peer"], "peer"],
    ids=["unknown-name", "duplicate", "scalar"],
)
async def test_a_marker_that_breaks_the_contract_keeps_rows(adapter_client, scope_marker):
    device_id = await seed_device(nso_device_name=DEVICE)
    nso = CoverageNso({"bgp-config": _bgp_wire(_groupless_scope(""))})
    assert await _refresh(device_id, refresh_bgp_config_for_device, nso) is True
    first = await _family(adapter_client, device_id, "bgp-config")
    broken = _groupless_scope("")
    broken[MARKER] = scope_marker
    nso.sections["bgp-config"] = _bgp_wire(broken)
    assert await _refresh(device_id, refresh_bgp_config_for_device, nso) is False
    second = await _family(adapter_client, device_id, "bgp-config")
    assert (second["read_state"]["outcome"], second["read_state"]["reason"]) == ("unavailable", "read_error")
    assert second["read_state"]["payload_revision"] == first["read_state"]["payload_revision"]


async def test_a_collection_served_without_a_statement_is_unavailable_not_an_error(adapter_client):
    device_id = await seed_device(nso_device_name=DEVICE)
    nso = CoverageNso({"bgp-config": _bgp_wire(_groupless_scope(""))})
    assert await _refresh(device_id, refresh_bgp_config_for_device, nso) is True
    first = await _family(adapter_client, device_id, "bgp-config")
    unmarked = _bgp_wire(_groupless_scope(""))
    unmarked["router"][0]["scope"][0]["peer"][0][MARKER] = []  # the exporter skipped part of it
    nso.sections["bgp-config"] = unmarked
    assert await _refresh(device_id, refresh_bgp_config_for_device, nso) is True
    second = await _family(adapter_client, device_id, "bgp-config")
    assert (second["read_state"]["outcome"], second["read_state"]["reason"]) == ("unavailable", "not_covered")
    assert second["read_state"]["payload_revision"] == first["read_state"]["payload_revision"]


async def test_an_unmarked_collection_in_one_family_leaves_the_other_families_of_the_document(
    adapter_client, monkeypatch
):
    device_id = await seed_device(nso_device_name=DEVICE, attributes=["description", "enabled"])
    unmarked = _bgp_wire(_groupless_scope(""))
    unmarked["router"][0]["scope"][0]["peer"][0][MARKER] = []
    nso = CoverageNso(
        {
            "bgp-config": unmarked,
            "interface-attributes": exported(
                "interface-attributes", {"status": "ok", "interface": [{"interface-name": "port0", "enabled": True}]}
            ),
        }
    )
    monkeypatch.setitem(importer._nso_clients, "nso-dev", nso)
    monkeypatch.setattr(importer, "_netbox_client", None)
    async with session() as db:
        await importer.sync_device(device_id, db)
    bgp = await _family(adapter_client, device_id, "bgp-config")
    assert (bgp["read_state"]["outcome"], bgp["read_state"]["reason"]) == ("unavailable", "not_covered")
    attributes = await _family(adapter_client, device_id, "interfaces-doc")
    assert attributes["read_state"]["result"] == "replaced"
    assert attributes["observation"]["document"]["interfaces"][0]["name"] == "port0"


async def test_a_marker_inside_a_container_keeps_rows_and_revision(adapter_client):
    device_id = await seed_device(nso_device_name=DEVICE)
    section = {"status": "ok", MARKER: ["host"], "host": [{"address": "198.18.0.10"}]}
    nso = CoverageNso({"logging-config": section})
    assert await _refresh(device_id, refresh_logging_config_for_device, nso) is True
    first = await _family(adapter_client, device_id, "logging-config")
    nso.sections["logging-config"] = {"status": "ok", MARKER: ["host"], "local-levels": {MARKER: []}}
    assert await _refresh(device_id, refresh_logging_config_for_device, nso) is False
    second = await _family(adapter_client, device_id, "logging-config")
    assert (second["read_state"]["outcome"], second["read_state"]["reason"]) == ("unavailable", "read_error")
    assert second["read_state"]["payload_revision"] == first["read_state"]["payload_revision"]
    async with session() as db:
        assert (await db.execute(select(DeviceLoggingHost.address))).scalars().all() == ["198.18.0.10"]


@pytest.mark.parametrize("atomic", [False, True], ids=["document", "action"])
async def test_sync_decodes_the_document_and_the_action(adapter_client, monkeypatch, atomic):
    device_id = await seed_device(nso_device_name=DEVICE, attributes=["description", "enabled"])
    nso = CoverageNso(
        {
            "bgp-config": _bgp_wire(_groupless_scope("")),
            "interface-attributes": exported(
                "interface-attributes", {"status": "ok", "interface": [{"interface-name": "port0", "enabled": True}]}
            ),
        }
    )
    monkeypatch.setitem(importer._nso_clients, "nso-dev", nso)
    monkeypatch.setattr(importer, "_netbox_client", None)
    async with session() as db:
        await importer.sync_device(device_id, db, atomic=atomic)
    scope = _scope_by_vrf(await _family(adapter_client, device_id, "bgp-config"))[""]
    assert scope["peer_group"] == [] and "peer_group" in scope["present"]


async def test_a_service_without_saps_reports_a_covered_empty_sap_list(adapter_client):
    device_id = await seed_device(nso_device_name=DEVICE)
    section = {"status": "ok", MARKER: ["service"], "service": [{"service-name": "S1", MARKER: ["sap"]}]}
    assert await _refresh(device_id, refresh_l2_services_for_device, CoverageNso({"l2-service": section})) is True
    body = await _family(adapter_client, device_id, "l2-services")
    service = body["observation"]["document"]["services"][0]
    assert (service["saps"], "saps" in service["present"]) == ([], True)
    assert body["observation"]["document"]["unprojectable"] == []


async def test_interface_ip_states_address_presence_per_interface(adapter_client):
    device_id = await seed_device(nso_device_name=DEVICE)
    addressed = {"interface-name": "port1", MARKER: ["address"], "address": [{"address": "198.18.0.1/24", "vrf": ""}]}
    section = {
        "status": "ok",
        MARKER: ["interface"],
        "interface": [{"interface-name": "port0", MARKER: ["address"]}, addressed],
    }
    nso = CoverageNso({"interface-ip": section})
    assert await _refresh(device_id, refresh_interface_ips_for_device, nso) is True
    body = await _family(adapter_client, device_id, "interface-ips")
    document = body["observation"]["document"]
    assert document["present"] == ["interfaces"]
    assert [(item["interface"], item["addresses"], item["present"]) for item in document["interfaces"]][0] == (
        "port0",
        [],
        ["addresses"],
    )
    assert document["interfaces"][1]["addresses"][0]["address"] == "198.18.0.1/24"

    section["interface"][0][MARKER] = []
    section["interface"][1]["address"] = []
    assert await _refresh(device_id, refresh_interface_ips_for_device, nso) is True
    kept = await _family(adapter_client, device_id, "interface-ips")
    assert (kept["read_state"]["outcome"], kept["read_state"]["reason"]) == ("unavailable", "not_covered")
    async with session() as db:
        addresses = (await db.execute(select(InterfaceIpAddress.address))).scalars().all()
    assert addresses == ["198.18.0.1/24"]


@pytest.mark.parametrize("seam", ["sync", "drift"])
async def test_attributes_without_interface_coverage_keep_the_mapping_and_publish_nothing(
    adapter_client, monkeypatch, seam
):
    device_id = await seed_device(nso_device_name=DEVICE, attributes=["description", "enabled"])
    nso = CoverageNso({"interface-attributes": {"status": "ok", MARKER: []}})
    monkeypatch.setitem(importer._nso_clients, "nso-dev", nso)
    monkeypatch.setattr(importer, "_netbox_client", None)
    async with session() as db:
        before = (await db.get(Device, device_id)).mapping_status
        if seam == "sync":
            await importer.sync_device(device_id, db)
        else:
            await importer.detect_drift(device_id, db)
    async with session() as db:
        assert (await db.get(Device, device_id)).mapping_status == before
    body = await _family(adapter_client, device_id, "interfaces-doc")
    assert (body["read_state"]["outcome"], body["read_state"]["reason"]) == ("unavailable", "not_covered")
    assert body["observation"] is None


async def test_attributes_with_a_covered_empty_inventory_publish_it(adapter_client, monkeypatch):
    device_id = await seed_device(nso_device_name=DEVICE, attributes=["description", "enabled"])
    nso = CoverageNso({"interface-attributes": {"status": "ok", MARKER: ["interface"]}})
    monkeypatch.setitem(importer._nso_clients, "nso-dev", nso)
    monkeypatch.setattr(importer, "_netbox_client", None)
    async with session() as db:
        await importer.sync_device(device_id, db)
    async with session() as db:
        assert (await db.get(Device, device_id)).mapping_status == MappingStatus.unmatched_interfaces
    body = await _family(adapter_client, device_id, "interfaces-doc")
    assert body["observation"]["document"] == {"interfaces": [], "present": ["interfaces"], "unprojectable": []}


def _ospf_wire(source: str) -> dict:
    instance = {"process-id": "1", MARKER: ["area", "redistribute"], "redistribute": [{"source-protocol": source}]}
    return {"status": "ok", MARKER: ["instance", "interface"], "instance": [instance]}


def _bgp_redistribute_wire(af_marker: list[str]) -> dict:
    family = {"afi": "ipv4-unicast", MARKER: af_marker}
    if "redistribute" in af_marker:
        family["redistribute"] = [{"source-protocol": "static"}]
    scope = {"vrf": "", MARKER: ["address-family", "peer", "peer-group"], "address-family": [family]}
    return _bgp_wire(scope)


async def test_redistribution_replaces_covered_components_and_keeps_unavailable_ones(adapter_client):
    device_id = await seed_device(nso_device_name=DEVICE)
    nso = CoverageNso({"ospf-config": _ospf_wire("connected"), "bgp-config": _bgp_redistribute_wire(["redistribute"])})
    assert await _refresh(device_id, refresh_redistribution_for_device, nso) is True

    nso.sections = {"ospf-config": _ospf_wire("static"), "bgp-config": _bgp_redistribute_wire([])}
    assert await _refresh(device_id, refresh_redistribution_for_device, nso) is True
    async with session() as db:
        rows = (await db.execute(select(DeviceRedistribution))).scalars().all()
    assert sorted((row.dest_protocol, row.source_protocol) for row in rows) == [("bgp", "static"), ("ospf", "static")]
    body = await _family(adapter_client, device_id, "redistribution")
    assert (body["read_state"]["outcome"], body["read_state"]["result"]) == ("present", "replaced")
    components = {component["protocol"]: component for component in body["observation"]["document"]["components"]}
    assert {protocol: component["status"] for protocol, component in components.items()} == {
        "bgp": "unavailable",
        "isis": "unavailable",
        "ospf": "covered",
    }
    assert components["bgp"]["inventory"] is None and components["bgp"]["present"] == []
    assert [entry["dest_protocol"] for entry in body["observation"]["document"]["entries"]] == ["ospf"]
    assert [component["protocol"] for component in body["observation"]["coverage"]["components"]] == ["ospf"]


ISIS_PROCESS_COLLECTIONS = ["setting", "level", "srv6-locator", "flex-algo", "redistribute"]


def _isis_wire(process_marker: list[str], **process) -> dict:
    entry = {"process-tag": "CORE", MARKER: process_marker, **process}
    return {"status": "ok", MARKER: ["process", "interface"], "process": [entry]}


async def test_isis_keeps_its_rows_when_a_nested_process_collection_is_not_covered(adapter_client):
    device_id = await seed_device(nso_device_name=DEVICE)
    nso = CoverageNso({"isis-interface": _isis_wire(ISIS_PROCESS_COLLECTIONS, level=[{"level": 2, "metric": 10}])})
    assert await _refresh(device_id, refresh_isis_interfaces_for_device, nso) is True
    first = await _family(adapter_client, device_id, "isis-interfaces")
    assert first["observation"]["document"]["unprojectable"] == []

    nso.sections["isis-interface"] = _isis_wire([name for name in ISIS_PROCESS_COLLECTIONS if name != "level"])
    assert await _refresh(device_id, refresh_isis_interfaces_for_device, nso) is True
    kept = await _family(adapter_client, device_id, "isis-interfaces")
    assert (kept["read_state"]["outcome"], kept["read_state"]["reason"]) == ("unavailable", "not_covered")
    assert kept["read_state"]["payload_revision"] == first["read_state"]["payload_revision"]
    async with session() as db:
        row = (await db.execute(select(DeviceIsisProcess))).scalar_one()
        assert row.levels == [{"level": 2, "metric": 10}]

    nso.sections["isis-interface"] = _isis_wire(ISIS_PROCESS_COLLECTIONS)
    assert await _refresh(device_id, refresh_isis_interfaces_for_device, nso) is True
    cleared = await _family(adapter_client, device_id, "isis-interfaces")
    process = cleared["observation"]["document"]["processes"][0]
    assert (process["level"], "level" in process["present"]) == ([], True)
    async with session() as db:
        assert (await db.execute(select(DeviceIsisProcess))).scalar_one().levels is None
