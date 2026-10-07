# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Observation publication through real family refreshes and API reads."""

from copy import deepcopy

import pytest

from nso_adapter.core import importer
from nso_adapter.core.bgp import BGP_SPEC
from nso_adapter.core.redistribution import refresh_redistribution_from_outcomes
from nso_adapter.core.refresh_engine import run_family_refresh_from_outcome
from nso_adapter.core.vlan import VLAN_DATABASE_SPEC
from nso_adapter.nso.read_outcome import Freshness, Present, Unavailable, UnavailableReason
from nso_adapter.store.models import Device
from tests.conftest import AUTH, push_seq, seed_device, session
from tests.core.test_read_observation import DeviceReadNso, assert_publication

BGP_PAYLOAD = {
    "router": [{"asn": "64512", "scope": [{"vrf": "", "peer": [{"peer-address": "198.18.0.2", "enabled": False}]}]}]
}
VLAN_PAYLOAD = {"vlan": [{"vlan-id": 10, "name": "device"}]}
OSPF_PAYLOAD = {"instance": [{"process-id": "1", "redistribute": [{"source-protocol": "connected", "metric": 0}]}]}


def _family_payload(family: str) -> dict:
    from tests.core.test_service_observers import READ_PAYLOADS
    from tests.fixtures.switching_read_payloads import SWITCHING_READ_PAYLOADS

    return deepcopy({**READ_PAYLOADS, **SWITCHING_READ_PAYLOADS}[family])


def _family_spec(family: str):
    return importer.projectable_spec("lag_topology" if family == "lag" else family)


class FamilyReadNso(DeviceReadNso):
    def __init__(self):
        super().__init__([])
        self.sections = {
            "interface-attributes": self.section,
            "bgp-config": {"status": "ok", **BGP_PAYLOAD},
            "ospf-config": {"status": "ok", **OSPF_PAYLOAD},
            "isis-interface": {"status": "unsupported"},
            **{
                _family_spec(family).wire_name: {"status": "ok", **_family_payload(family)}
                for family, _path in SEAM_ENDPOINTS
            },
        }

    def respond(self, request):
        import httpx

        if "device-state" in request.url.path:
            wire = request.url.path.rsplit("/", 1)[-1]
            if wire.startswith("device="):
                return httpx.Response(
                    200, json={"device": [{"device-name": wire.removeprefix("device="), **self.sections}]}
                )
            return httpx.Response(200, json={wire: self.sections.get(wire, {"status": "unsupported"})})
        return super().respond(request)


@pytest.mark.parametrize("seam", ["refresh", "importer"])
async def test_new_family_observations_are_stored_and_served(adapter_client, monkeypatch, seam):
    device_id = await seed_device(nso_device_name="observation-device")
    nso = FamilyReadNso()
    monkeypatch.setitem(importer._nso_clients, "nso-dev", nso)
    monkeypatch.setattr(importer, "_netbox_client", None)
    if seam == "refresh":
        async with session() as db:
            device = await db.get(Device, device_id)
            for family, _path in SEAM_ENDPOINTS:
                await run_family_refresh_from_outcome(
                    db, device, _family_spec(family), Present(_family_payload(family), Freshness.fresh)
                )
            await run_family_refresh_from_outcome(db, device, BGP_SPEC, Present(BGP_PAYLOAD, Freshness.fresh))
            await refresh_redistribution_from_outcomes(
                db,
                device,
                {
                    "ospf": Present(OSPF_PAYLOAD, Freshness.fresh),
                    "bgp": Present(BGP_PAYLOAD, Freshness.fresh),
                    "isis": Unavailable(UnavailableReason.unsupported),
                },
            )
    else:
        async with session() as db:
            await importer.sync_device(device_id, db, comprehensive=True)
    for family, path in SEAM_ENDPOINTS:
        response = await adapter_client.get(f"/api/v1/devices/{device_id}/{path}", headers=AUTH)
        assert response.status_code == 200, (family, response.text)
        assert_publication(response.json(), family)
    for family, path in [("bgp", "bgp-config"), ("redistribution", "redistribution")]:
        response = await adapter_client.get(f"/api/v1/devices/{device_id}/{path}", headers=AUTH)
        assert response.status_code == 200, response.text
        observation = assert_publication(response.json(), family)
        assert observation["document"]["unprojectable"] == []
        if family == "redistribution":
            components = observation["coverage"]["components"]
            assert {component["protocol"] for component in components} == {"bgp", "ospf"}
            ospf = next(component for component in components if component["protocol"] == "ospf")
            assert ospf["destinations"] == ["1"]
            assert ospf["sources"] == [{"protocol": "connected", "reference": ""}]
            assert observation["document"]["entries"][0]["metric"] == 0


async def test_vlan_intent_cannot_change_the_device_observation(adapter_client):
    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        await run_family_refresh_from_outcome(db, device, VLAN_DATABASE_SPEC, Present(VLAN_PAYLOAD, Freshness.fresh))
    path = f"/api/v1/devices/{device_id}/vlan-database"
    old = (await adapter_client.get(path, headers=AUTH)).json()["observation"]
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent",
        headers=AUTH | push_seq(),
        json={"vlans": [{"vlan_id": 10, "name": "intent"}]},
    )
    assert response.status_code == 200, response.text
    assert (await adapter_client.get(path, headers=AUTH)).json()["observation"] == old


FAMILY_ENDPOINTS = [
    ("lag", "lag-topology"),
    ("lag_config", "lag-config"),
    ("vlan", "vlan-database"),
    ("switchport", "switchport"),
    ("interface_mtu", "interface-mtu"),
    ("svi", "svi"),
    ("subinterface", "subinterface"),
    ("bfd", "bfd"),
    ("l2_service", "l2-services"),
    ("logging", "logging-config"),
    ("snmp", "snmp-config"),
    ("static_route", "static-routes"),
    ("bgp", "bgp-config"),
    ("isis", "isis-interfaces"),
    ("ospf", "ospf"),
    ("route_policy", "route-policy"),
]

# Routing families have explicit payloads in the seam test; the rest use the shared read fixtures.
SEAM_ENDPOINTS = [(f, p) for f, p in FAMILY_ENDPOINTS if f not in {"bgp", "isis", "ospf", "route_policy"}]


@pytest.mark.parametrize("family,path", FAMILY_ENDPOINTS)
async def test_family_api_distinguishes_no_read_empty_and_failed_read(adapter_client, family, path):
    device_id = await seed_device(nso_device_name="observation-device")
    endpoint = f"/api/v1/devices/{device_id}/{path}"
    unread = await adapter_client.get(endpoint, headers=AUTH)
    assert unread.status_code == 200, unread.text
    assert unread.json()["observation"] is None
    assert unread.json()["read_state"]["payload_revision"] is None
    spec = importer.projectable_spec("lag_topology" if family == "lag" else family)
    async with session() as db:
        device = await db.get(Device, device_id)
        await run_family_refresh_from_outcome(db, device, spec, Present({}, Freshness.fresh))
    published = await adapter_client.get(endpoint, headers=AUTH)
    assert published.status_code == 200, published.text
    old = assert_publication(published.json(), family)
    assert old["document"]["unprojectable"] == []
    async with session() as db:
        device = await db.get(Device, device_id)
        await run_family_refresh_from_outcome(db, device, spec, Unavailable(UnavailableReason.read_error))
    failed = await adapter_client.get(endpoint, headers=AUTH)
    assert failed.status_code == 200, failed.text
    assert failed.json()["observation"] == old
    assert failed.json()["read_state"]["attempt_id"] > old["revision"]
    assert failed.json()["read_state"]["payload_revision"] == old["revision"]


async def test_partial_redistribution_never_claims_retained_component_as_observed(adapter_client):
    device_id = await seed_device(nso_device_name="observation-device")
    initial = {
        "ospf": Present(OSPF_PAYLOAD, Freshness.fresh),
        "isis": Present({}, Freshness.fresh),
        "bgp": Present({}, Freshness.fresh),
    }
    async with session() as db:
        device = await db.get(Device, device_id)
        await refresh_redistribution_from_outcomes(db, device, initial)
    endpoint = f"/api/v1/devices/{device_id}/redistribution"
    old = (await adapter_client.get(endpoint, headers=AUTH)).json()["observation"]
    async with session() as db:
        device = await db.get(Device, device_id)
        await refresh_redistribution_from_outcomes(
            db,
            device,
            {
                "ospf": Unavailable(UnavailableReason.read_error),
                "isis": Present({}, Freshness.fresh),
                "bgp": Present({}, Freshness.fresh),
            },
        )
    response = await adapter_client.get(endpoint, headers=AUTH)
    assert response.status_code == 200, response.text
    body = response.json()
    observation = assert_publication(body, "redistribution")
    assert len(body["entries"]) == 1
    assert observation["document"]["entries"] == []
    assert {component["protocol"] for component in observation["coverage"]["components"]} == {"bgp", "isis"}
    assert observation["revision"] > old["revision"]
    async with session() as db:
        device = await db.get(Device, device_id)
        await refresh_redistribution_from_outcomes(
            db, device, {protocol: Unavailable(UnavailableReason.read_error) for protocol in ["bgp", "isis", "ospf"]}
        )
    assert (await adapter_client.get(endpoint, headers=AUTH)).json()["observation"] == observation


@pytest.mark.parametrize(
    "protocol,key,identity,value", [("ospf", "instance", "process-id", "1"), ("isis", "process", "process-tag", "CORE")]
)
async def test_redistribution_rejects_malformed_asn_on_duplicate_destination(
    adapter_client, protocol, key, identity, value
):
    from nso_adapter.domain.asn import AsnRuleViolation

    device_id = await seed_device(nso_device_name="observation-device")
    destination = {identity: value, "redistribute": [{"source-protocol": "connected"}]}
    initial = {name: Present({}, Freshness.fresh) for name in ["ospf", "isis", "bgp"]}
    initial[protocol] = Present({key: [destination]}, Freshness.fresh)
    async with session() as db:
        device = await db.get(Device, device_id)
        await refresh_redistribution_from_outcomes(db, device, initial)
    endpoint = f"/api/v1/devices/{device_id}/redistribution"
    old = (await adapter_client.get(endpoint, headers=AUTH)).json()["observation"]
    invalid = {identity: value, "redistribute": [{"source-protocol": "bgp", "source-ref": "064512"}]}
    initial[protocol] = Present({key: [destination, invalid]}, Freshness.fresh)
    async with session() as db:
        device = await db.get(Device, device_id)
        with pytest.raises(AsnRuleViolation):
            await refresh_redistribution_from_outcomes(db, device, initial)
    body = (await adapter_client.get(endpoint, headers=AUTH)).json()
    assert body["observation"] == old
    assert body["read_state"]["payload_revision"] == old["revision"]
    assert body["read_state"]["attempt_id"] > old["revision"]
    assert body["entries"][0]["source_protocol"] == "connected"


@pytest.mark.parametrize("family,path", FAMILY_ENDPOINTS + [("redistribution", "redistribution")])
@pytest.mark.parametrize(
    "placeholder", ["placeholder-observation-secret", "$8$secret=="], ids=["plaintext", "arcos-ciphertext"]
)
async def test_observation_json_never_contains_credentials(adapter_client, family, path, placeholder):
    import json
    from copy import deepcopy

    from nso_adapter.secrets.refs import secret_fingerprint
    from tests._secret_discipline import assert_text_contains, assert_text_free_of
    from tests.core.test_read_observation import stored_observations
    from tests.core.test_service_observers import READ_PAYLOADS
    from tests.fixtures.switching_read_payloads import SWITCHING_READ_PAYLOADS

    payload = deepcopy({**READ_PAYLOADS, **SWITCHING_READ_PAYLOADS}.get(family, {}))
    payload["unsupported-credential"] = placeholder
    if family == "bgp":
        payload = {
            "router": [
                {
                    "asn": "64512",
                    "scope": [{"vrf": "", "peer": [{"peer-address": "198.18.0.2", "password": placeholder}]}],
                }
            ]
        }
        if placeholder == "$8$secret==":
            from tests.fixtures.routing_read_payloads import BGP_ARCOS_CIPHERTEXT_READ

            payload = deepcopy(BGP_ARCOS_CIPHERTEXT_READ)
    elif family == "isis":
        payload = {
            "process": [
                {
                    "process-tag": "CORE",
                    "area-auth-key": placeholder,
                    "domain-auth-key": placeholder,
                    "area-auth-present": True,
                    "level": [{"level": 2, "auth-key": placeholder}],
                }
            ],
            "interface": [
                {
                    "interface-name": "Gi0/1",
                    "af": "ipv4",
                    "process-tag": "CORE",
                    "hello-auth-key": placeholder,
                    "level": [{"level": 2, "auth-key": placeholder}],
                }
            ],
        }
    elif family == "ospf":
        from tests.fixtures.routing_read_payloads import OSPF_INSTANCES_READ

        payload = {
            "instance": deepcopy(OSPF_INSTANCES_READ),
            "interface": [
                {
                    "interface-name": "Gi0/1",
                    "process-id": "1",
                    "auth-key": placeholder,
                }
            ],
        }
    elif family == "route_policy":
        from tests.fixtures.routing_read_payloads import ROUTE_POLICY_COMMUNITIES_READ

        payload = deepcopy(ROUTE_POLICY_COMMUNITIES_READ)
        next(iter(payload.values()))[0]["credential"] = placeholder
    elif family == "redistribution":
        payload = {
            "bgp": {
                "router": [
                    {
                        "asn": "64512",
                        "scope": [
                            {
                                "vrf": "",
                                "peer": [
                                    {"peer-address": "198.18.0.2", "password": placeholder},
                                ],
                            }
                        ],
                    }
                ]
            },
            "isis": {
                "process": [{"process-tag": "CORE", "area-auth-key": placeholder, "domain-auth-key": placeholder}]
            },
            "ospf": OSPF_PAYLOAD,
        }
    elif family == "snmp":
        payload = {
            "community": [{"name": secret_fingerprint(placeholder), "has-secret": True}],
            "v3-user": [{"username": "monitor", "has-auth-secret": True, "auth-key": placeholder}],
        }
    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        if family == "redistribution":
            await refresh_redistribution_from_outcomes(
                db, device, {protocol: Present(component, Freshness.fresh) for protocol, component in payload.items()}
            )
        else:
            spec = importer.projectable_spec("lag_topology" if family == "lag" else family)
            await run_family_refresh_from_outcome(db, device, spec, Present(payload, Freshness.fresh))
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/{path}", headers=AUTH)
    assert_text_free_of(json.dumps(response.json()["observation"]), [placeholder])
    assert response.status_code == 200
    observation = assert_publication(response.json(), family)
    rows = await stored_observations(device_id)
    for row in rows:
        assert_text_free_of(json.dumps(row.document), [placeholder])
    assert rows
    if family == "bgp":
        peer = observation["document"]["routers"][0]["scope"][0]["peer"][0]
        assert peer["password_present"] is True
        assert "password_fingerprint" not in peer
        assert "password" in observation["coverage"]["not_comparable"]
        assert_text_contains(response.json()["routers"][0]["scopes"][0]["peers"][0]["password"], [placeholder])
    elif family == "isis":
        process = observation["document"]["processes"][0]
        assert process["area_auth_key_present"] is True
        assert "area_auth_key_fingerprint" not in process
        assert "domain_auth_key_fingerprint" not in process
        assert {"area_auth_key", "domain_auth_key"} <= set(observation["coverage"]["not_comparable"])
        assert process["domain_auth_key_present"] is True
        assert process["level"][0]["auth_key_present"] is True
        interface = observation["document"]["interfaces"][0]
        assert interface["hello_auth_key_present"] is True
        assert interface["level"][0]["auth_key_present"] is True
        assert {"hello_auth_key", "level.auth_key"} <= set(observation["coverage"]["not_comparable"])
    elif family == "ospf":
        assert observation["document"]["interfaces"][0]["auth_key_present"] is True
        assert "auth_key" in observation["coverage"]["not_comparable"]


async def test_ospf_redistribution_keeps_same_process_in_two_vrfs(adapter_client):
    payload = {
        "instance": [
            {"process-id": "1", "vrf": "BLUE", "redistribute": [{"source-protocol": "connected"}]},
            {"process-id": "1", "vrf": "RED", "redistribute": [{"source-protocol": "static"}]},
        ]
    }
    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        await refresh_redistribution_from_outcomes(
            db,
            device,
            {
                "ospf": Present(payload, Freshness.fresh),
                "bgp": Present({}, Freshness.fresh),
                "isis": Present({}, Freshness.fresh),
            },
        )
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
    assert response.status_code == 200, response.text
    body = response.json()
    assert sorted((entry["dest_ref"], entry["source_protocol"]) for entry in body["entries"]) == [
        ("1", "connected"),
        ("1", "static"),
    ]
    observation = assert_publication(body, "redistribution")
    component = next(item for item in observation["document"]["components"] if item["protocol"] == "ospf")
    assert [(entry["process_id"], entry["vrf"]) for entry in component["inventory"]] == [("1", "BLUE"), ("1", "RED")]
    assert observation["document"]["unprojectable"] == []


@pytest.mark.parametrize("bandwidth", ["0", "400000", "9007199254740993"])
async def test_isis_wire_uint64_keeps_process_in_mirror_and_observation(adapter_client, bandwidth):
    from sqlalchemy import select

    from nso_adapter.store.models import DeviceIsisProcess

    device_id = await seed_device(nso_device_name="bandwidth-fixture")
    async with session() as db:
        device = await db.get(Device, device_id)
        spec = importer.projectable_spec("isis")
        await run_family_refresh_from_outcome(
            db,
            device,
            spec,
            Present({"process": [{"process-tag": "CORE", "reference-bandwidth": 100}]}, Freshness.fresh),
        )
        await run_family_refresh_from_outcome(
            db,
            device,
            spec,
            Present({"process": [{"process-tag": "CORE", "reference-bandwidth": bandwidth}]}, Freshness.fresh),
        )
        rows = (await db.scalars(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device_id))).all()
        assert [(row.process_tag, row.reference_bandwidth) for row in rows] == [("CORE", int(bandwidth))]
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/isis-interfaces", headers=AUTH)
    assert response.status_code == 200
    observed = assert_publication(response.json(), "isis")
    assert observed["document"]["processes"][0]["reference_bandwidth"] == int(bandwidth)
    assert observed["document"]["unprojectable"] == []


DROPPED_ENTRY_COLLECTIONS = {
    "lag": "lag",
    "bfd": "interface",
    "l2_service": "service",
    "logging": "host",
    "snmp": "host",
    "static_route": "route",
    "interface_mtu": "interface",
    "lag_config": "lag",
}


def _mirror_rows(body: object) -> object:
    if isinstance(body, dict):
        return {
            key: _mirror_rows(value)
            for key, value in body.items()
            if key not in {"read_state", "observation", "id"} and not key.endswith("_at")
        }
    if isinstance(body, list):
        return [_mirror_rows(item) for item in body]
    return body


@pytest.mark.parametrize(("family", "collection"), DROPPED_ENTRY_COLLECTIONS.items())
async def test_refresh_logs_each_entry_the_projection_drops(adapter_client, family, collection):
    from structlog.testing import capture_logs

    device_id = await seed_device(nso_device_name="observation-device")
    path = dict(FAMILY_ENDPOINTS)[family]
    spec = _family_spec(family)
    payload = _family_payload(family)
    async with session() as db:
        device = await db.get(Device, device_id)
        await run_family_refresh_from_outcome(db, device, spec, Present(deepcopy(payload), Freshness.fresh))
    clean = await adapter_client.get(f"/api/v1/devices/{device_id}/{path}", headers=AUTH)
    payload[collection] = [*payload[collection], "not-an-object"]
    async with session() as db:
        device = await db.get(Device, device_id)
        with capture_logs() as logs:
            await run_family_refresh_from_outcome(db, device, spec, Present(payload, Freshness.fresh))
    dirty = await adapter_client.get(f"/api/v1/devices/{device_id}/{path}", headers=AUTH)
    event = "lag_topology.entry_skipped" if family == "lag" else f"{spec.name}.entry_skipped"
    skipped = [record for record in logs if record["event"] == event]
    assert [(record["device_id"], record["reason"].endswith("expected object")) for record in skipped] == [
        (device_id, True)
    ], logs
    assert (clean.status_code, dirty.status_code) == (200, 200)
    assert _mirror_rows(dirty.json()) == _mirror_rows(clean.json())
