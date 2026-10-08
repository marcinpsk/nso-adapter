# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Observation publication through real family refreshes and API reads."""

from copy import deepcopy

import pytest

from nso_adapter.core import importer
from nso_adapter.core.refresh_engine import run_family_refresh_from_outcome
from nso_adapter.core.vlan import VLAN_DATABASE_SPEC
from nso_adapter.nso.read_outcome import Freshness, Present, Unavailable, UnavailableReason
from nso_adapter.store.models import Device
from tests.conftest import AUTH, push_seq, seed_device, session
from tests.core.test_read_observation import DeviceReadNso, assert_publication

VLAN_PAYLOAD = {"vlan": [{"vlan-id": 10, "name": "device"}]}


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
            "isis-interface": {"status": "unsupported"},
            **{
                _family_spec(family).wire_name: {"status": "ok", **_family_payload(family)}
                for family, _path in FAMILY_ENDPOINTS
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
            for family, _path in FAMILY_ENDPOINTS:
                await run_family_refresh_from_outcome(
                    db, device, _family_spec(family), Present(_family_payload(family), Freshness.fresh)
                )
    else:
        async with session() as db:
            await importer.sync_device(device_id, db, comprehensive=True)
    for family, path in FAMILY_ENDPOINTS:
        response = await adapter_client.get(f"/api/v1/devices/{device_id}/{path}", headers=AUTH)
        assert response.status_code == 200, (family, response.text)
        assert_publication(response.json(), family)


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
]


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


@pytest.mark.parametrize("family,path", FAMILY_ENDPOINTS)
async def test_observation_json_never_contains_credentials(adapter_client, family, path):
    import json
    from copy import deepcopy

    from nso_adapter.secrets.refs import secret_fingerprint
    from tests._secret_discipline import assert_text_free_of
    from tests.core.test_read_observation import stored_observations
    from tests.core.test_service_observers import READ_PAYLOADS
    from tests.fixtures.switching_read_payloads import SWITCHING_READ_PAYLOADS

    placeholder = "placeholder-observation-secret"
    payload = deepcopy({**READ_PAYLOADS, **SWITCHING_READ_PAYLOADS}.get(family, {}))
    payload["unsupported-credential"] = placeholder
    if family == "snmp":
        payload = {
            "community": [{"name": secret_fingerprint(placeholder), "has-secret": True}],
            "v3-user": [{"username": "monitor", "has-auth-secret": True, "auth-key": placeholder}],
        }
    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        spec = importer.projectable_spec("lag_topology" if family == "lag" else family)
        await run_family_refresh_from_outcome(db, device, spec, Present(payload, Freshness.fresh))
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/{path}", headers=AUTH)
    assert_text_free_of(json.dumps(response.json()["observation"]), [placeholder])
    assert response.status_code == 200
    assert_publication(response.json(), family)
    rows = await stored_observations(device_id)
    for row in rows:
        assert_text_free_of(json.dumps(row.document), [placeholder])
    assert rows


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
