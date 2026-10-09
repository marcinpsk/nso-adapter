# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Observation publication through real importers, PostgreSQL, and family GETs."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime

import httpx
import pytest
from sqlalchemy import event, select, text
from structlog.testing import capture_logs

from nso_adapter.config import NsoInstanceConfig
from nso_adapter.core import importer
from nso_adapter.core.interface_ip import INTERFACE_IP_SPEC, refresh_interface_ips_for_device
from nso_adapter.core.refresh_engine import run_family_refresh_from_outcome
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import Freshness, Present
from nso_adapter.store import outcome_store
from nso_adapter.store.models import Device, RefreshOutcome
from tests.conftest import AUTH, push_seq, seed_device, session

SEAMS = ["sync", "drift", "ip"]


class DeviceReadNso(NsoClient):
    """Run the real RESTCONF reader against an in-process network boundary."""

    def __init__(self, entries: list, *, status: str = "ok"):
        super().__init__(
            NsoInstanceConfig(name="nso-dev", base_url="http://nso.example", username_ref="USER", password_ref="PASS"),
            "placeholder-user",
            "placeholder-password",
        )
        self.section = {"status": status, "interface": entries}

    def _client(self, timeout=None):
        return httpx.AsyncClient(transport=httpx.MockTransport(self.respond))

    def respond(self, request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"tailf-ncs:output": {"result": True}})
        if "device-state" in request.url.path:
            wire = request.url.path.rsplit("/", 1)[-1]
            if wire in {"interface-attributes", "interface-ip"}:
                return httpx.Response(200, json={wire: self.section})
            name = wire.removeprefix("device=")
            return httpx.Response(
                200,
                json={"device": [{"device-name": name, "interface-attributes": self.section}]},
            )
        return httpx.Response(200, json={"tailf-ncs:device": {"device-type": {"cli": {"ned-id": "test-ned"}}}})


async def publish(device_id: int, seam: str, nso: DeviceReadNso, monkeypatch):
    monkeypatch.setitem(importer._nso_clients, "nso-dev", nso)
    monkeypatch.setattr(importer, "_netbox_client", None)
    async with session() as db:
        if seam == "sync":
            return await importer.sync_device(device_id, db)
        if seam == "drift":
            return await importer.detect_drift(device_id, db)
        device = await db.get(Device, device_id)
        return await refresh_interface_ips_for_device(db, device, nso)


async def read_family(client, device_id: int, seam: str) -> dict:
    endpoint = "interface-ips" if seam == "ip" else "interfaces-doc"
    response = await client.get(f"/api/v1/devices/{device_id}/{endpoint}", headers=AUTH)
    assert response.status_code == 200, response.text
    return response.json()


def assert_publication(body: dict, family: str):
    observation = body["observation"]
    assert set(observation) == {"family", "revision", "source_epoch", "digest", "observed_at", "coverage", "document"}
    assert observation["family"] == family
    assert observation["revision"] == body["read_state"]["payload_revision"]
    assert observation["source_epoch"] == body["read_state"]["source_epoch"]
    assert datetime.fromisoformat(observation["observed_at"]).utcoffset() is not None
    canonical = json.dumps(observation["document"], sort_keys=True, separators=(",", ":"))
    assert observation["digest"] == hashlib.sha256(canonical.encode()).hexdigest()
    return observation


@pytest.mark.parametrize("seam", SEAMS)
async def test_successful_read_publishes_device_observation(adapter_client, monkeypatch, seam):
    device_id = await seed_device(nso_device_name="observation-device", attributes=["description", "enabled"])
    if seam == "ip":
        entries = [{"interface-name": "port0", "address": [{"address": "198.18.0.1/24", "family": "ipv4"}]}]
        expected = {
            "interface": "port0",
            "bound_port": None,
            "addresses": [
                {"address": "198.18.0.1/24", "prefix_length": 24, "family": "ipv4", "secondary": False, "vrf": ""}
            ],
        }
    else:
        entries = [{"interface-name": "port0", "description": "", "enabled": False, "kind": "", "vrf": ""}]
        expected = {
            "name": "port0",
            "description": "",
            "enabled": False,
            "kind": "",
            "parent_binding": None,
            "encap_tag": None,
            "vrf": "",
            "service": None,
        }
    await publish(device_id, seam, DeviceReadNso(entries), monkeypatch)
    body = await read_family(adapter_client, device_id, seam)
    family = "interface_ip" if seam == "ip" else "interface_attributes"
    observation = assert_publication(body, family)
    assert observation["document"] == {"interfaces": [expected], "unprojectable": []}
    assert observation["coverage"] == {
        "attributes": ["address", "prefix_length", "secondary", "vrf"] if seam == "ip" else ["description", "enabled"]
    }
    if seam == "drift":
        assert body["interfaces"] == []


@pytest.mark.parametrize("seam", ["sync", "ip"])
async def test_no_successful_read_has_null_observation(adapter_client, seam):
    device_id = await seed_device(nso_device_name="observation-device")
    body = await read_family(adapter_client, device_id, seam)
    assert body["read_state"]["payload_revision"] is None
    assert body["observation"] is None


@pytest.mark.parametrize("family", ["interface_attributes", "interface_ip"])
async def test_publication_requires_observation(adapter_client, family):
    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        attempt = await outcome_store.record_read_outcome(
            db, device_id, family, Present({}, Freshness.fresh), refresh_source="poll"
        )
        row = await db.get(RefreshOutcome, attempt)
        await outcome_store.acquire_family_fence(db, device_id, family)
        with pytest.raises(ValueError, match="observation"):
            await outcome_store.stage_result(
                db, row, result="replaced", succeeded=True, row_count=0, publish_payload=True
            )
        assert await db.scalar(select(RefreshOutcome.id).where(RefreshOutcome.result.is_not(None))) is None


async def stored_observations(device_id: int):
    async with session() as db:
        return (
            await db.execute(
                text("SELECT revision, document, digest FROM read_observation WHERE device_id = :id"), {"id": device_id}
            )
        ).all()


@pytest.mark.parametrize("seam", SEAMS)
async def test_empty_read_publishes_empty_observation(adapter_client, monkeypatch, seam):
    device_id = await seed_device(nso_device_name="observation-device")
    await publish(
        device_id,
        seam,
        DeviceReadNso([{"interface-name": "port0", "address": [{"address": "198.18.0.1/24"}]}]),
        monkeypatch,
    )
    old = (await read_family(adapter_client, device_id, seam))["observation"]
    await publish(device_id, seam, DeviceReadNso([]), monkeypatch)
    body = await read_family(adapter_client, device_id, seam)
    observation = assert_publication(body, "interface_ip" if seam == "ip" else "interface_attributes")
    assert observation["document"] == {"interfaces": [], "unprojectable": []}
    assert observation["revision"] > old["revision"]
    assert len(await stored_observations(device_id)) == 1


@pytest.mark.parametrize("seam", SEAMS)
@pytest.mark.parametrize("status", ["error", "unsupported"])
async def test_unavailable_read_preserves_published_observation(adapter_client, monkeypatch, seam, status):
    device_id = await seed_device(nso_device_name="observation-device")
    await publish(device_id, seam, DeviceReadNso([{"interface-name": "port0", "description": "device"}]), monkeypatch)
    old = (await read_family(adapter_client, device_id, seam))["observation"]
    await publish(device_id, seam, DeviceReadNso([], status=status), monkeypatch)
    body = await read_family(adapter_client, device_id, seam)
    assert body["read_state"]["result"] == "kept"
    assert body["read_state"]["attempt_id"] > old["revision"]
    assert body["observation"] == old
    assert len(await stored_observations(device_id)) == 1


@pytest.mark.parametrize("seam", SEAMS)
async def test_malformed_entry_is_reported_in_observation(adapter_client, monkeypatch, seam):
    device_id = await seed_device(nso_device_name="observation-device")
    entries = [
        {"interface-name": "port2", "description": None, "address": [{"address": "198.18.0.2/24"}]},
        {"description": "missing name"},
        "not a mapping",
        {"interface-name": "port1", "description": "", "address": [{"address": "198.18.0.1/24"}]},
    ]
    await publish(device_id, seam, DeviceReadNso(entries), monkeypatch)
    body = await read_family(adapter_client, device_id, seam)
    document = body["observation"]["document"]
    key = "interface" if seam == "ip" else "name"
    assert [entry[key] for entry in document["interfaces"]] == ["port1", "port2"]
    assert document["unprojectable"] == [
        {"index": 1, "reason": "missing or invalid interface-name"},
        {"index": 2, "reason": "missing or invalid interface-name"},
    ]
    if seam == "ip":
        assert body["interfaces"] == document["interfaces"]


async def test_ip_observation_uses_the_mirror_projection(adapter_client, monkeypatch):
    device_id = await seed_device(nso_device_name="observation-device")
    entries = [
        {
            "interface-name": "port0",
            "bound-port": "lag-99:100",
            "address": [
                {"address": "2001:db8::1/64", "family": "ipv6", "vrf": "BLUE"},
                {"address": "198.18.0.2/24", "family": None, "vrf": None, "secondary": True},
                {"address": "198.18.0.1", "family": "ipv4", "vrf": ""},
                {"address": "198.18.0.3/24", "family": "ipv4", "vrf": ""},
            ],
        }
    ]
    await publish(device_id, "ip", DeviceReadNso(entries), monkeypatch)
    body = await read_family(adapter_client, device_id, "ip")
    observation = assert_publication(body, "interface_ip")
    assert observation["document"]["interfaces"][0] == {
        "interface": "port0",
        "bound_port": "lag-99:100",
        "addresses": [
            {"address": "198.18.0.1", "prefix_length": None, "family": "ipv4", "secondary": False, "vrf": ""},
            {"address": "198.18.0.2/24", "prefix_length": 24, "family": "ipv4", "secondary": True, "vrf": ""},
            {"address": "198.18.0.3/24", "prefix_length": 24, "family": "ipv4", "secondary": False, "vrf": ""},
            {"address": "2001:db8::1/64", "prefix_length": 64, "family": "ipv6", "secondary": False, "vrf": "BLUE"},
        ],
    }


async def test_canonical_observation_preserves_legacy_address_order(adapter_client, monkeypatch):
    device_id = await seed_device(nso_device_name="observation-device")
    entries = [
        {
            "interface-name": "port0",
            "address": [
                {"address": "2001:db8::1/64", "family": "ipv6"},
                {"address": "198.18.0.2/24", "family": "ipv4"},
                {"address": "198.18.0.1/24", "family": "ipv4"},
            ],
        }
    ]
    await publish(device_id, "ip", DeviceReadNso(entries), monkeypatch)
    body = await read_family(adapter_client, device_id, "ip")
    assert [item["address"] for item in body["interfaces"][0]["addresses"]] == [
        "2001:db8::1/64",
        "198.18.0.2/24",
        "198.18.0.1/24",
    ]
    assert [item["address"] for item in body["observation"]["document"]["interfaces"][0]["addresses"]] == [
        "198.18.0.1/24",
        "198.18.0.2/24",
        "2001:db8::1/64",
    ]


async def publish_unlocked(db, device_id: int, seam: str, nso: DeviceReadNso):
    device = await db.get(Device, device_id)
    if seam == "sync":
        return await importer._consume_interface_attributes(
            db, device, Present(nso.section, Freshness.fresh), None, refresh_source="sync"
        )
    if seam == "drift":
        return await importer._detect_drift_attributes(device_id, db, device, nso)
    return await run_family_refresh_from_outcome(
        db, device, INTERFACE_IP_SPEC, Present(nso.section, Freshness.fresh), own_lock=False
    )


@pytest.mark.parametrize("seam", SEAMS)
async def test_superseded_attempt_preserves_newer_observation(adapter_client, store_engine, monkeypatch, seam):
    device_id = await seed_device(nso_device_name="observation-device")
    await publish(device_id, seam, DeviceReadNso([{"interface-name": "base"}]), monkeypatch)
    family = "interface_ip" if seam == "ip" else "interface_attributes"
    fence_requested = asyncio.Event()

    def record_fence(conn, cursor, statement, parameters, context, executemany):
        if "pg_advisory_xact_lock" in statement:
            fence_requested.set()

    async with session() as blocker:
        await outcome_store.acquire_family_fence(blocker, device_id, family)
        event.listen(store_engine.sync_engine, "before_cursor_execute", record_fence)
        slow = asyncio.create_task(publish(device_id, seam, DeviceReadNso([{"interface-name": "older"}]), monkeypatch))
        try:
            await asyncio.wait_for(fence_requested.wait(), timeout=10)
            await publish_unlocked(blocker, device_id, seam, DeviceReadNso([{"interface-name": "newer"}]))
            newer = (await read_family(adapter_client, device_id, seam))["observation"]
            await asyncio.wait_for(slow, timeout=10)
        finally:
            event.remove(store_engine.sync_engine, "before_cursor_execute", record_fence)
            if not slow.done():
                slow.cancel()
                await asyncio.gather(slow, return_exceptions=True)
    assert (await read_family(adapter_client, device_id, seam))["observation"] == newer
    observations = await stored_observations(device_id)
    assert [row.revision for row in observations] == [newer["revision"]]
    async with session() as db:
        superseded = (
            await db.execute(
                select(RefreshOutcome).where(
                    RefreshOutcome.device_id == device_id,
                    RefreshOutcome.family == family,
                    RefreshOutcome.result == "superseded",
                )
            )
        ).scalar_one()
        assert superseded.id < newer["revision"]


@pytest.mark.parametrize("seam", SEAMS)
async def test_transaction_rollback_keeps_previous_observation(adapter_client, monkeypatch, seam):
    device_id = await seed_device(nso_device_name="observation-device")
    await publish(device_id, seam, DeviceReadNso([{"interface-name": "base"}]), monkeypatch)
    old = (await read_family(adapter_client, device_id, seam))["observation"]
    family = "interface_ip" if seam == "ip" else "interface_attributes"
    from nso_adapter.domain.observation import observe_family

    async with session() as db:
        raw = {"interface": [{"interface-name": "uncommitted"}]}
        attempt = await outcome_store.record_read_outcome(
            db, device_id, family, Present(raw, Freshness.fresh), refresh_source="poll"
        )
        await outcome_store.acquire_family_fence(db, device_id, family)
        row = await db.get(RefreshOutcome, attempt)
        await outcome_store.stage_result(
            db, row, result="replaced", succeeded=True, row_count=1, observation=observe_family(family, raw)
        )
        assert (await read_family(adapter_client, device_id, seam))["observation"] == old
        await db.rollback()
    assert (await read_family(adapter_client, device_id, seam))["observation"] == old
    assert [row.revision for row in await stored_observations(device_id)] == [old["revision"]]


async def test_intent_writes_preserve_observations_and_apply_telemetry(adapter_client, monkeypatch):
    from nso_adapter.store.models import InterfaceIntent

    device_id = await seed_device(nso_device_name="observation-device", attributes=["description", "enabled"])
    entries = [
        {
            "interface-name": "port0",
            "description": "device",
            "enabled": False,
            "address": [{"address": "198.18.0.1/24"}],
        }
    ]
    for seam in ["sync", "ip"]:
        await publish(device_id, seam, DeviceReadNso(entries), monkeypatch)
    old = {seam: (await read_family(adapter_client, device_id, seam))["observation"] for seam in ["sync", "ip"]}
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent?store_only=true",
        headers={**AUTH, **push_seq()},
        json={
            "attributes": [
                {"interface": "port0", "attribute": "description", "intent_value": "intent"},
                {"interface": "logical0", "attribute": "description", "intent_value": "greenfield"},
            ]
        },
    )
    assert response.status_code == 200, response.text
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ip-intent?store_only=true",
        headers={**AUTH, **push_seq()},
        json={
            "addresses": [
                {
                    "interface": "logical1",
                    "address": "198.18.1.1/24",
                    "family": "ipv4",
                    "routed": True,
                    "parent_binding": "lag-99",
                    "encap_tag": "100",
                }
            ]
        },
    )
    assert response.status_code == 200, response.text
    async with session() as db:
        row = (
            await db.execute(select(InterfaceIntent).where(InterfaceIntent.intent_value == "greenfield"))
        ).scalar_one()
        row.last_apply_at = datetime.fromisoformat("2026-10-01T00:00:00+00:00")
        row.last_apply_error = {"code": "nso_error", "message": "placeholder failure"}
        await db.commit()
    body = await read_family(adapter_client, device_id, "sync")
    assert body["observation"] == old["sync"]
    assert (await read_family(adapter_client, device_id, "ip"))["observation"] == old["ip"]
    ifaces = {entry["name"]: entry for entry in body["interfaces"]}
    assert set(ifaces) == {"port0", "logical0", "logical1"}
    attr = ifaces["logical0"]["attrs"]["description"]
    assert attr["intent_value"] == "greenfield"
    assert attr["last_apply_at"] == "2026-10-01T00:00:00Z"
    assert attr["last_apply_error"] == {"code": "nso_error", "message": "placeholder failure"}
    assert len(await stored_observations(device_id)) == 2


@pytest.mark.parametrize("seam", ["sync", "ip"])
@pytest.mark.parametrize("corruption", ["missing", "epoch", "revision"])
async def test_missing_observation_is_an_internal_error(adapter_client, monkeypatch, seam, corruption):
    device_id = await seed_device(nso_device_name="observation-device")
    await publish(device_id, seam, DeviceReadNso([]), monkeypatch)
    async with session() as db:
        statement = {
            "missing": "DELETE FROM read_observation WHERE device_id = :id",
            "epoch": "UPDATE devices SET source_epoch = source_epoch + 1 WHERE id = :id",
            "revision": "UPDATE refresh_outcome_pointer SET payload_revision = payload_revision + 1000 WHERE device_id = :id",
        }[corruption]
        await db.execute(text(statement), {"id": device_id})
        await db.commit()
    endpoint = "interface-ips" if seam == "ip" else "interfaces-doc"
    response = await adapter_client.get(f"/api/v1/devices/{device_id}/{endpoint}", headers=AUTH)
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal"


@pytest.mark.parametrize("mutation", ["document = '{}'::jsonb", "revision = revision + 1", "digest = digest"])
async def test_observation_update_is_refused_by_postgresql(adapter_client, monkeypatch, mutation):
    from sqlalchemy.exc import IntegrityError

    device_id = await seed_device(nso_device_name="observation-device")
    await publish(device_id, "ip", DeviceReadNso([]), monkeypatch)
    old = (await read_family(adapter_client, device_id, "ip"))["observation"]
    async with session() as db:
        with pytest.raises(IntegrityError, match="read_observation is immutable"):
            await db.execute(text(f"UPDATE read_observation SET {mutation} WHERE device_id = :id"), {"id": device_id})
        await db.rollback()
    assert (await read_family(adapter_client, device_id, "ip"))["observation"] == old


async def test_rekey_deletes_observations_from_old_source_epoch(adapter_client_with_nso, monkeypatch):
    from nso_adapter.core.onboarding import rekey_device

    device_id = await seed_device(nso_device_name="observation-device")
    for seam in ["sync", "ip"]:
        await publish(device_id, seam, DeviceReadNso([]), monkeypatch)
    async with session() as db:
        device = await db.get(Device, device_id)
        await rekey_device(db, device, nso_device_name="replacement-device")
    assert await stored_observations(device_id) == []
    for seam in ["sync", "ip"]:
        body = await read_family(adapter_client_with_nso, device_id, seam)
        assert body["read_state"]["source_epoch"] == 2
        assert body["read_state"]["payload_revision"] is None
        assert body["observation"] is None
        await publish(device_id, seam, DeviceReadNso([]), monkeypatch)
        body = await read_family(adapter_client_with_nso, device_id, seam)
        assert_publication(body, "interface_ip" if seam == "ip" else "interface_attributes")


async def test_device_delete_cascades_observations(adapter_client, monkeypatch):
    device_id = await seed_device(nso_device_name="observation-device", attributes=[])
    for seam in ["sync", "ip"]:
        await publish(device_id, seam, DeviceReadNso([]), monkeypatch)
    async with session() as db:
        await db.execute(text("DELETE FROM devices WHERE id = :id"), {"id": device_id})
        await db.commit()
    assert await stored_observations(device_id) == []


@pytest.mark.parametrize("seam", SEAMS)
@pytest.mark.parametrize("failure_at", ["mirror", "observation"])
async def test_materialization_error_preserves_observation(adapter_client, monkeypatch, seam, failure_at):
    from sqlalchemy.exc import IntegrityError

    device_id = await seed_device(nso_device_name="observation-device")
    initial = [{"interface-name": "port0", "description": "before", "address": [{"address": "198.18.0.1/24"}]}]
    await publish(device_id, "sync" if seam == "drift" else seam, DeviceReadNso(initial), monkeypatch)
    if seam == "drift":
        await publish(device_id, seam, DeviceReadNso(initial), monkeypatch)
    old_body = await read_family(adapter_client, device_id, seam)
    old = old_body["observation"]
    table = "interface_ip_address" if seam == "ip" else "interface_attr_state"
    if failure_at == "observation":
        table = "read_observation"
    async with session() as db:
        await db.execute(
            text("""
            CREATE FUNCTION refuse_mirror_write() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION USING ERRCODE = 'integrity_constraint_violation', MESSAGE = 'mirror write refused';
            END;
            $$ LANGUAGE plpgsql
        """)
        )
        await db.execute(
            text(
                f"CREATE TRIGGER refuse_mirror_write BEFORE INSERT OR UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION refuse_mirror_write()"
            )
        )
        await db.commit()
    changed = [{"interface-name": "port0", "description": "after", "address": [{"address": "198.18.0.2/24"}]}]
    with pytest.raises(IntegrityError, match="mirror write refused"):
        await publish(device_id, seam, DeviceReadNso(changed), monkeypatch)
    body = await read_family(adapter_client, device_id, seam)
    assert body["observation"] == old
    assert body["interfaces"] == old_body["interfaces"]
    if seam != "drift":
        assert body["read_state"]["result"] == "error"
        assert body["read_state"]["payload_revision"] == old["revision"]
    assert len(await stored_observations(device_id)) == 1


@pytest.mark.parametrize("family", ["static_route"])
async def test_new_family_refuses_another_familys_observation(adapter_client, family):
    from nso_adapter.domain.observation import observe_family

    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        attempt = await outcome_store.record_read_outcome(
            db, device_id, family, Present({}, Freshness.fresh), refresh_source="poll"
        )
        row = await db.get(RefreshOutcome, attempt)
        await outcome_store.acquire_family_fence(db, device_id, family)
        with pytest.raises(ValueError, match="family does not match"):
            await outcome_store.stage_result(
                db, row, result="replaced", succeeded=True, row_count=0, observation=observe_family("interface_ip", {})
            )


async def test_mismatched_observation_family_is_refused(adapter_client):
    from nso_adapter.domain.observation import observe_family

    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        attempt = await outcome_store.record_read_outcome(
            db, device_id, "interface_ip", Present({}, Freshness.fresh), refresh_source="poll"
        )
        row = await db.get(RefreshOutcome, attempt)
        await outcome_store.acquire_family_fence(db, device_id, "interface_ip")
        with pytest.raises(ValueError, match="family does not match"):
            await outcome_store.stage_result(
                db,
                row,
                result="replaced",
                succeeded=True,
                row_count=0,
                observation=observe_family("interface_attributes", {}),
            )


async def test_authoritative_absence_publishes_empty_ip_observation(adapter_client):
    from nso_adapter.nso.read_outcome import AbsentAuthoritative

    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        device = await db.get(Device, device_id)
        await run_family_refresh_from_outcome(db, device, INTERFACE_IP_SPEC, AbsentAuthoritative())
    body = await read_family(adapter_client, device_id, "ip")
    observation = assert_publication(body, "interface_ip")
    assert body["read_state"]["result"] == "cleared"
    assert observation["document"] == {"interfaces": [], "unprojectable": []}


async def test_invalid_address_remains_visible_in_observation(adapter_client, monkeypatch):
    device_id = await seed_device(nso_device_name="observation-device")
    entries = [{"interface-name": "port0", "address": ["bad", {}, {"address": "198.18.0.1/24"}]}]
    await publish(device_id, "ip", DeviceReadNso(entries), monkeypatch)
    body = await read_family(adapter_client, device_id, "ip")
    document = body["observation"]["document"]
    assert document["interfaces"] == body["interfaces"]
    assert document["unprojectable"] == [
        {"index": 0, "reason": "address[0]: missing or invalid address"},
        {"index": 0, "reason": "address[1]: missing or invalid address"},
    ]


async def test_non_boolean_secondary_is_unprojectable(adapter_client, monkeypatch):
    device_id = await seed_device(nso_device_name="observation-device")
    entries = [
        {
            "interface-name": "port0",
            "address": [
                {"address": "198.18.0.1/24", "secondary": "false"},
                {"address": "198.18.0.2/24", "secondary": None},
                {"address": "198.18.0.3/24"},
            ],
        }
    ]
    await publish(device_id, "ip", DeviceReadNso(entries), monkeypatch)
    body = await read_family(adapter_client, device_id, "ip")
    document = body["observation"]["document"]
    assert document["interfaces"] == body["interfaces"]
    assert [(item["address"], item["secondary"]) for item in document["interfaces"][0]["addresses"]] == [
        ("198.18.0.2/24", False),
        ("198.18.0.3/24", False),
    ]
    assert document["unprojectable"] == [{"index": 0, "reason": "address[0]: invalid secondary"}]


async def test_non_string_ip_fields_are_unprojectable(adapter_client, monkeypatch):
    device_id = await seed_device(nso_device_name="observation-device")
    entries = [
        {
            "interface-name": "port0",
            "address": [
                {"address": "198.18.0.1/24", "family": 0},
                {"address": "198.18.0.2/24", "vrf": False},
                {"address": "198.18.0.3/24", "family": None, "vrf": None},
                {"address": "198.18.0.4/24", "family": ""},
            ],
        },
        {"interface-name": "port1", "bound-port": 0, "address": [{"address": "198.18.1.1/24"}]},
        {"interface-name": "port2", "bound-port": "", "address": [{"address": "198.18.2.1/24"}]},
    ]
    await publish(device_id, "ip", DeviceReadNso(entries), monkeypatch)
    body = await read_family(adapter_client, device_id, "ip")
    document = body["observation"]["document"]
    assert document["interfaces"] == body["interfaces"]
    assert [(item["interface"], item["bound_port"]) for item in document["interfaces"]] == [
        ("port0", None),
        ("port2", None),
    ]
    assert [(item["address"], item["family"], item["vrf"]) for item in document["interfaces"][0]["addresses"]] == [
        ("198.18.0.3/24", "ipv4", ""),
        ("198.18.0.4/24", "ipv4", ""),
    ]
    assert document["unprojectable"] == [
        {"index": 0, "reason": "address[0]: invalid family"},
        {"index": 0, "reason": "address[1]: invalid vrf"},
        {"index": 1, "reason": "invalid bound_port"},
    ]


@pytest.mark.parametrize("seam", ["sync", "drift"])
async def test_invalid_attribute_is_unprojectable(adapter_client, monkeypatch, seam):
    device_id = await seed_device(nso_device_name="observation-device")
    entries = [{"interface-name": "port0", "enabled": "false"}, {"interface-name": "port1", "enabled": None}]
    with capture_logs() as logs:
        await publish(device_id, seam, DeviceReadNso(entries), monkeypatch)
    body = await read_family(adapter_client, device_id, seam)
    document = body["observation"]["document"]
    assert [entry["name"] for entry in document["interfaces"]] == ["port1"]
    assert document["interfaces"][0]["enabled"] is None
    assert document["unprojectable"] == [{"index": 0, "reason": "invalid enabled"}]
    skipped = [record for record in logs if record["event"] == "interface_attributes.entry_skipped"]
    assert skipped == [
        {
            "event": "interface_attributes.entry_skipped",
            "log_level": "warning",
            "device_id": device_id,
            "family": "interface-attributes",
            "index": 0,
            "reason": "invalid enabled",
        }
    ]


@pytest.mark.parametrize("seam", ["sync", "ip"])
async def test_get_pins_observation_and_pointer_snapshot(adapter_client, store_engine, pg_url, monkeypatch, seam):
    from datetime import UTC

    import sqlalchemy as sa
    from sqlalchemy.orm import Session as SyncSession

    from nso_adapter.domain.observation import digest_document, observe_family
    from nso_adapter.store.models import ReadObservation, RefreshOutcomePointer

    device_id = await seed_device(nso_device_name="observation-device")
    await publish(device_id, seam, DeviceReadNso([{"interface-name": "base"}]), monkeypatch)
    old = (await read_family(adapter_client, device_id, seam))["observation"]
    family = "interface_ip" if seam == "ip" else "interface_attributes"
    writer = sa.create_engine(sa.engine.make_url(pg_url).set(drivername="postgresql+psycopg2"))
    new_revision = None

    def publish_during_get(conn, cursor, statement, parameters, context, executemany):
        nonlocal new_revision
        if "refresh_outcome_pointer" not in statement or new_revision is not None:
            return
        raw = {"interface": [{"interface-name": "newer"}]}
        observation = observe_family(family, raw)
        document = observation.document.model_dump(mode="json")
        with SyncSession(writer) as db:
            attempt = RefreshOutcome(
                device_id=device_id,
                family=family,
                source_epoch=1,
                read_outcome="present",
                freshness="fresh",
                result="replaced",
                succeeded=True,
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
            )
            db.add(attempt)
            db.flush()
            new_revision = attempt.id
            db.execute(sa.delete(ReadObservation).where(ReadObservation.device_id == device_id))
            db.add(
                ReadObservation(
                    device_id=device_id,
                    family=family,
                    revision=attempt.id,
                    source_epoch=1,
                    document=document,
                    digest=digest_document(document),
                    coverage=observation.coverage.model_dump(mode="json"),
                    observed_at=attempt.started_at,
                )
            )
            db.execute(
                sa.update(RefreshOutcomePointer)
                .where(RefreshOutcomePointer.device_id == device_id, RefreshOutcomePointer.family == family)
                .values(attempt_id=attempt.id, payload_revision=attempt.id)
            )
            db.commit()

    event.listen(store_engine.sync_engine, "before_cursor_execute", publish_during_get)
    try:
        body = await read_family(adapter_client, device_id, seam)
    finally:
        event.remove(store_engine.sync_engine, "before_cursor_execute", publish_during_get)
        writer.dispose()
    assert new_revision is not None
    assert body["observation"] == old
    assert body["read_state"]["payload_revision"] == old["revision"]
    next_body = await read_family(adapter_client, device_id, seam)
    assert next_body["observation"]["revision"] == new_revision
    assert next_body["read_state"]["payload_revision"] == new_revision


async def test_authority_reset_preserves_read_observations(adapter_client, monkeypatch):
    from nso_adapter.core.cutover import deauthorize_for_cutover

    device_id = await seed_device(nso_device_name="observation-device")
    for seam in ["sync", "ip"]:
        await publish(device_id, seam, DeviceReadNso([]), monkeypatch)
    old = {seam: (await read_family(adapter_client, device_id, seam))["observation"] for seam in ["sync", "ip"]}
    async with session() as db:
        await deauthorize_for_cutover(db)
        await db.commit()
    for seam in ["sync", "ip"]:
        assert (await read_family(adapter_client, device_id, seam))["observation"] == old[seam]


@pytest.mark.parametrize("family", ["bgp"])
async def test_family_without_observer_refuses_observation(adapter_client, family):
    from nso_adapter.domain.observation import observe_family

    device_id = await seed_device(nso_device_name="observation-device")
    async with session() as db:
        attempt = await outcome_store.record_read_outcome(
            db, device_id, family, Present({}, Freshness.fresh), refresh_source="poll"
        )
        row = await db.get(RefreshOutcome, attempt)
        await outcome_store.acquire_family_fence(db, device_id, family)
        with pytest.raises(ValueError, match="no observation observer"):
            await outcome_store.stage_result(
                db, row, result="replaced", succeeded=True, row_count=0, observation=observe_family("interface_ip", {})
            )
