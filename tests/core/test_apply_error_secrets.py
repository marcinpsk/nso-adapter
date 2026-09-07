# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Sender failures must not put credentials in logs or stored errors."""

import json
import logging
import traceback
from datetime import UTC, datetime

import httpx
import pytest
import structlog

from nso_adapter.core.apply import run_apply
from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.nso.apply import NsoApplyError, SectionExecution, apply_device_intent, encode_snmp
from nso_adapter.nso.client import DEVICE_INTENT_ROOT
from nso_adapter.store.models import Job, JobStatus, OspfInterfaceIntent, SnmpCommunityIntent
from tests._secret_discipline import assert_chain_free_of
from tests.conftest import seed_device, session
from tests.core.test_static_route_put import seed_apply_job
from tests.nso.test_apply_send import _client_with

pytestmark = pytest.mark.anyio
_DEVICE = "secret-test"
_REF = "placeholder-mount/placeholder-path#placeholder-key"
_SECRET = "placeholder-resolved-community"


@pytest.fixture
def recorded_logs(caplog):
    previous = structlog.get_config().copy()
    structlog.configure(logger_factory=structlog.stdlib.LoggerFactory(), cache_logger_on_first_use=False)
    caplog.set_level(logging.INFO)
    yield caplog
    structlog.configure(**previous)


async def _run(device_id, client, monkeypatch, job_id=None):
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda _: client)
    job_id = job_id or await seed_apply_job(device_id)
    await run_apply(job_id=job_id, device_id=device_id)
    async with session() as db:
        return await db.get(Job, job_id)


def _assert_safe(exc, job, row_error, logs, secrets):
    assert job.status == JobStatus.failed
    assert row_error is not None
    assert logs.records
    surfaces = [
        str(exc),
        repr(exc.detail),
        "".join(traceback.format_exception(exc)),
        json.dumps(job.error),
        json.dumps(row_error),
        repr([record.__dict__ for record in logs.records]),
    ]
    for surface in surfaces:
        for secret in secrets:
            assert secret not in surface
    assert_chain_free_of(exc, secrets)


async def _community():
    device_id = await seed_device(nso_device_name=_DEVICE)
    async with session() as db:
        row = SnmpCommunityIntent(
            device_id=device_id, label="ro", vault_ref=_REF, access="RO", accepted_at=datetime.now(UTC)
        )
        db.add(row)
        await db.commit()
    return device_id, row


@pytest.mark.parametrize("phase", ["commit", "verify"])
@pytest.mark.parametrize("shape", ["object", "string"])
async def test_snmp_rejections_keep_no_resolved_secret_or_vault_reference(
    adapter_client, monkeypatch, recorded_logs, phase, shape
):
    device_id, row = await _community()
    message = f"device-intent: refused [family=snmp field=community]: {_SECRET} {_REF}"
    body = (
        message
        if shape == "string"
        else {
            "ietf-restconf:errors": {
                "error": [
                    {
                        "error-message": message,
                        "vault-mount": "placeholder-mount",
                        "vault-path": "placeholder-path",
                        "vault-key": "placeholder-key",
                    }
                ]
            }
        }
    )

    def respond(request):
        if request.method == "GET":
            return httpx.Response(404)
        if request.method == "PUT" and ("dry-run=" in str(request.url)) == (phase == "verify"):
            return httpx.Response(400, json=body)
        return httpx.Response(200, json={"dry-run-result": {"native": {}}})

    client = _client_with(httpx.MockTransport(respond))
    containers = {
        "snmp": encode_snmp(
            {
                "snmp_community_intent": [row],
                "snmp_v3_user_intent": [],
                "snmp_host_intent": [],
                "snmp_system_info_intent": [],
            },
            SectionExecution(None, community_dialect_for(None)),
        )
    }
    with pytest.raises(NsoApplyError) as caught:
        await apply_device_intent(client, _DEVICE, containers)
    job = await _run(device_id, client, monkeypatch)
    async with session() as db:
        stored = await db.get(SnmpCommunityIntent, row.id)
        _assert_safe(
            caught.value,
            job,
            stored.last_apply_error,
            recorded_logs,
            [_SECRET, _REF, "placeholder-mount", "placeholder-path", "placeholder-key"],
        )
    assert any("nso.apply." in record.getMessage() for record in recorded_logs.records)


async def test_verification_delta_keeps_no_secret_in_logs_or_errors(adapter_client, monkeypatch, recorded_logs):
    device_id = await seed_device(nso_device_name=_DEVICE)
    async with session() as db:
        row = OspfInterfaceIntent(
            device_id=device_id,
            interface_name="GigabitEthernet0/1",
            process_id="1",
            area_id="0",
            auth_type="simple",
            auth_key=_SECRET,
            accepted_at=datetime.now(UTC),
        )
        db.add(row)
        await db.commit()
    delta = f"ip ospf authentication-key {_SECRET}\n"

    def respond(request):
        if request.method == "GET":
            return httpx.Response(404)
        return httpx.Response(200, json={"dry-run-result": {"native": {"device": [{"name": _DEVICE, "data": delta}]}}})

    client = _client_with(httpx.MockTransport(respond))
    with pytest.raises(NsoApplyError) as caught:
        await apply_device_intent(client, _DEVICE, {"ospf": {"auth-key": _SECRET}})
    job = await _run(device_id, client, monkeypatch)
    async with session() as db:
        stored = await db.get(OspfInterfaceIntent, row.id)
        _assert_safe(caught.value, job, stored.last_apply_error, recorded_logs, [_SECRET])
    assert any("nso.apply.verify_mismatch" in record.getMessage() for record in recorded_logs.records)


async def test_a_non_reference_secret_never_reaches_the_projection_refusal_chain(adapter_client):
    """The serialization guard refuses raw secret material; its chain must not repeat it."""
    from sqlalchemy import update

    from nso_adapter.core.projection import snapshot_stream

    device_id, row = await _community()
    raw = "placeholder-raw-secret-not-a-reference"
    async with session() as db:
        await db.execute(update(SnmpCommunityIntent).where(SnmpCommunityIntent.id == row.id).values(vault_ref=raw))
        await db.commit()
    async with session() as db:
        with pytest.raises(ValueError) as caught:
            await snapshot_stream(db, device_id, "snmp")
    assert_chain_free_of(caught.value, [raw])


async def test_malformed_snmp_reference_keeps_no_reference_in_exception_or_row(adapter_client, recorded_logs):
    from nso_adapter.core.apply import (
        _NO_INTERFACE,
        _ApplyPlan,
        _finalize_unsent,
        _SectionApply,
        build_device_containers,
    )
    from nso_adapter.store.models import Device
    from tests.core.projection_helpers import freeze_snapshot

    device_id, row = await _community()
    job_id = await seed_apply_job(device_id)
    bad_ref = "placeholder-mount/placeholder path#placeholder-key"
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(404)

    client = _client_with(httpx.MockTransport(respond))
    async with session() as db:
        document = {"snmp": await freeze_snapshot(db, device_id, "snmp")}
        document["snmp"]["snmp_community_intent"][0]["vault_ref"] = bad_ref
        device = await db.get(Device, device_id)
        body = await build_device_containers(client, device, document)
        exc = body.errors["snmp"]
        stored = await db.get(SnmpCommunityIntent, row.id)
        section = _SectionApply({}, [stored], [stored], {}, None)
        plan = _ApplyPlan(device_id, document, {"snmp": section}, _NO_INTERFACE, None, {}, True)
        await _finalize_unsent(db, plan, body.errors, job_id=job_id, reg=None)
    async with session() as db:
        job = await db.get(Job, job_id)
        stored = await db.get(SnmpCommunityIntent, row.id)
        _assert_safe(
            exc,
            job,
            stored.last_apply_error,
            recorded_logs,
            [bad_ref, "placeholder-mount", "placeholder path", "placeholder-key"],
        )
    assert not requests


# ── the collateral guard: a refusal names the orphans, never the device delta ──

_ORPHAN_INSTANCE = {
    DEVICE_INTENT_ROOT: [{"device": _DEVICE, "snmp": {"community": [{"name": "ro"}, {"name": "legacy"}]}}]
}


def _blocked_client(delta: str):
    """A live instance carrying an orphan community; every dry-run answers *delta*."""

    def respond(request):
        if request.method == "GET":
            return httpx.Response(200, json=_ORPHAN_INSTANCE)
        return httpx.Response(200, json={"dry-run-result": {"native": {"device": [{"name": _DEVICE, "data": delta}]}}})

    return _client_with(httpx.MockTransport(respond))


async def test_a_blocked_apply_keeps_the_device_delta_out_of_the_job_and_row_errors(
    adapter_client, monkeypatch, recorded_logs
):
    """The apply worker persists the orphan identifiers on the row, and no native delta."""
    device_id, row = await _community()
    client = _blocked_client(f"snmp-server community {_SECRET} RO\n")
    job = await _run(device_id, client, monkeypatch)
    async with session() as db:
        stored = await db.get(SnmpCommunityIntent, row.id)
    assert job.status == JobStatus.failed
    assert stored.last_apply_error["code"] == "removal_blocked_collateral"
    assert stored.last_apply_error["detail"]["orphans"] == {"snmp/community": [["legacy"]]}
    for surface in (json.dumps(job.error), json.dumps(stored.last_apply_error), repr(recorded_logs.records)):
        assert _SECRET not in surface


async def test_a_blocked_removal_keeps_the_device_delta_out_of_the_job_error(adapter_client, recorded_logs):
    """The removal worker persists the same orphan identifiers, and no native delta."""
    from unittest.mock import patch

    from nso_adapter.core import removal as removal_mod
    from tests.core.test_removal import _seed_removal_job

    device_id, _row = await _community()
    job_id = await _seed_removal_job(device_id, scope="snmp")
    client = _blocked_client(f"snmp-server community {_SECRET} RO\n")
    with patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        await removal_mod.run_removal(job_id, device_id)
    async with session() as db:
        job = await db.get(Job, job_id)
    assert job.status == JobStatus.failed
    assert job.error["code"] == "removal_blocked_collateral"
    assert job.error["detail"]["orphans"] == {"snmp/community": [["legacy"]]}
    assert _SECRET not in json.dumps(job.error)
    assert _SECRET not in repr(recorded_logs.records)


# ── a device rejection: the construct is attributed, the device text is not kept ──

_NED = "cisco-ios-cli-6.95"
_SW = "15.5"
_REJECTION = (
    f"external error (device {_DEVICE}) Aborted: syntax error\n"
    "command: set extcommunity color 12\n"
    f"config: snmp-server community {_SECRET} RO\n"
)


async def test_a_device_rejection_attributes_its_construct_and_keeps_no_device_text(
    adapter_client, monkeypatch, recorded_logs
):
    """Redaction must not cost the capability verdict the rejection is the only source of.

    A dry-run renders an unsupported route-policy construct cleanly, so the commit error is
    the one place the device names it. The construct identifier survives; the rest does not.
    """
    from nso_adapter.core.capability import get_device_capability
    from nso_adapter.store.models import Device, RoutePolicyObjectIntent

    device_id, row = await _community()
    async with session() as db:
        device = await db.get(Device, device_id)
        device.ned_id, device.sw_version = _NED, _SW
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id, family="ipv4", name="RM-IN", entries=[], accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()
    body = {"ietf-restconf:errors": {"error": [{"error-message": _REJECTION}]}}

    def respond(request):
        if request.method == "GET":
            return httpx.Response(404)
        if request.method == "PUT" and "dry-run=" not in str(request.url):
            return httpx.Response(400, json=body)
        return httpx.Response(200, json={"dry-run-result": {"native": {}}})

    client = _client_with(httpx.MockTransport(respond))
    job = await _run(device_id, client, monkeypatch)
    async with session() as db:
        recorded = {(r.scope, r.name): r for r in await get_device_capability(db, _NED, _SW) if r.source == "apply"}
        stored = await db.get(SnmpCommunityIntent, row.id)
    assert job.status == JobStatus.failed
    assert ("rm-set", "set extcommunity color") in recorded, f"the construct was not attributed: {sorted(recorded)}"
    surfaces = [
        json.dumps(job.error),
        json.dumps(stored.last_apply_error),
        repr([record.__dict__ for record in recorded_logs.records]),
        *(f"{r.detail} {r.name}" for r in recorded.values()),
    ]
    for surface in surfaces:
        assert _SECRET not in surface
        assert "snmp-server" not in surface, "opaque device text left the redaction boundary"
