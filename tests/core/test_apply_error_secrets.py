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
from sqlalchemy import select

from nso_adapter.core.apply import run_apply
from nso_adapter.core.community_dialect import community_dialect_for
from nso_adapter.nso.apply import NsoApplyError, SectionExecution, apply_device_intent, encode_snmp
from nso_adapter.nso.client import DEVICE_INTENT_ROOT
from nso_adapter.store.models import BgpRouterIntent, Job, JobStatus, OspfInterfaceIntent, SnmpCommunityIntent
from tests._secret_discipline import assert_chain_free_of
from tests.conftest import VALID_TOKEN, push_seq, seed_device, session
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


def _log_surface(logs) -> str:
    """Every attribute of every captured record, not ``LogRecord.__repr__``.

    ``__repr__`` renders only name, level, path, line and ``msg``, so it misses a secret in
    ``args`` (%-style logging) or in the cached ``exc_text`` of a traceback, which is exactly
    where this module's redaction failures would land.
    """
    return repr([record.__dict__ for record in logs.records])


def test_the_log_surface_sees_what_logrecord_repr_hides(caplog):
    """Pins why the surface reads every attribute: two leaks `repr(records)` cannot show."""
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("surface-contract")
    logger.error("community=%s", "ARGS-ONLY-SECRET")
    try:
        raise ValueError("EXC-ONLY-SECRET")
    except ValueError:
        logger.exception("apply failed")
    logging.Formatter().format(caplog.records[-1])  # caches exc_text, as a real handler does

    weak = repr(caplog.records)
    surface = _log_surface(caplog)
    for secret in ("ARGS-ONLY-SECRET", "EXC-ONLY-SECRET"):
        assert secret not in weak, "LogRecord.__repr__ renders only msg"
        assert secret in surface, "the surface must expose args and exc_text"


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
        _log_surface(logs),
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


async def test_unexpected_commit_exception_keeps_secret_out_of_logs_and_errors(
    adapter_client, monkeypatch, recorded_logs
):
    device_id, row = await _community()

    async def fail_commit(*args, **kwargs):
        raise RuntimeError(_SECRET)

    monkeypatch.setattr("nso_adapter.nso.apply.apply_device_intent", fail_commit)
    client = _client_with(httpx.MockTransport(lambda request: httpx.Response(404)))
    job = await _run(device_id, client, monkeypatch)
    async with session() as db:
        stored = await db.get(SnmpCommunityIntent, row.id)

    assert stored.last_apply_error["code"] == "internal"
    assert stored.last_apply_error["message"] == "apply error (internal); see the server log"
    for surface in (json.dumps(job.error), json.dumps(stored.last_apply_error), _log_surface(recorded_logs)):
        assert _SECRET not in surface


async def test_typed_commit_failure_keeps_its_message_out_of_logs_and_errors(
    adapter_client, monkeypatch, recorded_logs
):
    device_id, row = await _community()

    async def fail_commit(*args, **kwargs):
        raise NsoApplyError("nso_error", f"device rejected {_SECRET}", {"stage": "commit"})

    monkeypatch.setattr("nso_adapter.nso.apply.apply_device_intent", fail_commit)
    client = _client_with(httpx.MockTransport(lambda request: httpx.Response(404)))
    job = await _run(device_id, client, monkeypatch)
    async with session() as db:
        stored = await db.get(SnmpCommunityIntent, row.id)

    assert stored.last_apply_error == {
        "code": "nso_error",
        "message": "apply error (nso_error); see the server log",
        "detail": {"stage": "commit"},
    }
    for surface in (json.dumps(job.error), json.dumps(stored.last_apply_error), _log_surface(recorded_logs)):
        assert _SECRET not in surface


async def test_typed_build_failure_keeps_its_message_out_of_logs_and_errors(adapter_client, monkeypatch, recorded_logs):
    invalid_asn = "placeholder-sensitive-asn"
    device_id = await seed_device(nso_device_name=_DEVICE)
    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/bgp-intent",
        json={"routers": [{"asn": invalid_asn}]},
        headers={"Authorization": f"Bearer {VALID_TOKEN}"} | push_seq(),
    )
    assert response.status_code == 200

    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(404)

    job = await _run(device_id, _client_with(httpx.MockTransport(respond)), monkeypatch)
    async with session() as db:
        stored = (
            (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalars().one()
        )

    assert stored.last_apply_error == {
        "code": "invalid_asn",
        "message": "apply error (invalid_asn); see the server log",
        "detail": {},
    }
    for surface in (json.dumps(job.error), json.dumps(stored.last_apply_error), _log_surface(recorded_logs)):
        assert invalid_asn not in surface
    assert not any(request.method == "PUT" for request in requests)


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
    for surface in (json.dumps(job.error), json.dumps(stored.last_apply_error), _log_surface(recorded_logs)):
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
    assert _SECRET not in _log_surface(recorded_logs)


# ── a device rejection: the construct is attributed, the device text is not kept ──

_NED = "cisco-ios-cli-6.95"
_SW = "15.5"


def _rejection(command: str) -> str:
    return (
        f"external error (device {_DEVICE}) Aborted: syntax error\n"
        f"command: {command}\n"
        f"config: snmp-server community {_SECRET} RO\n"
    )


@pytest.mark.parametrize(
    ("command", "construct", "condemned", "cleared"),
    [
        ("set extcommunity color 12", ("rm-set", "set extcommunity color"), None, None),
        (
            "ip community-list standard CL permit 65000:1",
            ("community", "ip community-list standard"),
            "65000:1",
            "^65000:",
        ),
        (
            "ip community-list expanded CL permit ^65000:",
            ("community", "ip community-list expanded"),
            "^65000:",
            "65000:1",
        ),
    ],
)
async def test_a_device_rejection_attributes_its_construct_and_keeps_no_device_text(
    adapter_client, monkeypatch, recorded_logs, command, construct, condemned, cleared
):
    """Redaction must not cost the capability verdict the rejection is the only source of.

    A dry-run renders an unsupported route-policy construct cleanly, so the commit error is
    the one place the device names it. The construct identifier survives; the rest does not.
    A community construct must also reach the KIND index preflight reads with the kind the
    rejected LIST carries. One identifier for both community-list forms condemned the wrong
    members either way: a rejected regex kept passing, and standard members the device never
    refused were reported unsupported.
    """
    from nso_adapter.core.capability import get_device_capability, preflight
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
    body = {"ietf-restconf:errors": {"error": [{"error-message": _rejection(command)}]}}

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
    if condemned is not None:
        verdict = preflight(list(recorded.values()), community_members=[condemned])
        assert verdict["fully_supported"] is False, f"preflight claims support for the rejected {condemned!r}"
    if cleared is not None:
        verdict = preflight(list(recorded.values()), community_members=[cleared])
        assert verdict["fully_supported"] is True, f"preflight condemns {cleared!r}, which the device never refused"
    assert construct in recorded, f"the construct was not attributed: {sorted(recorded)}"
    surfaces = [
        json.dumps(job.error),
        json.dumps(stored.last_apply_error),
        _log_surface(recorded_logs),
        *(f"{r.detail} {r.name}" for r in recorded.values()),
    ]
    for surface in surfaces:
        assert _SECRET not in surface
        assert "snmp-server" not in surface, "opaque device text left the redaction boundary"


# ── a failed device-state read: the server's own reason reaches no sink ──

#: What a real `snmp-config` read failure can carry back: a path keyed by the community.
_READ_REASON = f"read of /snmp:snmp/community[name='{_SECRET}'] failed for {_REF}"


def _residue_error_client():
    """Every send succeeds; the post-removal device-state read answers ``status=error``."""

    def respond(request):
        if "device-state-read/run" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "network-state-export:output": {
                        "atomic": True,
                        "device-name": _DEVICE,
                        "snmp-config": {"status": "error", "error-reason": _READ_REASON},
                    }
                },
            )
        if request.method == "GET":
            return httpx.Response(404)
        return httpx.Response(200, json={"dry-run-result": {"native": {}}})

    return _client_with(httpx.MockTransport(respond))


async def test_a_failed_residue_read_keeps_the_server_reason_out_of_the_log_and_the_chain(adapter_client):
    """The residue sink names the scope and the failure TYPE, and nothing the server said.

    A `snmp-config` read failure answers an `error-reason` the device chose, and it can name a
    community-keyed path. Carrying it into the exception put it in the operator's log verbatim.
    """
    from unittest.mock import patch

    from structlog.testing import capture_logs

    from nso_adapter.core import removal as removal_mod
    from nso_adapter.store.models import Device
    from tests._secret_discipline import assert_chain_free_of, assert_records_free_of
    from tests.core.test_removal import _seed_removal_job

    removed = {"removed": {"v3-user": [["nms"]]}}
    device_id, _row = await _community()
    job_id = await _seed_removal_job(device_id, scope="snmp", context_extra=removed)
    client = _residue_error_client()
    with capture_logs() as logs, patch("nso_adapter.core.importer.get_nso_client", return_value=client):
        await removal_mod.run_removal(job_id, device_id)

    async with session() as db:
        job = await db.get(Job, job_id)
        device = await db.get(Device, device_id)
    assert job.status == JobStatus.succeeded
    assert job.result["residue_check"] == "error"
    reported = [record for record in logs if record["event"] == "removal.residue_check_error"]
    assert reported, "the failed read was not reported at all"
    assert reported[0]["scope"] == "snmp", "the scope is the half the operator needs"
    secrets = [_SECRET, _REF, _READ_REASON, "placeholder-mount", "placeholder-path", "placeholder-key"]
    assert_records_free_of(logs, secrets)
    assert reported[0]["error_type"] == "RuntimeError"

    # The same read again, through the same real client: the exception the sink formats must
    # carry nothing of the server's reason on any node of its cause/context chain.
    with pytest.raises(RuntimeError) as caught:
        await removal_mod._residue_after_removal(client, device, "snmp", {"scope": "snmp", **removed})
    assert_chain_free_of(caught.value, secrets)


# ── an envelope section that reports status=error: the wire reason reaches no sink ──

#: What a real `snmp-config` extract failure can answer in the envelope's own error-reason.
_SECTION_REASON = f"extract of /snmp:snmp/community[name='{_SECRET}'] failed for {_REF}"
_SECRETS = [_SECRET, _REF, "placeholder-mount", "placeholder-path", "placeholder-key"]


def _envelope_client(wire: str, section: dict, action_output: dict | None = None):
    """A real NsoClient whose device-state envelope answers *section* for *wire*."""

    def respond(request):
        if "device-state-read/run" in str(request.url):
            return httpx.Response(200, json={"network-state-export:output": action_output or {}})
        if request.url.path.endswith(f"/{wire}"):
            return httpx.Response(200, json={f"network-state-export:{wire}": section})
        return httpx.Response(404)

    return _client_with(httpx.MockTransport(respond))


async def _refresh_static_route(device_id: int, client):
    """One real engine refresh for the static_route family, on the real DB."""
    from nso_adapter.core.refresh_engine import run_family_refresh
    from nso_adapter.core.static_route import STATIC_ROUTE_SPEC
    from nso_adapter.store.models import Device

    async with session() as db:
        device = await db.get(Device, device_id)
        await run_family_refresh(db, device, client, STATIC_ROUTE_SPEC)


async def _outcome_rows(device_id: int) -> list[dict]:
    from sqlalchemy import select

    from nso_adapter.store.models import RefreshOutcome

    async with session() as db:
        rows = (await db.execute(select(RefreshOutcome).where(RefreshOutcome.device_id == device_id))).scalars().all()
        return [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in rows]


async def test_an_error_section_keeps_the_wire_reason_out_of_the_refresh_log(adapter_client):
    """`static_route.refresh.unavailable` classifies the failure; it never repeats the wire text.

    The envelope's `error-reason` is the server's own text. A `snmp-config` extract failure
    can answer a community-keyed path there, and the classifier carried it verbatim into the
    log record the poller writes on every failed read.
    """
    from structlog.testing import capture_logs

    from tests._secret_discipline import assert_records_free_of

    device_id = await seed_device(nso_device_name="refresh-error-section", netbox_device_id=9421)
    client = _envelope_client("static-route", {"status": "error", "error-reason": _SECTION_REASON})
    with capture_logs() as logs:
        await _refresh_static_route(device_id, client)

    reported = [record for record in logs if record["event"] == "static_route.refresh.unavailable"]
    assert reported, "the unavailable read was not reported at all"
    assert reported[0]["reason"] == "read_error", "the reason is the half the operator needs"
    assert reported[0]["detail"] == "the section reported status=error"
    assert_records_free_of(logs, _SECRETS)
    assert_records_free_of(await _outcome_rows(device_id), _SECRETS)


async def test_a_refused_escalation_keeps_the_certification_text_out_of_the_refresh_log(adapter_client):
    """The escalation's own exception is a sink too: only its TYPE may reach the record.

    A `not-ready` section escalates to the device-state-read action. The action's response is
    certified, and the refusal names what the server echoed — so carrying the exception repr
    into `detail` re-published it.
    """
    from structlog.testing import capture_logs

    from tests._secret_discipline import assert_records_free_of

    device_id = await seed_device(nso_device_name="refresh-escalation", netbox_device_id=9422)
    client = _envelope_client(
        "static-route",
        {"status": "not-ready"},
        action_output={"atomic": True, "device-name": f"other-device-{_SECRET}"},
    )
    with capture_logs() as logs:
        await _refresh_static_route(device_id, client)

    reported = [record for record in logs if record["event"] == "static_route.refresh.unavailable"]
    assert reported, "the unavailable read was not reported at all"
    assert reported[0]["reason"] == "read_error"
    assert reported[0]["detail"] == "the device-state-read action raised NsoReadContractError"
    assert_records_free_of(logs, _SECRETS)
    assert_records_free_of(await _outcome_rows(device_id), _SECRETS)


# ── a refused device-state read: the value the server echoed reaches no sink ──


def _action_client(output: dict):
    """A real NsoClient whose device-state-read action answers *output*."""

    def respond(request):
        if "device-state-read/run" in str(request.url):
            return httpx.Response(200, json={"network-state-export:output": output})
        return httpx.Response(404)

    return _client_with(httpx.MockTransport(respond))


@pytest.mark.parametrize("rejected", ["device", "status"])
async def test_a_refused_device_state_read_keeps_the_echoed_value_out_of_every_sink(adapter_client, rejected):
    """Certification names the CONSTRUCT it refused, never the value the server sent back.

    Both rejected values are server-chosen. The exception reaches the apply-side reader's
    `static_route.device_state_read_failed` record, which logged its repr.
    """
    from structlog.testing import capture_logs

    from nso_adapter.core import apply as apply_mod
    from nso_adapter.nso.client import NsoReadContractError
    from nso_adapter.store.models import Device
    from tests._secret_discipline import assert_chain_free_of, assert_records_free_of

    name = f"cert-{rejected}-refused"
    device_id = await seed_device(nso_device_name=name, netbox_device_id=9423 if rejected == "device" else 9424)
    output = (
        {"atomic": True, "device-name": f"other-device-{_SECRET}"}
        if rejected == "device"
        else {"atomic": True, "device-name": name, "static-route": {"status": f"pending-{_SECRET}"}}
    )
    client = _action_client(output)
    async with session() as db:
        device = await db.get(Device, device_id)
        with capture_logs() as logs:
            status, entries = await apply_mod._static_route_device_state(client, device)

    assert (status, entries) == ("error", {}), "a refused read must never report a clean device"
    failed = [record for record in logs if record["event"] == "static_route.device_state_read_failed"]
    assert failed, "the failed read was not reported at all"
    assert_records_free_of(logs, _SECRETS)
    assert failed[0]["error_type"] == "NsoReadContractError", "the type tells a contract breach from a blip"

    # The same read again, through the same real client: the refusal itself must carry nothing
    # of the echoed value on any node of its cause/context chain.
    with pytest.raises(NsoReadContractError) as caught:
        await client.run_device_state_read(name, ["static-route"])
    assert_chain_free_of(caught.value, _SECRETS)


# ── a failed host-key fetch: the action's own info text reaches no sink ──

#: What NSO answers when the SSH negotiation fails: free text it chose.
_KEY_INFO = f"ssh connect failed for {_REF} while reading community {_SECRET}"


async def test_a_failed_host_key_fetch_keeps_the_action_info_out_of_the_provisioning_steps(adapter_client_with_nso):
    """The raise names the action, the device and the failure kind, never the server's info.

    The provisioning result persists the step detail and the API returns it, so whatever
    `fetch-host-keys` put in its message became part of the job record.
    """
    from unittest.mock import AsyncMock, patch

    from structlog.testing import capture_logs

    from nso_adapter.core.onboarding import provision_nso_device
    from tests._secret_discipline import assert_chain_free_of, assert_records_free_of

    name = "host-key-secret"

    def respond(request):
        if "ssh/fetch-host-keys" in str(request.url):
            return httpx.Response(200, json={"tailf-ncs:output": {"result": "failed", "info": _KEY_INFO}})
        if request.method == "GET":
            return httpx.Response(200, json={"tailf-ncs:device": [{"name": name}]})
        return httpx.Response(200, json={})

    client = _client_with(httpx.MockTransport(respond))
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=client),
        patch("nso_adapter.core.onboarding.asyncio.sleep", new=AsyncMock()),
        capture_logs() as logs,
    ):
        async with session() as db:
            result = await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name=name,
                address="10.0.0.9",
                ned_id="cisco-ios-cli-6.114:cisco-ios-cli-6.114",
                authgroup="network",
            )

    assert result["ok"] is False
    step = next(entry for entry in result["steps"] if entry["step"] == "fetch_host_keys")
    assert step["status"] == "failed"
    assert "fetch-host-keys" in step["detail"], "the step must still say what failed"
    for secret in _SECRETS:
        assert secret not in json.dumps(result), "the persisted step detail repeats server text"
    assert_records_free_of(logs, _SECRETS)

    with pytest.raises(RuntimeError) as caught:
        await client.fetch_host_keys(name)
    assert_chain_free_of(caught.value, _SECRETS)
