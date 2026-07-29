# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S3 — the engine's envelope fetch path (wire_name set on a FamilySpec).

Exercised end-to-end against a real DB with the real ``STATIC_ROUTE_SPEC`` materializer,
flipped onto the envelope via ``dataclasses.replace`` (the same one-line flip B1 makes
permanent). The client is an ``AsyncMock(spec=NsoClient)`` — the transport boundary; its
section/action payloads are exactly the live wire shapes.
"""

from __future__ import annotations

import dataclasses
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.refresh_engine import run_family_refresh, run_family_refresh_from_section
from nso_adapter.core.static_route import STATIC_ROUTE_SPEC
from nso_adapter.nso.client import NsoClient, NsoExportUnavailableError
from nso_adapter.store.models import Device, DeviceStaticRoute, RefreshOutcome
from tests.conftest import seed_device, session

ENV_SPEC = dataclasses.replace(STATIC_ROUTE_SPEC, wire_name="static-route")

OK_SECTION = {
    "status": "ok",
    "last-updated": "2026-07-20T12:00:00+00:00",
    "route": [{"vrf": "", "prefix": "172.16.0.0/12", "next-hop": "10.0.0.1"}],
}


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


async def _seed_one_route(device_id: int) -> None:
    async with session() as db:
        db.add(DeviceStaticRoute(device_id=device_id, vrf="", prefix="10.0.0.0/8", next_hop="1.1.1.1"))
        await db.commit()


async def _routes(db, device_id: int) -> list[str]:
    rows = (await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id))).scalars().all()
    return [r.prefix for r in rows]


async def _latest_outcome(db, device_id: int) -> RefreshOutcome:
    rows = (
        (
            await db.execute(
                select(RefreshOutcome).where(RefreshOutcome.device_id == device_id).order_by(RefreshOutcome.id.desc())
            )
        )
        .scalars()
        .all()
    )
    assert rows, "no refresh_outcome row recorded"
    return rows[0]


def _client(section=None, action_output=None) -> AsyncMock:
    client = AsyncMock(spec=NsoClient)
    client.get_device_state_section.return_value = section
    if action_output is not None:
        client.run_device_state_read.return_value = action_output
    return client


# ── the envelope fetch path ──────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_ok_section_replaces_rows_and_records_fresh(adapter_client):
    device_id = await seed_device(nso_device_name="eng-env-ok", netbox_device_id=9701)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = _client(section=OK_SECTION)

        ok = await run_family_refresh(db, device, client, ENV_SPEC)

        assert ok is True
        assert await _routes(db, device_id) == ["172.16.0.0/12"]
        client.get_device_state_section.assert_awaited_once_with("eng-env-ok", "static-route")
        client.run_device_state_read.assert_not_awaited()
        outcome_row = await _latest_outcome(db, device_id)
        assert (outcome_row.read_outcome, outcome_row.freshness) == ("present", "fresh")
        assert (outcome_row.result, outcome_row.succeeded) == ("replaced", True)


@pytest.mark.anyio
async def test_ok_without_list_keys_is_the_authoritative_clear(adapter_client):
    """RESTCONF omits empty lists — ok + absent keys must CLEAR, not keep."""
    device_id = await seed_device(nso_device_name="eng-env-okempty", netbox_device_id=9702)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        ok = await run_family_refresh(db, device, _client(section={"status": "ok"}), ENV_SPEC)

        assert ok is True
        assert await _routes(db, device_id) == []


@pytest.mark.anyio
async def test_stale_replaces_rows_and_records_degraded(adapter_client):
    """Operator decision: stale = degraded-success — rows replace, phase-1 carries the marker."""
    device_id = await seed_device(nso_device_name="eng-env-stale", netbox_device_id=9703)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        section = dict(OK_SECTION, status="stale")
        ok = await run_family_refresh(db, device, _client(section=section), ENV_SPEC)

        assert ok is True
        assert await _routes(db, device_id) == ["172.16.0.0/12"]
        outcome_row = await _latest_outcome(db, device_id)
        assert (outcome_row.read_outcome, outcome_row.freshness) == ("present", "stale")
        assert (outcome_row.result, outcome_row.succeeded) == ("replaced", True)


@pytest.mark.anyio
async def test_unsupported_keeps_rows_and_reports_success(adapter_client):
    """RED-FIRST behavior delta: legacy probe-confirmed 404 CLEARED an unsupported-NED
    device's rows; the envelope's declared `unsupported` keeps them (design §1 vocabulary)."""
    device_id = await seed_device(nso_device_name="eng-env-unsup", netbox_device_id=9704)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        ok = await run_family_refresh(db, device, _client(section={"status": "unsupported"}), ENV_SPEC)

        assert ok is True
        assert await _routes(db, device_id) == ["10.0.0.0/8"], "unsupported must KEEP rows"
        outcome_row = await _latest_outcome(db, device_id)
        assert (outcome_row.read_outcome, outcome_row.read_reason) == ("unavailable", "unsupported")
        assert (outcome_row.result, outcome_row.succeeded) == ("kept", True)


@pytest.mark.anyio
async def test_error_keeps_rows_and_reports_degraded(adapter_client):
    device_id = await seed_device(nso_device_name="eng-env-err", netbox_device_id=9705)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        section = {"status": "error", "error-reason": "extract boom"}
        ok = await run_family_refresh(db, device, _client(section=section), ENV_SPEC)

        assert ok is False
        assert await _routes(db, device_id) == ["10.0.0.0/8"]


# ── not-ready escalation (the record-warming path) ──────────────────────────────────


@pytest.mark.anyio
async def test_not_ready_escalates_to_the_action_and_uses_its_section(adapter_client):
    """Post-reload the envelope is not-ready fleet-wide; ONE action call answers the read
    AND re-warms the records (the envelope never extracts on its own)."""
    device_id = await seed_device(nso_device_name="eng-env-notready", netbox_device_id=9706)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = _client(
            section={"status": "not-ready"},
            action_output={"atomic": True, "static-route": OK_SECTION},
        )

        ok = await run_family_refresh(db, device, client, ENV_SPEC)

        assert ok is True
        assert await _routes(db, device_id) == ["172.16.0.0/12"]
        client.run_device_state_read.assert_awaited_once_with("eng-env-notready", ["static-route"])


@pytest.mark.anyio
async def test_hostile_always_not_ready_is_refused_not_looped(adapter_client):
    """Action sections are terminal by contract — a not-ready from the action is refused
    (read_error, rows kept) after EXACTLY one call, never retried in a loop."""
    device_id = await seed_device(nso_device_name="eng-env-hostile", netbox_device_id=9707)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = _client(
            section={"status": "not-ready"},
            action_output={"atomic": True, "static-route": {"status": "not-ready"}},
        )

        ok = await run_family_refresh(db, device, client, ENV_SPEC)

        assert ok is False
        assert await _routes(db, device_id) == ["10.0.0.0/8"]
        assert client.run_device_state_read.await_count == 1
        outcome_row = await _latest_outcome(db, device_id)
        assert (outcome_row.read_outcome, outcome_row.read_reason) == ("unavailable", "read_error")


@pytest.mark.anyio
async def test_escalation_action_error_keeps_rows(adapter_client):
    """Bracket exhaustion / unknown device surface as an action error → keep, degraded."""
    device_id = await seed_device(nso_device_name="eng-env-actfail", netbox_device_id=9708)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = _client(section={"status": "not-ready"})
        client.run_device_state_read.side_effect = RuntimeError("action exploded")

        ok = await run_family_refresh(db, device, client, ENV_SPEC)

        assert ok is False
        assert await _routes(db, device_id) == ["10.0.0.0/8"]


@pytest.mark.anyio
async def test_escalation_output_missing_the_section_keeps_rows(adapter_client):
    device_id = await seed_device(nso_device_name="eng-env-missect", netbox_device_id=9709)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = _client(section={"status": "not-ready"}, action_output={"atomic": True})

        ok = await run_family_refresh(db, device, client, ENV_SPEC)

        assert ok is False
        assert await _routes(db, device_id) == ["10.0.0.0/8"]


# ── device-level absence + export-down ──────────────────────────────────────────────


@pytest.mark.anyio
async def test_device_absent_keeps_rows_for_every_family(adapter_client):
    """READSEM S5 (1327): section None = device unknown to a HEALTHY export. With ``empty_policy``
    retired, device-absence resolves UNIFORMLY to ``Unavailable(not_authoritative)`` — KEEP the
    last-known rows for EVERY family (was: pop families cleared). A device blip must never wipe a
    mirror; a true removal is handled by the device-lifecycle deleting the Device row."""
    device_id = await seed_device(nso_device_name="eng-env-devgone", netbox_device_id=9710)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        ok = await run_family_refresh(db, device, _client(section=None), ENV_SPEC)

        assert ok is True  # not_authoritative = declared absence → kept, not a degraded surface
        assert await _routes(db, device_id) == ["10.0.0.0/8"]


@pytest.mark.anyio
async def test_export_down_keeps_rows(adapter_client):
    device_id = await seed_device(nso_device_name="eng-env-down", netbox_device_id=9712)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        client = AsyncMock(spec=NsoClient)
        client.get_device_state_section.side_effect = NsoExportUnavailableError("export down")

        ok = await run_family_refresh(db, device, client, ENV_SPEC)

        assert ok is False
        assert await _routes(db, device_id) == ["10.0.0.0/8"]


@pytest.mark.anyio
async def test_superseded_unavailable_attempt_does_not_degrade_newer_winner(adapter_client, monkeypatch):
    from nso_adapter.store import outcome_store

    device_id = await seed_device(nso_device_name="eng-env-superseded", netbox_device_id=9713)
    await _seed_one_route(device_id)

    async def superseded_result(*_args, **_kwargs):
        return False

    monkeypatch.setattr(outcome_store, "record_result", superseded_result)
    async with _device_session(device_id) as (db, device):
        ok = await run_family_refresh(
            db,
            device,
            _client(section={"status": "error", "error-reason": "older read failed"}),
            ENV_SPEC,
        )

        assert ok is True
        assert await _routes(db, device_id) == ["10.0.0.0/8"]


# ── run_family_refresh_from_section (grains b/c) ────────────────────────────────────


@pytest.mark.anyio
async def test_from_section_ok_replaces_without_any_client_call(adapter_client):
    device_id = await seed_device(nso_device_name="eng-env-fromsec", netbox_device_id=9714)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        ok = await run_family_refresh_from_section(db, device, ENV_SPEC, OK_SECTION, refresh_source="sync")

        assert ok is True
        assert await _routes(db, device_id) == ["172.16.0.0/12"]
        outcome_row = await _latest_outcome(db, device_id)
        assert outcome_row.refresh_source == "sync"


@pytest.mark.anyio
async def test_from_section_not_ready_keeps_and_reports_degraded(adapter_client):
    """No escalation in from_section — the doc supplier owns healing; honesty here."""
    device_id = await seed_device(nso_device_name="eng-env-fromnr", netbox_device_id=9715)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        ok = await run_family_refresh_from_section(db, device, ENV_SPEC, {"status": "not-ready"}, refresh_source="sync")

        assert ok is False
        assert await _routes(db, device_id) == ["10.0.0.0/8"]


# ── codex R1: materializer-failure terminalization (F6) + escalation coordination (F2) ──


@pytest.mark.anyio
async def test_materializer_failure_terminalizes_the_outcome_attempt(adapter_client):
    """A materializer exception must still terminalize phase 2 (result=error) so the
    newest FAILURE is visible via the pointer — a nonterminal row hides it from S4."""
    device_id = await seed_device(nso_device_name="eng-env-matfail", netbox_device_id=9716)
    await _seed_one_route(device_id)

    async def _boom_materialize(db, device, payload, refresh_source):
        raise RuntimeError("materializer exploded")

    spec = dataclasses.replace(ENV_SPEC, materialize=_boom_materialize)
    async with _device_session(device_id) as (db, device):
        with pytest.raises(RuntimeError, match="materializer exploded"):
            await run_family_refresh(db, device, _client(section=OK_SECTION), spec)

        outcome_row = await _latest_outcome(db, device_id)
        assert (outcome_row.result, outcome_row.succeeded) == ("error", False)
        assert outcome_row.completed_at is not None


@pytest.mark.anyio
async def test_concurrent_same_family_refreshes_escalate_once(adapter_client):
    """Post-reload alignment: two concurrent refreshes of ONE (device, family) must be
    singleflighted. The action mock YIELDS mid-flight (as the real HTTP call does), so
    without the per-(device,family) lock both callers classify not-ready before either
    action lands and the action fires TWICE."""
    import asyncio

    device_id = await seed_device(nso_device_name="eng-env-race", netbox_device_id=9717)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        acted = False

        async def _section(name, wire_family):
            # Records are warm only after the first action completed.
            return OK_SECTION if acted else {"status": "not-ready"}

        async def _action(name, wire_families):
            nonlocal acted
            await asyncio.sleep(0)  # a real yield point — the live HTTP call suspends here
            acted = True
            return {"atomic": True, "static-route": OK_SECTION}

        client = AsyncMock(spec=NsoClient)
        client.get_device_state_section.side_effect = _section
        client.run_device_state_read.side_effect = _action

        async with session() as db2:
            device2 = await db2.get(Device, device_id)
            ok1, ok2 = await asyncio.gather(
                run_family_refresh(db, device, client, ENV_SPEC),
                run_family_refresh(db2, device2, client, ENV_SPEC),
            )

        assert (ok1, ok2) == (True, True)
        assert client.run_device_state_read.await_count == 1, "the second refresh must not re-fire the action"
        assert await _routes(db, device_id) == ["172.16.0.0/12"]


# ── codex R1 F11: real-client → engine → DB integration (no AsyncMock boundary) ─────


@pytest.mark.anyio
async def test_real_client_envelope_refresh_end_to_end(adapter_client):
    """The full S3 grain-a path with NO mocked client: real NsoClient request construction
    against a routing transport, real classification, real materializer, real outcome rows.
    An AsyncMock client cannot catch URL/shape drift between client and engine — this does."""
    import httpx

    from tests.nso.test_device_state_client import EnvelopeTransport, _make_client

    device_id = await seed_device(nso_device_name="eng-env-realclient", netbox_device_id=9718)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):
        real_client = _make_client()
        transport = EnvelopeTransport(device_body={"network-state-export:static-route": OK_SECTION})
        real_client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

        ok = await run_family_refresh(db, device, real_client, ENV_SPEC)

        assert ok is True
        assert await _routes(db, device_id) == ["172.16.0.0/12"]
        url = str(transport.requests[0].url)
        assert url.endswith("/restconf/data/network-state-export:device-state/device=eng-env-realclient/static-route")
        outcome_row = await _latest_outcome(db, device_id)
        assert (outcome_row.read_outcome, outcome_row.result) == ("present", "replaced")


# ── live-found: a store failure that POISONS the session must stay best-effort ──────


@pytest.mark.anyio
async def test_poisoned_outcome_store_fails_closed_and_preserves_payload(adapter_client, monkeypatch):
    """An authoritative body cannot publish without its matching revision record."""
    from nso_adapter.store import outcome_store as outcome_store_mod

    device_id = await seed_device(nso_device_name="eng-env-poison", netbox_device_id=9719)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):

        async def _poisoning_record(db_, device_id_, family, outcome, *, refresh_source, source_epoch):
            # The live PG failure's EFFECT: the doomed transaction gets rolled back and
            # every ORM instance expires — then the store raises. (A plain `raise` leaves
            # the session healthy, which is why the old best-effort test stayed green.)
            await db_.rollback()
            raise RuntimeError("relation refresh_outcome does not exist")

        monkeypatch.setattr(outcome_store_mod, "record_read_outcome", _poisoning_record)

        with pytest.raises(RuntimeError, match="cannot publish an authoritative body"):
            await run_family_refresh(db, device, _client(section=OK_SECTION), ENV_SPEC)

        assert await _routes(db, device_id) == ["10.0.0.0/8"], "the prior payload must remain visible"


# ── codex R2: commit-time materializer failure (F1) + phase-2 store poisoning (F2) ──


@pytest.mark.anyio
async def test_commit_time_materializer_failure_recovers_and_terminalizes(adapter_client):
    """A transaction-neutral materializer leaves a bad row for the ENGINE commit.

    A DB error surfacing at that boundary dooms the whole transaction. The engine must recover
    the session and record a FRESH terminal failure row (the flushed phase-1 row died
    with the transaction), and the session must be usable afterwards."""
    from sqlalchemy.exc import IntegrityError

    from nso_adapter.store.models import RefreshOutcome

    device_id = await seed_device(nso_device_name="eng-env-commitfail", netbox_device_id=9721)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):

        async def _bad_materialize(db_, device_, payload, refresh_source):
            # A NOT NULL violation that must surface at the engine's commit.
            db_.add(RefreshOutcome(device_id=None, family=None, read_outcome=None))

        spec = dataclasses.replace(ENV_SPEC, materialize=_bad_materialize)
        with pytest.raises(IntegrityError):
            await run_family_refresh(db, device, _client(section=OK_SECTION), spec)

        outcome_row = await _latest_outcome(db, device_id)
        assert (outcome_row.result, outcome_row.succeeded) == ("error", False)
        # The session is usable and a follow-up refresh with a healthy client works.
        ok = await run_family_refresh(db, device, _client(section=OK_SECTION), ENV_SPEC)
        assert ok is True
        assert await _routes(db, device_id) == ["172.16.0.0/12"]


@pytest.mark.anyio
async def test_phase2_store_poisoning_recovers_the_session(adapter_client, monkeypatch):
    """Codex S3-R2 F2: a phase-2 store failure that dooms the transaction must not leave
    the session poisoned for the NEXT family in the caller's fan-out."""
    from nso_adapter.store import outcome_store as outcome_store_mod

    device_id = await seed_device(nso_device_name="eng-env-p2poison", netbox_device_id=9722)
    await _seed_one_route(device_id)
    async with _device_session(device_id) as (db, device):

        async def _poisoning_result(db_, attempt_id, *, result, succeeded, row_count=None):
            from nso_adapter.store.models import RefreshOutcome as _RO

            # An authentic doomed transaction: a failing FLUSH puts the session in
            # pending-rollback (a fake that merely raises leaves it healthy).
            db_.add(_RO(device_id=None, family=None, read_outcome=None))
            await db_.flush()

        monkeypatch.setattr(outcome_store_mod, "record_result", _poisoning_result)

        ok = await run_family_refresh(db, device, _client(section=OK_SECTION), ENV_SPEC)

        assert ok is True, "phase-2 telemetry failure must never fail the refresh"
        async with session() as fresh_db:
            assert await _routes(fresh_db, device_id) == ["172.16.0.0/12"], (
                "phase-2 recovery must not roll back the committed mirror"
            )
        # The next family's refresh on the SAME session must work (sync_device fan-out shape).
        monkeypatch.undo()
        ok2 = await run_family_refresh(db, device, _client(section=OK_SECTION), ENV_SPEC)
        assert ok2 is True
        assert await _routes(db, device_id) == ["172.16.0.0/12"]
