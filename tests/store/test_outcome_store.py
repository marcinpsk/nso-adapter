# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Two-phase refresh-outcome store (READSEM §2.4).

Covers the store functions directly (phase 1 / phase 2 / pointer CAS) AND their integration
through the shared refresh engine (run_family_refresh records both phases). The store commits in
its OWN session on the caller's engine, so every assertion reads back in a FRESH session — which
also proves the outcome was committed independently of the caller's transaction.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.nso.read_outcome import (
    AbsentAuthoritative,
    Freshness,
    Present,
    Unavailable,
    UnavailableReason,
)
from nso_adapter.store import outcome_store
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, RefreshOutcome, RefreshOutcomePointer
from tests.conftest import seed_device


async def _pointer(device_id: int, family: str) -> RefreshOutcomePointer | None:
    async for db in get_session():
        return (
            await db.execute(
                select(RefreshOutcomePointer).where(
                    RefreshOutcomePointer.device_id == device_id,
                    RefreshOutcomePointer.family == family,
                )
            )
        ).scalar_one_or_none()
    return None


# ── store functions directly ─────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_record_read_outcome_phase1_flushed_in_session(adapter_client):
    """Phase 1 flushes the classified read outcome onto the caller's session — visible in-session
    with its attempt id assigned, no result/pointer yet (it rides the caller's transaction)."""
    device_id = await seed_device(nso_device_name="oc-p1", netbox_device_id=8801)
    async for db in get_session():
        attempt_id = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present({"route": []}, Freshness.fresh), refresh_source="poll"
        )
        assert attempt_id is not None
        row = await db.get(RefreshOutcome, attempt_id)
        assert (row.device_id, row.family, row.refresh_source) == (device_id, "static_route", "poll")
        assert row.read_outcome == "present"
        assert row.read_reason is None
        assert row.freshness == "fresh"
        assert row.result is None and row.completed_at is None  # phase 2 not recorded
        # no pointer until an attempt terminalizes
        ptr = (
            await db.execute(
                select(RefreshOutcomePointer).where(
                    RefreshOutcomePointer.device_id == device_id,
                    RefreshOutcomePointer.family == "static_route",
                )
            )
        ).scalar_one_or_none()
        assert ptr is None
        break


@pytest.mark.anyio
async def test_unavailable_read_reason_recorded(adapter_client):
    """An unavailable read records its reason; record_result commits it so a fresh session sees it."""
    device_id = await seed_device(nso_device_name="oc-unavail", netbox_device_id=8802)
    async for db in get_session():
        attempt_id = await outcome_store.record_read_outcome(
            db, device_id, "bgp", Unavailable(UnavailableReason.export_down), refresh_source="sse"
        )
        await outcome_store.record_result(db, attempt_id, result="kept", succeeded=False, row_count=None)
        break
    async for db in get_session():
        row = await db.get(RefreshOutcome, attempt_id)
        break
    assert row.read_outcome == "unavailable"
    assert row.read_reason == "export_down"
    assert row.freshness is None


@pytest.mark.anyio
async def test_record_result_terminalizes_and_creates_pointer(adapter_client):
    device_id = await seed_device(nso_device_name="oc-p2", netbox_device_id=8803)
    async for db in get_session():
        attempt_id = await outcome_store.record_read_outcome(
            db, device_id, "isis", AbsentAuthoritative(), refresh_source="poll"
        )
        await outcome_store.record_result(db, attempt_id, result="cleared", succeeded=True, row_count=0)
        break
    async for db in get_session():
        row = await db.get(RefreshOutcome, attempt_id)
        break
    assert row.read_outcome == "absent_authoritative"
    assert row.result == "cleared"
    assert row.succeeded is True
    assert row.row_count == 0
    assert row.completed_at is not None
    ptr = await _pointer(device_id, "isis")
    assert ptr is not None and ptr.attempt_id == attempt_id


@pytest.mark.anyio
async def test_pointer_does_not_regress_when_older_attempt_finishes_late(adapter_client):
    """Two overlapping attempts A (older) then B (newer). B terminalizes FIRST → pointer=B. A
    terminalizes LATER — its id is older, so it must NOT regress the pointer onto its stale result.
    """
    device_id = await seed_device(nso_device_name="oc-race", netbox_device_id=8804)
    async for db in get_session():
        a = await outcome_store.record_read_outcome(
            db, device_id, "ospf", Unavailable(UnavailableReason.read_error), refresh_source="poll"
        )
        b = await outcome_store.record_read_outcome(
            db, device_id, "ospf", Present({}, Freshness.fresh), refresh_source="sse"
        )
        assert b > a  # start order = insertion order = attempt id
        # B (newer) terminalizes first, then A (older) terminalizes late.
        await outcome_store.record_result(db, b, result="replaced", succeeded=True, row_count=2)
        await outcome_store.record_result(db, a, result="kept", succeeded=False, row_count=None)
        break

    ptr = await _pointer(device_id, "ospf")
    assert ptr.attempt_id == b  # newest by start order, NOT the later-finishing older A


@pytest.mark.anyio
async def test_pointer_shows_newest_failure(adapter_client):
    """A newest attempt that FAILED must become the current pointer — a failure is never hidden
    behind an older success."""
    device_id = await seed_device(nso_device_name="oc-fail", netbox_device_id=8805)
    async for db in get_session():
        a = await outcome_store.record_read_outcome(
            db, device_id, "snmp", Present({}, Freshness.fresh), refresh_source="poll"
        )
        await outcome_store.record_result(db, a, result="replaced", succeeded=True, row_count=1)
        b = await outcome_store.record_read_outcome(
            db, device_id, "snmp", Unavailable(UnavailableReason.export_down), refresh_source="poll"
        )
        await outcome_store.record_result(db, b, result="kept", succeeded=False, row_count=None)
        break

    ptr = await _pointer(device_id, "snmp")
    assert ptr.attempt_id == b
    async for db in get_session():
        current = await db.get(RefreshOutcome, ptr.attempt_id)
        break
    assert current.succeeded is False  # newest failure is the current, visible outcome


@pytest.mark.anyio
async def test_record_result_unknown_attempt_is_noop(adapter_client):
    """A result for a nonexistent attempt id is a logged no-op, not a crash (best-effort store)."""
    async for db in get_session():
        await outcome_store.record_result(db, 999999, result="replaced", succeeded=True, row_count=0)
        break  # must not raise


# ── engine integration ───────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_engine_records_two_phase_outcome_and_pointer(adapter_client):
    """run_family_refresh records phase 1 (read) + phase 2 (result) and advances the pointer."""
    from nso_adapter.core.static_route import refresh_static_routes_for_device

    device_id = await seed_device(nso_device_name="oc-eng-ok", netbox_device_id=8806)
    async for db in get_session():
        device = await db.get(Device, device_id)
        client = AsyncMock()
        client.get_static_routes.return_value = {"route": [{"prefix": "10.0.0.0/8", "next-hop": "1.1.1.1"}]}
        await refresh_static_routes_for_device(db, device, client, refresh_source="poll")
        break

    async for db in get_session():
        rows = (await db.execute(select(RefreshOutcome).where(RefreshOutcome.device_id == device_id))).scalars().all()
        break
    assert len(rows) == 1
    row = rows[0]
    assert (row.family, row.read_outcome, row.result, row.succeeded, row.row_count) == (
        "static_route",
        "present",
        "replaced",
        True,
        1,
    )
    ptr = await _pointer(device_id, "static_route")
    assert ptr is not None and ptr.attempt_id == row.id


@pytest.mark.anyio
async def test_engine_records_unavailable_outcome(adapter_client):
    """A degraded read records read_outcome=unavailable + result=kept + succeeded=False."""
    from nso_adapter.core.static_route import refresh_static_routes_for_device
    from nso_adapter.nso.client import NsoExportUnavailableError

    device_id = await seed_device(nso_device_name="oc-eng-down", netbox_device_id=8807)
    async for db in get_session():
        device = await db.get(Device, device_id)
        client = AsyncMock()
        client.get_static_routes.side_effect = NsoExportUnavailableError("export down")
        ok = await refresh_static_routes_for_device(db, device, client)
        assert ok is False
        break

    async for db in get_session():
        row = (await db.execute(select(RefreshOutcome).where(RefreshOutcome.device_id == device_id))).scalars().one()
        break
    assert row.read_outcome == "unavailable"
    assert row.read_reason == "export_down"
    assert row.result == "kept"
    assert row.succeeded is False
