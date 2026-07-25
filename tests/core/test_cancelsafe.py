# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""S5a A3: cancellation-safe terminalization (codex R1-F4/R2-F6/R3-4/R3-5/R4-3/R5-1/R6-1).

A job-budget cancel (900s whale timeout) or shutdown cancel landing between the
engine's mirror COMMIT and outcome-store terminalization leaves NEW mirror rows under
the OLD outcome pointer — the plugin then gates fresh data on a stale outcome.
The `await_uncancellable` helper runs the [materialize → record_result] span as a task
the parent shields-and-absorbs: the parent keeps its session and family locks alive
until the span completes, then re-raises the cancellation.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select

from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device
from tests.conftest import seed_device


async def _fresh_outcome(device_id: int, family: str):
    """Read the current outcome on a FRESH session — the test's own session may be
    poisoned by the injected cancel (greenlet-bridge wedge), and post-fix asserts must
    not depend on the cancelled context anyway."""
    from nso_adapter.store import outcome_store

    async for db in get_session():
        return await outcome_store.get_current_outcome(db, device_id, family)
    raise RuntimeError("no session")


async def _fresh_static_route_prefixes(device_id: int) -> list[str]:
    from nso_adapter.store.models import DeviceStaticRoute

    async for db in get_session():
        rows = (
            (await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id)))
            .scalars()
            .all()
        )
        return [r.prefix for r in rows]
    raise RuntimeError("no session")


@pytest.fixture
async def device_db(adapter_client):
    device_id = await seed_device(nso_device_name="cancel-rtr", netbox_device_id=9301)
    async for db in get_session():
        device = await db.get(Device, device_id)
        yield db, device
        return


# ── the three producer windows (red against the unwrapped spans) ─────────────


async def test_engine_cancel_between_commit_and_terminalize_keeps_pointer_consistent(device_db, monkeypatch):
    """Engine window: cancel lands right after the engine committed staged rows.

    RED today: DeviceStaticRoute rows exist but the outcome pointer never advances —
    the exact new-rows-under-old-pointer inversion. GREEN with the span wrapped: the
    terminal outcome records anyway, and the CancelledError still propagates.
    """
    from nso_adapter.core import refresh_engine
    from nso_adapter.core.static_route import STATIC_ROUTE_SPEC

    db, device = device_db
    parent = asyncio.current_task()
    real = refresh_engine._materialize_guarded

    async def cancel_after_materialize(db_, device_, spec_, attempt_id_, payload_fn, refresh_source_, outcome_):
        result = await real(db_, device_, spec_, attempt_id_, payload_fn, refresh_source_, outcome_)
        parent.cancel()  # the outer budget/shutdown cancel arrives NOW
        return result

    monkeypatch.setattr(refresh_engine, "_materialize_guarded", cancel_after_materialize)

    section = {"status": "ok", "route": [{"vrf": "", "prefix": "10.99.0.0/16", "next-hop": "1.2.3.4"}]}
    with pytest.raises(asyncio.CancelledError):
        await refresh_engine.run_family_refresh_from_section(
            db, device, STATIC_ROUTE_SPEC, section, refresh_source="poll"
        )

    assert await _fresh_static_route_prefixes(device.id) == ["10.99.0.0/16"], "engine committed pre-cancel"
    outcome = await _fresh_outcome(device.id, "static_route")
    assert outcome is not None, "terminal outcome must be recorded despite the cancel"
    assert outcome.result == "replaced"
    assert outcome.succeeded is True


async def test_attrs_cancel_between_commit_and_terminalize_records_anyway(device_db, monkeypatch):
    """Importer attrs window (importer.py commit → _record_attrs_result)."""
    from unittest.mock import AsyncMock

    from nso_adapter.core import importer as imp
    from nso_adapter.nso.client import NsoClient

    db, device = device_db
    device.ned_id = "cisco-ios-cli-6.95"
    await db.commit()

    client = AsyncMock(spec=NsoClient)
    client.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")
    client.get_device_state_doc = AsyncMock(
        return_value={
            "interface-attributes": {
                "status": "ok",
                "device-name": device.nso_device_name,
                "interface": [],
            }
        }
    )
    imp._nso_clients[device.nso_instance] = client
    imp._netbox_client = None

    parent = asyncio.current_task()
    real = imp._record_attrs_result

    async def cancel_then_record(db_, device_, attempt_id_, **kw):
        parent.cancel()  # cancel pending on the parent; unwrapped code dies at the next await
        return await real(db_, device_, attempt_id_, **kw)

    monkeypatch.setattr(imp, "_record_attrs_result", cancel_then_record)

    from unittest.mock import patch as _patch

    with (
        _patch("nso_adapter.core.importer.nso_actions.sync_from", new=AsyncMock(return_value={"result": True})),
        pytest.raises(asyncio.CancelledError),
    ):
        await imp.sync_device(device.id, db)

    outcome = await _fresh_outcome(device.id, "interface_attributes")
    assert outcome is not None, "attrs outcome must terminalize despite the cancel"


async def test_attrs_commit_failure_after_parent_cancel_terminalizes_error(device_db, monkeypatch):
    """A child commit error cannot hide behind the parent's absorbed cancellation."""
    from unittest.mock import AsyncMock

    from nso_adapter.core import importer as imp
    from nso_adapter.nso.client import NsoClient

    db, device = device_db
    device.ned_id = "cisco-ios-cli-6.95"
    await db.commit()
    device_id = device.id

    client = AsyncMock(spec=NsoClient)
    client.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")
    client.get_device_state_doc = AsyncMock(
        return_value={
            "interface-attributes": {
                "status": "ok",
                "device-name": device.nso_device_name,
                "interface": [{"interface-name": "Loopback1331"}],
            }
        }
    )
    imp._nso_clients[device.nso_instance] = client
    imp._netbox_client = None

    commit_started = asyncio.Event()
    release_commit = asyncio.Event()
    real_commit = db.commit
    calls = 0

    async def fail_first_commit():
        nonlocal calls
        calls += 1
        if calls == 1:
            commit_started.set()
            await release_commit.wait()
            raise RuntimeError("injected commit failure")
        await real_commit()

    monkeypatch.setattr(db, "commit", fail_first_commit)

    from unittest.mock import patch as _patch

    with _patch("nso_adapter.core.importer.nso_actions.sync_from", new=AsyncMock(return_value={"result": True})):
        task = asyncio.create_task(imp.sync_device(device_id, db))
        await commit_started.wait()
        task.cancel()
        release_commit.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    outcome = await _fresh_outcome(device_id, "interface_attributes")
    assert outcome is not None
    assert (outcome.result, outcome.succeeded) == ("error", False)
    assert await db.get(Device, device_id) is not None, "cleanup must leave the caller session usable"


async def test_attrs_cancel_after_phase_one_flush_terminalizes_error(device_db, monkeypatch):
    """Cancellation before savepoint assignment still recovers the started attempt."""
    from unittest.mock import AsyncMock

    from nso_adapter.core import importer as imp
    from nso_adapter.nso.client import NsoClient

    db, device = device_db
    device.ned_id = "cisco-ios-cli-6.95"
    await db.commit()
    device_id = device.id

    client = AsyncMock(spec=NsoClient)
    client.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")
    client.get_device_state_doc = AsyncMock(
        return_value={
            "interface-attributes": {
                "status": "ok",
                "device-name": device.nso_device_name,
                "interface": [],
            }
        }
    )
    imp._nso_clients[device.nso_instance] = client
    imp._netbox_client = None

    phase_one_flushed = asyncio.Event()
    real_record = imp._record_attrs_read

    async def _pause_after_real_flush(*args, **kwargs):
        attempt_id = await real_record(*args, **kwargs)
        phase_one_flushed.set()
        await asyncio.Event().wait()
        return attempt_id

    monkeypatch.setattr(imp, "_record_attrs_read", _pause_after_real_flush)

    from unittest.mock import patch as _patch

    with _patch("nso_adapter.core.importer.nso_actions.sync_from", new=AsyncMock(return_value={"result": True})):
        task = asyncio.create_task(imp.sync_device(device_id, db))
        await phase_one_flushed.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    outcome = await _fresh_outcome(device_id, "interface_attributes")
    assert outcome is not None
    assert (outcome.result, outcome.succeeded) == ("error", False)
    assert await db.get(Device, device_id) is not None


async def test_redistribution_cancel_between_commit_and_terminalize_records_anyway(device_db, monkeypatch):
    """Redistribution window (redistribution.py tier-2 commit → record_result)."""
    from nso_adapter.core import redistribution as redi
    from nso_adapter.nso.read_outcome import Freshness, Present

    db, device = device_db
    parent = asyncio.current_task()
    real = redi.outcome_store.record_result
    fired = {"n": 0}

    async def cancel_then_record(db_, attempt_id_, **kw):
        if fired["n"] == 0:
            fired["n"] = 1
            parent.cancel()
        return await real(db_, attempt_id_, **kw)

    monkeypatch.setattr(redi.outcome_store, "record_result", cancel_then_record)

    outcomes = {
        "connected": Present({"redistribute": []}, Freshness.fresh),
        "static": Present({"redistribute": []}, Freshness.fresh),
        "isis": Present({"redistribute": []}, Freshness.fresh),
    }
    with pytest.raises(asyncio.CancelledError):
        await redi.refresh_redistribution_from_outcomes(db, device, outcomes, refresh_source="poll")

    outcome = await _fresh_outcome(device.id, "redistribution")
    assert outcome is not None, "redistribution outcome must terminalize despite the cancel"


# ── await_uncancellable contract (codex-pinned semantics) ────────────────────


async def test_single_cancel_absorbed_until_span_completes(adapter_client):
    from nso_adapter.core.cancelsafe import await_uncancellable

    state = {"done": False}

    async def span():
        await asyncio.sleep(0.05)
        state["done"] = True
        return 42

    async def runner():
        return await await_uncancellable(span())

    task = asyncio.create_task(runner())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert state["done"] is True, "the span must complete before the cancellation re-raises"


async def test_double_cancel_still_absorbed(adapter_client):
    """The 5s shutdown wait_for wrappers deliver a SECOND cancel — also absorbed."""
    from nso_adapter.core.cancelsafe import await_uncancellable

    state = {"done": False}

    async def span():
        await asyncio.sleep(0.08)
        state["done"] = True

    async def runner():
        await await_uncancellable(span())

    task = asyncio.create_task(runner())
    await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert state["done"] is True


async def test_span_exception_propagates_without_cancel(adapter_client):
    from nso_adapter.core.cancelsafe import await_uncancellable

    async def span():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await await_uncancellable(span())


async def test_cancellation_wins_over_span_exception(adapter_client):
    """Precedence: after an absorbed cancel, the caller sees CancelledError; the span's
    own failure is logged, never masks the cancellation."""
    from nso_adapter.core.cancelsafe import await_uncancellable

    async def span():
        await asyncio.sleep(0.05)
        raise RuntimeError("span died during drain")

    async def runner():
        await await_uncancellable(span())

    task = asyncio.create_task(runner())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_nonterminating_cancel_suppressor_is_bounded(adapter_client):
    """codex R4-3/R5-1/R6-1: a span that NEVER finishes and suppresses cancellation must
    not hang shutdown — deadline expiry cancels the span and the drain is a
    non-cancelling bounded wait; the helper returns within the bound regardless."""
    from nso_adapter.core.cancelsafe import await_uncancellable

    absorbed = {"n": 0}

    async def stubborn():
        # Survives the helper's ENTIRE protocol (absorbs its expiry cancel and outlives
        # the drain window = genuinely non-terminating from the helper's perspective),
        # but yields to a SECOND cancel so pytest-asyncio's loop teardown — which also
        # gathers-after-cancel — can reap the abandoned task instead of hanging forever.
        while True:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                absorbed["n"] += 1
                if absorbed["n"] >= 2:
                    raise
                continue

    async def runner():
        await await_uncancellable(stubborn(), absorb_deadline_s=0.1, drain_timeout_s=0.1)

    task = asyncio.create_task(runner())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)


async def test_deadline_expiry_cancels_a_slow_span(adapter_client):
    """Past the absorb deadline the span itself is cancelled (and honors it here)."""
    from nso_adapter.core.cancelsafe import await_uncancellable

    state = {"cancelled": False}

    async def slow():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            state["cancelled"] = True
            raise

    async def runner():
        await await_uncancellable(slow(), absorb_deadline_s=0.05, drain_timeout_s=0.5)

    task = asyncio.create_task(runner())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)
    assert state["cancelled"] is True
