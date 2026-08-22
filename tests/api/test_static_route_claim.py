# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R1b chunk 3 / Q8: the static-route intent PUT under the device claim.

The endpoint acquires the device claim BEFORE it reads the state it mutates, so a
concurrent writer can never leave it planning against a snapshot that is already gone
(M2.6). Payload-internal refusals still run first and never claim anything (M2.5), and a
claim it cannot get inside the OQ6 budget is a 409, not a wait forever.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select

from nso_adapter.core.claim import acquire_claim, release_claim
from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

A = ("", "10.0.0.0/24", "192.0.2.1")
B = ("", "10.0.1.0/24", "192.0.2.2")
C = ("", "10.0.2.0/24", "192.0.2.3")


def entry(triple, *, route_id: int | None = None) -> dict:
    vrf, prefix, next_hop = triple
    body = {"vrf": vrf, "prefix": prefix, "next_hop": next_hop}
    if route_id is not None:
        body["route_id"] = route_id
    return body


async def _seed_rows(device_id: int, rows: list[tuple[int, tuple[str, str, str]]]) -> None:
    from datetime import UTC, datetime

    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        for route_id, (vrf, prefix, next_hop) in rows:
            db.add(
                StaticRouteIntent(
                    device_id=device_id,
                    route_id=route_id,
                    vrf=vrf,
                    prefix=prefix,
                    next_hop=next_hop,
                    accepted_at=datetime(2026, 6, 1, tzinfo=UTC),
                )
            )
        await db.commit()


async def _triples_by_route_id(device_id: int) -> dict[int, tuple[str, str, str]]:
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        rows = (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id))).scalars()
        return {r.route_id: (r.vrf, r.prefix, r.next_hop) for r in rows}


async def _claim_row(device_id: int):
    from nso_adapter.store.models import DeviceClaim

    async with session() as db:
        return await db.get(DeviceClaim, device_id)


def _shrink_claim_wait(monkeypatch, seconds: float) -> None:
    """Make the OQ6 budget short enough to assert on. The knob is the production one."""
    from nso_adapter.config import get_config

    monkeypatch.setattr(get_config(), "intent_claim_wait_seconds", seconds)


async def test_the_wait_budget_defaults_to_five_seconds(adapter_client):
    """OQ6's decided default, and the knob that overrides it, are both production config."""
    from nso_adapter.config import get_config

    assert get_config().intent_claim_wait_seconds == 5.0


async def test_a_successful_put_leaves_no_claim_behind(adapter_client):
    device_id = await seed_device(nso_device_name="sr-claim-release", netbox_device_id=9400)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent",
        json={"routes": [entry(A, route_id=7)]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    assert await _claim_row(device_id) is None


async def test_payload_refusal_never_acquires_a_claim(adapter_client):
    """M2.5 — the payload-internal refusals fire BEFORE acquisition, so nothing is claimed."""
    device_id = await seed_device(nso_device_name="sr-claim-422", netbox_device_id=9401)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent",
        json={"routes": [entry(A, route_id=7), entry(A, route_id=8)]},
        headers=AUTH,
    )
    assert resp.status_code == 422

    assert await _claim_row(device_id) is None


async def test_a_held_claim_turns_the_put_into_a_409(adapter_client, monkeypatch):
    """OQ6 — 5s by default, configurable; on expiry the standard envelope, not a hang."""
    device_id = await seed_device(nso_device_name="sr-claim-409", netbox_device_id=9402)
    _shrink_claim_wait(monkeypatch, 0.3)

    holder = await acquire_claim(device_id, "job")
    try:
        resp = await adapter_client.put(
            f"/api/v1/devices/{device_id}/static-route-intent",
            json={"routes": [entry(A, route_id=7)]},
            headers=AUTH,
        )
    finally:
        await release_claim(holder)

    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "conflict"
    assert resp.json()["error"]["detail"]["reason"] == "device_claimed"
    # Refused, not half-applied.
    assert await _triples_by_route_id(device_id) == {}

    # The published 409 must name the cause the endpoint just refused on. It refuses on ANY
    # competing device claim — here a plain "job" claim with no job row behind it — so the
    # fragment saying "a job is already running for this device" documented a narrower
    # condition than the one that fires (#1558 rework 3, finding 5).
    described = _published_409_description()
    assert "busy with another operation" in described, described
    assert "job is already running" not in described, described


def _published_409_description() -> str:
    """The 409 description the OpenAPI schema publishes for the static-route intent PUT."""
    from nso_adapter.main import create_app

    schema = create_app().openapi()
    put = schema["paths"]["/api/v1/devices/{device_id}/static-route-intent"]["put"]
    return put["responses"]["409"]["description"]


async def test_the_put_waits_for_the_claim_instead_of_reading_around_it(adapter_client, monkeypatch):
    """M2.6 — the reload happens under the claim, so a stale plan is not constructible.

    A holds the device (as an in-flight endpoint would) and moves route 8 B→C while B's PUT
    is already in flight. B's payload was computed against the PRE-A snapshot: it says
    "move route 7 to C, leave route 8 at B". Under Q8, B cannot read anything until A lets
    go, so it recomputes and issues BOTH updates. Against a read-then-acquire endpoint B
    plans from the stale snapshot, never updates row 8 (unchanged in its own view), and the
    planned outcome is C/C — which the deferred identity constraint rejects at COMMIT.
    """
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-claim-reload", netbox_device_id=9403)
    await _seed_rows(device_id, [(7, A), (8, B)])
    _shrink_claim_wait(monkeypatch, 30.0)

    holder = await acquire_claim(device_id, "job")

    task = asyncio.create_task(
        adapter_client.put(
            f"/api/v1/devices/{device_id}/static-route-intent",
            json={"routes": [entry(C, route_id=7), entry(B, route_id=8)]},
            headers=AUTH,
        )
    )
    try:
        await asyncio.sleep(0.5)
        assert not task.done(), "the PUT read the store without waiting for the claim"

        # A's committed mutation: route 8 moves B -> C.
        async with session() as db:
            row = (
                await db.execute(
                    select(StaticRouteIntent).where(
                        StaticRouteIntent.device_id == device_id, StaticRouteIntent.route_id == 8
                    )
                )
            ).scalar_one()
            row.vrf, row.prefix, row.next_hop = C
            await db.commit()
    finally:
        await release_claim(holder)

    resp = await asyncio.wait_for(task, timeout=30)
    assert resp.status_code == 200, resp.text
    assert await _triples_by_route_id(device_id) == {7: C, 8: B}


async def test_a_failure_after_the_guard_lock_neither_hangs_nor_leaks_the_claim(adapter_client, monkeypatch):
    """The body dies AFTER ``lock_claim`` took the claim row FOR UPDATE in the request
    session. Releasing through a second session then waits on our own uncommitted lock:
    the request hangs forever and the 500 never leaves. The guarded transaction must be
    rolled back before the standalone release."""
    from nso_adapter.api import static_route as sr_mod

    device_id = await seed_device(nso_device_name="sr-claim-lockfail", netbox_device_id=9405)
    guard_locked = False
    body_reached = False
    lock_claim = sr_mod.lock_claim

    async def _record_guard_lock(*args, **kwargs):
        nonlocal guard_locked
        result = await lock_claim(*args, **kwargs)
        guard_locked = True
        return result

    async def _boom(*args, **kwargs):
        nonlocal body_reached
        body_reached = True
        assert guard_locked, "the request body ran before its claim-row guard was locked"
        raise RuntimeError("forced post-lock failure")

    monkeypatch.setattr(sr_mod, "lock_claim", _record_guard_lock)
    monkeypatch.setattr(sr_mod, "_apply_static_route_intent", _boom)

    # The catch-all answers the unhandled body error as a 500; the contract under test is
    # that the request UNWINDS (no hang on our own lock) and the claim is gone afterwards.
    resp = await asyncio.wait_for(
        adapter_client.put(
            f"/api/v1/devices/{device_id}/static-route-intent",
            json={"routes": [entry(A)]},
            headers=AUTH,
        ),
        timeout=15,
    )
    assert resp.status_code == 500, resp.text
    assert guard_locked, "the failing body stub was never reached after the guard lock"
    assert body_reached, "the request did not reach the failing body stub"
    assert await _claim_row(device_id) is None


async def test_a_failure_after_acquisition_still_releases_the_claim(adapter_client):
    """M2.7 — drive the endpoint's claim context manager directly and raise inside it."""
    from nso_adapter.core.claim import held_claim

    device_id = await seed_device(nso_device_name="sr-claim-m27", netbox_device_id=9404)

    with pytest.raises(RuntimeError):
        async with held_claim(device_id, "intent_put", timeout_s=5.0) as reg:
            assert reg is not None
            assert await _claim_row(device_id) is not None
            raise RuntimeError("forced post-acquisition failure")

    assert await _claim_row(device_id) is None
