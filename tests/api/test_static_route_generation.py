# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R3 chunk P0 — generation carriage, the fingerprint echo and the read-back GET.

Pins P0.1, P0.3 (the PUT half) and P0.11. Every case drives the REAL
``PUT``/``GET /api/v1/devices/{id}/static-route-intent`` against a real PostgreSQL clone
and reads the real ``static_route_intent`` rows back — no store double.

The apply half of P0.3 (the echoed fingerprint equals the one the result reports for an
unmutated row, and differs for a mutated one) and P0.2 live in
``tests/core/test_static_route_proof.py``, where the real ``run_apply`` runs.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, seed_device, session

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

A = ("", "10.0.0.0/24", "192.0.2.1")
B = ("", "10.0.1.0/24", "192.0.2.2")


def entry(triple: tuple[str, str, str], **extra) -> dict:
    vrf, prefix, next_hop = triple
    return {"vrf": vrf, "prefix": prefix, "next_hop": next_hop, **extra}


async def push(client, device_id: int, routes: list[dict]):
    return await client.put(f"/api/v1/devices/{device_id}/static-route-intent", json={"routes": routes}, headers=AUTH)


async def stored_generations(device_id: int) -> dict[tuple, int | None]:
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        rows = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return {(r.vrf, r.prefix, r.next_hop): r.intent_generation for r in rows}


# ── P0.1 — generation is adopted only when non-null ──────────────────────────


async def test_p0_1_an_omitted_generation_never_clears_the_stored_one(adapter_client):
    """P0.1 — push ``generation=7``, then a push omitting it, then an explicit ``8``.

    ``route_id`` is already adopt-only-when-non-null (A3) because a pre-R3 push must not
    undo the fleet backfill; the generation rides the same rule for the same reason —
    an old pusher that never learned the field must not erase the correlation the new one
    established. A plain assignment would write NULL on the second push.
    """
    device_id = await seed_device(nso_device_name="sr-gen-01", netbox_device_id=7401)

    resp = await push(adapter_client, device_id, [entry(A, route_id=41, generation=7)])
    assert resp.status_code == 200
    assert await stored_generations(device_id) == {A: 7}

    resp = await push(adapter_client, device_id, [entry(A, route_id=41)])
    assert resp.status_code == 200
    assert await stored_generations(device_id) == {A: 7}, "an omitted generation is not a clear"

    resp = await push(adapter_client, device_id, [entry(A, route_id=41, generation=8)])
    assert resp.status_code == 200
    assert await stored_generations(device_id) == {A: 8}, "an explicit generation overwrites"


async def test_p0_1_a_new_row_carries_the_generation_it_was_pushed_with(adapter_client):
    """A row the payload creates keeps its generation; one pushed without stays NULL."""
    device_id = await seed_device(nso_device_name="sr-gen-02", netbox_device_id=7402)

    resp = await push(adapter_client, device_id, [entry(A, route_id=41, generation=3), entry(B, route_id=42)])
    assert resp.status_code == 200
    assert await stored_generations(device_id) == {A: 3, B: None}


# ── P0.3 (PUT half) / P0.11 — the echo and the read-back ─────────────────────


def triples(payload: dict) -> list[tuple]:
    return [(r["route_id"], r["generation"], r["fingerprint"]) for r in payload["routes"]]


async def test_p0_3_the_put_echoes_a_settlement_triple_per_route(adapter_client):
    """P0.3 (PUT half) — the response carries ``{route_id, generation, fingerprint}`` per row.

    The plugin records the echo as its settlement expectation, so an echo that omitted the
    generation, or reported one route's, could never be correlated with a per-route result.
    """
    device_id = await seed_device(nso_device_name="sr-gen-03", netbox_device_id=7403)

    resp = await push(
        adapter_client,
        device_id,
        [entry(A, route_id=41, generation=7, metric=5), entry(B, route_id=42, generation=9)],
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["count"] == 2
    echoed = triples(body)
    assert [(rid, gen) for rid, gen, _fp in echoed] == [(41, 7), (42, 9)]
    fps = [fp for _r, _g, fp in echoed]
    assert all(len(fp) == 64 for fp in fps), "a sha256 hex digest per route"
    assert fps[0] != fps[1], "distinct content must hash differently"


async def test_p0_3_the_echoed_fingerprint_hashes_the_content_the_adapter_stored(adapter_client):
    """The echo moves with the content — a constant would be recomputable but prove nothing."""
    device_id = await seed_device(nso_device_name="sr-gen-04", netbox_device_id=7404)

    first = triples((await push(adapter_client, device_id, [entry(A, route_id=41, generation=1, metric=5)])).json())
    same = triples((await push(adapter_client, device_id, [entry(A, route_id=41, generation=2, metric=5)])).json())
    changed = triples((await push(adapter_client, device_id, [entry(A, route_id=41, generation=3, metric=6)])).json())

    assert first[0][2] == same[0][2], "the generation is not part of the content hash"
    assert changed[0][2] != first[0][2], "a metric edit moves the fingerprint"


async def test_p0_11_the_read_back_get_returns_exactly_what_the_put_echoed(adapter_client):
    """P0.11 — the recovery path for a PUT whose response was lost after the commit landed.

    The GET must be able to stand in for the response, so it has to serve the identical
    triples; anything it adds or drops leaves the plugin unable to record the expectation.
    """
    device_id = await seed_device(nso_device_name="sr-gen-05", netbox_device_id=7405)

    put = await push(
        adapter_client,
        device_id,
        [entry(A, route_id=41, generation=7, metric=5), entry(B, route_id=42, generation=9, tag=3)],
    )
    assert put.status_code == 200

    got = await adapter_client.get(f"/api/v1/devices/{device_id}/static-route-intent", headers=AUTH)
    assert got.status_code == 200
    assert got.json()["device_id"] == device_id
    assert sorted(triples(got.json())) == sorted(triples(put.json()))


async def test_p0_11_the_read_back_reports_a_pre_r3_row_honestly(adapter_client):
    """A fence-shut device has rows with no pk and no generation — reported as null, not hidden.

    Hiding them would make the read-back disagree with the PUT echo on exactly the devices
    the rollout still has to backfill.
    """
    device_id = await seed_device(nso_device_name="sr-gen-06", netbox_device_id=7406)

    put = await push(adapter_client, device_id, [entry(A)])
    got = await adapter_client.get(f"/api/v1/devices/{device_id}/static-route-intent", headers=AUTH)

    assert got.status_code == 200
    assert triples(got.json()) == triples(put.json()) == [(None, None, triples(put.json())[0][2])]


async def test_p0_11_the_read_back_404s_for_an_unknown_device(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/987654/static-route-intent", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_p0_11_the_read_back_requires_the_token(adapter_client):
    device_id = await seed_device(nso_device_name="sr-gen-07", netbox_device_id=7407)
    assert (await adapter_client.get(f"/api/v1/devices/{device_id}/static-route-intent")).status_code == 401
