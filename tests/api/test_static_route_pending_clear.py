# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R2 chunk C1 — the ``pending_clear`` carrier written by the intent PUT (§4.11).

Pins C1.10-C1.13. Every case drives the real
``PUT /api/v1/devices/{id}/static-route-intent`` against a real PostgreSQL clone and reads
the persisted ``static_route_intent.pending_clear`` column back out.

A cleared leaf leaves the device only via a networked PUT, and two paths cannot issue one
(a clear riding with an unmarked shrink becomes a ``no-networking`` detach; a re-issued
removal rebuilds its context from the tombstone, which carries no clear). The carrier is
the durable record that survives both.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.conftest import VALID_TOKEN, push_seq, seed_device, session

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

X = ("", "10.0.0.0/24", "192.0.2.1")
Y = ("", "10.0.1.0/24", "192.0.2.2")


def route(triple, *, route_id=None, **extra) -> dict:
    vrf, prefix, next_hop = triple
    body = {"vrf": vrf, "prefix": prefix, "next_hop": next_hop, **extra}
    if route_id is not None:
        body["route_id"] = route_id
    return body


async def put_intent(
    client, device_id: int, routes: list[dict], *, query: str = "", deleted_routes: list[dict] | None = None
):
    resp = await client.put(
        f"/api/v1/devices/{device_id}/static-route-intent{query}",
        json={"routes": routes, "deleted_routes": deleted_routes or []},
        headers=AUTH | push_seq(),
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


async def intent_rows(device_id: int) -> list:
    """Every ``StaticRouteIntent`` row of the device; each reader below projects one field."""
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        return list(
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )


async def carriers(device_id: int) -> dict[tuple, dict | None]:
    """``{triple: pending_clear}`` for every intent row of the device."""
    return {(r.vrf, r.prefix, r.next_hop): r.pending_clear for r in await intent_rows(device_id)}


async def removal_jobs(device_id: int) -> list[dict]:
    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal).order_by(Job.id)
                )
            )
            .scalars()
            .all()
        )
        return [r.context for r in rows]


# ── C1.10 — the carrier names exactly the cleared fields ─────────────────────


async def test_c1_10_a_clear_records_only_the_cleared_field_on_only_that_row(adapter_client):
    """C1.10 — clearing ``metric`` while EDITING ``tag`` on the same row.

    An edit is not a clear: recording ``tag`` here would make a later removal delete a leaf
    the operator just set. And row Y, untouched by this push, must stay NULL.
    """
    device_id = await seed_device(nso_device_name="sr-pc-basic", netbox_device_id=7101)
    await put_intent(
        adapter_client,
        device_id,
        [route(X, route_id=1, metric=10, tag=100), route(Y, route_id=2, metric=5)],
    )
    assert await carriers(device_id) == {X: None, Y: None}

    await put_intent(
        adapter_client,
        device_id,
        [route(X, route_id=1, tag=200), route(Y, route_id=2, metric=5)],
    )
    assert await carriers(device_id) == {X: {"authorized": ["metric"], "store_only": []}, Y: None}

    # Discriminating variant: re-setting the field empties the carrier again.
    await put_intent(
        adapter_client,
        device_id,
        [route(X, route_id=1, metric=10, tag=200), route(Y, route_id=2, metric=5)],
    )
    assert await carriers(device_id) == {X: None, Y: None}


async def test_c1_10b_metric_ten_to_zero_is_not_a_clear(adapter_client):
    """The renderer emits ``metric: 0`` as a real value, so it is not wire-unset."""
    device_id = await seed_device(nso_device_name="sr-pc-zero", netbox_device_id=7102)
    await put_intent(adapter_client, device_id, [route(X, route_id=1, metric=10, tag=100)])
    await put_intent(adapter_client, device_id, [route(X, route_id=1, metric=0, tag=0)])
    assert await carriers(device_id) == {X: None}
    assert await removal_jobs(device_id) == []

    # ...but 0 -> unset IS a clear: the leaf disappears from the body.
    await put_intent(adapter_client, device_id, [route(X, route_id=1)])
    assert await carriers(device_id) == {X: {"authorized": ["metric", "tag"], "store_only": []}}

    # ...and re-setting to 0 empties the carrier: `metric: 0` will be on the wire, so
    # there is nothing left to delete. A falsiness reset check would strand it forever.
    await put_intent(adapter_client, device_id, [route(X, route_id=1, metric=0, tag=0)])
    assert await carriers(device_id) == {X: None}


async def test_c1_10c_every_wire_bearing_field_is_covered(adapter_client):
    """All five clearable fields, cleared in one push."""
    device_id = await seed_device(nso_device_name="sr-pc-all", netbox_device_id=7103)
    await put_intent(
        adapter_client,
        device_id,
        [
            route(
                X,
                route_id=1,
                interface_next_hop="GigabitEthernet0/0",
                next_hop_vrf="BLUE",
                metric=10,
                permanent=True,
                tag=100,
            )
        ],
    )
    await put_intent(adapter_client, device_id, [route(X, route_id=1)])
    assert await carriers(device_id) == {
        X: {
            "authorized": ["interface_next_hop", "metric", "next_hop_vrf", "permanent", "tag"],
            "store_only": [],
        }
    }


# ── C1.11 — permanent True -> False is a clear FOR STATIC ROUTES ONLY ────────


async def test_c1_11_permanent_true_to_false_is_recorded_and_enqueues(adapter_client):
    """C1.11 — today's silent drop: the renderer never emits ``permanent: false``, so a
    merge-PATCH leaves the device permanent forever while the apply reports success.
    """
    device_id = await seed_device(nso_device_name="sr-pc-perm", netbox_device_id=7104)
    await put_intent(adapter_client, device_id, [route(X, route_id=1, permanent=True)])
    assert await removal_jobs(device_id) == []

    await put_intent(adapter_client, device_id, [route(X, route_id=1, permanent=False)])
    assert await carriers(device_id) == {X: {"authorized": ["permanent"], "store_only": []}}
    assert len(await removal_jobs(device_id)) == 1

    # Back to True: nothing left to clear.
    await put_intent(adapter_client, device_id, [route(X, route_id=1, permanent=True)])
    assert await carriers(device_id) == {X: None}


@pytest.mark.parametrize(("before", "after"), [(True, False), (True, None), (False, True)])
def test_c1_11b_the_shared_predicate_is_untouched(before, after):
    """The static-route rule must not leak into ``is_cleared`` itself.

    The other twelve scopes' writers DO emit ``False`` explicitly (IS-IS
    ``microloop-avoidance: false``), so their merge-PATCH carries it and treating a
    toggle-off as a clear would fire a real device PUT-replace on every one.
    """
    from nso_adapter.core.removal import is_cleared

    expected = {(True, False): False, (True, None): True, (False, True): False}[(before, after)]
    assert is_cleared(before, after) is expected


async def test_c1_11c_another_scope_still_enqueues_on_a_real_clear(adapter_client):
    """...and the shared predicate is still WIRED where it was: a genuine clear on a
    non-static scope still enqueues its retract.
    """
    device_id = await seed_device(nso_device_name="sr-pc-vlan", netbox_device_id=7105)
    for name in ("mgmt", ""):  # the endpoint stores "" as NULL — a real clear
        resp = await adapter_client.put(
            f"/api/v1/devices/{device_id}/vlan-intent",
            json={"vlans": [{"vlan_id": 10, "name": name}]},
            headers=AUTH | push_seq(),
        )
        assert resp.status_code == 200, resp.text
    assert len(await removal_jobs(device_id)) == 1


# ── C1.12 — clearing `name` is a documented no-op ────────────────────────────


async def test_c1_12_clearing_name_writes_no_carrier_and_enqueues_nothing(adapter_client):
    """C1.12 — ``name`` has no wire leaf, so a recorded clear could never be delivered or
    proven: the carrier would live forever and the row would report ``unproven`` forever.
    Refusing the push would be equally wrong — there is nothing to refuse.
    """
    device_id = await seed_device(nso_device_name="sr-pc-name", netbox_device_id=7106)
    await put_intent(adapter_client, device_id, [route(X, route_id=1, name="to-core", metric=10)])
    await put_intent(adapter_client, device_id, [route(X, route_id=1, metric=10)])
    assert await carriers(device_id) == {X: None}
    assert await removal_jobs(device_id) == []

    # Discriminating variant: `name` + `metric` cleared together carries exactly `metric`.
    await put_intent(adapter_client, device_id, [route(X, route_id=1, name="to-core", metric=10)])
    await put_intent(adapter_client, device_id, [route(X, route_id=1)])
    assert await carriers(device_id) == {X: {"authorized": ["metric"], "store_only": []}}
    assert len(await removal_jobs(device_id)) == 1


# ── C1.13 — the store-only half ──────────────────────────────────────────────


async def test_c1_13_a_store_only_clear_lands_in_the_half_a_removal_must_ignore(adapter_client):
    """C1.13 (store half) — ``?store_only=true`` may mutate the store but must never cause
    a device write.

    The plugin's resync genuinely re-pushes nullable values, so an undifferentiated carrier
    would let an unrelated networked removal for route Y delete X's live ``metric`` —
    breaking the store-only contract AND "a removal never forward-deploys store intent".
    The device half of this pin (the removal leaving X's entry alone) lands with C4's
    live-service-relative body; here the split itself is asserted, plus the accessor a
    removal reads.
    """
    from nso_adapter.core.static_route_plan import authorized_clear_fields, pending_clear_fields

    device_id = await seed_device(nso_device_name="sr-pc-storeonly", netbox_device_id=7107)
    await put_intent(
        adapter_client,
        device_id,
        [route(X, route_id=1, metric=10), route(Y, route_id=2, metric=5)],
    )
    await put_intent(
        adapter_client,
        device_id,
        [route(X, route_id=1), route(Y, route_id=2, metric=5)],
        query="?store_only=true",
    )
    carrier = (await carriers(device_id))[X]
    assert carrier == {"authorized": [], "store_only": ["metric"]}
    assert authorized_clear_fields(carrier) == set(), "a networked removal may deliver nothing here"
    assert pending_clear_fields(carrier) == {"metric"}, "but the row is still not provably in sync"
    # store-only never enqueues a device-touching job at all
    assert await removal_jobs(device_id) == []


async def test_c1_13b_an_authorized_clear_promotes_out_of_store_only(adapter_client):
    """A later authorized push over the same field moves it, and never back."""
    device_id = await seed_device(nso_device_name="sr-pc-promote", netbox_device_id=7108)
    await put_intent(adapter_client, device_id, [route(X, route_id=1, metric=10)])
    await put_intent(adapter_client, device_id, [route(X, route_id=1)], query="?store_only=true")
    assert (await carriers(device_id))[X] == {"authorized": [], "store_only": ["metric"]}

    await put_intent(adapter_client, device_id, [route(X, route_id=1, metric=10)])
    await put_intent(adapter_client, device_id, [route(X, route_id=1)])
    assert (await carriers(device_id))[X] == {"authorized": ["metric"], "store_only": []}

    # A store-only push over an already-authorized clear must not demote it.
    await put_intent(adapter_client, device_id, [route(X, route_id=1)], query="?store_only=true")
    assert (await carriers(device_id))[X] == {"authorized": ["metric"], "store_only": []}


async def test_c1_13c_a_store_only_reset_still_empties_the_carrier(adapter_client):
    """Re-setting the value is not a device write, so store-only may clear the record."""
    device_id = await seed_device(nso_device_name="sr-pc-reset", netbox_device_id=7109)
    await put_intent(adapter_client, device_id, [route(X, route_id=1, metric=10)])
    await put_intent(adapter_client, device_id, [route(X, route_id=1)])
    assert (await carriers(device_id))[X] == {"authorized": ["metric"], "store_only": []}

    await put_intent(adapter_client, device_id, [route(X, route_id=1, metric=7)], query="?store_only=true")
    assert (await carriers(device_id))[X] is None


# ── the carrier is written before job classification, unconditionally ────────


async def test_the_carrier_is_written_even_when_the_job_is_a_detach(adapter_client):
    """§4.11 — a clear riding with an UNMARKED shrink becomes a ``no-networking`` detach,
    which can never deliver a clear (R1 records ``retract_deferred`` and nothing reads it).
    The carrier is the durable record that survives that classification.
    """
    device_id = await seed_device(nso_device_name="sr-pc-detach", netbox_device_id=7110)
    await put_intent(
        adapter_client,
        device_id,
        [route(X, route_id=1, metric=10), route(Y, route_id=2, metric=5)],
    )
    # Y dropped (an un-own) and X's metric cleared, in one push.
    await put_intent(adapter_client, device_id, [route(X, route_id=1)])

    contexts = await removal_jobs(device_id)
    assert len(contexts) == 1
    assert contexts[0].get("detach") is True
    assert contexts[0].get("retract_deferred") is True
    assert (await carriers(device_id))[X] == {"authorized": ["metric"], "store_only": []}


async def test_a_genuine_deletion_push_with_a_clear_still_carries_it(adapter_client):
    """A genuine removal is networked, but neither ``retract`` nor the cleared field
    survives into its job context, so the carrier is the only place it can find them.
    """
    device_id = await seed_device(nso_device_name="sr-pc-delorigin", netbox_device_id=7111)
    await put_intent(
        adapter_client,
        device_id,
        [route(X, route_id=1, metric=10), route(Y, route_id=2, metric=5)],
    )
    await put_intent(
        adapter_client,
        device_id,
        [route(X, route_id=1)],
        deleted_routes=[{"route_id": 2, "triples": [route(Y)], "unverified": False}],
    )

    contexts = await removal_jobs(device_id)
    assert len(contexts) == 1
    assert "detach" not in contexts[0]
    assert "retract_deferred" not in contexts[0]
    assert set(contexts[0]) & {"cleared", "retract"} == set(), "G26: the clear does not survive into the context"
    assert (await carriers(device_id))[X] == {"authorized": ["metric"], "store_only": []}


async def test_a_new_row_carries_nothing(adapter_client):
    """A row created by this push has no before-image and therefore no clear."""
    device_id = await seed_device(nso_device_name="sr-pc-new", netbox_device_id=7112)
    await put_intent(adapter_client, device_id, [route(X, route_id=1)])
    assert await carriers(device_id) == {X: None}
    assert await removal_jobs(device_id) == []


async def stored_metrics(device_id: int) -> dict[tuple, int | None]:
    """``{triple: metric}`` for every intent row of the device."""
    return {(r.vrf, r.prefix, r.next_hop): r.metric for r in await intent_rows(device_id)}


async def test_p1_3_a_timos_metric_edit_records_no_clear_and_no_removal(adapter_client):
    """#1396 P1.3's owed adapter half (R3-A2(iii)): an edit 3 → 5 clears nothing.

    Nokia's default preference 5 used to be suppressed on the pusher's wire, so the edit
    arrived as an *omission* — and an omitted leaf is a clear, NED-agnostically. That cost a
    clear record and a networked retract for a value the device already had. The pusher now
    always sends the metric; this is the other half of that contract, read off the store.

    The second arm is what makes the first discriminating: with the metric omitted, the very
    same edit does record the clear, so the green above is a statement about the payload and
    not about this test being unable to see one.
    """
    device_id = await seed_device(nso_device_name="sr-pc-timos", netbox_device_id=7113)
    await put_intent(adapter_client, device_id, [route(X, route_id=41, metric=3)])
    await put_intent(adapter_client, device_id, [route(X, route_id=41, metric=5)])

    assert await stored_metrics(device_id) == {X: 5}
    assert await carriers(device_id) == {X: None}
    assert await removal_jobs(device_id) == []

    await put_intent(adapter_client, device_id, [route(X, route_id=41)])
    assert await carriers(device_id) == {X: {"authorized": ["metric"], "store_only": []}}
