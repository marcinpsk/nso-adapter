# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1503 Appendix-O chunk O2b: the receipt read surface (O2b.9).

``GET /api/v1/intent-receipts`` is what makes §4.6's restore command implementable at all.
The command is quiesced, reads every per-key receipt plus the two fleet-wide maxima, advances
the plugin's own push sequence above ``global_max_push_seq``, advances ``StaticRoute``'s pk
sequence above ``global_max_route_id`` — the identity-namespace rule (R9-B4) — and only then
resolves each restored claim against the receipt it finds here.

Every case drives the real endpoint over real HTTP against real receipts written by real
intent PUTs, never against injected rows: a surface that omitted a field would still pass a
test that injected the value it was supposed to read.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.api.test_static_route_identity import (
    enable_auto_apply,
    entry,
    read_intent,
    read_tombstones,
    seed_intent,
)
from tests.conftest import VALID_TOKEN, seed_device, session

pytestmark = pytest.mark.anyio

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
URL = "/api/v1/intent-receipts"

A = ("", "10.0.0.0/24", "192.0.2.1")


async def push_vlan(client, device_id: int, seq: int, vlans: list[int]):
    return await client.put(
        f"/api/v1/devices/{device_id}/vlan-intent",
        json={"vlans": [{"vlan_id": v, "name": f"v{v}"} for v in vlans]},
        headers={**AUTH, "X-Push-Seq": str(seq)},
    )


async def push_static(client, device_id: int, seq: int, routes: list[dict]):
    return await client.put(
        f"/api/v1/devices/{device_id}/static-route-intent",
        json={"routes": routes, "deleted_routes": []},
        headers={**AUTH, "X-Push-Seq": str(seq)},
    )


def by_key(payload: dict) -> dict[tuple[int, str], dict]:
    return {(r["device_id"], r["section"]): r for r in payload["receipts"]}


async def test_o2b_9_the_read_surface_needs_a_token(adapter_client):
    """O2b.9 — the receipts carry every stored response body the fleet has returned."""
    assert (await adapter_client.get(URL)).status_code == 401


async def test_o2b_9_every_per_key_receipt_is_served_with_both_global_maxima(adapter_client):
    """O2b.9 — receipts across two streams, plus the two values the restore reads.

    A surface missing ``global_max_route_id`` fails: without it the restore cannot advance
    ``StaticRoute``'s pk sequence, and a re-allocated pk binds an unrelated adapter row as
    GENUINE in pass 1 — a device write with no authority behind it.
    """
    device_id = await seed_device(nso_device_name="rcpt-two-streams", netbox_device_id=9960)
    assert (await push_vlan(adapter_client, device_id, 11, [10])).status_code == 200
    static = await push_static(adapter_client, device_id, 4, [entry(A, route_id=4242)])
    assert static.status_code == 200

    resp = await adapter_client.get(URL, headers=AUTH)

    assert resp.status_code == 200
    payload = resp.json()
    receipts = by_key(payload)
    assert (device_id, "vlan") in receipts
    assert (device_id, "static_route") in receipts
    assert receipts[(device_id, "static_route")]["push_seq"] == 4
    assert receipts[(device_id, "static_route")]["response"] == static.json(), (
        "the stored response is what §4.6's same-sequence arm re-validates against"
    )
    assert receipts[(device_id, "vlan")]["status_code"] == 200
    assert payload["global_max_push_seq"] == 11
    assert payload["global_max_route_id"] == 4242


async def test_o2b_9_the_receipt_reports_the_mode_it_was_admitted_under(adapter_client):
    """O2b.9 — a restored claim's digest comparison is only decidable beside its mode."""
    device_id = await seed_device(nso_device_name="rcpt-mode", netbox_device_id=None)
    await seed_intent(device_id, [{"triple": A, "route_id": None}])
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent?store_only=true",
        json={"vlans": []},
        headers={**AUTH, "X-Push-Seq": "3"},
    )
    assert resp.status_code == 200
    backfill = await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent?backfill_only=true",
        json={"routes": [entry(A, route_id=4242)], "deleted_routes": []},
        headers={**AUTH, "X-Push-Seq": "4"},
    )
    assert backfill.status_code == 200

    rows = by_key((await adapter_client.get(URL, headers=AUTH)).json())
    store_only_row = rows[(device_id, "vlan")]
    backfill_row = rows[(device_id, "static_route")]

    assert (
        store_only_row["store_only"],
        store_only_row["delete_origin"],
        store_only_row["backfill_only"],
    ) == (True, False, False)
    assert (
        backfill_row["store_only"],
        backfill_row["delete_origin"],
        backfill_row["backfill_only"],
    ) == (False, False, True)
    assert len(store_only_row["request_digest"]) == len(backfill_row["request_digest"]) == 64


async def test_o2b_9_the_surface_filters_by_device_and_by_section(adapter_client):
    """O2b.9 — filterable, so a per-key restore does not read the whole fleet."""
    first = await seed_device(nso_device_name="rcpt-filter-1", netbox_device_id=None)
    second = await seed_device(nso_device_name="rcpt-filter-2", netbox_device_id=None)
    assert (await push_vlan(adapter_client, first, 5, [10])).status_code == 200
    assert (await push_static(adapter_client, first, 6, [])).status_code == 200
    assert (await push_vlan(adapter_client, second, 7, [20])).status_code == 200

    only_first = (await adapter_client.get(f"{URL}?device_id={first}", headers=AUTH)).json()
    assert {key[1] for key in by_key(only_first)} == {"vlan", "static_route"}
    assert {key[0] for key in by_key(only_first)} == {first}

    only_vlan = (await adapter_client.get(f"{URL}?section=vlan", headers=AUTH)).json()
    assert {key[1] for key in by_key(only_vlan)} == {"vlan"}
    assert {first, second} <= {key[0] for key in by_key(only_vlan)}


async def test_o2b_9_the_maxima_stay_fleet_wide_under_a_filter(adapter_client):
    """O2b.9 — the restore advances ONE sequence for the whole fleet, so both maxima are global.

    Scoping them to the filter would let a per-key restore advance past its own key only and
    re-allocate a pk another key's receipt still names.
    """
    first = await seed_device(nso_device_name="rcpt-global-1", netbox_device_id=9961)
    second = await seed_device(nso_device_name="rcpt-global-2", netbox_device_id=9962)
    assert (await push_vlan(adapter_client, first, 3, [10])).status_code == 200
    assert (await push_static(adapter_client, second, 900, [entry(A, route_id=7777)])).status_code == 200

    payload = (await adapter_client.get(f"{URL}?device_id={first}", headers=AUTH)).json()

    assert {key[0] for key in by_key(payload)} == {first}
    assert payload["global_max_push_seq"] == 900
    assert payload["global_max_route_id"] == 7777


async def test_o2b_9_a_tombstoned_route_id_still_counts_toward_the_maximum(adapter_client):
    """O2b.9 — a deleted route's pk is still one the adapter HOLDS, in its carrier.

    Reading only the live intent rows lets the restore re-allocate the pk of a route whose
    deletion is still in flight, and pass 1 then binds the recycled pk to that carrier.
    """
    from nso_adapter.store.models import StaticRouteTombstone

    device_id = await seed_device(nso_device_name="rcpt-tombstone", netbox_device_id=9963)
    await seed_intent(device_id, [{"triple": A, "route_id": 10}])
    async with session() as db:
        db.add(
            StaticRouteTombstone(
                device_id=device_id,
                route_id=8888,
                vrf="",
                prefix="10.9.9.0/24",
                next_hop="192.0.2.9",
                marking="delete_origin",
            )
        )
        await db.commit()

    payload = (await adapter_client.get(URL, headers=AUTH)).json()

    assert payload["global_max_route_id"] == 8888


async def test_o2b_9_the_receipt_names_the_generation_its_push_authorized(adapter_client):
    """O2b.9 — ``generation_id`` on the wire is the LAST generation the push enqueued.

    The restore reads this to tell a push that reached a deployment from one that only
    touched the store: a non-authorizing mode has no generation to name and serves null.
    """
    from sqlalchemy import select

    from nso_adapter.store.models import DeploymentGeneration

    device_id = await seed_device(nso_device_name="rcpt-generation", netbox_device_id=9961)
    await enable_auto_apply(device_id)

    assert (await push_vlan(adapter_client, device_id, 21, [10])).status_code == 200
    store_only = await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent?store_only=true",
        json={"routes": [entry(A, route_id=4243)], "deleted_routes": []},
        headers={**AUTH, "X-Push-Seq": "22"},
    )
    assert store_only.status_code == 200

    async with session() as db:
        generation = await db.scalar(select(DeploymentGeneration).where(DeploymentGeneration.device_id == device_id))

    rows = by_key((await adapter_client.get(URL, headers=AUTH)).json())
    assert rows[(device_id, "vlan")]["generation_id"] == generation.id
    assert rows[(device_id, "static_route")]["generation_id"] is None


async def test_o2b_9_a_receipt_held_route_id_still_counts_toward_the_maximum(adapter_client):
    """A store-only deletion leaves its route id in the receipt as the sole carrier."""
    device_id = await seed_device(nso_device_name="rcpt-pending-deletion", netbox_device_id=9964)
    await seed_intent(device_id, [{"triple": A, "route_id": 9999, "deployed_key": list(A)}])
    deletion = {
        "route_id": 9999,
        "triples": [{"vrf": A[0], "prefix": A[1], "next_hop": A[2]}],
        "unverified": False,
    }

    deleted = await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent?store_only=true",
        json={"routes": [], "deleted_routes": [deletion]},
        headers={**AUTH, "X-Push-Seq": "1"},
    )
    assert deleted.status_code == 200, deleted.text
    assert await read_intent(device_id) == []
    assert await read_tombstones(device_id) == []

    payload = (await adapter_client.get(URL, headers=AUTH)).json()

    assert payload["global_max_route_id"] == 9999


async def test_o2b_9_malformed_receipt_provenance_does_not_break_the_maximum(adapter_client):
    """The database aggregate ignores non-array and non-integer provenance values."""
    from nso_adapter.store.models import IntentPushReceipt

    device_id = await seed_device(nso_device_name="rcpt-malformed-provenance", netbox_device_id=None)
    assert (await push_vlan(adapter_client, device_id, 1, [10])).status_code == 200
    async with session() as db:
        receipt = await db.scalar(
            select(IntentPushReceipt).where(
                IntentPushReceipt.device_id == device_id,
                IntentPushReceipt.section == "vlan",
            )
        )
        receipt.response = {
            "_promotion_deletions": [
                None,
                {},
                {"route_id": "200"},
                {"route_id": 1.5},
                {"route_id": True},
                {"route_id": 123},
            ]
        }
        await db.commit()

    payload = (await adapter_client.get(URL, headers=AUTH)).json()

    assert payload["global_max_route_id"] == 123


async def test_o2b_9_an_unknown_section_is_refused_rather_than_served_empty(adapter_client):
    """O2b.9 — the plugin's ``interface`` key is the adapter's ``interface_config`` section.

    Answering an unknown name with an empty page would let that mismatch read as "this key
    has no receipt", which the restore resolves by replaying normally — the wrong branch.
    """
    resp = await adapter_client.get(f"{URL}?section=interface", headers=AUTH)

    assert resp.status_code == 422
    assert resp.json()["error"]["detail"]["reason"] == "unknown_section"
    assert "interface_config" in resp.json()["error"]["detail"]["sections"]


async def test_o2b_9_an_empty_fleet_reports_null_maxima(adapter_client):
    """O2b.9 — nothing to advance past is not zero, and the caller must be able to tell."""
    payload = (await adapter_client.get(URL, headers=AUTH)).json()

    assert payload["receipts"] == []
    assert payload["global_max_push_seq"] is None
    assert payload["global_max_route_id"] is None
