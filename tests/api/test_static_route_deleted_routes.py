# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1503 Appendix-O chunk O2b: ``deleted_routes``, its partition and its carriers.

Rows O2b.8, O2b.11, O2b.13 and O2b.14, every arm driven through the REAL
``PUT /api/v1/devices/{id}/static-route-intent`` against real PostgreSQL rows. The response
carries three lists and ``removed_uncorrelated``; §4.4 requires them to PARTITION the
requested set exactly, so every arm asserts all four and not just the one it is about.

O2b.12's own case is unreachable through the endpoint — a genuine id beside a
``route_id IS NULL`` removed row is a fence-shut request, refused before any partition is
emitted — and is pinned against the classifier in ``tests/core/test_deleted_routes_partition.py``.
"""

from __future__ import annotations

import pytest

from tests.api.test_static_route_identity import (
    AUTH,
    A,
    B,
    C,
    entry,
    read_intent_all_columns,
    read_jobs,
    read_tombstones,
    seed_intent,
)
from tests.conftest import push_seq, seed_device, session

pytestmark = pytest.mark.anyio

D = ("", "10.0.3.0/24", "192.0.2.4")
E = ("", "10.0.4.0/24", "192.0.2.5")


def wire_triple(triple: tuple[str, str, str]) -> dict:
    vrf, prefix, next_hop = triple
    return {"vrf": vrf, "prefix": prefix, "next_hop": next_hop}


def deleted(route_id: int, triples, *, unverified: bool = False) -> dict:
    return {
        "route_id": route_id,
        "triples": [wire_triple(t) for t in triples],
        "unverified": unverified,
    }


async def put(
    client, device_id: int, routes: list[dict], *, deleted_routes=None, query: str = "", seq: int | None = None
):
    body: dict = {"routes": routes}
    if deleted_routes is not None:
        body["deleted_routes"] = deleted_routes
    headers = AUTH | push_seq(seq)
    return await client.put(
        f"/api/v1/devices/{device_id}/static-route-intent{query}",
        json=body,
        headers=headers,
    )


async def receipt(device_id: int):
    from nso_adapter.core.receipt import latest_receipt

    async with session() as db:
        return await latest_receipt(db, device_id, "static_route")


def partition_of(payload: dict) -> dict:
    """The four acknowledgement fields, which every mode must carry."""
    return {
        "executed": payload["deleted_executed_ids"],
        "degraded": payload["deleted_degraded_ids"],
        "moot": payload["deleted_moot_ids"],
        "uncorrelated": payload["removed_uncorrelated"],
    }


# ── O2b.8: the immediate fence, store-only carriage, and the three classes ────


async def test_o2b_8_a_a_genuine_deletion_on_a_fence_shut_device_is_refused_before_any_effect(adapter_client):
    """O2b.8(a) — a removed row's ``route_id`` matches, but no tombstone can be written.

    Processing it would delete the before-image the deletion needs (`:476-477`, O-A4) and
    then have no carrier to record the removal with.
    """
    device_id = await seed_device(nso_device_name="sr-o2b8-a", netbox_device_id=9890)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 100, "deployed_key": list(A)},
            {"triple": B, "route_id": None, "deployed_key": list(B)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )
    before = await read_intent_all_columns(device_id)

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(100, [A])],
        seq=1,
    )

    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "conflict"
    assert resp.json()["error"]["detail"]["reason"] == "fence_shut"
    assert await read_intent_all_columns(device_id) == before, "a refused request deleted a row"
    assert await read_tombstones(device_id) == []
    assert await read_jobs(device_id) == []
    assert await receipt(device_id) is None, "the refusal burned the sequence the plugin must re-use"


async def test_o2b_8_b_a_null_row_matched_by_triple_is_degraded_and_detached(adapter_client):
    """O2b.8(b) — the ratified class: a legacy row, no correlation, so the job DETACHES."""
    device_id = await seed_device(nso_device_name="sr-o2b8-b", netbox_device_id=9891)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[deleted(7, [A])], seq=1)

    assert resp.status_code == 200
    assert partition_of(resp.json()) == {"executed": [], "degraded": [7], "moot": [], "uncorrelated": []}
    jobs = await read_jobs(device_id)
    assert [j["job_type"] for j in jobs] == ["removal"]
    assert jobs[0]["context"]["detach"] is True
    assert jobs[0]["context"]["removed"] == {"route": [list(A)]}


async def test_o2b_8_c_an_id_matching_no_removed_row_is_moot(adapter_client):
    """O2b.8(c) — Rev 6's poison: a moot id in NO list at all fails the exact-set check."""
    device_id = await seed_device(nso_device_name="sr-o2b8-c", netbox_device_id=9892)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[deleted(7, [D])], seq=1)

    assert resp.status_code == 200
    assert partition_of(resp.json()) == {
        "executed": [],
        "degraded": [],
        "moot": [7],
        "uncorrelated": [wire_triple(A)],
    }


@pytest.mark.parametrize(
    ("residue", "expected"),
    [
        (A, {"executed": [], "degraded": [7], "moot": [], "uncorrelated": []}),
        (D, {"executed": [], "degraded": [], "moot": [7], "uncorrelated": [wire_triple(D)]}),
    ],
    ids=["triple-matches", "triple-does-not-match"],
)
async def test_o2b_8_d_the_triple_separates_two_otherwise_identical_worlds(adapter_client, residue, expected):
    """O2b.8(d) — codex's input-identical pair: the SAME body, one differing stored triple.

    An id-only wire cannot tell these apart, which is why ``triples`` is on the record.
    """
    device_id = await seed_device(nso_device_name=f"sr-o2b8-d-{residue[1][-6:-3]}", netbox_device_id=None)
    await seed_intent(
        device_id,
        [
            {"triple": residue, "route_id": None, "deployed_key": list(residue)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[deleted(7, [A])], seq=1)

    assert resp.status_code == 200
    assert partition_of(resp.json()) == expected


async def test_o2b_8_e_a_request_mixing_a_genuine_and_a_degraded_id_is_refused_whole(adapter_client):
    """O2b.8(e) — one unmarkable genuine entry refuses the WHOLE request, before any effect."""
    device_id = await seed_device(nso_device_name="sr-o2b8-e", netbox_device_id=9893)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 100, "deployed_key": list(A)},
            {"triple": B, "route_id": None, "deployed_key": list(B)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )
    before = await read_intent_all_columns(device_id)

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(100, [A]), deleted(7, [B])],
        seq=1,
    )

    assert resp.status_code == 409
    assert resp.json()["error"]["detail"]["reason"] == "fence_shut"
    assert await read_intent_all_columns(device_id) == before
    assert await read_tombstones(device_id) == []
    assert await read_jobs(device_id) == []
    assert await receipt(device_id) is None


async def test_o2b_8_f_store_only_carries_a_genuine_deletion_with_the_fence_shut(adapter_client):
    """A store-only receipt preserves provenance without creating a tombstone or job."""
    device_id = await seed_device(nso_device_name="sr-o2b8-f", netbox_device_id=9894)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 100, "deployed_key": list(A)},
            {"triple": B, "route_id": None, "deployed_key": list(B)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )
    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(100, [A])],
        query="?store_only=true",
        seq=1,
    )

    assert resp.status_code == 200
    assert {row["route_id"] for row in await read_intent_all_columns(device_id)} == {101}
    stored = await receipt(device_id)
    assert stored.response["_promotion_deletions"] == [
        {"table": "static_route_intent", "route_id": 100, "key": list(A), "marking": "delete_origin"},
        {"table": "static_route_intent", "route_id": None, "key": list(B), "marking": "detach"},
    ]
    assert await read_tombstones(device_id) == []
    assert await read_jobs(device_id) == []


async def test_o2b_8_f_store_only_carries_a_genuine_deletion_with_the_fence_open(adapter_client):
    """The same receipt carrier works after the immediate-removal fence opens."""
    device_id = await seed_device(nso_device_name="sr-o2b8-f2", netbox_device_id=9895)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 100, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )
    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(100, [A])],
        query="?store_only=true",
        seq=1,
    )

    assert resp.status_code == 200
    assert {row["route_id"] for row in await read_intent_all_columns(device_id)} == {101}
    stored = await receipt(device_id)
    assert stored.response["_promotion_deletions"] == [
        {"table": "static_route_intent", "route_id": 100, "key": list(A), "marking": "delete_origin"}
    ]
    assert await read_tombstones(device_id) == []
    assert await read_jobs(device_id) == []


async def test_o2b_8_f_store_only_acknowledges_a_degraded_deletion_without_a_carrier(adapter_client):
    """O2b.8(f) positive arm: acknowledgement does not depend on a device-work carrier."""
    device_id = await seed_device(nso_device_name="sr-o2b8-f3", netbox_device_id=9898)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(7, [A])],
        query="?store_only=true",
        seq=1,
    )

    assert resp.status_code == 200
    assert {
        field: resp.json()[field]
        for field in (
            "deleted_executed_ids",
            "deleted_degraded_ids",
            "deleted_moot_ids",
            "removed_uncorrelated",
        )
    } == {
        "deleted_executed_ids": [],
        "deleted_degraded_ids": [7],
        "deleted_moot_ids": [],
        "removed_uncorrelated": [],
    }
    assert await read_tombstones(device_id) == []
    assert await read_jobs(device_id) == []


async def test_o2b_8_a_genuine_deletion_over_an_open_fence_executes(adapter_client):
    """O2b.8 — the positive control: with the fence open the same id is EXECUTED.

    Without it every arm above would pass against an implementation that refuses everything.
    """
    device_id = await seed_device(nso_device_name="sr-o2b8-open", netbox_device_id=9896)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 100, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[deleted(100, [A])], seq=1)

    assert resp.status_code == 200
    assert partition_of(resp.json()) == {"executed": [100], "degraded": [], "moot": [], "uncorrelated": []}
    tombstones = await read_tombstones(device_id)
    assert [(t["route_id"], t["marking"]) for t in tombstones] == [(100, "delete_origin")]
    jobs = await read_jobs(device_id)
    assert [j["job_type"] for j in jobs] == ["removal"]
    assert "detach" not in jobs[0]["context"], "a genuine deletion must retract, not un-own"


async def test_o2b_8_an_unclaimed_row_beside_a_genuine_one_detaches(adapter_client):
    """O2b.8 — marking is PER OBJECT: the genuine row retracts, the unclaimed one detaches."""
    device_id = await seed_device(nso_device_name="sr-o2b8-split", netbox_device_id=9897)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 100, "deployed_key": list(A)},
            {"triple": B, "route_id": 102, "deployed_key": list(B)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[deleted(100, [A])], seq=1)

    assert resp.status_code == 200
    assert partition_of(resp.json()) == {"executed": [100], "degraded": [], "moot": [], "uncorrelated": []}
    tombstones = await read_tombstones(device_id)
    assert sorted((t["route_id"], t["marking"]) for t in tombstones) == [(100, "delete_origin"), (102, "detach")]
    jobs = await read_jobs(device_id)
    assert [j["job_type"] for j in jobs] == ["removal", "removal"]
    assert [bool(j["context"].get("detach")) for j in jobs] == [False, True], "the retraction leads (§4.5)"


# ── O2b.11 — one row classifies its whole equivalence class ───────────────────


async def _same_triple_class(client, tag: str, netbox_id: int, records: list[dict]) -> tuple[dict, dict]:
    device_id = await seed_device(nso_device_name=f"sr-o2b11-{tag}", netbox_device_id=netbox_id)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )
    resp = await put(client, device_id, [entry(C, route_id=101)], deleted_routes=records, seq=1)
    assert resp.status_code == 200, resp.text
    stored = (await receipt(device_id)).response
    return resp.json(), stored


async def test_o2b_11_two_ids_sharing_one_triple_are_classified_identically(adapter_client):
    """O2b.11 — NetBox dropped ``UniqueConstraint(vrf, prefix, next_hop)`` in migration 0030.

    Two deleted pks can therefore carry one triple against ONE adapter row. A one-to-one row
    match degrades whichever id it reaches first and moots the other, and the answer then
    depends on request order.
    """
    forward, forward_receipt = await _same_triple_class(adapter_client, "fwd", 9900, [deleted(1, [A]), deleted(2, [A])])
    reverse, reverse_receipt = await _same_triple_class(adapter_client, "rev", 9901, [deleted(2, [A]), deleted(1, [A])])

    assert partition_of(forward) == {"executed": [], "degraded": [1, 2], "moot": [], "uncorrelated": []}
    assert partition_of(forward) == partition_of(reverse)
    # Byte-identical apart from the device id, which is the only difference by construction.
    assert {k: v for k, v in forward.items() if k != "device_id"} == {
        k: v for k, v in reverse.items() if k != "device_id"
    }
    assert {k: v for k, v in forward_receipt.items() if k != "device_id"} == {
        k: v for k, v in reverse_receipt.items() if k != "device_id"
    }


async def test_o2b_11_the_control_still_partitions_per_row(adapter_client):
    """O2b.11 control — distinct triples: only the id whose triple was removed is degraded."""
    payload, _ = await _same_triple_class(adapter_client, "ctl", 9902, [deleted(1, [A]), deleted(2, [D])])

    assert partition_of(payload) == {"executed": [], "degraded": [1], "moot": [2], "uncorrelated": []}


# ── O2b.13 — the unverified lineage, conservatively ───────────────────────────


async def test_o2b_13_an_unverified_id_that_matched_nothing_is_degraded_not_moot(adapter_client):
    """O2b.13 — the adapter holds legacy row A; the overlay's mirror C was never acknowledged.

    Calling the id moot detaches A SILENTLY, which is exactly what a fabricated
    ``last_acked_triple`` produces. The device outcome stays a detach; what changes is that it
    is recorded, with the triple that actually left the service.
    """
    device_id = await seed_device(nso_device_name="sr-o2b13-a", netbox_device_id=9910)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(7, [B], unverified=True)],
        seq=1,
    )

    assert resp.status_code == 200
    assert partition_of(resp.json()) == {
        "executed": [],
        "degraded": [7],
        "moot": [],
        "uncorrelated": [wire_triple(A)],
    }


async def test_o2b_13_the_verified_control_classifies_by_triple_as_usual(adapter_client):
    """O2b.13 control — a verified lineage that matches nothing is MOOT, not degraded."""
    device_id = await seed_device(nso_device_name="sr-o2b13-b", netbox_device_id=9911)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(7, [B], unverified=False)],
        seq=1,
    )

    assert resp.status_code == 200
    assert partition_of(resp.json()) == {
        "executed": [],
        "degraded": [],
        "moot": [7],
        "uncorrelated": [wire_triple(A)],
    }


async def test_o2b_13_an_unverified_id_is_moot_when_nothing_uncorrelated_was_removed(adapter_client):
    """O2b.13 control — nothing was detached, so degrading the id would invent a record."""
    device_id = await seed_device(nso_device_name="sr-o2b13-c", netbox_device_id=9912)
    await seed_intent(
        device_id,
        [
            {"triple": B, "route_id": 102, "deployed_key": list(B)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(7, [D], unverified=True)],
        seq=1,
    )

    assert resp.status_code == 200
    assert partition_of(resp.json()) == {"executed": [], "degraded": [], "moot": [7], "uncorrelated": []}


# ── O2b.14 — the residue is not consumed by the first id that reaches it ──────


async def _two_unverified(client, tag: str, netbox_id: int, records: list[dict]) -> tuple[dict, dict]:
    device_id = await seed_device(nso_device_name=f"sr-o2b14-{tag}", netbox_device_id=netbox_id)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )
    resp = await put(client, device_id, [entry(C, route_id=101)], deleted_routes=records, seq=1)
    assert resp.status_code == 200, resp.text
    return resp.json(), (await receipt(device_id)).response


async def test_o2b_14_two_unverified_ids_against_one_residue_row_are_both_degraded(adapter_client):
    """O2b.14 — an implementation that CONSUMES the residue emits a valid partition anyway.

    Degrading the first id and mooting the second is unique, disjoint and exactly covering,
    so the plugin's validator accepts it — and the second detach goes unrecorded. The residue
    is a request-wide fact, not a token.
    """
    forward, forward_receipt = await _two_unverified(
        adapter_client,
        "fwd",
        9920,
        [deleted(1, [D], unverified=True), deleted(2, [E], unverified=True)],
    )
    reverse, reverse_receipt = await _two_unverified(
        adapter_client,
        "rev",
        9921,
        [deleted(2, [E], unverified=True), deleted(1, [D], unverified=True)],
    )

    assert partition_of(forward) == {
        "executed": [],
        "degraded": [1, 2],
        "moot": [],
        "uncorrelated": [wire_triple(A)],
    }
    assert {k: v for k, v in forward.items() if k != "device_id"} == {
        k: v for k, v in reverse.items() if k != "device_id"
    }
    assert {k: v for k, v in forward_receipt.items() if k != "device_id"} == {
        k: v for k, v in reverse_receipt.items() if k != "device_id"
    }


# ── the payload refusals the partition depends on ────────────────────────────


async def test_a_deletion_record_with_no_triples_is_refused(adapter_client):
    """§4.5 — an id alone restores the undecidable degraded-versus-moot partition."""
    device_id = await seed_device(nso_device_name="sr-o2b-notriples", netbox_device_id=9930)
    await seed_intent(device_id, [{"triple": C, "route_id": 101}])

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[{"route_id": 7, "triples": [], "unverified": False}],
    )

    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


async def test_a_deletion_record_with_a_third_triple_is_refused(adapter_client):
    """R9-M2 — the lineage is provably at most two: the last acknowledged triple, then the
    current one. A third is classification evidence the contract never grants — a
    ``route_id IS NULL`` row matching only it flips the acknowledgement from moot to
    degraded — and an unbounded list makes the lineage deduplication quadratic.
    """
    device_id = await seed_device(nso_device_name="sr-o2b-3triples", netbox_device_id=9933)
    await seed_intent(device_id, [{"triple": C, "route_id": 101}])
    before = await read_intent_all_columns(device_id)

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(7, [A, B, D])],
    )

    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"
    assert await read_intent_all_columns(device_id) == before
    assert await receipt(device_id) is None


async def test_a_two_triple_lineage_still_classifies_on_its_last_acknowledged_triple(adapter_client):
    """The control the bound must not break: §4.1's whole reason for carrying a lineage.

    A content edit whose push never landed leaves the adapter on the OLDER triple, so a
    record carrying only the current one would match nothing and be called moot.
    """
    device_id = await seed_device(nso_device_name="sr-o2b-2triples", netbox_device_id=9934)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(7, [A, B])],
    )

    assert resp.status_code == 200, resp.text
    assert partition_of(resp.json()) == {
        "executed": [],
        "degraded": [7],
        "moot": [],
        "uncorrelated": [],
    }


async def test_two_deletion_records_claiming_one_route_id_are_refused(adapter_client):
    """One pk cannot have two outcomes: emission is id-oriented, exactly once per id."""
    device_id = await seed_device(nso_device_name="sr-o2b-duprid", netbox_device_id=9931)
    await seed_intent(device_id, [{"triple": C, "route_id": 101}])

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(7, [A]), deleted(7, [B])],
    )

    assert resp.status_code == 422
    assert resp.json()["error"]["detail"]["reason"] == "duplicate_deleted_route_id"


async def test_a_push_without_deleted_routes_still_reports_the_uncorrelated_rows(adapter_client):
    """§4.4 (R11-B2) — the field is reported on EVERY mode, including a push carrying none."""
    device_id = await seed_device(nso_device_name="sr-o2b-nolist", netbox_device_id=9932)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )

    resp = await put(adapter_client, device_id, [entry(C, route_id=101)], seq=1)

    assert resp.status_code == 200
    assert partition_of(resp.json()) == {
        "executed": [],
        "degraded": [],
        "moot": [],
        "uncorrelated": [wire_triple(A)],
    }
