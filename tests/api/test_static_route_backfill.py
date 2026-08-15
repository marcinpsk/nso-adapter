# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1503 Appendix-O chunk O2b: the backfill-only pass, end to end (O2b.10, O2b.13 arm 4).

The whole sequence a fence-withheld key goes through, driven over real HTTP against real
PostgreSQL rows and, at the end, through the real ``run_removal`` against the stateful
RESTCONF substrate ``tests/core/test_static_route_removal.py`` already owns:

    genuine deletion → 409 fence_shut (nothing written, sequence not burned)
    → backfill-only claim at a FRESH sequence → the fence opens
    → the deletion re-claimed at a NEW sequence → a delete_origin tombstone
    → the removal EXECUTES and the route leaves the device.

The plugin half of that sequence — abandoning the claim on its proven-no-effect 409, rehoming
the authority to ``queued_deletions`` and clearing ``fence_withheld_since`` on the
acknowledged backfill — is the plugin's own state machine and is pinned there; what is pinned
here is every adapter-side fact those transitions depend on.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.api.test_static_route_deleted_routes import deleted, partition_of, put, receipt, wire_triple
from tests.api.test_static_route_identity import (
    A,
    B,
    C,
    entry,
    read_intent_all_columns,
    read_jobs,
    read_tombstones,
    seed_intent,
)
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio


async def _seed_fence_shut_key(tag: str, netbox_device_id: int | None) -> int:
    """R's row (a pending genuine deletion), the residue L that shuts the fence, and S.

    L carries no ``route_id``: it is legacy residue the plugin never owned, so it is absent
    from the owned payload and no ordinary push can ever remove it without also removing R.
    """
    device_id = await seed_device(nso_device_name=f"sr-bf-{tag}", netbox_device_id=netbox_device_id)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 100, "deployed_key": list(A)},  # R — the pending deletion
            {"triple": B, "route_id": None, "deployed_key": list(B)},  # L — the residue
            {"triple": C, "route_id": 101, "deployed_key": list(C)},  # S — retained throughout
        ],
    )
    return device_id


def _row(rows: list[dict], triple) -> dict | None:
    return next((r for r in rows if (r["vrf"], r["prefix"], r["next_hop"]) == triple), None)


async def test_o2b_10_the_whole_fence_withheld_sequence_reaches_the_device(adapter_client):
    """O2b.10 — the deliverable, driven from the refusal through to the executed removal."""
    from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client

    device_id = await _seed_fence_shut_key("full", 9940)
    before = await read_intent_all_columns(device_id)

    # 1. the genuine deletion, refused before any effect.
    refused = await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[deleted(100, [A])], seq=1)
    assert refused.status_code == 409
    assert refused.json()["error"]["detail"]["reason"] == "fence_shut"
    assert await read_intent_all_columns(device_id) == before
    assert await receipt(device_id) is None, "the refusal burned the sequence the abandon must release"

    # 2. the backfill-only claim, at a fresh sequence.
    backfill = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[],
        query="?backfill_only=true",
        seq=2,
    )
    assert backfill.status_code == 200, backfill.text
    assert backfill.json()["removed_uncorrelated"] == [wire_triple(B)], "L must be reported, never pruned silently"
    assert backfill.json()["replaced"] is False
    # An ordinary prune queues a detach. A backfill only repairs correlation, so it must
    # leave device work and its tombstone carrier absent.
    assert await read_jobs(device_id) == [], "the pass spawned a job"
    assert await read_tombstones(device_id) == [], "the pass wrote a carrier"

    # R's row survives BYTE-IDENTICAL — its triple, its route_id and its deployed_key.
    after_backfill = await read_intent_all_columns(device_id)
    assert _row(after_backfill, A) == _row(before, A), "the before-image the deletion needs was destroyed"
    assert _row(after_backfill, B) is None, "L still holds the fence shut"

    # 3. the deletion, re-claimed at a NEW sequence over the now-open fence.
    executed = await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[deleted(100, [A])], seq=3)
    assert executed.status_code == 200, executed.text
    assert partition_of(executed.json()) == {"executed": [100], "degraded": [], "moot": [], "uncorrelated": []}

    tombstones = await read_tombstones(device_id)
    assert [(t["route_id"], t["marking"]) for t in tombstones] == [(100, "delete_origin")]
    jobs = await read_jobs(device_id)
    assert [j["job_type"] for j in jobs] == ["removal"]
    assert "detach" not in jobs[0]["context"]

    # 4. execution: the route leaves the device.
    fake = SrFake(
        "sr-bf-full",
        service=[
            {"vrf": "", "prefix": A[1], "next-hop": A[2]},
            {"vrf": "", "prefix": C[1], "next-hop": C[2]},
        ],
    )
    job = await run_removal_job(device_id, jobs[0]["id"], sr_client(fake))
    assert job.status.value == "succeeded", job.error
    assert A not in fake.device_keys, "the authorized route is still on the device"
    assert C in fake.device_keys, "the retained route was retracted as collateral"


async def test_o2b_10_the_pass_leaves_a_row_the_payload_still_names(adapter_client):
    """O2b.10 control — L PRESENT in the owned payload is adopted, not pruned."""
    device_id = await _seed_fence_shut_key("present", 9941)

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101), entry(B, route_id=103)],
        deleted_routes=[],
        query="?backfill_only=true",
        seq=1,
    )

    assert resp.status_code == 200
    assert resp.json()["removed_uncorrelated"] == []
    assert resp.json()["removed"] == 0
    rows = await read_intent_all_columns(device_id)
    assert _row(rows, B)["route_id"] == 103, "the pass did not adopt the id the payload named"
    assert _row(rows, A) is not None, "the omitted non-NULL row was pruned"


async def test_o2b_10_a_backfill_cannot_acknowledge_a_matched_row_without_a_route_id(adapter_client):
    """A successful backfill must leave no NULL route id behind to hold the fence shut."""
    device_id = await _seed_fence_shut_key("residual-null", 9949)
    before = await read_intent_all_columns(device_id)

    response = await put(
        adapter_client,
        device_id,
        [entry(B), entry(C, route_id=101)],
        deleted_routes=[],
        query="?backfill_only=true",
        seq=1,
    )

    assert response.status_code == 422
    assert response.json()["error"]["detail"] == {
        "reason": "backfill_missing_route_id",
        "routes": [wire_triple(B)],
    }
    assert await read_intent_all_columns(device_id) == before
    assert await receipt(device_id) is None


async def test_o2b_10_the_pass_writes_no_content_onto_a_matched_row(adapter_client):
    """O2b.10 — the mode adopts ids and NOTHING else, so a drifted row stays drifted.

    Writing content here would let the pusher record an acknowledgement of state the adapter
    never accepted, which is exactly what O1.32 forbids the backfill success from stamping.
    """
    device_id = await seed_device(nso_device_name="sr-bf-drift", netbox_device_id=9942)
    await seed_intent(device_id, [{"triple": C, "route_id": None, "deployed_key": list(C)}])
    before = await read_intent_all_columns(device_id)

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101, metric=42, name="renamed")],
        deleted_routes=[],
        query="?backfill_only=true",
        seq=1,
    )

    assert resp.status_code == 200
    row = _row(await read_intent_all_columns(device_id), C)
    assert row["route_id"] == 101, "the id was not adopted"
    assert (row["metric"], row["name"]) == (before[0]["metric"], before[0]["name"]), "content was written"


async def test_o2b_10_a_backfill_body_carrying_deletion_authority_is_refused(adapter_client):
    """O2b.10 negative — the mode carries no authority, so a non-empty list is a 422."""
    device_id = await _seed_fence_shut_key("auth", 9943)
    before = await read_intent_all_columns(device_id)

    resp = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(100, [A])],
        query="?backfill_only=true",
        seq=1,
    )

    assert resp.status_code == 422
    assert resp.json()["error"]["detail"]["reason"] == "backfill_carries_deletions"
    assert await read_intent_all_columns(device_id) == before
    assert await receipt(device_id) is None


async def test_o2b_10_a_malformed_backfill_flag_has_no_effect(adapter_client):
    """A typo cannot turn a backfill request into an ordinary full replacement."""
    device_id = await _seed_fence_shut_key("malformed-flag", None)
    before = await read_intent_all_columns(device_id)
    raw = "private-mode-value"

    response = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[],
        query=f"?backfill_only={raw}",
        seq=1,
    )

    assert response.status_code == 422
    assert response.json()["error"]["detail"] == {"parameter": "backfill_only"}
    assert raw not in response.text
    assert await read_intent_all_columns(device_id) == before
    assert await read_jobs(device_id) == []
    assert await receipt(device_id) is None


async def test_o2b_10_a_replay_at_the_same_sequence_returns_the_stored_response(adapter_client):
    """O2b.10 negative — the pass is an admitted operation, so a redelivery applies nothing."""
    device_id = await _seed_fence_shut_key("replay", 9944)

    first = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[],
        query="?backfill_only=true",
        seq=2,
    )
    assert first.status_code == 200
    after_first = await read_intent_all_columns(device_id)

    replay = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[],
        query="?backfill_only=true",
        seq=2,
    )

    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert await read_intent_all_columns(device_id) == after_first
    assert await read_jobs(device_id) == []


async def test_o2b_10_a_backfill_cannot_replay_an_ordinary_push_at_the_same_sequence(adapter_client):
    """O2b.10 — the two modes write completely different rows, so the mode is receipt identity.

    Without the mode on the receipt this backfill would return the ordinary push's stored
    response and prune nothing, and the fence would stay shut with the pusher told it opened.
    """
    device_id = await seed_device(nso_device_name="sr-bf-mode", netbox_device_id=9945)
    await seed_intent(device_id, [{"triple": C, "route_id": 101, "deployed_key": list(C)}])
    body = [entry(C, route_id=101)]

    assert (await put(adapter_client, device_id, body, deleted_routes=[], seq=4)).status_code == 200

    resp = await put(adapter_client, device_id, body, deleted_routes=[], query="?backfill_only=true", seq=4)

    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "sequence_reuse"


async def test_backfill_only_is_refused_on_a_stream_that_does_not_implement_it(adapter_client):
    """A silent full-replace under the flag is the before-image destruction the mode prevents."""
    device_id = await seed_device(nso_device_name="sr-bf-wrong-stream", netbox_device_id=None)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/vlan-intent?backfill_only=true",
        json={"vlans": []},
        headers={"Authorization": "Bearer test-bearer-token", "X-Push-Seq": "1"},
    )

    assert resp.status_code == 422
    assert resp.json()["error"]["detail"]["reason"] == "backfill_only_unsupported"


async def test_o2b_13_an_unverified_deletion_survives_the_backfill_that_prunes_its_row(adapter_client):
    """O2b.13 arm 4 (R10-B3) — a genuine R and an unverified U in one fence-shut request.

    The whole request takes ``409 fence_shut`` before U is ever classified, so the backfill
    then prunes the residue row B, the only ``route_id IS NULL`` row. Recording only exact
    lineage matches is how codex's case escapes: ``[C]`` does not match B, and the next
    ordinary request moots U SILENTLY. The backfill's
    ``removed_uncorrelated`` is what drives the pusher's request-wide conservative rule, so it
    must name B's triple — and R's own deletion must still execute afterwards.
    """
    device_id = await _seed_fence_shut_key("mixed", 9946)

    refused = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[deleted(100, [A]), deleted(7, [C], unverified=True)],
        seq=1,
    )
    assert refused.status_code == 409
    assert refused.json()["error"]["detail"]["reason"] == "fence_shut"

    backfill = await put(
        adapter_client,
        device_id,
        [entry(C, route_id=101)],
        deleted_routes=[],
        query="?backfill_only=true",
        seq=2,
    )
    assert backfill.status_code == 200
    assert backfill.json()["removed_uncorrelated"] == [wire_triple(B)], "U's row was pruned with no attribution"

    executed = await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[deleted(100, [A])], seq=3)
    assert executed.status_code == 200
    assert partition_of(executed.json())["executed"] == [100]


async def test_the_backfill_receipt_records_the_mode_it_was_admitted_under(adapter_client):
    """The receipt's third mode flag is what makes the two operations distinguishable."""
    device_id = await _seed_fence_shut_key("receipt", 9947)

    assert (
        await put(
            adapter_client,
            device_id,
            [entry(C, route_id=101)],
            deleted_routes=[],
            query="?backfill_only=true",
            seq=2,
        )
    ).status_code == 200

    row = await receipt(device_id)
    assert (row.store_only, row.delete_origin, row.backfill_only) == (False, False, True)


async def test_an_ordinary_push_records_backfill_only_false(adapter_client):
    device_id = await seed_device(nso_device_name="sr-bf-normal", netbox_device_id=9948)
    await seed_intent(device_id, [{"triple": C, "route_id": 101}])

    assert (await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[], seq=1)).status_code == 200

    row = await receipt(device_id)
    assert (row.store_only, row.delete_origin, row.backfill_only) == (False, False, False)


# ── O2b.6 — the single commit ────────────────────────────────────────────────


async def test_o2b_6_a_crash_after_the_tombstone_write_rolls_the_receipt_back_with_it(adapter_client):
    """O2b.6 — the tombstone, the intent rows, the jobs and the receipt are ONE commit (O-A5).

    A receipt that outlived a rolled-back operation turns the pusher's retry into a silent
    no-op; intent applied with no receipt double-applies it. Both are the same failure of the
    single-commit contract, so both are asserted here.
    """
    from unittest.mock import patch

    from nso_adapter.store.models import StaticRouteTombstone

    device_id = await seed_device(nso_device_name="sr-o2b6", netbox_device_id=9950)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 100, "deployed_key": list(A)},
            {"triple": C, "route_id": 101, "deployed_key": list(C)},
        ],
    )
    before = await read_intent_all_columns(device_id)

    async def boom(*_args, **_kwargs):
        raise RuntimeError("crash after the tombstone write")

    with patch("nso_adapter.core.removal.enqueue_removal", new=boom), pytest.raises(RuntimeError):
        await put(adapter_client, device_id, [entry(C, route_id=101)], deleted_routes=[deleted(100, [A])], seq=1)

    assert await read_intent_all_columns(device_id) == before
    assert await read_jobs(device_id) == []
    assert await receipt(device_id) is None, "a receipt survived the operation it admitted"
    async with session() as db:
        surviving = (
            (await db.execute(select(StaticRouteTombstone).where(StaticRouteTombstone.device_id == device_id)))
            .scalars()
            .all()
        )
    assert surviving == [], "a carrier survived without the job that would consume it"
