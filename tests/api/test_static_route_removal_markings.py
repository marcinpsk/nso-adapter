# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1503 Appendix-O chunk O2: marking-homogeneous removal jobs.

O2.4 characterizes homogeneous genuine-deletion and detach pushes. O3 supplies genuine
deletions through explicit ``deleted_routes`` records. An empty list marks omissions as
per-object detaches. The tests pin the job count, context, generations, carriers, queue
order, and response body.

Written and made green BEFORE the split landed; it is the regression net for it.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.api.test_static_route_identity import (
    A,
    B,
    C,
    enable_auto_apply,
    entry,
    put_intent,
    read_jobs,
    read_tombstones,
    seed_intent,
)
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio


def deleted(route_id: int, triple: tuple[str, str, str]) -> dict:
    return {"route_id": route_id, "triples": [entry(triple)], "unverified": False}


async def read_generations(device_id: int) -> list[dict]:
    """Return seq, mode, job_id, and the full removal_context for each generation."""
    from nso_adapter.store.models import DeploymentGeneration

    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(DeploymentGeneration)
                    .where(DeploymentGeneration.device_id == device_id)
                    .order_by(DeploymentGeneration.seq)
                )
            )
            .scalars()
            .all()
        )
        return [
            {
                "seq": r.seq,
                "mode": r.mode.value,
                "job_id": r.job_id,
                "removal_context": r.removal_context,
            }
            for r in rows
        ]


async def carriers(device_id: int) -> dict[tuple, dict | None]:
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        rows = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return {(r.vrf, r.prefix, r.next_hop): r.pending_clear for r in rows}


async def test_o2_4_a_genuine_shrink_is_one_networked_job(adapter_client):
    """O2.4, a marked shrink: ONE job, no ``detach``, both carriers on it, one generation."""
    device_id = await seed_device(nso_device_name="sr-o24-a", netbox_device_id=9860)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 1, "deployed_key": list(A)},
            {"triple": B, "route_id": 2, "deployed_key": list(B)},
            {"triple": C, "route_id": 3, "deployed_key": list(C)},
        ],
    )

    resp = await put_intent(
        adapter_client,
        device_id,
        [entry(C, route_id=3)],
        deleted_routes=[deleted(1, A), deleted(2, B)],
    )
    assert resp.status_code == 200
    assert resp.json()["removed"] == 2
    assert resp.json()["replaced"] is True

    jobs = await read_jobs(device_id)
    assert [j["job_type"] for j in jobs] == ["removal"]
    # The key ORDER inside one marking's list is the store's scan order, not a contract.
    assert set(jobs[0]["context"]) == {"scope", "removed"}
    assert jobs[0]["context"]["scope"] == "static_route"
    assert sorted(jobs[0]["context"]["removed"]["route"]) == sorted([list(A), list(B)])
    tombstones = await read_tombstones(device_id)
    assert [t["marking"] for t in tombstones] == ["delete_origin", "delete_origin"]
    assert {t["job_id"] for t in tombstones} == {jobs[0]["id"]}
    generations = await read_generations(device_id)
    assert [(g["seq"], g["mode"], g["job_id"]) for g in generations] == [(1, "networked", jobs[0]["id"])]
    assert generations[0]["removal_context"] == jobs[0]["context"]


async def test_o2_4_b_an_unmarked_shrink_is_one_detach_job(adapter_client):
    """O2.4, an un-own: ONE job, ``detach`` true, a detach generation."""
    device_id = await seed_device(nso_device_name="sr-o24-b", netbox_device_id=9861)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 1, "deployed_key": list(A)},
            {"triple": B, "route_id": 2, "deployed_key": list(B)},
        ],
    )

    assert (await put_intent(adapter_client, device_id, [entry(B, route_id=2)])).status_code == 200

    jobs = await read_jobs(device_id)
    assert [j["job_type"] for j in jobs] == ["removal"]
    assert jobs[0]["context"] == {
        "scope": "static_route",
        "removed": {"route": [list(A)]},
        "detach": True,
    }
    tombstones = await read_tombstones(device_id)
    assert [(t["marking"], t["job_id"]) for t in tombstones] == [("detach", jobs[0]["id"])]
    assert [(g["seq"], g["mode"], g["job_id"]) for g in await read_generations(device_id)] == [
        (1, "detach", jobs[0]["id"])
    ]


async def test_o2_4_c_an_unmarked_shrink_with_a_clear_defers_the_retract(adapter_client):
    """O2.4: one PUT-replace cannot honour both, so the clear is deferred and recorded."""
    device_id = await seed_device(nso_device_name="sr-o24-c", netbox_device_id=9862)
    await put_intent(
        adapter_client,
        device_id,
        [entry(A, route_id=1, metric=10), entry(B, route_id=2)],
    )

    assert (await put_intent(adapter_client, device_id, [entry(A, route_id=1)])).status_code == 200

    jobs = [j for j in await read_jobs(device_id) if j["job_type"] == "removal"]
    assert len(jobs) == 1
    assert jobs[0]["context"] == {
        "scope": "static_route",
        "removed": {"route": [list(B)]},
        "retract_deferred": True,
        "detach": True,
    }
    assert (await carriers(device_id))[A] == {"authorized": ["metric"], "store_only": []}


async def test_o2_4_d_a_genuine_shrink_with_a_clear_stays_networked(adapter_client):
    """O2.4: nothing is un-owned, so the clear rides out with the deletion."""
    device_id = await seed_device(nso_device_name="sr-o24-d", netbox_device_id=9863)
    await put_intent(
        adapter_client,
        device_id,
        [entry(A, route_id=1, metric=10), entry(B, route_id=2)],
    )

    resp = await put_intent(
        adapter_client,
        device_id,
        [entry(A, route_id=1)],
        deleted_routes=[deleted(2, B)],
    )
    assert resp.status_code == 200

    jobs = [j for j in await read_jobs(device_id) if j["job_type"] == "removal"]
    assert len(jobs) == 1
    assert jobs[0]["context"] == {
        "scope": "static_route",
        "removed": {"route": [list(B)]},
    }
    assert (await carriers(device_id))[A] == {"authorized": ["metric"], "store_only": []}


async def test_o2_4_e_a_pure_clear_is_one_networked_job_with_no_removed_keys(adapter_client):
    """O2.4: a clear removes no key at all, and must still reach the device."""
    device_id = await seed_device(nso_device_name="sr-o24-e", netbox_device_id=9864)
    await put_intent(adapter_client, device_id, [entry(A, route_id=1, metric=10)])

    assert (await put_intent(adapter_client, device_id, [entry(A, route_id=1)])).status_code == 200

    jobs = [j for j in await read_jobs(device_id) if j["job_type"] == "removal"]
    assert len(jobs) == 1
    assert jobs[0]["context"] == {"scope": "static_route"}
    assert await read_tombstones(device_id) == []
    assert [(g["mode"], g["job_id"]) for g in await read_generations(device_id)] == [
        ("networked", jobs[0]["id"]),
    ]


async def test_o2_4_f_a_store_only_shrink_creates_no_job_and_no_carrier(adapter_client):
    """O2.4: the resync path stays store-side, split or not."""
    device_id = await seed_device(nso_device_name="sr-o24-f", netbox_device_id=9865)
    await seed_intent(device_id, [{"triple": A, "route_id": 1, "deployed_key": list(A)}])

    assert (await put_intent(adapter_client, device_id, [], query="?store_only=true")).status_code == 200

    assert await read_jobs(device_id) == []
    assert await read_tombstones(device_id) == []
    assert await read_generations(device_id) == []


async def test_o2_4_g_the_query_flag_does_not_override_empty_per_object_authority(adapter_client):
    """O3: an empty list marks a fence-shut omission as a detach, even with the old flag."""
    device_id = await seed_device(nso_device_name="sr-o24-g", netbox_device_id=9866)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": None, "deployed_key": list(A)},
            {"triple": B, "route_id": None, "deployed_key": list(B)},
        ],
    )

    assert (await put_intent(adapter_client, device_id, [entry(B)], query="?delete_origin=true")).status_code == 200

    jobs = await read_jobs(device_id)
    assert [j["context"] for j in jobs] == [{"scope": "static_route", "removed": {"route": [list(A)]}, "detach": True}]
    assert await read_tombstones(device_id) == []


async def test_o2_4_h_the_removal_precedes_the_apply(adapter_client):
    """O2.4: the queue order the worker's per-device head claim reads."""
    device_id = await seed_device(nso_device_name="sr-o24-h", netbox_device_id=9867)
    await enable_auto_apply(device_id)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 1, "deployed_key": list(A)},
            {"triple": B, "route_id": 2, "deployed_key": list(B)},
        ],
    )

    assert (await put_intent(adapter_client, device_id, [entry(B, route_id=2)])).status_code == 200

    jobs = await read_jobs(device_id)
    assert [j["job_type"] for j in jobs] == ["removal", "apply"]
    assert jobs[0]["id"] < jobs[1]["id"]
    # The removal's generation is the earlier one, so the apply queues behind it (§H2).
    assert [(g["seq"], g["mode"], g["job_id"]) for g in await read_generations(device_id)] == [
        (1, "detach", jobs[0]["id"]),
        (2, "networked", jobs[1]["id"]),
    ]


async def test_o3_3_a_stray_query_flag_is_inert_and_the_list_decides(adapter_client):
    """O3.3, adapter half: ``?delete_origin=true`` beside a list changes NOTHING.

    The unlisted omitted row detaches despite the flag; only the listed id retracts.
    """
    device_id = await seed_device(nso_device_name="sr-o33-a", netbox_device_id=9868)
    await seed_intent(
        device_id,
        [
            {"triple": A, "route_id": 1, "deployed_key": list(A)},
            {"triple": B, "route_id": 2, "deployed_key": list(B)},
            {"triple": C, "route_id": 3, "deployed_key": list(C)},
        ],
    )

    resp = await put_intent(
        adapter_client,
        device_id,
        [entry(C, route_id=3)],
        query="?delete_origin=true",
        deleted_routes=[deleted(1, A)],
    )
    assert resp.status_code == 200
    assert resp.json()["removed"] == 2

    tombstones = await read_tombstones(device_id)
    assert sorted((t["route_id"], t["marking"]) for t in tombstones) == [
        (1, "delete_origin"),
        (2, "detach"),
    ]
