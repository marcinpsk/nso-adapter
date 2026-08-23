# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R1a: route_id-first matching, payload refusals and the rollout fence.

Every case drives the real ``PUT /api/v1/devices/{id}/static-route-intent`` against a
real PostgreSQL clone and asserts on real ``StaticRouteIntent`` / ``StaticRouteTombstone``
/ ``Job`` rows. The matrix labels (M1.x / M2.x / M3.x / M4.x) are the brief's.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select, text

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

# Asked of PostgreSQL, not of Python: a JSON `null` also deserializes to Python None, so
# comparing the loaded attribute against None cannot tell SQL NULL from 'null'::jsonb.
# Only the former satisfies `IS NULL`, which is how "no proven predecessor" is tested.
# Static statements per table — no SQL is ever assembled from a variable.
_SQL_NULL_COUNT = {
    "static_route_intent": text(
        "SELECT count(*) FROM static_route_intent WHERE device_id = :d AND deployed_key IS NULL"
    ),
    "static_route_tombstone": text(
        "SELECT count(*) FROM static_route_tombstone WHERE device_id = :d AND deployed_key IS NULL"
    ),
}


async def count_sql_null_deployed_key(table: str, device_id: int) -> int:
    """Rows whose ``deployed_key`` is SQL NULL. Pins the ``none_as_null=True`` binding."""
    async with session() as db:
        return await db.scalar(_SQL_NULL_COUNT[table], {"d": device_id})


A = ("", "10.0.0.0/24", "192.0.2.1")
B = ("", "10.0.1.0/24", "192.0.2.2")
C = ("", "10.0.2.0/24", "192.0.2.3")


def entry(triple: tuple[str, str, str], *, route_id: int | None = None, **extra) -> dict:
    vrf, prefix, next_hop = triple
    body = {"vrf": vrf, "prefix": prefix, "next_hop": next_hop, **extra}
    if route_id is not None:
        body["route_id"] = route_id
    return body


async def seed_intent(device_id: int, rows: list[dict]) -> dict[tuple, int]:
    """Insert StaticRouteIntent rows; return {triple: row id}.

    ``deployed_key`` is seeded directly — R1 never writes it at runtime.
    """
    from nso_adapter.store.models import StaticRouteIntent

    out: dict[tuple, int] = {}
    async with session() as db:
        for spec in rows:
            vrf, prefix, next_hop = spec["triple"]
            row = StaticRouteIntent(
                device_id=device_id,
                vrf=vrf,
                prefix=prefix,
                next_hop=next_hop,
                route_id=spec.get("route_id"),
                deployed_key=spec.get("deployed_key"),
                accepted_at=datetime(2026, 6, 1, tzinfo=UTC),
                last_apply_at=spec.get("last_apply_at"),
            )
            db.add(row)
            await db.flush()
            out[spec["triple"]] = row.id
        await db.commit()
    return out


async def read_intent(device_id: int) -> list[dict]:
    from nso_adapter.store.models import StaticRouteIntent

    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(StaticRouteIntent)
                    .where(StaticRouteIntent.device_id == device_id)
                    .order_by(StaticRouteIntent.id)
                )
            )
            .scalars()
            .all()
        )
        return [
            {
                "id": r.id,
                "triple": (r.vrf, r.prefix, r.next_hop),
                "route_id": r.route_id,
                "deployed_key": r.deployed_key,
            }
            for r in rows
        ]


async def read_intent_all_columns(device_id: int) -> list[dict]:
    """Every persisted column, derived from the mapper rather than hand-listed.

    A field added later is compared automatically instead of silently escaping the
    "nothing changed" assertions.
    """
    from nso_adapter.store.models import StaticRouteIntent

    names = [c.name for c in StaticRouteIntent.__table__.columns]
    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(StaticRouteIntent)
                    .where(StaticRouteIntent.device_id == device_id)
                    .order_by(StaticRouteIntent.id)
                )
            )
            .scalars()
            .all()
        )
        return [{name: getattr(row, name) for name in names} for row in rows]


async def read_tombstones(device_id: int) -> list[dict]:
    from nso_adapter.store.models import StaticRouteTombstone

    async with session() as db:
        rows = (
            (
                await db.execute(
                    select(StaticRouteTombstone)
                    .where(StaticRouteTombstone.device_id == device_id)
                    .order_by(StaticRouteTombstone.id)
                )
            )
            .scalars()
            .all()
        )
        return [
            {
                "route_id": r.route_id,
                "triple": (r.vrf, r.prefix, r.next_hop),
                "deployed_key": r.deployed_key,
                "marking": r.marking,
                "job_id": r.job_id,
            }
            for r in rows
        ]


async def removal_job_id(device_id: int) -> int | None:
    """The removal job the same transaction created — what its tombstones point at."""
    from nso_adapter.store.models import Job, JobType

    async with session() as db:
        return await db.scalar(
            select(Job.id).where(Job.device_id == device_id, Job.job_type == JobType.removal).order_by(Job.id)
        )


async def read_jobs(device_id: int) -> list[dict]:
    from nso_adapter.store.models import Job

    async with session() as db:
        rows = (await db.execute(select(Job).where(Job.device_id == device_id).order_by(Job.id))).scalars().all()
        return [{"id": r.id, "job_type": r.job_type.value, "context": r.context} for r in rows]


async def put_intent(client, device_id: int, routes: list[dict], *, query: str = ""):
    return await client.put(
        f"/api/v1/devices/{device_id}/static-route-intent{query}",
        json={"routes": routes},
        headers=AUTH,
    )


async def enable_auto_apply(device_id: int) -> None:
    from nso_adapter.store.models import DeviceSettings

    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()


async def test_python_none_binds_to_sql_null_on_both_columns(adapter_client):
    """``none_as_null=True`` on both JSONB columns, pinned behaviorally.

    Binds a Python ``None`` explicitly through the ORM — the case the flag governs.
    Without it SQLAlchemy writes ``'null'::jsonb``, which is NOT NULL, so R2's
    "no proven predecessor" test (`deployed_key IS NULL`) would silently stop matching
    while every Python-side ``== None`` assertion stayed green.
    """
    from nso_adapter.store.models import StaticRouteTombstone

    device_id = await seed_device(nso_device_name="sr-none-bind", netbox_device_id=9770)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": None}])
    async with session() as db:
        db.add(
            StaticRouteTombstone(
                device_id=device_id,
                route_id=7,
                vrf=A[0],
                prefix=A[1],
                next_hop=A[2],
                deployed_key=None,
                marking="detach",
            )
        )
        await db.commit()

    assert await count_sql_null_deployed_key("static_route_intent", device_id) == 1
    assert await count_sql_null_deployed_key("static_route_tombstone", device_id) == 1


# ── M1: matching matrix ──────────────────────────────────────────────────────


async def test_route_id_match_unchanged_triple_keeps_the_row(adapter_client):
    """M1.1 — same row id, no tombstone, deployed_key untouched."""
    device_id = await seed_device(nso_device_name="sr-m1-1", netbox_device_id=9701)
    ids = await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7)])
    assert resp.status_code == 200

    rows = await read_intent(device_id)
    assert [(r["id"], r["triple"], r["route_id"], r["deployed_key"]) for r in rows] == [(ids[A], A, 7, list(A))]
    assert await read_tombstones(device_id) == []


async def test_identity_edit_updates_row_in_place(adapter_client):
    """M1.2 — the central R1 behavior. Today the endpoint hard-deletes A and inserts B."""
    device_id = await seed_device(nso_device_name="sr-m1-2", netbox_device_id=9702)
    ids = await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [entry(B, route_id=7)])
    assert resp.status_code == 200
    assert resp.json()["removed"] == 0

    rows = await read_intent(device_id)
    assert len(rows) == 1
    assert rows[0]["id"] == ids[A], "the row was deleted and re-inserted instead of updated in place"
    assert rows[0]["triple"] == B
    assert rows[0]["route_id"] == 7
    # R1 never writes deployed_key at runtime: the proven predecessor is still A.
    assert rows[0]["deployed_key"] == list(A)


async def test_identity_edit_writes_no_detach_job(adapter_client):
    """M1.2 — no removal job at all: nothing was un-owned, the row moved."""
    device_id = await seed_device(nso_device_name="sr-m1-2b", netbox_device_id=9703)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    await put_intent(adapter_client, device_id, [entry(B, route_id=7)])

    assert [j for j in await read_jobs(device_id) if j["job_type"] == "removal"] == []
    assert await read_tombstones(device_id) == []


async def test_triple_match_without_route_id_updates_in_place(adapter_client):
    """M1.3 — the pre-R3 shape: matched by triple, row reused, fence stays shut."""
    device_id = await seed_device(nso_device_name="sr-m1-3", netbox_device_id=9704)
    ids = await seed_intent(device_id, [{"triple": A}])

    resp = await put_intent(adapter_client, device_id, [entry(A, metric=5)])
    assert resp.status_code == 200

    rows = await read_intent(device_id)
    assert [(r["id"], r["route_id"]) for r in rows] == [(ids[A], None)]


async def test_triple_match_adopts_the_payload_route_id(adapter_client):
    """M1.4 — the backfill: the row adopts route_id 7 and the fence can open."""
    device_id = await seed_device(nso_device_name="sr-m1-4", netbox_device_id=9705)
    ids = await seed_intent(device_id, [{"triple": A}])

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7)])
    assert resp.status_code == 200

    rows = await read_intent(device_id)
    assert [(r["id"], r["route_id"]) for r in rows] == [(ids[A], 7)]


async def test_no_match_inserts_with_null_deployed_key(adapter_client):
    """M1.5 — a fresh row has no proven predecessor."""
    device_id = await seed_device(nso_device_name="sr-m1-5", netbox_device_id=9706)

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7)])
    assert resp.status_code == 200

    rows = await read_intent(device_id)
    assert [(r["triple"], r["route_id"], r["deployed_key"]) for r in rows] == [(A, 7, None)]
    # SQL NULL asked of PostgreSQL. The endpoint leaves the column unset on an insert, so
    # this pins the server default; the ORM binding itself is pinned by
    # test_python_none_binds_to_sql_null_on_both_columns.
    assert await count_sql_null_deployed_key("static_route_intent", device_id) == 1


async def test_disappearing_route_id_writes_a_tombstone(adapter_client):
    """M1.6 — the row is deleted and a tombstone records the deletion, one transaction."""
    device_id = await seed_device(nso_device_name="sr-m1-6", netbox_device_id=9707)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [])
    assert resp.status_code == 200
    assert resp.json()["removed"] == 1

    assert await read_intent(device_id) == []
    assert await read_tombstones(device_id) == [
        {
            "route_id": 7,
            "triple": A,
            "deployed_key": list(A),
            "marking": "detach",
            "job_id": await removal_job_id(device_id),
        }
    ]


async def test_delete_and_tombstone_roll_back_together(adapter_client, monkeypatch):
    """M1.6, the other half: co-persistence on success proves nothing about rollback.

    Fails after the tombstone insert and the row delete are both issued but before the
    commit, by making the auto_apply enqueue raise. Neither half may survive — a
    persisted tombstone for a row that is still live would authorize a deletion nothing
    asked for, and a persisted delete with no carrier is the lost deletion R1 exists to
    stop.
    """
    device_id = await seed_device(nso_device_name="sr-m1-6b", netbox_device_id=9715)
    await enable_auto_apply(device_id)
    ids = await seed_intent(
        device_id,
        [{"triple": A, "route_id": 7, "deployed_key": list(A)}, {"triple": B, "route_id": 8, "deployed_key": list(B)}],
    )

    boom = RuntimeError("forced failure after the tombstone/delete DML")

    async def _explode(*args, **kwargs):
        raise boom

    # Imported inside the handler, so patch it at its source module.
    monkeypatch.setattr("nso_adapter.core.apply.enqueue_apply", _explode)

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7)])
    assert resp.status_code == 500, resp.text

    # Row 8 is still live, and nothing claims authority to delete it.
    rows = await read_intent(device_id)
    assert sorted((r["id"], r["triple"], r["route_id"]) for r in rows) == sorted([(ids[A], A, 7), (ids[B], B, 8)])
    assert await read_tombstones(device_id) == []
    assert await read_jobs(device_id) == []


async def test_a_to_b_to_a_leaves_no_tombstone(adapter_client):
    """M1.7 — the cancellation: two in-place edits, no deletion ever happened."""
    device_id = await seed_device(nso_device_name="sr-m1-7", netbox_device_id=9708)
    ids = await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    assert (await put_intent(adapter_client, device_id, [entry(B, route_id=7)])).status_code == 200
    assert (await put_intent(adapter_client, device_id, [entry(A, route_id=7)])).status_code == 200

    rows = await read_intent(device_id)
    assert [(r["id"], r["triple"], r["deployed_key"]) for r in rows] == [(ids[A], A, list(A))]
    assert await read_tombstones(device_id) == []


async def test_route_id_match_beats_a_triple_match(adapter_client):
    """M1.8 — a naive triple-first matcher pairs payload (7,B) with the row holding B."""
    device_id = await seed_device(nso_device_name="sr-m1-8", netbox_device_id=9709)
    ids = await seed_intent(
        device_id,
        [{"triple": A, "route_id": 7, "deployed_key": list(A)}, {"triple": B, "route_id": 8, "deployed_key": list(B)}],
    )

    resp = await put_intent(adapter_client, device_id, [entry(B, route_id=7), entry(C, route_id=8)])
    assert resp.status_code == 200

    rows = {r["route_id"]: r for r in await read_intent(device_id)}
    assert rows[7]["id"] == ids[A] and rows[7]["triple"] == B
    assert rows[8]["id"] == ids[B] and rows[8]["triple"] == C
    assert await read_tombstones(device_id) == []


async def test_identity_edit_swap_preserves_row_ids(adapter_client):
    """M1.9 — an immediate unique constraint rejects this; a delete+reinsert loses the ids."""
    device_id = await seed_device(nso_device_name="sr-m1-9", netbox_device_id=9710)
    ids = await seed_intent(
        device_id,
        [{"triple": A, "route_id": 7, "deployed_key": list(A)}, {"triple": B, "route_id": 8, "deployed_key": list(B)}],
    )

    resp = await put_intent(adapter_client, device_id, [entry(B, route_id=7), entry(A, route_id=8)])
    assert resp.status_code == 200, resp.text

    rows = {r["route_id"]: r for r in await read_intent(device_id)}
    assert rows[7]["id"] == ids[A] and rows[7]["triple"] == B
    assert rows[8]["id"] == ids[B] and rows[8]["triple"] == A
    assert await read_tombstones(device_id) == []
    assert [j for j in await read_jobs(device_id) if j["job_type"] == "removal"] == []


async def test_delete_then_reclaim_moves_one_row_and_tombstones_the_other(adapter_client):
    """M1.10 — route 7 takes route 8's triple while 8 is deleted in the same payload."""
    device_id = await seed_device(nso_device_name="sr-m1-10", netbox_device_id=9711)
    ids = await seed_intent(
        device_id,
        [{"triple": A, "route_id": 7, "deployed_key": list(A)}, {"triple": B, "route_id": 8, "deployed_key": list(B)}],
    )

    resp = await put_intent(adapter_client, device_id, [entry(B, route_id=7)])
    assert resp.status_code == 200, resp.text

    rows = await read_intent(device_id)
    assert [(r["id"], r["triple"], r["route_id"]) for r in rows] == [(ids[A], B, 7)]
    assert await read_tombstones(device_id) == [
        {
            "route_id": 8,
            "triple": B,
            "deployed_key": list(B),
            "marking": "detach",
            "job_id": await removal_job_id(device_id),
        }
    ]


async def test_never_applied_row_gets_a_detach_tombstone(adapter_client):
    """M1.11 — OQ2: every fence-open deletion writes a tombstone, deployed_key or not."""
    device_id = await seed_device(nso_device_name="sr-m1-11", netbox_device_id=9712)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": None}])

    resp = await put_intent(adapter_client, device_id, [])
    assert resp.status_code == 200

    assert await read_tombstones(device_id) == [
        {
            "route_id": 7,
            "triple": A,
            "deployed_key": None,
            "marking": "detach",
            "job_id": await removal_job_id(device_id),
        }
    ]
    # SQL NULL on the carrier too — the tombstone is what R2's CAS reads.
    assert await count_sql_null_deployed_key("static_route_tombstone", device_id) == 1
    # today's detach path is unchanged for the device side
    removals = [j for j in await read_jobs(device_id) if j["job_type"] == "removal"]
    assert len(removals) == 1
    assert removals[0]["context"]["detach"] is True


async def test_never_applied_delete_origin_authorizes_the_triple_only(adapter_client):
    """M1.12 — a NULL deployed_key must not read as unrestricted deletion authority."""
    device_id = await seed_device(nso_device_name="sr-m1-12", netbox_device_id=9713)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": None}])

    resp = await put_intent(adapter_client, device_id, [], query="?delete_origin=true")
    assert resp.status_code == 200

    tombs = await read_tombstones(device_id)
    assert tombs == [
        {
            "route_id": 7,
            "triple": A,
            "deployed_key": None,
            "marking": "delete_origin",
            "job_id": await removal_job_id(device_id),
        }
    ]
    # The authorized set is exactly {triple}: the tombstone carries no second key.
    assert tombs[0]["deployed_key"] is None
    removals = [j for j in await read_jobs(device_id) if j["job_type"] == "removal"]
    assert removals[0]["context"].get("detach") is None
    assert removals[0]["context"]["removed"] == {"route": [list(A)]}


async def test_applied_delete_origin_tombstone_carries_the_deployed_key(adapter_client):
    """OQ2 — with a proven predecessor the authorized set is {triple} ∪ {deployed_key}."""
    device_id = await seed_device(nso_device_name="sr-m1-12b", netbox_device_id=9714)
    await seed_intent(device_id, [{"triple": B, "route_id": 7, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [], query="?delete_origin=true")
    assert resp.status_code == 200

    assert await read_tombstones(device_id) == [
        {
            "route_id": 7,
            "triple": B,
            "deployed_key": list(A),
            "marking": "delete_origin",
            "job_id": await removal_job_id(device_id),
        }
    ]


# ── M2: duplicate refusals ───────────────────────────────────────────────────


async def test_duplicate_triples_in_payload_rejected(adapter_client):
    """M2.1 — today the endpoint loops the uncollapsed body and silently last-writer-wins."""
    device_id = await seed_device(nso_device_name="sr-m2-1", netbox_device_id=9720)

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7), entry(A, route_id=8)])
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "validation_error"
    assert error["detail"]["reason"] == "duplicate_triple"
    assert error["detail"]["triple"] == list(A)


async def test_duplicate_non_null_route_ids_rejected(adapter_client):
    """M2.2 — two payload entries claiming the same NetBox route pk."""
    device_id = await seed_device(nso_device_name="sr-m2-2", netbox_device_id=9721)

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7), entry(B, route_id=7)])
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "validation_error"
    assert error["detail"]["reason"] == "duplicate_route_id"
    assert error["detail"]["route_id"] == 7


async def test_reclaiming_a_deleted_rows_triple_is_not_a_duplicate(adapter_client):
    """M2.4 — the store row holding T is absent from the payload, so it is deleted."""
    device_id = await seed_device(nso_device_name="sr-m2-4", netbox_device_id=9722)
    await seed_intent(
        device_id,
        [{"triple": C, "route_id": 7, "deployed_key": list(C)}, {"triple": A, "route_id": 8, "deployed_key": list(A)}],
    )

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7)])
    assert resp.status_code == 200, resp.text


@pytest.mark.parametrize(
    ("routes", "reason"),
    [
        ([entry(B, route_id=9), entry(B, route_id=10)], "duplicate_triple"),
        ([entry(B, route_id=9), entry(C, route_id=9)], "duplicate_route_id"),
    ],
    ids=["duplicate_triple", "duplicate_route_id"],
)
async def test_payload_refusal_leaves_the_store_untouched(adapter_client, routes, reason):
    """M2.5 — the refusals fire before any DML: no row change, no job, no tombstone.

    Compares EVERY persisted column, both directions of the refusal. A four-field
    snapshot would miss a mutation to accepted_at, an optional scalar or the apply
    bookkeeping, all of which the endpoint writes on the success path.
    """
    device_id = await seed_device(nso_device_name="sr-m2-5", netbox_device_id=9723)
    await seed_intent(
        device_id,
        [{"triple": A, "route_id": 7, "deployed_key": list(A), "last_apply_at": datetime(2026, 6, 2, tzinfo=UTC)}],
    )
    before = await read_intent_all_columns(device_id)
    assert before, "the fixture must persist a row for the comparison to mean anything"
    # Guards the "every persisted column" claim: the fields a narrow snapshot would miss
    # are exactly the ones the success path writes.
    assert {"accepted_at", "last_apply_at", "last_apply_error", "metric", "tag", "name"} <= before[0].keys()

    resp = await put_intent(adapter_client, device_id, routes)
    assert resp.status_code == 422
    assert resp.json()["error"]["detail"]["reason"] == reason

    assert await read_intent_all_columns(device_id) == before
    assert await read_tombstones(device_id) == []
    assert await read_jobs(device_id) == []


async def test_a_new_route_id_does_not_steal_another_routes_row(adapter_client):
    """A triple shared by two DIFFERENT route pks is a delete plus an insert.

    Store row 8 holds A and the payload does not carry route 8, so route 8 was deleted
    and its deletion must be recorded. A triple fallback that adopts a row already
    carrying a different route_id silently rewrites 8 to 7 and loses the tombstone.
    """
    device_id = await seed_device(nso_device_name="sr-steal", netbox_device_id=9750)
    ids = await seed_intent(device_id, [{"triple": A, "route_id": 8, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7)])
    assert resp.status_code == 200, resp.text

    rows = await read_intent(device_id)
    assert [(r["triple"], r["route_id"], r["deployed_key"]) for r in rows] == [(A, 7, None)]
    assert rows[0]["id"] != ids[A], "route 7 adopted route 8's row instead of replacing it"
    assert await read_tombstones(device_id) == [
        {
            "route_id": 8,
            "triple": A,
            "deployed_key": list(A),
            "marking": "detach",
            "job_id": await removal_job_id(device_id),
        }
    ]


async def test_route_id_less_payload_keeps_a_backfilled_route_id(adapter_client):
    """A pre-R3 push onto an already-backfilled device must not undo the backfill.

    The entry asserts no route_id, so it matches by triple and the stored id survives.
    Against a fallback restricted to route_id-less rows, every row is deleted, tombstoned
    and re-inserted with a NULL route_id — the fence slams shut and the rollout regresses.
    """
    device_id = await seed_device(nso_device_name="sr-keep-rid", netbox_device_id=9751)
    ids = await seed_intent(device_id, [{"triple": A, "route_id": 8, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [entry(A, metric=3)])
    assert resp.status_code == 200, resp.text

    rows = await read_intent(device_id)
    assert [(r["id"], r["triple"], r["route_id"], r["deployed_key"]) for r in rows] == [(ids[A], A, 8, list(A))]
    assert await read_tombstones(device_id) == []


def test_final_planned_state_rule_refuses_a_double_claimed_triple():
    """The store-dependent refusal, tested at its helper.

    Under full-replace the planned triples are exactly the payload triples, so no
    request can reach this branch once payload-internal duplicates are refused (M2.5's
    scope note). It is exercised directly rather than left as unreachable code.
    """
    from nso_adapter.api.static_route import _double_claimed_triple

    assert _double_claimed_triple([(A, 7), (B, 8)]) is None
    assert _double_claimed_triple([(A, None), (A, None)]) is None  # no route_id claims it
    assert _double_claimed_triple([(A, 7), (A, 8)]) == A
    assert _double_claimed_triple([(A, 7), (A, 7)]) is None  # one claimant, not a conflict


# ── M3: the rollout fence ────────────────────────────────────────────────────


async def test_fence_shut_keeps_todays_detach_behavior(adapter_client):
    """M3.1 — a pre-existing NULL route_id row makes the whole device fall back."""
    from structlog.testing import capture_logs

    device_id = await seed_device(nso_device_name="sr-m3-1", netbox_device_id=9730)
    await seed_intent(device_id, [{"triple": A, "route_id": None}, {"triple": B, "route_id": 8}])

    with capture_logs() as logs:
        resp = await put_intent(adapter_client, device_id, [entry(A, route_id=None)])
    assert resp.status_code == 200

    # B disappeared: hard-deleted with the detach job and NO tombstone.
    assert [r["triple"] for r in await read_intent(device_id)] == [A]
    assert await read_tombstones(device_id) == []
    removals = [j for j in await read_jobs(device_id) if j["job_type"] == "removal"]
    assert len(removals) == 1
    assert removals[0]["context"]["detach"] is True

    fallback = [e for e in logs if e["event"] == "static_route.null_route_id_fallback"]
    assert len(fallback) == 1
    assert fallback[0]["device_id"] == device_id
    assert fallback[0]["null_route_id_count"] == 1
    assert fallback[0]["log_level"] == "warning"


async def test_fence_opens_on_the_next_request(adapter_client):
    """M3.2 — PUT 1 fills the last NULL; PUT 2 takes the in-place path."""
    device_id = await seed_device(nso_device_name="sr-m3-2", netbox_device_id=9731)
    ids = await seed_intent(device_id, [{"triple": A, "route_id": None, "deployed_key": list(A)}])

    assert (await put_intent(adapter_client, device_id, [entry(A, route_id=7)])).status_code == 200
    assert (await put_intent(adapter_client, device_id, [entry(B, route_id=7)])).status_code == 200

    rows = await read_intent(device_id)
    assert [(r["id"], r["triple"], r["route_id"]) for r in rows] == [(ids[A], B, 7)]
    assert await read_tombstones(device_id) == []
    assert [j for j in await read_jobs(device_id) if j["job_type"] == "removal"] == []


async def test_fence_is_per_device(adapter_client):
    """M3.3 — device 2's NULL row must not fence device 1."""
    open_id = await seed_device(nso_device_name="sr-m3-3-open", netbox_device_id=9732)
    shut_id = await seed_device(nso_device_name="sr-m3-3-shut", netbox_device_id=9733)
    await seed_intent(open_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])
    await seed_intent(shut_id, [{"triple": A, "route_id": None, "deployed_key": list(A)}])

    assert (await put_intent(adapter_client, open_id, [])).status_code == 200
    assert (await put_intent(adapter_client, shut_id, [])).status_code == 200

    assert len(await read_tombstones(open_id)) == 1
    assert await read_tombstones(shut_id) == []


async def test_backfilling_put_stays_fallback_classified(adapter_client):
    """M3.4 — nothing correlates A with B, so nothing may claim deletion authority for A.

    Fails against a POST-payload fence, which reads open here (no NULL survives) and
    writes a tombstone for a triple no route pk ever correlated.
    """
    from structlog.testing import capture_logs

    device_id = await seed_device(nso_device_name="sr-m3-4", netbox_device_id=9734)
    await seed_intent(device_id, [{"triple": A, "route_id": None, "deployed_key": list(A)}])

    with capture_logs() as logs:
        resp = await put_intent(adapter_client, device_id, [entry(B, route_id=7)])
    assert resp.status_code == 200

    assert [(r["triple"], r["route_id"]) for r in await read_intent(device_id)] == [(B, 7)]
    assert await read_tombstones(device_id) == []
    removals = [j for j in await read_jobs(device_id) if j["job_type"] == "removal"]
    assert len(removals) == 1
    assert removals[0]["context"]["detach"] is True
    assert [e["event"] for e in logs].count("static_route.null_route_id_fallback") == 1


async def test_the_request_after_the_backfill_classifies(adapter_client):
    """M3.5 — continue M3.4: the pre-mutation fence is now open."""
    device_id = await seed_device(nso_device_name="sr-m3-5", netbox_device_id=9735)
    await seed_intent(device_id, [{"triple": A, "route_id": None, "deployed_key": list(A)}])

    assert (await put_intent(adapter_client, device_id, [entry(B, route_id=7)])).status_code == 200
    row_id = (await read_intent(device_id))[0]["id"]

    assert (await put_intent(adapter_client, device_id, [entry(C, route_id=7)])).status_code == 200

    rows = await read_intent(device_id)
    assert [(r["id"], r["triple"]) for r in rows] == [(row_id, C)]
    assert await read_tombstones(device_id) == []


async def test_pre_r3_payload_without_any_route_ids_is_accepted(adapter_client):
    """M3.6 — the compatibility pin: `[None, None]` is not a duplicate route_id."""
    device_id = await seed_device(nso_device_name="sr-m3-6", netbox_device_id=9736)
    await seed_intent(device_id, [{"triple": C, "route_id": None}])

    resp = await put_intent(adapter_client, device_id, [entry(A), entry(B)])
    assert resp.status_code == 200, resp.text
    assert resp.json()["count"] == 2

    assert sorted(r["triple"] for r in await read_intent(device_id)) == sorted([A, B])


# ── M4: store-only resync ────────────────────────────────────────────────────


async def test_store_only_shrink_writes_no_tombstone(adapter_client):
    """M4.1 — the sharpest constraint: a tombstone without a job would be swept into one."""
    device_id = await seed_device(nso_device_name="sr-m4-1", netbox_device_id=9740)
    await seed_intent(
        device_id,
        [{"triple": A, "route_id": 7, "deployed_key": list(A)}, {"triple": B, "route_id": 8, "deployed_key": list(B)}],
    )

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7)], query="?store_only=true")
    assert resp.status_code == 200

    assert [r["triple"] for r in await read_intent(device_id)] == [A]  # still deleted
    assert await read_tombstones(device_id) == []  # but no carrier
    assert await read_jobs(device_id) == []  # and no job


async def test_store_only_backfills_route_ids(adapter_client):
    """M4.2 — this is how the fleet resync opens the fence."""
    device_id = await seed_device(nso_device_name="sr-m4-2", netbox_device_id=9741)
    ids = await seed_intent(device_id, [{"triple": A, "route_id": None, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7)], query="?store_only=true")
    assert resp.status_code == 200

    assert [(r["id"], r["route_id"]) for r in await read_intent(device_id)] == [(ids[A], 7)]


async def test_store_only_never_clears_deployed_key(adapter_client):
    """M4.3 — a surviving row keeps its proven predecessor."""
    device_id = await seed_device(nso_device_name="sr-m4-3", netbox_device_id=9742)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [entry(A, route_id=7)], query="?store_only=true")
    assert resp.status_code == 200

    assert [r["deployed_key"] for r in await read_intent(device_id)] == [list(A)]


async def test_store_only_on_auto_apply_device_enqueues_nothing(adapter_client):
    """M4.4 — the apply suppression is unchanged."""
    device_id = await seed_device(nso_device_name="sr-m4-4", netbox_device_id=9743)
    await enable_auto_apply(device_id)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [entry(B, route_id=7)], query="?store_only=true")
    assert resp.status_code == 200

    assert await read_jobs(device_id) == []


async def test_identity_edit_that_clears_an_owned_scalar_retracts(adapter_client):
    """An in-place edit can now clear an owned scalar, and that still reaches the device.

    Under the old delete-then-insert path the moved route was a fresh row, so the
    cleared-scalar check never ran on it and the blanked leaf stayed on the device.
    """
    device_id = await seed_device(nso_device_name="sr-clear", netbox_device_id=9760)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])
    assert (await put_intent(adapter_client, device_id, [entry(A, route_id=7, tag=99)])).status_code == 200

    resp = await put_intent(adapter_client, device_id, [entry(B, route_id=7)])
    assert resp.status_code == 200

    removals = [j for j in await read_jobs(device_id) if j["job_type"] == "removal"]
    assert len(removals) == 1
    # A clear is not a shrink: the job must NOT be a no-networking detach.
    assert removals[0]["context"].get("detach") is None
    assert await read_tombstones(device_id) == []


async def test_auto_apply_identity_edit_still_enqueues_an_apply(adapter_client):
    """The in-place update is still a change worth applying."""
    device_id = await seed_device(nso_device_name="sr-m4-5", netbox_device_id=9744)
    await enable_auto_apply(device_id)
    await seed_intent(device_id, [{"triple": A, "route_id": 7, "deployed_key": list(A)}])

    resp = await put_intent(adapter_client, device_id, [entry(B, route_id=7)])
    assert resp.status_code == 200

    assert [j["job_type"] for j in await read_jobs(device_id)] == ["apply"]
