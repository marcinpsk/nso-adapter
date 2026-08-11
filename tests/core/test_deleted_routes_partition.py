# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1503 Appendix-O chunk O2b: the ``deleted_routes`` partition itself (O2b.12).

The endpoint arms live in ``tests/api/test_static_route_deleted_routes.py``. This file
addresses the classifier directly, because codex's O2b.12 case cannot be driven through the
PUT at all: it needs a GENUINE id (a removed row whose ``route_id`` the request names) beside
a ``route_id IS NULL`` removed row, and one NULL row is exactly what shuts the fence — so the
whole request takes §4.4's ``409 fence_shut`` before any partition is emitted. The partition
is still what decides the answer once the fence is open, so it is pinned here, over REAL
``StaticRouteIntent`` rows read back out of PostgreSQL rather than over stand-ins.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tests.api.test_static_route_identity import A, B, C, seed_intent
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio

D = ("", "10.0.3.0/24", "192.0.2.4")


def record(route_id: int, triples, *, unverified: bool = False):
    from nso_adapter.core.deleted_routes import DeletionRecord

    return DeletionRecord(route_id=route_id, triples=tuple(triples), unverified=unverified)


async def removed_rows(device_id: int, triples):
    """The persisted rows for *triples*, in store order — a real removal candidate set."""
    from nso_adapter.store.models import StaticRouteIntent

    wanted = set(triples)
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
        return [row for row in rows if (row.vrf, row.prefix, row.next_hop) in wanted]


async def test_o2b_12_pass_one_binds_by_route_id_and_pass_two_sees_only_the_remainder(adapter_client):
    """O2b.12 — request ``{1: [A, C], 2: [C]}`` against rows ``{1: A}`` and ``{NULL: C}``.

    Rev 8 stated the genuine rule and the equivalence-class rule side by side and they
    contradict each other here: id 1 is genuine by the first and belongs to C's class by the
    second. Precedence is route_id first, EXCLUSIVELY, and pass 2 then runs over what is left.
    """
    from nso_adapter.core.deleted_routes import classify_deletions

    device_id = await seed_device(nso_device_name="sr-o2b12-a", netbox_device_id=9880)
    await seed_intent(device_id, [{"triple": A, "route_id": 1}, {"triple": C, "route_id": None}])
    rows = await removed_rows(device_id, [A, C])

    partition = classify_deletions([record(1, [A, C]), record(2, [C])], rows)

    assert partition.executed == (1,)
    assert partition.degraded == (2,)
    assert partition.moot == ()
    assert partition.uncorrelated == ()


async def test_o2b_12_the_partition_does_not_depend_on_request_order(adapter_client):
    """O2b.12 — the same two records sent in the other order give the identical partition."""
    from nso_adapter.core.deleted_routes import classify_deletions

    device_id = await seed_device(nso_device_name="sr-o2b12-b", netbox_device_id=9881)
    await seed_intent(device_id, [{"triple": A, "route_id": 1}, {"triple": C, "route_id": None}])
    rows = await removed_rows(device_id, [A, C])

    forward = classify_deletions([record(1, [A, C]), record(2, [C])], rows)
    reverse = classify_deletions([record(2, [C]), record(1, [A, C])], rows)

    assert forward == reverse


async def test_o2b_12_an_id_belonging_to_two_classes_is_emitted_exactly_once(adapter_client):
    """O2b.12 (R9-M3) — lineage ``[A, B]`` against two NULL rows A and B.

    Emission is id-oriented. A row-oriented implementation emits the id twice, and §4.4's
    plugin-side validator then rejects the immutable stored response forever.
    """
    from nso_adapter.core.deleted_routes import classify_deletions

    device_id = await seed_device(nso_device_name="sr-o2b12-c", netbox_device_id=9882)
    await seed_intent(device_id, [{"triple": A, "route_id": None}, {"triple": B, "route_id": None}])
    rows = await removed_rows(device_id, [A, B])

    partition = classify_deletions([record(5, [A, B])], rows)

    assert partition.degraded == (5,)
    assert partition.executed == () and partition.moot == ()
    assert partition.uncorrelated == (), "both rows were claimed by the class"


async def test_o2b_12_a_genuine_binding_takes_its_row_out_of_the_triple_pool(adapter_client):
    """O2b.12 — the row bound in pass 1 can no longer classify anybody in pass 2.

    Without the exclusion, row A would also classify id 2 (whose lineage names A), and the
    two rules would produce two contradictory answers for one request.
    """
    from nso_adapter.core.deleted_routes import classify_deletions

    device_id = await seed_device(nso_device_name="sr-o2b12-d", netbox_device_id=9883)
    await seed_intent(device_id, [{"triple": A, "route_id": 1}, {"triple": D, "route_id": None}])
    rows = await removed_rows(device_id, [A, D])

    partition = classify_deletions([record(1, [A]), record(2, [A])], rows)

    assert partition.executed == (1,)
    assert partition.moot == (2,), "id 2 matched a row pass 1 had already consumed"
    assert partition.uncorrelated == (D,), "D was removed and nobody claimed it"
