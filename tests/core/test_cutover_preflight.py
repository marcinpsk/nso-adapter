# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The blocking drain preflight C9's cutover window runs first (#1683)."""

from __future__ import annotations

import pytest

from tests.conftest import seed_device, session
from tests.core.removal_helpers import authorize_static_route, seed_tomb
from tests.core.test_static_route_put import A, B, seed_rows

pytestmark = pytest.mark.anyio


async def _job(job_id: int):
    from nso_adapter.store.models import Job

    async with session() as db:
        return await db.get(Job, job_id)


async def _generation_statuses(device_id: int) -> list:
    import sqlalchemy as sa

    from nso_adapter.store.models import DeploymentGeneration

    async with session() as db:
        rows = (
            await db.execute(
                sa.select(DeploymentGeneration)
                .where(DeploymentGeneration.device_id == device_id)
                .order_by(DeploymentGeneration.seq)
            )
        ).scalars()
        return [row.status for row in rows]


async def _parked(device_id: int):
    from nso_adapter.core.cutover import parked_static_route_carriers

    async with session() as db:
        return [carrier for carrier in await parked_static_route_carriers(db) if carrier.device_id == device_id]


async def test_a_tombstone_only_route_refuses_the_cutover_and_passes_once_its_removal_settles(adapter_client):
    """A carrier no authorized positive row renders loses its payload source at the cutover.

    The clearance is asserted through the REAL removal, not by deleting the row: what the
    operator is told to do is Apply, and only a removal that transmits the omission, proves
    the service clean and settles may clear the window.
    """
    from nso_adapter.core.cutover import CutoverBlocked, refuse_cutover_while_carriers_are_parked
    from nso_adapter.core.tombstone_sweep import sweep_tombstones
    from nso_adapter.store.models import GenerationStatus, JobStatus
    from tests.core.test_generation_protocol import run_head
    from tests.core.test_static_route_removal import SrFake, sr_client, wire

    device_id = await seed_device(nso_device_name="cutover-parked", netbox_device_id=17101)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    tomb = await seed_tomb(device_id, A, route_id=1)
    await authorize_static_route(device_id)

    (carrier,) = await _parked(device_id)
    assert carrier.tombstone_id == tomb
    assert A in carrier.keys
    async with session() as db:
        with pytest.raises(CutoverBlocked, match="must drain first"):
            await refuse_cutover_while_carriers_are_parked(db)

    # The operator's remedy: the sweeper carries the parked deletion, and the worker runs it.
    fake = SrFake("cutover-parked", service=[wire(A), wire(B)])
    assert await sweep_tombstones() == 1
    job_id = await run_head(device_id, sr_client(fake))
    assert job_id is not None

    job = await _job(job_id)
    assert job.status is JobStatus.succeeded
    assert fake.sent_keys() == {B}, "the removal must transmit the omission it is authorized for"
    assert fake.service_keys == {B}
    # Emit-on-failure: removal writes service_clean only when it is False, so `is not False`
    # also passed on a missing key. The absence IS the clean bill.
    assert "service_clean" not in job.result, "consumption requires a CERTIFIED clean service"
    assert await _generation_statuses(device_id) == [GenerationStatus.settled]

    assert await _parked(device_id) == []
    async with session() as db:
        await refuse_cutover_while_carriers_are_parked(db)


async def test_an_inconclusive_cleanup_read_keeps_the_carrier_and_the_window_shut(adapter_client):
    """The negative control: an unprovable removal may not clear the window.

    An uncertifiable post-commit service read proves nothing, so the carrier is retained under
    a failed owner and the preflight still refuses. A cleanup that consumed here would open the
    cutover with the key still on the service, which is the parked state it exists to catch.
    """
    from nso_adapter.core.cutover import CutoverBlocked, refuse_cutover_while_carriers_are_parked
    from nso_adapter.core.tombstone_sweep import sweep_tombstones
    from nso_adapter.store.models import JobStatus
    from tests.core.test_generation_protocol import run_head
    from tests.core.test_static_route_removal import SrFake, sr_client, wire

    device_id = await seed_device(nso_device_name="cutover-unproven", netbox_device_id=17104)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    tomb = await seed_tomb(device_id, A, route_id=1)
    await authorize_static_route(device_id)

    fake = SrFake("cutover-unproven", service=[wire(A), wire(B)])
    original = fake.state
    calls = {"reads": 0}

    def _inconclusive_after_the_commit():
        calls["reads"] += 1
        # The pre-PUT read certifies; the post-commit proof read does not.
        if calls["reads"] == 1:
            return original()
        from nso_adapter.nso.client import ServiceInstanceState

        return ServiceInstanceState("inconclusive", None)

    fake.state = _inconclusive_after_the_commit
    assert await sweep_tombstones() == 1
    job_id = await run_head(device_id, sr_client(fake))
    assert job_id is not None

    job = await _job(job_id)
    assert job.status is JobStatus.failed
    assert job.error["code"] == "static_route_removal_unproven"
    (still_parked,) = await _parked(device_id)
    assert still_parked.tombstone_id == tomb
    async with session() as db:
        with pytest.raises(CutoverBlocked, match="must drain first"):
            await refuse_cutover_while_carriers_are_parked(db)


async def test_a_carrier_whose_key_an_authorized_row_still_renders_is_not_parked(adapter_client):
    """Supersession by a RENDERED key needs no cleanup, so it never blocks the window."""
    device_id = await seed_device(nso_device_name="cutover-rendered", netbox_device_id=17102)
    await seed_rows(device_id, [{"triple": A, "route_id": 2}])
    await seed_tomb(device_id, A, route_id=1)
    await authorize_static_route(device_id)

    assert await _parked(device_id) == []


async def test_a_carrier_whose_deployed_predecessor_no_row_renders_is_parked(adapter_client):
    """A deployed-only claim is not supersession: its predecessor key still owes cleanup."""
    device_id = await seed_device(nso_device_name="cutover-predecessor", netbox_device_id=17103)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    tomb = await seed_tomb(device_id, B, route_id=1, deployed_key=list(A))
    await authorize_static_route(device_id)

    (carrier,) = await _parked(device_id)
    assert carrier.tombstone_id == tomb
    assert A in carrier.keys
