# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The blocking drain preflight C9's cutover window runs first (#1683)."""

from __future__ import annotations

import pytest

from tests.conftest import seed_device, session
from tests.core.removal_helpers import authorize_static_route, seed_tomb
from tests.core.test_static_route_put import A, B, seed_rows

pytestmark = pytest.mark.anyio


async def _parked(device_id: int):
    from nso_adapter.core.cutover import parked_static_route_carriers

    async with session() as db:
        return [carrier for carrier in await parked_static_route_carriers(db) if carrier.device_id == device_id]


async def test_a_tombstone_only_route_refuses_the_cutover_and_passes_once_it_settles(adapter_client):
    """A carrier no authorized positive row renders loses its payload source at the cutover."""
    from nso_adapter.core.cutover import CutoverBlocked, refuse_cutover_while_carriers_are_parked

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

    # The removal settles and consumes the carrier: nothing is parked any more.
    from nso_adapter.store.models import StaticRouteTombstone

    async with session() as db:
        await db.delete(await db.get(StaticRouteTombstone, tomb))
        await db.commit()
    assert await _parked(device_id) == []
    async with session() as db:
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
