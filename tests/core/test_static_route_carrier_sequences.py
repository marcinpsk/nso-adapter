# SPDX-License-Identifier: Apache-2.0
"""Two removal carriers preserve the latest authorized route value."""

import pytest

from nso_adapter.core.tombstone_sweep import sweep_one_device
from tests.core.static_route_harness import K, S, fixture_r, route
from tests.core.test_generation_protocol import generations
from tests.core.test_static_route_removal import tombstone_ids

pytestmark = pytest.mark.anyio


async def two_carriers(api):
    harness, first = await fixture_r(api)
    await harness.drain()
    await harness.push([route(metric=20, tag=202), route(S, route_id=2)])
    await harness.run()
    (second,) = await harness.park([route(S, route_id=2)])
    await harness.drain()
    assert await tombstone_ids(harness.device_id) == [first, second]
    assert harness.fake.sent_keys() == {K, S}
    retained = [r for r in harness.fake.sent_routes() if r["prefix"] == K[1]]
    assert len(retained) == 1
    assert retained[0]["metric"] == 20
    assert retained[0]["tag"] == 202
    return harness, first, second


async def test_two_carriers_are_superseded_in_sweeper_order_without_put(adapter_client):
    harness, first, second = await two_carriers(adapter_client)
    await harness.push([route(metric=20, tag=202), route(S, route_id=2)])
    await harness.run()
    assert await sweep_one_device(harness.device_id) == 2
    reissues = (await generations(harness.device_id))[-2:]
    selected = [g.document["static_route"]["_execution"]["operation"]["removal"]["tombstone_ids"] for g in reissues]
    assert selected == [[first], [second]]
    writes = len(harness.fake.writes)
    assert (await harness.run()).id == reissues[0].job_id
    assert await tombstone_ids(harness.device_id) == [second]
    assert (await harness.run()).id == reissues[1].job_id
    assert len(harness.fake.writes) == writes
    assert await tombstone_ids(harness.device_id) == []
    await harness.drain()
    await harness.unrelated()
    await harness.run()
    assert harness.fake.sent_keys() == {K, S}
    entries = [r for r in harness.fake.sent_routes() if r["prefix"] == K[1]]
    assert len(entries) == 1
    assert (entries[0]["metric"], entries[0]["tag"]) == (20, 202)


async def test_second_carrier_cleanup_omits_key_while_first_carrier_survives(adapter_client):
    harness, first = await fixture_r(adapter_client)
    await harness.drain()
    await harness.push([route(metric=20, tag=202), route(S, route_id=2)])
    await harness.run()
    await harness.push([route(S, route_id=2)], removed=((1, K),))
    first_id, second = await tombstone_ids(harness.device_id)
    assert first_id == first and second > first
    reads = len(harness.fake.reads)
    await harness.run()
    assert harness.fake.sent_keys() == {S}
    proofs = harness.fake.reads[reads:]
    assert proofs[-1]["state"].entry["static-route"]["route"] == harness.fake.sent_routes()
    assert await tombstone_ids(harness.device_id) == [first]
    await harness.drain()
    await harness.unrelated()
    await harness.run()
    assert harness.fake.sent_keys() == {S}
    assert await tombstone_ids(harness.device_id) == [first]


async def test_store_only_metric_change_cannot_rewrite_retained_bytes(adapter_client):
    from tests.core.test_static_route_retention import retained_keys

    harness, first, second = await two_carriers(adapter_client)
    before = harness.fake.sent_routes()
    await harness.push([route(metric=999, tag=999), route(S, route_id=2)], store_only=True)
    generation = await harness.unrelated()
    assert retained_keys(generation.document) == {K}
    await harness.run()
    assert harness.fake.sent_routes() == before
    assert await tombstone_ids(harness.device_id) == [first, second]
