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


async def _enable_auto_apply(device_id: int) -> None:
    import sqlalchemy as sa

    from nso_adapter.store.models import DeviceSettings
    from tests.conftest import session

    async with session() as db:
        await db.execute(sa.update(DeviceSettings).where(DeviceSettings.device_id == device_id).values(auto_apply=True))
        await db.commit()


async def test_a_deployed_only_claim_is_cleaned_up_rather_than_superseded(adapter_client):
    """The F2 sequence: a carrier whose key only ever gets DEPLOYED is not superseded.

    Supersession needs a rendered key. After a K-to-L replacement the row renders L and only
    remembers K, so the sweeper's reissue has to do the real cleanup: transmit the omission,
    certify the section clean and only then consume. The successor frozen while the carrier
    was still live then carries no trace of K either.
    """
    from tests.core.static_route_harness import L
    from tests.core.test_action_apply_promotion import _put_vlans
    from tests.core.test_static_route_removal import key_of

    harness, tomb = await fixture_r(adapter_client)
    await harness.drain()

    # Reauthorize K and execute it, so the proof records it as deployed.
    await harness.push([route(), route(S, route_id=2)])
    await harness.run()
    assert harness.fake.sent_keys() == {K, S}
    await harness.drain()

    # The replacement: the same route_id now renders L and remembers K. Marked delete-origin,
    # so the predecessor is a retraction and not an un-own, which commits no-networking and
    # would strand K on the device. Auto-applied, because an operator Apply cannot select a
    # stream whose own push already queued the chain.
    await _enable_auto_apply(harness.device_id)
    await harness.push([route(L, route_id=1), route(S, route_id=2)], delete_origin=True)
    await harness.drain()
    assert harness.fake.sent_keys() == {K, L, S}, "L is rendered and K is still retained by its carrier"
    assert await tombstone_ids(harness.device_id) == [tomb]

    # The sweeper first, so the cleanup takes the lower sequence, and the successor is frozen
    # while the carrier is still live.
    assert await sweep_one_device(harness.device_id) == 1
    cleanup = (await generations(harness.device_id))[-1]
    # An operator Apply is refused while a job is queued, so the successor is frozen the one
    # way that stays open: an auto-applied push, admitted behind the cleanup.
    harness.seq += 1
    assert (await _put_vlans(harness.api, harness.device_id, [777], seq=harness.seq)).status_code == 200
    successor = (await generations(harness.device_id))[-1]
    assert successor.seq > cleanup.seq
    assert await tombstone_ids(harness.device_id) == [tomb], "the successor must be frozen while T is live"

    removal = cleanup.document["static_route"]["_execution"]["operation"]["removal"]
    assert [tuple(key) for key in removal["authorized_removal_keys"]] == [K]
    assert removal["tombstone_ids"] == [tomb]

    job = await harness.run()
    assert job.id == cleanup.job_id
    assert job.result["removal_branch"] == "networked", "a deployed-only claim was consumed as superseded"
    assert job.result.get("service_clean") is not False, "consumption requires a CERTIFIED clean section"
    assert harness.fake.sent_keys() == {L, S}
    assert K not in harness.fake.service_keys
    assert await tombstone_ids(harness.device_id) == []

    await harness.drain()  # the successor, and whatever the settlement queued behind it
    assert harness.fake.sent_keys() == {L, S}
    assert all(key_of(entry) != K for entry in harness.fake.sent_routes())
    assert (await generations(harness.device_id))[-1].id == successor.id
