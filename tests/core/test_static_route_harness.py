# SPDX-License-Identifier: Apache-2.0
"""The retained-route fixture uses real admission and the success barrier."""

import pytest

from tests.core.static_route_harness import K, S, fixture_r, retention_harness, route
from tests.core.test_static_route_removal import tombstone_ids

pytestmark = pytest.mark.anyio


async def test_rejected_removal_blocks_successor_until_api_abandonment(adapter_client):
    harness, tomb = await fixture_r(adapter_client)
    await harness.drain()
    assert harness.fake.sent_keys() == {K, S}
    assert await tombstone_ids(harness.device_id) == [tomb]


@pytest.mark.parametrize("outcome", ["reject", "lost_before_commit", "lost_after_commit"])
async def test_put_failure_preserves_or_commits_service_as_observed(adapter_client, outcome):
    harness = await retention_harness(adapter_client)
    await harness.push([route()])
    harness.fake.put_outcome = outcome
    await harness.run(status="failed")
    assert harness.fake.sent_keys() == {K}
    assert harness.fake.service_keys == ({K} if outcome == "lost_after_commit" else set())
