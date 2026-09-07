# SPDX-License-Identifier: Apache-2.0
"""HTTP authorization and worker execution for retained static routes."""

from dataclasses import dataclass

from tests.api.test_static_route_deleted_routes import deleted
from tests.api.test_static_route_identity import entry
from tests.conftest import seed_device
from tests.core.test_action_apply_promotion import AUTH, _apply, _put_routes, _put_vlans
from tests.core.test_generation_protocol import generations, job_row, recorded_client, run_head, seed_settings
from tests.core.test_static_route_removal import SrFake, tombstone_ids

K = ("", "198.18.0.0/24", "192.0.2.1")
S = ("", "198.18.1.0/24", "192.0.2.2")
L = ("", "198.18.2.0/24", "192.0.2.3")
U = ("", "198.18.3.0/24", "192.0.2.4")


def route(key=K, *, route_id=1, metric=10, tag=101):
    return entry(key, route_id=route_id, metric=metric, tag=tag, next_hop_vrf="retention-vrf")


@dataclass
class RetentionHarness:
    api: object
    device_id: int
    fake: SrFake
    client: object
    seq: int = 0

    async def push(self, routes, *, removed=(), store_only=False):
        self.seq += 1
        before = len(await generations(self.device_id))
        response = await _put_routes(
            self.api,
            self.device_id,
            routes,
            seq=self.seq,
            query=("?store_only=true" if store_only else "")
            + (("&" if store_only else "?") + "delete_origin=true" if removed else ""),
            deleted=[deleted(route_id, [key]) for route_id, key in removed],
        )
        assert response.status_code == 200, response.text
        if not store_only and len(await generations(self.device_id)) == before:
            response = await _apply(self.api, self.device_id, {"static_route": self.seq})
            assert response.status_code == 202, response.text
        return response

    async def unrelated(self):
        self.seq += 1
        response = await _put_vlans(self.api, self.device_id, [100 + self.seq], seq=self.seq, query="?store_only=true")
        assert response.status_code == 200, response.text
        response = await _apply(self.api, self.device_id, {"vlan": self.seq})
        assert response.status_code == 202, response.text
        return (await generations(self.device_id))[-1]

    async def run(self, *, status="succeeded"):
        job_id = await run_head(self.device_id, self.client)
        assert job_id is not None, "the admitted head must be executable"
        job = await job_row(job_id)
        assert job.status.value == status, (job.error, job.result)
        return job

    async def drain(self):
        while (job_id := await run_head(self.device_id, self.client)) is not None:
            job = await job_row(job_id)
            assert job.status.value == "succeeded", (job.error, job.result)

    async def abandon(self, generation):
        response = await self.api.post(
            f"/api/v1/devices/{self.device_id}/actions/abandon-generation",
            json={"generation_id": generation.id},
            headers=AUTH,
        )
        assert response.status_code == 202, response.text

    async def park(self, routes, *, removed=((1, K),)):
        before = set(await tombstone_ids(self.device_id))
        await self.push(routes, removed=removed)
        removal = (await generations(self.device_id))[-1]
        self.fake.put_outcome = "reject"
        previous_service = self.fake.state().entry
        await self.run(status="failed")
        assert self.fake.state().entry == previous_service
        carriers = set(await tombstone_ids(self.device_id)) - before
        assert len(carriers) == len(removed)
        await self.unrelated()
        writes = len(self.fake.writes)
        assert await run_head(self.device_id, self.client) is None
        assert len(self.fake.writes) == writes
        await self.abandon(removal)
        self.fake.put_outcome = "success"
        return carriers


async def retention_harness(api):
    device_id = await seed_device(nso_device_name="retention-device", netbox_device_id=17401)
    await seed_settings(device_id, auto_apply=False)
    fake = SrFake("retention-device", service=None)
    client, _ = recorded_client("retention-device", sr_fake=fake)
    return RetentionHarness(api, device_id, fake, client)


async def fixture_r(api):
    harness = await retention_harness(api)
    await harness.push([route(), route(S, route_id=2)])
    await harness.run()
    assert harness.fake.sent_keys() == {K, S}
    assert harness.fake.sent_routes()[0]["metric"] == 10
    carriers = await harness.park([route(S, route_id=2)])
    return harness, carriers.pop()
