# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Every networked document must enforce its carried replacement proof."""

import httpx
import pytest

from nso_adapter.core.generation import generation_execution_sections
from nso_adapter.store.models import GenerationMode, JobStatus
from tests.api.test_static_route_identity import entry as route_entry
from tests.conftest import seed_device, session
from tests.core.test_action_apply_promotion import _A, _B, AUTH, _apply, _generations, _put_routes, _put_vlans
from tests.core.test_generation_protocol import job_row, run_head, seed_settings
from tests.core.test_static_route_removal import SrFake
from tests.nso.test_apply_send import _client_with

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("removal", [False, True], ids=["apply", "removal"])
async def test_carried_replacement_refuses_unverified_mixed_stream_commit(adapter_client, monkeypatch, removal):
    device_name = "carried-replacement"
    device_id = await seed_device(nso_device_name=device_name)
    await seed_settings(device_id, auto_apply=False)
    fake = SrFake(device_name, service=None)

    async def respond(request):
        if request.url.path.endswith("device-state-read/run"):
            return httpx.Response(
                200,
                json={
                    "network-state-export:output": {
                        "atomic": True,
                        "device-name": device_name,
                        "static-route": fake.section(),
                        "vlan-database": {"status": "unsupported"},
                    }
                },
            )
        return await fake.handle(request.method.lower(), str(request.url), request.content or None, request.headers)

    client = _client_with(httpx.MockTransport(respond))
    assert (await _put_routes(adapter_client, device_id, [route_entry(_A, route_id=1)], seq=1)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"static_route": 1})).status_code == 202
    assert (await job_row(await run_head(device_id, client))).status == JobStatus.succeeded
    if removal:
        assert (await _put_vlans(adapter_client, device_id, [10], seq=1)).status_code == 200
        assert (await _apply(adapter_client, device_id, {"vlan": 1})).status_code == 202
        assert (await job_row(await run_head(device_id, client))).status == JobStatus.succeeded

    assert (await _put_routes(adapter_client, device_id, [route_entry(_B, route_id=1)], seq=2)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"static_route": 2})).status_code == 202
    fake.put_outcome = "reject"
    assert (await job_row(await run_head(device_id, client))).status == JobStatus.failed
    rejected = (await _generations(device_id))[-1]
    response = await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/abandon-generation",
        json={"generation_id": rejected.id},
        headers=AUTH,
    )
    assert response.status_code == 202, response.text
    fake.put_outcome = "success"

    assert (await _put_routes(adapter_client, device_id, [], seq=3, query="?store_only=true")).status_code == 200
    if removal:
        response = await adapter_client.put(
            f"/api/v1/devices/{device_id}/vlan-intent?store_only=true&delete_origin=true",
            json={"vlans": []},
            headers=AUTH | {"X-Push-Seq": "2"},
        )
    else:
        response = await _put_vlans(adapter_client, device_id, [20], seq=2, query="?store_only=true")
    assert response.status_code == 200, response.text
    response = await _apply(adapter_client, device_id, {"static_route": 3, "vlan": 2})
    assert response.status_code == 202, response.text
    intermediate = (await _generations(device_id))[-2]
    assert intermediate.mode == GenerationMode.networked
    route = intermediate.document["static_route"]["static_route_intent"][0]
    assert route["prefix"] == _B[1]
    assert route["deployed_key"] == list(_A)
    assert intermediate.document["static_route"]["_execution"]["proof"]["apply"]["mode"] == "PUT"
    async with session() as db:
        assert "static_route" not in await generation_execution_sections(db, intermediate.job_id)

    monkeypatch.setattr("nso_adapter.nso.apply.VERIFY_AFTER_APPLY", False)
    fake.calls.clear()
    assert await run_head(device_id, client) == intermediate.job_id
    assert fake.writes == [], "the carried replacement reached NSO without verification"
    job = await job_row(intermediate.job_id)
    assert job.status == JobStatus.failed
    assert job.error["code"] == "static_route_put_verify_disabled"
