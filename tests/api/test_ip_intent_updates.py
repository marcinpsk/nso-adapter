# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""An updated IP claim must reach the stored and deployable intent."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from nso_adapter.core.apply import build_device_containers
from nso_adapter.store.models import DbInterface, DeploymentGeneration, Device, InterfaceIpIntent
from tests.conftest import AUTH, push_seq, seed_device, session
from tests.core.test_generation_protocol import seed_settings


@pytest.mark.anyio
@pytest.mark.parametrize("secondary", [True, False])
async def test_existing_ip_updates_secondary_in_store_and_deployment(adapter_client, secondary):
    device_id = await seed_device(nso_device_name="placeholder-ip-update", netbox_device_id=None)
    await seed_settings(device_id, auto_apply=True)
    entry = {"interface": "GigabitEthernet0/1", "address": "198.18.0.1/24", "family": "ipv4"}
    path = f"/api/v1/devices/{device_id}/ip-intent"
    first = await adapter_client.put(
        path,
        json={"addresses": [{**entry, "secondary": not secondary, "accepted_at": "2026-10-19T12:00:00Z"}]},
        headers=AUTH | push_seq(1),
    )
    assert first.status_code == 200
    async with session() as db:
        identity = await db.scalar(select(InterfaceIpIntent.id))
    second = await adapter_client.put(
        path,
        json={"addresses": [{**entry, "secondary": secondary, "accepted_at": "2026-10-20T12:00:00Z"}]},
        headers=AUTH | push_seq(2),
    )
    assert second.status_code == 200
    async with session() as db:
        row = (await db.execute(select(InterfaceIpIntent).join(DbInterface))).scalar_one()
        assert row.id == identity
        assert row.secondary is secondary
        assert row.family == "ipv4"
        assert row.accepted_at == datetime(2026, 10, 20, 12, tzinfo=UTC)
        generation = await db.scalar(select(DeploymentGeneration).order_by(DeploymentGeneration.seq.desc()))
        body = await build_device_containers(None, await db.get(Device, device_id), generation.document)
    assert not body.errors
    assert body.containers["interface"]["interface"][0]["ipv4-address"] == [
        {"address": "198.18.0.1", "prefix-length": 24, "secondary": secondary}
    ]
