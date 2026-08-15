# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Removal generations precede auto-apply generations from one intent request."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

CASES = (
    (
        "bgp",
        {
            "routers": [
                {"asn": "65100", "scopes": []},
                {"asn": "65101", "scopes": []},
            ]
        },
        {"routers": [{"asn": "65100", "scopes": []}]},
    ),
    (
        "isis-interface",
        {
            "interfaces": [
                {"interface_name": "Gi0/1", "af": "ipv4"},
                {"interface_name": "Gi0/2", "af": "ipv4"},
            ],
            "processes": [],
        },
        {"interfaces": [{"interface_name": "Gi0/1", "af": "ipv4"}], "processes": []},
    ),
    (
        "isis-flex-algo",
        {
            "flex_algos": [
                {"process_tag": "1", "algo_id": 128},
                {"process_tag": "1", "algo_id": 129},
            ]
        },
        {"flex_algos": [{"process_tag": "1", "algo_id": 128}]},
    ),
    (
        "ospf",
        {
            "instances": [
                {"process_id": "1", "router_id": "198.18.1.1", "areas": []},
                {"process_id": "2", "router_id": "198.18.1.2", "areas": []},
            ],
            "interfaces": [],
        },
        {
            "instances": [{"process_id": "1", "router_id": "198.18.1.1", "areas": []}],
            "interfaces": [],
        },
    ),
    (
        "ip",
        {
            "addresses": [
                {"interface": "Gi0/1", "address": "198.18.2.1/32", "family": "ipv4"},
                {"interface": "Gi0/1", "address": "198.18.2.2/32", "family": "ipv4"},
            ]
        },
        {"addresses": [{"interface": "Gi0/1", "address": "198.18.2.1/32", "family": "ipv4"}]},
    ),
    (
        "snmp",
        {
            "communities": [
                {"label": "one", "vault_ref": "network/snmp#one", "access": "RO"},
                {"label": "two", "vault_ref": "network/snmp#two", "access": "RO"},
            ]
        },
        {"communities": [{"label": "one", "vault_ref": "network/snmp#one", "access": "RO"}]},
    ),
)


async def _enable_auto_apply(device_id: int, *, seed_interface: bool) -> None:
    from nso_adapter.store.models import DbInterface, DeviceSettings

    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        if seed_interface:
            db.add(DbInterface(device_id=device_id, name="Gi0/1"))
        await db.commit()


async def _generation_jobs(device_id: int) -> list[tuple[str, str]]:
    from nso_adapter.store.models import DeploymentGeneration, Job

    async with session() as db:
        rows = (
            await db.execute(
                sa.select(DeploymentGeneration.mode, Job.job_type)
                .join(Job, Job.id == DeploymentGeneration.job_id)
                .where(DeploymentGeneration.device_id == device_id)
                .order_by(DeploymentGeneration.seq)
            )
        ).all()
        return [(mode.value, job_type.value) for mode, job_type in rows]


@pytest.mark.parametrize(("endpoint", "before", "after"), CASES)
async def test_removal_generation_precedes_auto_apply(adapter_client, endpoint, before, after):
    device_id = await seed_device(nso_device_name=f"order-{endpoint}", netbox_device_id=None)
    await _enable_auto_apply(device_id, seed_interface=endpoint == "ip")
    url = f"/api/v1/devices/{device_id}/{endpoint}-intent"

    seeded = await adapter_client.put(f"{url}?store_only=true", json=before, headers=AUTH)
    assert seeded.status_code == 200, seeded.text
    changed = await adapter_client.put(url, json=after, headers=AUTH)
    assert changed.status_code == 200, changed.text

    assert await _generation_jobs(device_id) == [("detach", "removal"), ("networked", "apply")]
