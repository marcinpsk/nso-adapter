# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for PUT /api/v1/devices/{id}/ospf-intent."""

from __future__ import annotations

from sqlalchemy import select

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def test_put_ospf_intent_string_process_id(adapter_client):
    """process_id is a STRING end-to-end — a numeric value must store, not raise.

    Regression: the Pydantic schema declared process_id as int, so asyncpg rejected the
    coerced value against the String column. Only surfaced once OSPF intent was first
    pushed (greenfield Nokia OSPF).
    """
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import OspfInstanceIntent, OspfInterfaceIntent

    device_id = await seed_device(nso_device_name="ospf-intent-dev", netbox_device_id=920)
    payload = {
        "instances": [{"process_id": "1", "router_id": "84.116.250.117", "vrf": "", "areas": []}],
        "interfaces": [{"interface_name": "LAG99:99", "process_id": "1", "area_id": "0", "passive": False}],
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ospf-intent", headers=AUTH, json=payload)
    assert resp.status_code == 200

    async for db in get_session():
        inst = (
            await db.execute(select(OspfInstanceIntent).where(OspfInstanceIntent.device_id == device_id))
        ).scalar_one()
        assert inst.process_id == "1"
        assert inst.router_id == "84.116.250.117"
        iface = (
            await db.execute(select(OspfInterfaceIntent).where(OspfInterfaceIntent.device_id == device_id))
        ).scalar_one()
        assert iface.interface_name == "LAG99:99"
        assert iface.process_id == "1"
        assert iface.area_id == "0"
        break


async def test_put_ospf_intent_admin_state(adapter_client):
    """OSPF process admin-state (enabled) round-trips through put/get + into the apply body."""
    import json
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, patch

    from nso_adapter.nso.apply import apply_ospf_config
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await seed_device(nso_device_name="ospf-admin-dev", netbox_device_id=921)
    payload = {
        "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "enabled": True, "areas": []}],
        "interfaces": [],
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ospf-intent", headers=AUTH, json=payload)
    assert resp.status_code == 200

    async for db in get_session():
        inst = (
            await db.execute(select(OspfInstanceIntent).where(OspfInstanceIntent.device_id == device_id))
        ).scalar_one()
        assert inst.enabled is True
        # The dry-run apply body must carry the admin-state for the reconciler to write it.
        with patch("nso_adapter.nso.apply.native_dry_run", new_callable=AsyncMock, return_value="OK") as mock_dry:
            await apply_ospf_config(
                client=SimpleNamespace(_base="http://nso/restconf/data"),
                device_name="ospf-admin-dev",
                process_intent_rows=[inst],
                interface_intent_rows=[],
                dry_run=True,
            )
        sent = json.loads(mock_dry.call_args.args[2])  # native_dry_run(client, url, payload, ...)
        proc = sent["ospf-reconciler:ospf-config"][0]["process-config"][0]
        assert proc["enabled"] is True
        break


async def test_put_ospf_intent_removal_enqueues_job_not_inline(adapter_client):
    """Dropping an OSPF instance queues an async `removal` job (no inline device commit).

    The PUT must return promptly; the PUT-replace that reverts the dropped process on
    the device runs in the background via the removal job.
    """
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="ospf-removal-dev", netbox_device_id=922)
    # Seed two processes, then PUT a payload that drops one.
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH,
        json={
            "instances": [
                {"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []},
                {"process_id": "2", "router_id": "2.2.2.2", "vrf": "", "areas": []},
            ],
            "interfaces": [],
        },
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ospf-intent",
        headers=AUTH,
        json={
            "instances": [{"process_id": "1", "router_id": "1.1.1.1", "vrf": "", "areas": []}],
            "interfaces": [],
        },
    )
    assert resp.status_code == 200

    async for db in get_session():
        jobs = (
            (await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.removal)))
            .scalars()
            .all()
        )
        assert len(jobs) == 1
        assert jobs[0].status == JobStatus.queued
        assert jobs[0].context == {"scope": "ospf"}
        break
