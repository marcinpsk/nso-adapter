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
