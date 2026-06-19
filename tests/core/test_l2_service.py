# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for L2-service refresh + SSE handler."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.l2_service import refresh_l2_services_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceL2Sap
from tests.conftest import seed_device

_NSO_ENTRY = {
    "device-name": "l2-ra1",
    "service": [
        {
            "service-name": "TL",
            "service-type": "epipe",
            "service-id": 4022,
            "sap": [
                {"sap-id": "lag-60:3999", "port": "lag-60", "outer-tag": 3999},
                {"sap-id": "lag-60:4022", "port": "lag-60", "outer-tag": 4022},
            ],
        },
        {
            "service-name": "701",
            "service-type": "vpls",
            "sap": [{"sap-id": "1/1/c28/1:100.10", "port": "1/1/c28/1", "outer-tag": 100, "inner-tag": 10}],
        },
    ],
}


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


@pytest.mark.anyio
async def test_refresh_inserts_flat_sap_rows(adapter_client):
    device_id = await seed_device(nso_device_name="l2-insert", netbox_device_id=970)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_l2_services.return_value = _NSO_ENTRY

        await refresh_l2_services_for_device(db, device, nso_client, refresh_source="poll")

        nso_client.get_l2_services.assert_awaited_once_with("l2-insert")
        rows = (await db.execute(select(DeviceL2Sap).where(DeviceL2Sap.device_id == device.id))).scalars().all()
        assert len(rows) == 3
        qinq = next(r for r in rows if r.sap_id == "1/1/c28/1:100.10")
        assert (qinq.service_name, qinq.service_type, qinq.outer_tag, qinq.inner_tag) == ("701", "vpls", 100, 10)
        tl = next(r for r in rows if r.sap_id == "lag-60:3999")
        assert (tl.service_type, tl.service_id, tl.outer_tag) == ("epipe", 4022, 3999)


@pytest.mark.anyio
async def test_refresh_replaces_existing_rows(adapter_client):
    device_id = await seed_device(nso_device_name="l2-replace", netbox_device_id=971)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_l2_services.return_value = _NSO_ENTRY
        await refresh_l2_services_for_device(db, device, nso_client)

        # Second refresh with fewer services → full-replace prunes the rest.
        nso_client.get_l2_services.return_value = {
            "device-name": "l2-replace",
            "service": [
                {
                    "service-name": "701",
                    "service-type": "vpls",
                    "sap": [{"sap-id": "1/1/c31/3:701", "port": "1/1/c31/3", "outer-tag": 701}],
                }
            ],
        }
        await refresh_l2_services_for_device(db, device, nso_client)

        rows = (await db.execute(select(DeviceL2Sap).where(DeviceL2Sap.device_id == device.id))).scalars().all()
        assert len(rows) == 1
        assert rows[0].sap_id == "1/1/c31/3:701"


@pytest.mark.anyio
async def test_refresh_no_data_clears_rows(adapter_client):
    device_id = await seed_device(nso_device_name="l2-none", netbox_device_id=972)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_l2_services.return_value = None
        await refresh_l2_services_for_device(db, device, nso_client)
        rows = (await db.execute(select(DeviceL2Sap).where(DeviceL2Sap.device_id == device.id))).scalars().all()
        assert rows == []
