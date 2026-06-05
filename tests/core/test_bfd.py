# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/bfd._upsert_bfd_data — per-interface BFD mirror."""

from __future__ import annotations

from contextlib import asynccontextmanager

from sqlalchemy import select

from nso_adapter.core.bfd import _upsert_bfd_data
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceBfdInterface
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


async def test_bfd_interfaces_mirrored(adapter_client):
    """Timers + micro-BFD + bound-port are stored; micro vs normal preserved."""
    device_id = await seed_device(nso_device_name="bfd-dev", netbox_device_id=970)
    async with _device_session(device_id) as (db, device):
        interfaces = [
            {"interface-name": "ae10", "min-tx": 300, "min-rx": 300, "multiplier": 3, "micro-bfd": True, "enabled": True},
            {
                "interface-name": "lag-99",
                "bound-port": "lag-99",
                "min-tx": 100,
                "min-rx": 100,
                "multiplier": 3,
                "micro-bfd": False,
                "enabled": True,
            },
        ]
        await _upsert_bfd_data(db, device, interfaces, "test")
        rows = {
            r.interface_name: r
            for r in (await db.execute(select(DeviceBfdInterface).where(DeviceBfdInterface.device_id == device.id)))
            .scalars()
            .all()
        }
        assert rows["ae10"].micro_bfd is True
        assert (rows["ae10"].min_tx, rows["ae10"].min_rx, rows["ae10"].multiplier) == (300, 300, 3)
        assert rows["lag-99"].micro_bfd is False
        assert rows["lag-99"].bound_port == "lag-99"


async def test_bfd_full_replace(adapter_client):
    """A second refresh replaces the prior rows (no stale accumulation)."""
    device_id = await seed_device(nso_device_name="bfd-dev2", netbox_device_id=971)
    async with _device_session(device_id) as (db, device):
        await _upsert_bfd_data(db, device, [{"interface-name": "ae1", "micro-bfd": True}], "test")
        await _upsert_bfd_data(db, device, [{"interface-name": "ae2", "micro-bfd": True}], "test")
        names = sorted(
            r.interface_name
            for r in (await db.execute(select(DeviceBfdInterface).where(DeviceBfdInterface.device_id == device.id)))
            .scalars()
            .all()
        )
        assert names == ["ae2"]
