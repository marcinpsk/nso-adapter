# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/ospf._upsert_ospf_data — full-replace identity robustness.

Against the REAL in-memory DB + real ORM rows so the UniqueConstraint identity
actually runs; only the NSO boundary is out of scope here (we call the upsert
directly with oper-data dicts).
"""

from __future__ import annotations

from sqlalchemy import select

from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceOspfInstance, DeviceOspfInterface


async def _seed_device(nso_device_name: str = "rtr", netbox_device_id: int = 500) -> int:
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_device_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id
    raise RuntimeError("no session")


async def test_ospf_interface_in_two_processes_does_not_abort_refresh(adapter_client):
    """One interface enabled under two OSPF processes must yield two rows, not an
    IntegrityError that rolls back the whole full-replace (s3-1)."""
    from nso_adapter.core.ospf import _upsert_ospf_data

    device_id = await _seed_device(nso_device_name="ospf-2proc")
    instances = [{"process-id": "1"}, {"process-id": "2"}]
    interfaces = [
        {"interface-name": "GigabitEthernet0/0", "process-id": "1", "area-id": "0"},
        {"interface-name": "GigabitEthernet0/0", "process-id": "2", "area-id": "0"},
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_ospf_data(db, device, instances, interfaces, "test")
        rows = (
            (await db.execute(select(DeviceOspfInterface).where(DeviceOspfInterface.device_id == device_id)))
            .scalars()
            .all()
        )
        assert sorted(r.process_id for r in rows) == ["1", "2"]
        break


async def test_ospf_two_instances_same_process_across_vrfs(adapter_client):
    """Two OSPF instances sharing a process-id in different VRFs must both persist,
    not collide on (device_id, process_id) (s3-20)."""
    from nso_adapter.core.ospf import _upsert_ospf_data

    device_id = await _seed_device(nso_device_name="ospf-2vrf", netbox_device_id=501)
    instances = [
        {"process-id": "1", "vrf": ""},
        {"process-id": "1", "vrf": "BLUE"},
    ]
    async for db in get_session():
        device = await db.get(Device, device_id)
        await _upsert_ospf_data(db, device, instances, [], "test")
        rows = (
            (await db.execute(select(DeviceOspfInstance).where(DeviceOspfInstance.device_id == device_id)))
            .scalars()
            .all()
        )
        assert sorted((r.process_id, r.vrf) for r in rows) == [("1", ""), ("1", "BLUE")]
        break
