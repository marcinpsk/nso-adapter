# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""core/subinterface.py — refresh + full-replace."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.subinterface import refresh_subinterface_for_device
from nso_adapter.store.models import Device, DeviceSubinterface
from tests.conftest import seed_device, session


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


async def _rows(db, device_id):
    rows = (
        (await db.execute(select(DeviceSubinterface).where(DeviceSubinterface.device_id == device_id))).scalars().all()
    )
    return {r.interface_name: r for r in rows}


@pytest.mark.anyio
async def test_refresh_inserts_subinterfaces(adapter_client):
    device_id = await seed_device(nso_device_name="subif-rtr01", netbox_device_id=970)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "device-name": "subif-rtr01",
            "interface": [
                {
                    "interface-name": "GigabitEthernet0/1.100",
                    "parent-interface": "GigabitEthernet0/1",
                    "dot1q-vlan": 100,
                    "type": "subinterface",
                    "vrf": "TENANT_A",
                },
                {
                    "interface-name": "ge-0/0/0.200",
                    "parent-interface": "ge-0/0/0",
                    "dot1q-vlan": 200,
                    "type": "subinterface",
                },
            ],
        }
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        rows = await _rows(db, device_id)
        assert set(rows) == {"GigabitEthernet0/1.100", "ge-0/0/0.200"}
        assert rows["GigabitEthernet0/1.100"].dot1q_vlan == 100
        assert rows["GigabitEthernet0/1.100"].parent_interface == "GigabitEthernet0/1"
        assert rows["GigabitEthernet0/1.100"].vrf == "TENANT_A"


@pytest.mark.anyio
async def test_refresh_full_replace(adapter_client):
    device_id = await seed_device(nso_device_name="subif-rtr02", netbox_device_id=971)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "ge-0/0/0.10", "parent-interface": "ge-0/0/0", "dot1q-vlan": 10}],
        }
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "ge-0/0/0.20", "parent-interface": "ge-0/0/0", "dot1q-vlan": 20}],
        }
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        assert set(await _rows(db, device_id)) == {"ge-0/0/0.20"}


@pytest.mark.anyio
async def test_missing_tag_refuses_family_read_and_keeps_previous_rows(adapter_client):
    device_id = await seed_device(nso_device_name="subif-missing-tag", netbox_device_id=973)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "xe-0/0/1.5000", "dot1q-vlan": 100}],
        }
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [
                {"interface-name": "xe-0/0/1.200", "dot1q-vlan": 200},
                {"interface-name": "xe-0/0/1.0", "parent-interface": "xe-0/0/1"},
            ],
        }
        with pytest.raises(ValueError, match="dot1q-vlan"):
            await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        assert {name: row.dot1q_vlan for name, row in (await _rows(db, device_id)).items()} == {"xe-0/0/1.5000": 100}

    response = await adapter_client.get(
        f"/api/v1/devices/{device_id}/subinterface", headers={"Authorization": "Bearer test-bearer-token"}
    )
    assert response.status_code == 200
    assert [(row["interface_name"], row["dot1q_vlan"]) for row in response.json()["interfaces"]] == [
        ("xe-0/0/1.5000", 100)
    ]
    state = response.json()["read_state"]
    assert (state["outcome"], state["result"], state["succeeded"]) == ("present", "error", False)


@pytest.mark.anyio
async def test_refresh_authoritative_empty_clears(adapter_client):
    """An authoritatively-empty read (status=ok, no list keys) clears the rows. (Device-absence, section None, now KEEPS — READSEM S5.)"""
    device_id = await seed_device(nso_device_name="subif-rtr03", netbox_device_id=972)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "interface": [{"interface-name": "ge-0/0/0.10", "parent-interface": "ge-0/0/0", "dot1q-vlan": 10}],
        }
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        nso_client.get_device_state_section.return_value = {"status": "ok"}
        await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
        assert await _rows(db, device_id) == {}


@pytest.mark.anyio
async def test_a_BOOLEAN_dot1q_vlan_is_refused(adapter_client):
    """``int(True)`` stored dot1q VLAN 1 for a subinterface the device tagged with nothing."""
    device_id = await seed_device(nso_device_name="subif-bool", netbox_device_id=974)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "device-name": "subif-bool",
            "interface": [{"interface-name": "Gi0/1.100", "dot1q-vlan": True}],
        }
        with pytest.raises(TypeError):
            await refresh_subinterface_for_device(db, device, nso_client, refresh_source="test")
