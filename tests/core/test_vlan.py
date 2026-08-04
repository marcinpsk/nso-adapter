# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""VLAN database + switchport refresh tests (envelope-flipped, READSEM S3 B4)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.vlan import (
    refresh_switchport_for_device,
    refresh_vlan_database_for_device,
)
from nso_adapter.nso.client import NsoExportUnavailableError
from nso_adapter.store.models import Device, DeviceSwitchport, DeviceVlan
from tests.conftest import seed_device, session


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


def _serve_sections(nso: AsyncMock) -> dict:
    """Route ``get_device_state_section`` per wire family from a mutable dict.

    Values: a section dict (served as-is), ``None`` (confirmed device absence), or an
    Exception instance (raised) — one AsyncMock serves BOTH flipped families, and a test
    mutates the dict between refreshes.
    """
    sections: dict[str, object] = {}

    async def _get(device_name, wire_family):
        value = sections[wire_family]
        if isinstance(value, Exception):
            raise value
        return value

    nso.get_device_state_section.side_effect = _get
    return sections


@pytest.mark.anyio
async def test_refresh_vlan_database_upserts_and_prunes(adapter_client):
    device_id = await seed_device(nso_device_name="vsw", netbox_device_id=1300)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {
            "status": "ok",
            "vlan": [{"vlan-id": 10, "name": "MGMT"}, {"vlan-id": 20, "name": "DATA"}],
        }
        await refresh_vlan_database_for_device(db, device, nso)
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert {(r.vlan_id, r.name) for r in rows} == {(10, "MGMT"), (20, "DATA")}

        # second refresh drops 20, keeps 10
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "MGMT"}]}
        await refresh_vlan_database_for_device(db, device, nso)
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert {r.vlan_id for r in rows} == {10}


@pytest.mark.anyio
async def test_refresh_switchport_links_vlans(adapter_client):
    device_id = await seed_device(nso_device_name="vsw2", netbox_device_id=1301)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {
            "status": "ok",
            "vlan": [{"vlan-id": 10, "name": "A"}, {"vlan-id": 20, "name": "B"}, {"vlan-id": 99, "name": "N"}],
        }
        await refresh_vlan_database_for_device(db, device, nso)
        sections["switchport"] = {
            "status": "ok",
            "interface": [{"interface-name": "Gi0/1", "mode": "trunk", "untagged-vlan": 99, "tagged-vlans": "10,20"}],
        }
        await refresh_switchport_for_device(db, device, nso)

        sp = (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().one()
        assert sp.mode == "trunk"
        uv = await db.get(DeviceVlan, sp.untagged_vlan_id)
        assert uv.vlan_id == 99


@pytest.mark.anyio
async def test_refresh_vlan_database_authoritative_empty_prunes_all(adapter_client):
    """An authoritatively-empty read (status=ok, no vlan list — RESTCONF omits empties) prunes
    every VLAN row for this pop family. (Device-absence, section None, now KEEPS — READSEM S5.)"""
    device_id = await seed_device(nso_device_name="vsw-clr", netbox_device_id=1302)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "MGMT"}]}
        await refresh_vlan_database_for_device(db, device, nso)
        assert (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()

        sections["vlan-database"] = {"status": "ok"}  # authoritative empty → clear
        ok = await refresh_vlan_database_for_device(db, device, nso)
        assert ok is True
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert rows == []


@pytest.mark.anyio
async def test_refresh_vlan_database_keep_on_export_down(adapter_client):
    """A confirmed export outage keeps the last-known VLAN rows and reports degraded (False)."""
    device_id = await seed_device(nso_device_name="vsw-keep", netbox_device_id=1303)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "MGMT"}]}
        await refresh_vlan_database_for_device(db, device, nso)

        sections["vlan-database"] = NsoExportUnavailableError("export down")
        ok = await refresh_vlan_database_for_device(db, device, nso)
        assert ok is False
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert {r.vlan_id for r in rows} == {10}  # kept


@pytest.mark.anyio
async def test_refresh_switchport_authoritative_empty_prunes_all(adapter_client):
    """An authoritatively-empty read (status=ok, no interface list) prunes every switchport row.
    (Device-absence, section None, now KEEPS — READSEM S5.)"""
    device_id = await seed_device(nso_device_name="vsw-sp-clr", netbox_device_id=1304)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "A"}]}
        await refresh_vlan_database_for_device(db, device, nso)
        sections["switchport"] = {"status": "ok", "interface": [{"interface-name": "Gi0/1", "mode": "access"}]}
        await refresh_switchport_for_device(db, device, nso)
        assert (
            (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().all()
        )

        sections["switchport"] = {"status": "ok"}  # authoritative empty → clear
        ok = await refresh_switchport_for_device(db, device, nso)
        assert ok is True
        rows = (
            (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().all()
        )
        assert rows == []


@pytest.mark.anyio
async def test_refresh_switchport_keep_on_read_error(adapter_client):
    """A read error keeps the last-known switchport rows and reports degraded (False)."""
    device_id = await seed_device(nso_device_name="vsw-sp-keep", netbox_device_id=1305)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "A"}]}
        await refresh_vlan_database_for_device(db, device, nso)
        sections["switchport"] = {"status": "ok", "interface": [{"interface-name": "Gi0/1", "mode": "access"}]}
        await refresh_switchport_for_device(db, device, nso)

        sections["switchport"] = RuntimeError("timeout")
        ok = await refresh_switchport_for_device(db, device, nso)
        assert ok is False
        rows = (
            (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().all()
        )
        assert [r.interface_name for r in rows] == ["Gi0/1"]  # kept
