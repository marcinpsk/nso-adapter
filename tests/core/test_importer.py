# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for sync_device and check_compliance using NSO package oper-data."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from nso_adapter.core.importer import _attrs_to_interface_list, sync_device
from nso_adapter.store.models import Base, DbInterface, Device, ManagedScope, MappingStatus


@pytest_asyncio.fixture
async def db_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        yield session
    await engine.dispose()


def _make_nso_client(iface_entry=None):
    """Build a mock NsoClient. iface_entry is the dict | None returned by get_interface_attributes."""
    client = AsyncMock()
    client.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")
    client.get_interface_attributes = AsyncMock(return_value=iface_entry)
    return client


# ── _attrs_to_interface_list unit tests ──────────────────────────────────────


def test_attrs_to_interface_list_description_and_enabled():
    entry = {
        "device-name": "sw01",
        "interface": [
            {"interface-name": "GigabitEthernet0/1", "description": "uplink", "enabled": True},
            {"interface-name": "GigabitEthernet0/2", "enabled": False},
        ],
    }
    result = _attrs_to_interface_list(entry)
    assert len(result) == 2
    ge01 = next(i for i in result if i.name == "GigabitEthernet0/1")
    assert ge01.nso.description == "uplink"
    assert ge01.nso.enabled is True
    ge02 = next(i for i in result if i.name == "GigabitEthernet0/2")
    assert ge02.nso.description is None
    assert ge02.nso.enabled is False


def test_attrs_to_interface_list_returns_empty_on_none():
    assert _attrs_to_interface_list(None) == []


def test_attrs_to_interface_list_returns_empty_when_no_interface_key():
    assert _attrs_to_interface_list({"device-name": "sw01"}) == []


def test_attrs_to_interface_list_skips_malformed_entry():
    """Entries without interface-name are skipped; valid entries are returned."""
    entry = {
        "interface": [
            {"interface-name": "GigabitEthernet0/1", "enabled": True},
            {"enabled": True},  # malformed — no interface-name
            {"interface-name": "GigabitEthernet0/2", "enabled": False},
        ]
    }
    result = _attrs_to_interface_list(entry)
    assert len(result) == 2
    assert result[0].name == "GigabitEthernet0/1"
    assert result[1].name == "GigabitEthernet0/2"

def test_attrs_to_interface_list_enabled_absent_yields_none():
    """When NSO package omits 'enabled', the domain object carries None — not True/False."""
    entry = {"interface": [{"interface-name": "GigabitEthernet0/1", "description": "uplink"}]}
    result = _attrs_to_interface_list(entry)
    assert len(result) == 1
    assert result[0].nso.enabled is None





async def test_sync_device_creates_interface_rows(db_session: AsyncSession):
    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw01",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=1,
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    db_session.add(ManagedScope(device=device, attribute="enabled"))
    await db_session.commit()

    iface_entry = {
        "device-name": "sw01",
        "interface": [
            {"interface-name": "GigabitEthernet0/1", "description": "link", "enabled": True},
        ],
    }
    nso_client = _make_nso_client(iface_entry)

    from nso_adapter.core import importer as imp
    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        summary = await sync_device(device.id, db_session)

    assert summary["interfaces_created"] == 1
    result = await db_session.execute(select(DbInterface).where(DbInterface.device_id == device.id))
    ifaces = result.scalars().all()
    assert len(ifaces) == 1
    assert ifaces[0].name == "GigabitEthernet0/1"


async def test_sync_device_calls_get_interface_attributes(db_session: AsyncSession):
    """sync_device() uses get_interface_attributes() instead of get_device_config() + normalize."""
    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw04",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=4,
    )
    db_session.add(device)
    await db_session.commit()

    nso_client = _make_nso_client(None)

    from nso_adapter.core import importer as imp
    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        await sync_device(device.id, db_session)

    nso_client.get_interface_attributes.assert_called_once_with("sw04")
    # get_device_config must NOT be called
    nso_client.get_device_config.assert_not_called()


async def test_sync_device_marks_unmatched_interfaces_when_empty(db_session: AsyncSession):
    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw02",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=2,
    )
    db_session.add(device)
    await db_session.commit()

    nso_client = _make_nso_client(None)

    from nso_adapter.core import importer as imp
    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        await sync_device(device.id, db_session)

    await db_session.refresh(device)
    assert device.mapping_status == MappingStatus.unmatched_interfaces


# ── check_compliance integration test ────────────────────────────────────────


async def test_check_compliance_uses_interface_attributes(db_session: AsyncSession):
    from nso_adapter.core.importer import check_compliance
    from nso_adapter.store.models import ComplianceStatus, InterfaceAttrState

    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw03",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=3,
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    iface_row = DbInterface(device=device, name="GigabitEthernet0/1")
    db_session.add(iface_row)
    await db_session.commit()

    # Pre-existing attr state so compliance can compare
    attr_state = InterfaceAttrState(
        interface_id=iface_row.id,
        attribute="description",
        netbox_value="old-desc",
        nso_value="old-desc",
        compliance_status=ComplianceStatus.imported,
    )
    db_session.add(attr_state)
    await db_session.commit()

    iface_entry = {
        "device-name": "sw03",
        "interface": [
            {"interface-name": "GigabitEthernet0/1", "description": "new-desc", "enabled": True},
        ],
    }
    nso_client = _make_nso_client(iface_entry)
    from nso_adapter.core import importer as imp
    imp._nso_clients["nso-dev"] = nso_client

    with patch("nso_adapter.core.importer.nso_actions.compare_config", new_callable=AsyncMock):
        summary = await check_compliance(device.id, db_session)

    nso_client.get_interface_attributes.assert_called_once_with("sw03")
    assert summary["changes_detected"] == 1
