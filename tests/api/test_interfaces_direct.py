# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Direct unit tests for interfaces.py endpoint functions."""

from __future__ import annotations

from datetime import datetime

import pytest

from nso_adapter.api.errors import ApiError
from nso_adapter.api.interfaces import get_state, list_interfaces
from nso_adapter.store.db import get_session
from nso_adapter.store.models import (
    DbInterface,
    Device,
    InterfaceAttrState,
    InterfaceIntent,
    ManagedScope,
    SyncState,
)


async def _seed_full(nso_device_name: str, netbox_id: int):
    """Seed device + interface + attr state and return (device_id, iface_id)."""
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.flush()
        iface = DbInterface(device_id=d.id, name="GigabitEthernet0/1", netbox_interface_id=200)
        db.add(iface)
        await db.flush()
        attr = InterfaceAttrState(
            interface_id=iface.id,
            attribute="description",
            nso_value="nso-val",
            netbox_value="nb-val",
            sync_state=SyncState.imported,
            last_checked_at=datetime(2025, 1, 1, 12, 0, 0),
        )
        db.add(attr)
        db.add(ManagedScope(device_id=d.id, attribute="description"))
        await db.commit()
        return d.id, iface.id
    raise RuntimeError("no DB session")


# ── list_interfaces ───────────────────────────────────────────────────────────


async def test_list_interfaces_not_found(adapter_client):
    """list_interfaces() raises 404 for unknown device_id."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await list_interfaces(device_id=99994, db=db)
        assert exc_info.value.status_code == 404
        break


async def test_list_interfaces_empty(adapter_client):
    """list_interfaces() returns empty list for device with no interfaces."""
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name="iface-empty-01", netbox_device_id=1100)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        result = await list_interfaces(device_id=d.id, db=db)
        assert result == []
        break


async def test_list_interfaces_with_attrs(adapter_client):
    """list_interfaces() returns interfaces with attribute states."""
    device_id, iface_id = await _seed_full("iface-dev-01", 1110)
    async for db in get_session():
        result = await list_interfaces(device_id=device_id, db=db)
        assert len(result) == 1
        row = result[0]
        assert row["name"] == "GigabitEthernet0/1"
        assert "description" in row["attrs"]
        attr = row["attrs"]["description"]
        assert attr["nso_value"] == "nso-val"
        assert attr["status"] == "imported"
        assert attr["intent_value"] is None
        break


async def test_list_interfaces_with_intent(adapter_client):
    """list_interfaces() includes intent_value when InterfaceIntent row exists."""
    device_id, iface_id = await _seed_full("iface-dev-02", 1120)
    async for db in get_session():
        intent = InterfaceIntent(
            interface_id=iface_id,
            attribute="description",
            intent_value="my-intent",
            accepted_at=datetime(2025, 6, 1, 0, 0, 0),
        )
        db.add(intent)
        await db.commit()

        result = await list_interfaces(device_id=device_id, db=db)
        attr = result[0]["attrs"]["description"]
        assert attr["intent_value"] == "my-intent"
        assert attr["last_apply_at"] is None  # not set
        break


# ── get_state ────────────────────────────────────────────────────────────


async def test_get_state_not_found(adapter_client):
    """get_state() raises 404 for unknown device_id."""
    async for db in get_session():
        with pytest.raises(ApiError) as exc_info:
            await get_state(device_id=99993, db=db)
        assert exc_info.value.status_code == 404
        break


async def test_get_state_empty(adapter_client):
    """get_state() returns zero counts for device with no interfaces."""
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name="comp-empty-01", netbox_device_id=1130)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        result = await get_state(device_id=d.id, db=db)
        assert result["managed_interfaces"] == 0
        assert result["last_checked_at"] is None
        break


async def test_get_state_with_attrs(adapter_client):
    """get_state() aggregates by_status counts and last_checked_at."""
    device_id, _ = await _seed_full("comp-dev-01", 1140)
    async for db in get_session():
        result = await get_state(device_id=device_id, db=db)
        assert result["device_id"] == device_id
        assert result["managed_interfaces"] == 1
        assert result["by_status"]["imported"] == 1
        assert result["last_checked_at"] is not None
        break


async def _seed_many_interfaces(nso_device_name: str, netbox_id: int, count: int) -> int:
    """Seed a device with *count* interfaces, each with one attr state + one intent row."""
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=nso_device_name, netbox_device_id=netbox_id)
        db.add(d)
        await db.flush()
        db.add(ManagedScope(device_id=d.id, attribute="description"))
        for j in range(count):
            iface = DbInterface(device_id=d.id, name=f"Gi0/{j}", netbox_interface_id=netbox_id * 100 + j)
            db.add(iface)
            await db.flush()
            db.add(
                InterfaceAttrState(
                    interface_id=iface.id,
                    attribute="description",
                    nso_value="nso",
                    netbox_value="nb",
                    sync_state=SyncState.imported,
                )
            )
            db.add(InterfaceIntent(interface_id=iface.id, attribute="description", intent_value="x"))
        await db.commit()
        return d.id
    raise RuntimeError("no DB session")


async def test_list_interfaces_is_not_n_plus_one(adapter_client):
    """s3-10: list_interfaces must not run 2 queries per interface (attr + intent)."""
    from tests.conftest import count_queries

    device_id = await _seed_many_interfaces("iface-nplus", 1150, count=6)
    async for db in get_session():
        with count_queries() as qc:
            result = await list_interfaces(device_id=device_id, db=db)
        assert len(result) == 6
        assert all(r["attrs"]["description"]["intent_value"] == "x" for r in result)
        # was 1 (device) + 1 (interfaces) + 6×2; batched stays a small constant.
        assert qc.count <= 5, f"list_interfaces ran {qc.count} queries — N+1 across interfaces"
        break


async def test_get_state_is_not_n_plus_one(adapter_client):
    """s3-10: get_state must not run one attr query per interface."""
    from tests.conftest import count_queries

    device_id = await _seed_many_interfaces("state-nplus", 1160, count=6)
    async for db in get_session():
        with count_queries() as qc:
            result = await get_state(device_id=device_id, db=db)
        assert result["managed_interfaces"] == 6
        assert result["by_status"]["imported"] == 6
        assert qc.count <= 5, f"get_state ran {qc.count} queries — N+1 across interfaces"
        break
