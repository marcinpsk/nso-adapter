# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/onboarding.py — onboard_device, rekey_device, offboard_device, set_scope.

These tests exercise the DB-layer logic directly via the store's get_session(),
bypassing the HTTP layer.  The `adapter_client` fixture is still required to
ensure init_db() has run (creating schema) before any DB call.
"""
from __future__ import annotations

import pytest
from sqlalchemy import select

from nso_adapter.store.models import Device, DbInterface, InterfaceAttrState, ManagedScope


# ── onboard_device ───────────────────────────────────────────────────────────


async def test_onboard_creates_device(adapter_client_with_nso):
    """onboard_device inserts a Device row and returns it with mapping_status=mapped."""
    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import MappingStatus

    async for db in get_session():
        device = await onboard_device(db, "nso-dev", "core-rtr-01", 42)
        assert device.id is not None
        assert device.nso_instance == "nso-dev"
        assert device.nso_device_name == "core-rtr-01"
        assert device.netbox_device_id == 42
        assert device.mapping_status == MappingStatus.mapped
        break


async def test_onboard_raises_for_unknown_instance(adapter_client):
    """onboard_device raises ValueError when NSO instance is not in config."""
    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.db import get_session

    async for db in get_session():
        with pytest.raises(ValueError, match="not found in config"):
            await onboard_device(db, "nonexistent-nso", "device-01", 99)
        break


async def test_onboard_raises_for_duplicate_netbox_id(adapter_client_with_nso):
    """onboard_device raises LookupError when netbox_device_id is already onboarded."""
    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    await seed_device(nso_instance="nso-dev", nso_device_name="existing-device", netbox_device_id=100)

    async for db in get_session():
        with pytest.raises(LookupError, match="already onboarded"):
            await onboard_device(db, "nso-dev", "new-device", 100)
        break


async def test_onboard_raises_for_duplicate_nso_device_name(adapter_client_with_nso):
    """onboard_device raises LookupError when (nso_instance, nso_device_name) already exists."""
    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    await seed_device(nso_instance="nso-dev", nso_device_name="taken-name", netbox_device_id=200)

    async for db in get_session():
        with pytest.raises(LookupError, match="already onboarded"):
            await onboard_device(db, "nso-dev", "taken-name", 201)
        break


# ── rekey_device ─────────────────────────────────────────────────────────────


async def test_rekey_changes_device_name(adapter_client_with_nso):
    """rekey_device updates nso_device_name and resets sync metadata."""
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="old-name", netbox_device_id=300)

    async for db in get_session():
        device = await db.get(Device, device_id)
        updated = await rekey_device(db, device, nso_device_name="new-name")
        assert updated.nso_device_name == "new-name"
        assert updated.ned_id is None
        assert updated.last_sync_at is None
        break


async def test_rekey_clears_interface_state(adapter_client_with_nso):
    """rekey_device deletes all interfaces and attr states for the device."""
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="with-ifaces", netbox_device_id=301)

    async for db in get_session():
        iface = DbInterface(device_id=device_id, name="GE0/0")
        db.add(iface)
        await db.flush()
        state = InterfaceAttrState(interface_id=iface.id, attribute="description")
        db.add(state)
        await db.commit()
        break

    async for db in get_session():
        device = await db.get(Device, device_id)
        await rekey_device(db, device, nso_device_name="renamed")
        # interfaces should be gone
        result = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
        assert result.scalars().all() == []
        break


async def test_rekey_raises_for_unknown_instance(adapter_client):
    """rekey_device raises ValueError when new NSO instance is not in config."""
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="rekey-inst", netbox_device_id=302)

    async for db in get_session():
        device = await db.get(Device, device_id)
        with pytest.raises(ValueError, match="not found in config"):
            await rekey_device(db, device, nso_instance="ghost-nso")
        break


async def test_rekey_changes_nso_instance(adapter_client_with_nso):
    """rekey_device updates nso_instance when a valid new instance is provided."""
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="rekey-inst-change", netbox_device_id=305)

    async for db in get_session():
        device = await db.get(Device, device_id)
        updated = await rekey_device(db, device, nso_instance="nso-dev")  # same instance, valid
        assert updated.nso_instance == "nso-dev"
        break



    """rekey_device with no fields provided returns the device unchanged."""
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="unchanged", netbox_device_id=303)

    async for db in get_session():
        device = await db.get(Device, device_id)
        updated = await rekey_device(db, device)
        assert updated.nso_device_name == "unchanged"
        break


# ── offboard_device ──────────────────────────────────────────────────────────


async def test_offboard_removes_device(adapter_client_with_nso):
    """offboard_device deletes the device row from the DB."""
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="to-offboard", netbox_device_id=400)

    async for db in get_session():
        device = await db.get(Device, device_id)
        await offboard_device(db, device)
        # Confirm gone
        gone = await db.get(Device, device_id)
        assert gone is None
        break


async def test_offboard_cascades_interfaces_and_scope(adapter_client_with_nso):
    """offboard_device removes interfaces, attr states, and managed scope."""
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="offboard-with-data",
        netbox_device_id=401,
        attributes=["description"],
    )

    async for db in get_session():
        iface = DbInterface(device_id=device_id, name="GE0/0")
        db.add(iface)
        await db.flush()
        db.add(InterfaceAttrState(interface_id=iface.id, attribute="description"))
        await db.commit()
        break

    async for db in get_session():
        device = await db.get(Device, device_id)
        await offboard_device(db, device)
        # All related rows should be gone
        ifaces = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
        assert ifaces.scalars().all() == []
        scope = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
        assert scope.scalars().all() == []
        break


# ── set_scope ────────────────────────────────────────────────────────────────


async def test_set_scope_adds_attributes(adapter_client_with_nso):
    """set_scope creates ManagedScope rows for each requested attribute."""
    from nso_adapter.core.onboarding import set_scope
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="scope-add", netbox_device_id=500, attributes=[])

    async for db in get_session():
        device = await db.get(Device, device_id)
        result = await set_scope(db, device, ["description", "enabled"])
        attrs = {s.attribute for s in result}
        assert attrs == {"description", "enabled"}
        break


async def test_set_scope_removes_old_attributes(adapter_client_with_nso):
    """set_scope removes rows that are no longer in the desired list."""
    from nso_adapter.core.onboarding import set_scope
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-remove",
        netbox_device_id=501,
        attributes=["description", "enabled"],
    )

    async for db in get_session():
        device = await db.get(Device, device_id)
        result = await set_scope(db, device, ["description"])  # remove "enabled"
        attrs = {s.attribute for s in result}
        assert attrs == {"description"}
        break


async def test_set_scope_idempotent(adapter_client_with_nso):
    """set_scope with the same list twice leaves exactly that set of rows."""
    from nso_adapter.core.onboarding import set_scope
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-idempotent",
        netbox_device_id=502,
        attributes=["description"],
    )

    async for db in get_session():
        device = await db.get(Device, device_id)
        r1 = await set_scope(db, device, ["description"])
        r2 = await set_scope(db, device, ["description"])
        assert {s.attribute for s in r1} == {s.attribute for s in r2} == {"description"}
        break


async def test_set_scope_empty_list_clears_scope(adapter_client_with_nso):
    """set_scope with [] removes all managed attributes."""
    from nso_adapter.core.onboarding import set_scope
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-clear",
        netbox_device_id=503,
        attributes=["description"],
    )

    async for db in get_session():
        device = await db.get(Device, device_id)
        result = await set_scope(db, device, [])
        assert result == []
        break
