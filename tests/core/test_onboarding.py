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

from nso_adapter.store.models import DbInterface, Device, InterfaceAttrState, ManagedScope

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
    """onboard_device raises LookupError when (nso_instance, nso_device_name) is already onboarded
    to a DIFFERENT NetBox device — that is a genuine conflict; don't steal it."""
    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    await seed_device(nso_instance="nso-dev", nso_device_name="taken-name", netbox_device_id=200)

    async for db in get_session():
        with pytest.raises(LookupError, match="already onboarded"):
            await onboard_device(db, "nso-dev", "taken-name", 201)
        break


async def test_onboard_adopts_unlinked_existing_device(adapter_client_with_nso):
    """A device provisioned INTO NSO without a NetBox link (netbox_device_id IS NULL) must be
    ADOPTED when the operator later marks it managed: onboard_device fills the mapping in on the
    SAME row instead of raising a spurious 'already onboarded'. Regression — an unlinked leftover
    row silently blocked linking (409 -> plugin swallowed it), so the plugin's adapter_device_id
    stayed None and the device never onboarded (live: netbox device 23 / prod-lab03c-ri6 vs the
    June-provisioned adapter device 343, netbox_device_id NULL)."""
    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import MappingStatus
    from tests.conftest import seed_device

    existing_id = await seed_device(nso_instance="nso-dev", nso_device_name="preprovisioned", netbox_device_id=None)

    async for db in get_session():
        device = await onboard_device(db, "nso-dev", "preprovisioned", 77)
        assert device.id == existing_id  # adopted the SAME row — not a second device
        assert device.netbox_device_id == 77
        assert device.mapping_status == MappingStatus.mapped
        break

    # Exactly one row for that NSO node — adoption must not create a duplicate.
    async for db in get_session():
        rows = (await db.execute(select(Device).where(Device.nso_device_name == "preprovisioned"))).scalars().all()
        assert len(rows) == 1
        assert rows[0].netbox_device_id == 77
        break


async def test_onboard_is_idempotent_for_same_link(adapter_client_with_nso):
    """Re-onboarding the same (instance, name) already linked to the SAME netbox_device_id returns
    the existing row (idempotent no-op), not a 409 — so a re-fired manage signal is safe."""
    from nso_adapter.core.onboarding import onboard_device
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    existing_id = await seed_device(nso_instance="nso-dev", nso_device_name="already-linked", netbox_device_id=55)

    async for db in get_session():
        device = await onboard_device(db, "nso-dev", "already-linked", 55)
        assert device.id == existing_id
        assert device.netbox_device_id == 55
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
        device.ned_id = "old-ned"
        device.sw_version = "old-version"
        device.degraded_surfaces = ["ospf"]
        await db.commit()
        updated = await rekey_device(db, device, nso_device_name="new-name")
        assert updated.nso_device_name == "new-name"
        assert updated.ned_id is None
        assert updated.sw_version is None
        assert updated.last_sync_at is None
        assert updated.degraded_surfaces is None
        assert updated.source_epoch == 2
        break


async def test_rekey_same_source_is_true_noop(adapter_client_with_nso):
    """An idempotent source PATCH preserves the generation and read publications."""
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.nso.read_outcome import Present
    from nso_adapter.store import outcome_store
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="same-source", netbox_device_id=306)
    async for db in get_session():
        attempt = await outcome_store.record_read_outcome(db, device_id, "bfd", Present([]), refresh_source="poll")
        await outcome_store.record_result(
            db, attempt, result="replaced", succeeded=True, row_count=0, publish_payload=True
        )
        db.add(DbInterface(device_id=device_id, name="GE0/0"))
        await db.commit()
        device = await db.get(Device, device_id)
        updated = await rekey_device(
            db, device, nso_instance=device.nso_instance, nso_device_name=device.nso_device_name
        )
        assert updated.source_epoch == 1
        assert (await outcome_store.get_current_outcome(db, device_id, "bfd")).id == attempt
        assert await db.scalar(select(DbInterface).where(DbInterface.device_id == device_id)) is not None
        break


async def test_rekey_invalidates_all_read_publications(adapter_client_with_nso):
    """A real source change clears routing mirrors and every family pointer atomically."""
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.nso.read_outcome import Present
    from nso_adapter.store import outcome_store
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceStaticRoute
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="old-source", netbox_device_id=307)
    async for db in get_session():
        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="198.18.20.0/24",
                next_hop="198.18.0.2",
                refresh_source="poll",
            )
        )
        attempt = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present([]), refresh_source="poll"
        )
        await outcome_store.record_result(
            db, attempt, result="replaced", succeeded=True, row_count=1, publish_payload=True
        )
        device = await db.get(Device, device_id)
        updated = await rekey_device(db, device, nso_device_name="new-source")
        assert updated.source_epoch == 2
        assert await db.scalar(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id)) is None
        assert await outcome_store.get_current_outcome(db, device_id, "static_route") is None
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


async def test_rekey_preserves_interface_intent_and_its_identity_anchor(adapter_client_with_nso):
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import InterfaceIntent
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="intent-source", netbox_device_id=308)
    async for db in get_session():
        iface = DbInterface(
            device_id=device_id,
            name="GE0/0",
            parent_binding="old-parent",
            kind="physical",
        )
        db.add(iface)
        await db.flush()
        db.add(InterfaceAttrState(interface_id=iface.id, attribute="description", nso_value="old"))
        db.add(
            InterfaceIntent(
                interface_id=iface.id,
                attribute="description",
                intent_value="operator-owned",
            )
        )
        await db.commit()

        device = await db.get(Device, device_id)
        await rekey_device(db, device, nso_device_name="replacement-source")

        kept_iface = await db.scalar(select(DbInterface).where(DbInterface.device_id == device_id))
        assert kept_iface is not None
        assert kept_iface.name == "GE0/0"
        assert kept_iface.parent_binding is None
        assert kept_iface.kind is None
        intent = await db.scalar(select(InterfaceIntent).where(InterfaceIntent.interface_id == kept_iface.id))
        assert intent is not None
        assert intent.intent_value == "operator-owned"
        assert (
            await db.scalar(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == kept_iface.id)) is None
        )
        break


async def test_rekey_preserves_ip_only_intent_and_its_identity_anchor(adapter_client_with_nso):
    from nso_adapter.core.onboarding import rekey_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import InterfaceIpIntent
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="ip-intent-source", netbox_device_id=310)
    async for db in get_session():
        iface = DbInterface(device_id=device_id, name="GE0/1")
        db.add(iface)
        await db.flush()
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id,
                address="198.18.0.1/24",
                vrf="",
                family="ipv4",
                secondary=False,
            )
        )
        await db.commit()

        device = await db.get(Device, device_id)
        await rekey_device(db, device, nso_device_name="replacement-ip-source")

        kept_iface = await db.scalar(select(DbInterface).where(DbInterface.device_id == device_id))
        assert kept_iface is not None
        intent = await db.scalar(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == kept_iface.id))
        assert intent is not None
        assert intent.address == "198.18.0.1/24"
        break


async def test_old_source_sync_metadata_cannot_overwrite_rekey_reset(adapter_client_with_nso):
    from nso_adapter.core.importer import _publish_sync_metadata
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import LastSyncStatus
    from tests.conftest import seed_device

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="metadata-source", netbox_device_id=309)
    async for db in get_session():
        device = await db.get(Device, device_id)
        device.source_epoch = 2
        await db.commit()

        published = await _publish_sync_metadata(
            db,
            device_id,
            source_epoch=1,
            status=LastSyncStatus.succeeded,
            degraded_surfaces=None,
        )

        assert published is False
        await db.refresh(device)
        assert device.last_sync_at is None
        assert device.last_sync_status is None
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


async def test_offboard_cascades_mirror_rows_the_keeprows_change_relies_on(adapter_client_with_nso):
    """READSEM S5 (1327): device-absence now KEEPS mirror rows, so a device removed from NSO is
    cleaned up ONLY by offboard. Prove offboard removes the mirror for the families a bare 404 no
    longer clears — a pop family (static_route) + device_settings — so keep-rows can't strand
    immortal rows on a deleted device."""
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSettings, DeviceStaticRoute
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="offboard-mirror",
        netbox_device_id=402,
        attributes=["description"],
    )

    async for db in get_session():
        db.add(DeviceStaticRoute(device_id=device_id, vrf="", prefix="10.9.0.0/16", next_hop="1.1.1.1"))
        db.add(DeviceSettings(device_id=device_id, auto_apply=True))
        await db.commit()
        break

    async for db in get_session():
        device = await db.get(Device, device_id)
        await offboard_device(db, device)
        routes = await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id))
        assert routes.scalars().all() == [], "static_route rows orphaned after offboard"
        settings = await db.execute(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
        assert settings.scalars().all() == [], "device_settings row orphaned after offboard"
        scope = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
        assert scope.scalars().all() == []
        assert await db.get(Device, device_id) is None
        break


# ── set_scope ────────────────────────────────────────────────────────────────


async def test_set_scope_adds_attributes(adapter_client_with_nso):
    """set_scope creates ManagedScope rows for each requested attribute."""
    from nso_adapter.core.onboarding import set_scope
    from nso_adapter.store.db import get_session
    from tests.conftest import seed_device

    device_id = await seed_device(
        nso_instance="nso-dev", nso_device_name="scope-add", netbox_device_id=500, attributes=[]
    )

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
