# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for intent endpoints: PUT/GET /api/v1/devices/{id}/intent."""

from __future__ import annotations

from nso_adapter.store.models import (
    DbInterface,
    DeviceSettings,
    InterfaceAttrState,
    SyncState,
)
from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _seed_interface(db, device_id: int, name: str = "GigabitEthernet0/0") -> int:
    """Insert a DbInterface for the given device, return interface id."""
    iface = DbInterface(device_id=device_id, name=name, netbox_interface_id=None)
    db.add(iface)
    await db.flush()
    return iface.id


async def _seed_attr_state(
    db, interface_id: int, attribute: str = "description", status: SyncState = SyncState.imported
) -> None:
    state = InterfaceAttrState(
        interface_id=interface_id,
        attribute=attribute,
        nso_value="old-value",
        sync_state=status,
    )
    db.add(state)
    await db.flush()


# ── PUT /api/v1/devices/{id}/intent ─────────────────────────────────────────


async def test_put_intent_happy_path(adapter_client):
    """PUT with valid attributes for known interfaces → 200 with attribute_count."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="intent-router", netbox_device_id=500)

    # Seed an interface with attr_state so intent can be stamped
    from nso_adapter.store.db import get_session

    async for db in get_session():
        iface_id = await _seed_interface(db, device_id, "GigabitEthernet0/0")
        await _seed_attr_state(db, iface_id, "description", SyncState.imported)
        await db.commit()
        break

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={
            "attributes": [
                {"interface": "GigabitEthernet0/0", "attribute": "description", "intent_value": "uplink to core"}
            ]
        },
        headers=AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["attribute_count"] == 1
    assert "updated_at" in body


async def test_put_intent_replaces_existing(adapter_client):
    """Second PUT fully replaces the previous intent (idempotent full-replace)."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="intent-replace", netbox_device_id=501)

    from nso_adapter.store.db import get_session

    async for db in get_session():
        iface_id = await _seed_interface(db, device_id, "Loopback0")
        await _seed_attr_state(db, iface_id, "description", SyncState.imported)
        await db.commit()
        break

    # First PUT
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "Loopback0", "attribute": "description", "intent_value": "first-value"}]},
        headers=AUTH,
    )

    # Second PUT with different value — should replace
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "Loopback0", "attribute": "description", "intent_value": "second-value"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["attribute_count"] == 1

    # GET should reflect second value
    get_resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent", headers=AUTH)
    assert get_resp.status_code == 200
    attrs = get_resp.json()["attributes"]
    assert len(attrs) == 1
    assert attrs[0]["intent_value"] == "second-value"


async def test_put_intent_unknown_interface_skipped(adapter_client):
    """Intent for an interface not tracked by the adapter is silently skipped."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="intent-skip", netbox_device_id=502)

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "DoesNotExist0/0", "attribute": "description", "intent_value": "whatever"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200
    # count is 0 — unknown interface skipped, not an error
    assert resp.json()["attribute_count"] == 0


async def test_put_intent_stamps_accepted_on_imported(adapter_client):
    """PUT intent transitions an 'imported' attr_state to 'accepted'."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="intent-stamp", netbox_device_id=503)

    from nso_adapter.store.db import get_session

    async for db in get_session():
        iface_id = await _seed_interface(db, device_id, "eth0")
        await _seed_attr_state(db, iface_id, "description", SyncState.imported)
        await db.commit()
        break

    await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "eth0", "attribute": "description", "intent_value": "stamped"}]},
        headers=AUTH,
    )

    from sqlalchemy import select

    from nso_adapter.store.db import get_session

    async for db in get_session():
        result = await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.attribute == "description"))
        state = result.scalar_one()
        assert state.sync_state == SyncState.accepted
        break


async def test_put_intent_does_not_override_in_sync(adapter_client):
    """PUT intent does NOT downgrade an already in_sync attr_state to accepted."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="intent-insync", netbox_device_id=504)

    from nso_adapter.store.db import get_session

    async for db in get_session():
        iface_id = await _seed_interface(db, device_id, "eth1")
        await _seed_attr_state(db, iface_id, "description", SyncState.in_sync)
        await db.commit()
        break

    await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "eth1", "attribute": "description", "intent_value": "in-sync-value"}]},
        headers=AUTH,
    )

    from sqlalchemy import select

    from nso_adapter.store.db import get_session

    async for db in get_session():
        result = await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.attribute == "description"))
        state = result.scalar_one()
        # Must remain in_sync, not downgraded to accepted
        assert state.sync_state == SyncState.in_sync
        break


async def test_put_intent_auto_apply_enqueues_job(adapter_client):
    """PUT intent with auto_apply enabled enqueues an apply job."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="intent-autoapply", netbox_device_id=505)

    from nso_adapter.store.db import get_session

    async for db in get_session():
        iface_id = await _seed_interface(db, device_id, "eth2")
        await _seed_attr_state(db, iface_id, "description", SyncState.accepted)
        # Enable auto_apply
        settings = DeviceSettings(device_id=device_id, auto_apply=True)
        db.add(settings)
        await db.commit()
        break

    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "eth2", "attribute": "description", "intent_value": "auto-applied"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200

    # Verify an apply job was created
    from sqlalchemy import select

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobType

    async for db in get_session():
        result = await db.execute(select(Job).where(Job.device_id == device_id, Job.job_type == JobType.apply))
        job = result.scalar_one_or_none()
        assert job is not None
        break


async def test_put_intent_unknown_device_returns_404(adapter_client):
    """PUT intent for non-existent device → 404."""
    resp = await adapter_client.put(
        "/api/v1/devices/9999/intent",
        json={"attributes": []},
        headers=AUTH,
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_put_intent_requires_auth(adapter_client):
    """PUT intent without token → 401."""
    resp = await adapter_client.put("/api/v1/devices/1/intent", json={"attributes": []})
    assert resp.status_code == 401


async def test_put_intent_unmanaged_attribute_returns_422(adapter_client):
    """PUT intent with an attribute not in the device's managed scope → 422."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-check-device",
        netbox_device_id=512,
        attributes=["description"],  # only 'description' is in scope
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "Gi0/0", "attribute": "vlan", "intent_value": "100"}]},
        headers=AUTH,
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"
    assert "vlan" in resp.json()["error"]["message"]


# ── GET /api/v1/devices/{id}/intent ─────────────────────────────────────────


async def test_get_intent_returns_empty_for_new_device(adapter_client):
    """GET intent on a device with no intent rows → 200 with empty attributes list."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="get-intent-empty", netbox_device_id=510)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["attributes"] == []


async def test_get_intent_returns_set_attributes(adapter_client):
    """GET intent after PUT returns the stored intent rows."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="get-intent-full", netbox_device_id=511)

    from nso_adapter.store.db import get_session

    async for db in get_session():
        iface_id = await _seed_interface(db, device_id, "GE0/1")
        await _seed_attr_state(db, iface_id, "description", SyncState.imported)
        await db.commit()
        break

    # PUT intent first
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "GE0/1", "attribute": "description", "intent_value": "test-desc"}]},
        headers=AUTH,
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    attrs = body["attributes"]
    assert len(attrs) == 1
    assert attrs[0]["interface"] == "GE0/1"
    assert attrs[0]["attribute"] == "description"
    assert attrs[0]["intent_value"] == "test-desc"


async def test_get_intent_unknown_device_returns_404(adapter_client):
    """GET intent for non-existent device → 404."""
    resp = await adapter_client.get("/api/v1/devices/9999/intent", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_get_intent_requires_auth(adapter_client):
    """GET intent without token → 401."""
    resp = await adapter_client.get("/api/v1/devices/1/intent")
    assert resp.status_code == 401
