# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for interfaces and sync_state endpoints.

GET /api/v1/devices/{id}/interfaces
GET /api/v1/devices/{id}/state
"""

from __future__ import annotations

from nso_adapter.store.models import DbInterface, InterfaceAttrState, SyncState
from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _seed_interface_with_state(
    device_id: int,
    iface_name: str = "GigabitEthernet0/0",
    attribute: str = "description",
    nso_value: str = "nso-desc",
    netbox_value: str = "nb-desc",
    status: SyncState = SyncState.imported,
) -> None:
    """Seed a DbInterface + InterfaceAttrState for a device."""

    async with session() as db:
        iface = DbInterface(device_id=device_id, name=iface_name, netbox_interface_id=1000)
        db.add(iface)
        await db.flush()
        state = InterfaceAttrState(
            interface_id=iface.id,
            attribute=attribute,
            nso_value=nso_value,
            netbox_value=netbox_value,
            sync_state=status,
        )
        db.add(state)
        await db.commit()


# ── GET /api/v1/devices/{id}/interfaces ─────────────────────────────────────


async def test_list_interfaces_empty(adapter_client):
    """GET interfaces for a device with no interfaces → 200 empty list."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="iface-empty-device",
        netbox_device_id=700,
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interfaces", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == []


async def test_list_interfaces_with_data(adapter_client):
    """GET interfaces returns interface name, netbox_interface_id, and attrs."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="iface-full-device",
        netbox_device_id=701,
    )
    await _seed_interface_with_state(device_id, "GE0/0", "description", "nso-v", "nb-v", SyncState.imported)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interfaces", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    iface = body[0]
    assert iface["name"] == "GE0/0"
    assert "description" in iface["attrs"]
    attr = iface["attrs"]["description"]
    assert attr["nso_value"] == "nso-v"
    assert attr["netbox_value"] == "nb-v"
    assert attr["status"] == "imported"
    assert attr["intent_value"] is None


async def test_list_interfaces_multiple(adapter_client):
    """GET interfaces returns all interfaces for the device."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="iface-multi-device",
        netbox_device_id=702,
    )
    await _seed_interface_with_state(device_id, "GE0/1", "description", "v1", "v1", SyncState.imported)
    await _seed_interface_with_state(device_id, "GE0/2", "description", "v2", "v2", SyncState.changed)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interfaces", headers=AUTH)
    assert resp.status_code == 200
    names = {i["name"] for i in resp.json()}
    assert names == {"GE0/1", "GE0/2"}


async def test_list_interfaces_unknown_device_returns_404(adapter_client):
    """GET interfaces for non-existent device → 404."""
    resp = await adapter_client.get("/api/v1/devices/9999/interfaces", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_list_interfaces_requires_auth(adapter_client):
    """GET interfaces without auth → 401."""
    resp = await adapter_client.get("/api/v1/devices/1/interfaces")
    assert resp.status_code == 401


# ── GET /api/v1/devices/{id}/state ─────────────────────────────────────


async def test_get_state_empty_device(adapter_client):
    """GET sync_state for a device with no interfaces → 200 with all-zero counts."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="comp-empty-device",
        netbox_device_id=710,
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/state", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["managed_interfaces"] == 0
    assert body["by_status"]["unknown"] == 0
    assert body["last_checked_at"] is None


async def test_get_state_counts_by_status(adapter_client):
    """GET sync_state aggregates attr states by sync_state."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="comp-counts-device",
        netbox_device_id=711,
    )
    await _seed_interface_with_state(device_id, "GE0/1", "description", "v", "v", SyncState.imported)
    await _seed_interface_with_state(device_id, "GE0/2", "description", "v", "v2", SyncState.changed)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/state", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["managed_interfaces"] == 2
    assert body["by_status"]["imported"] == 1
    assert body["by_status"]["changed"] == 1


async def test_get_state_includes_phase2_statuses_structure(adapter_client):
    """GET sync_state response includes phase-2 status keys (accepted, deploying, etc.)."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="comp-phase2-device",
        netbox_device_id=712,
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/state", headers=AUTH)
    assert resp.status_code == 200
    by_status = resp.json()["by_status"]
    # All phase-2 status keys must be present
    for key in ("accepted", "deploying", "in_sync", "apply_failed", "drifted"):
        assert key in by_status, f"Phase-2 status key '{key}' missing from sync_state response"


async def test_get_state_unknown_device_returns_404(adapter_client):
    """GET sync_state for non-existent device → 404."""
    resp = await adapter_client.get("/api/v1/devices/9999/state", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_get_state_requires_auth(adapter_client):
    """GET sync_state without auth → 401."""
    resp = await adapter_client.get("/api/v1/devices/1/state")
    assert resp.status_code == 401
