# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for scope endpoints: GET/PUT /api/v1/devices/{id}/scope."""

from __future__ import annotations

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


# ── GET /api/v1/devices/{id}/scope ──────────────────────────────────────────


async def test_get_scope_returns_attributes(adapter_client):
    """GET scope returns the managed attribute list and auto_apply flag."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-get-device",
        netbox_device_id=600,
        attributes=["description", "enabled"],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/scope", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert set(body["attributes"]) == {"description", "enabled"}
    assert body["auto_apply"] is False
    assert "updated_at" in body


async def test_get_scope_empty_device(adapter_client):
    """GET scope on a device with no scope rows returns empty list."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-get-empty",
        netbox_device_id=601,
        attributes=[],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/scope", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["attributes"] == []


async def test_get_scope_unknown_device_returns_404(adapter_client):
    """GET scope for non-existent device → 404."""
    resp = await adapter_client.get("/api/v1/devices/9999/scope", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_get_scope_requires_auth(adapter_client):
    """GET scope without auth → 401."""
    resp = await adapter_client.get("/api/v1/devices/1/scope")
    assert resp.status_code == 401


# ── PUT /api/v1/devices/{id}/scope ──────────────────────────────────────────


async def test_put_scope_replaces_attributes(adapter_client):
    """PUT scope replaces the managed attribute list."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-put-device",
        netbox_device_id=610,
        attributes=["description"],
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/scope",
        json={"attributes": ["description", "enabled"], "auto_apply": False},
        headers=AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert set(body["attributes"]) == {"description", "enabled"}


async def test_put_scope_removes_attributes(adapter_client):
    """PUT scope with empty list removes all managed attributes."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-remove-device",
        netbox_device_id=611,
        attributes=["description", "enabled"],
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/scope",
        json={"attributes": [], "auto_apply": False},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["attributes"] == []


async def test_put_scope_creates_device_settings(adapter_client):
    """PUT scope creates a DeviceSettings row when none exists."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-settings-create",
        netbox_device_id=612,
    )
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/scope",
        json={"attributes": ["description"], "auto_apply": True},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["auto_apply"] is True

    # Verify via GET
    get_resp = await adapter_client.get(f"/api/v1/devices/{device_id}/scope", headers=AUTH)
    assert get_resp.json()["auto_apply"] is True


async def test_put_scope_updates_existing_device_settings(adapter_client):
    """PUT scope updates auto_apply on an existing DeviceSettings row."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="scope-settings-update",
        netbox_device_id=613,
    )
    # Create with auto_apply=False
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/scope",
        json={"attributes": ["description"], "auto_apply": False},
        headers=AUTH,
    )
    # Update to auto_apply=True
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/scope",
        json={"attributes": ["description"], "auto_apply": True},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["auto_apply"] is True


async def test_put_scope_unknown_device_returns_404(adapter_client):
    """PUT scope for non-existent device → 404."""
    resp = await adapter_client.put(
        "/api/v1/devices/9999/scope",
        json={"attributes": ["description"], "auto_apply": False},
        headers=AUTH,
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_put_scope_requires_auth(adapter_client):
    """PUT scope without auth → 401."""
    resp = await adapter_client.put(
        "/api/v1/devices/1/scope",
        json={"attributes": ["description"], "auto_apply": False},
    )
    assert resp.status_code == 401
