# SPDX-License-Identifier: Apache-2.0
"""Tests for GET /api/v1/devices/by-nso."""

from __future__ import annotations

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def test_get_by_nso_hit_returns_device_object(adapter_client):
    """by-nso with matching (instance, name) → 200 with same shape as GET /devices/{id}."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="core-rtr-01",
        netbox_device_id=42,
    )
    resp = await adapter_client.get(
        "/api/v1/devices/by-nso",
        params={"instance": "nso-dev", "name": "core-rtr-01"},
        headers=AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    # Must carry all fields that GET /devices/{id} carries
    for key in ("id", "nso_instance", "nso_device_name", "netbox_device_id", "mapping_status", "scope", "last_job_id"):
        assert key in body, f"Key '{key}' missing in by-nso response"
    assert body["id"] == device_id
    assert body["nso_instance"] == "nso-dev"
    assert body["nso_device_name"] == "core-rtr-01"
    assert body["netbox_device_id"] == 42
    assert body["scope"] == {"attributes": ["description"]}


async def test_get_by_nso_miss_returns_404(adapter_client):
    """by-nso with no matching row → 404 with not_found code."""
    resp = await adapter_client.get(
        "/api/v1/devices/by-nso",
        params={"instance": "nso-dev", "name": "does-not-exist"},
        headers=AUTH,
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_get_by_nso_missing_instance_param_returns_422(adapter_client):
    """Missing required 'instance' query param → 422."""
    resp = await adapter_client.get(
        "/api/v1/devices/by-nso",
        params={"name": "core-rtr-01"},
        headers=AUTH,
    )
    assert resp.status_code == 422


async def test_get_by_nso_missing_name_param_returns_422(adapter_client):
    """Missing required 'name' query param → 422."""
    resp = await adapter_client.get(
        "/api/v1/devices/by-nso",
        params={"instance": "nso-dev"},
        headers=AUTH,
    )
    assert resp.status_code == 422


async def test_get_by_nso_requires_auth(adapter_client):
    """Endpoint requires bearer token."""
    resp = await adapter_client.get(
        "/api/v1/devices/by-nso",
        params={"instance": "nso-dev", "name": "core-rtr-01"},
    )
    assert resp.status_code == 401
