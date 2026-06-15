# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for the /api/v1/devices/{id}/lag-config endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from tests.conftest import VALID_TOKEN, seed_device, seed_lag_config

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_lag_config_empty_returns_never(adapter_client):
    device_id = await seed_device(nso_device_name="lag-config-empty", netbox_device_id=1100)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "never"
    assert body["last_refreshed_at"] is None
    assert body["bundles"] == []


@pytest.mark.anyio
async def test_lag_config_requires_auth(adapter_client):
    device_id = await seed_device(nso_device_name="lag-config-noauth", netbox_device_id=1101)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_lag_config_device_not_found(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/99999/lag-config", headers=AUTH)
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_lag_config_bundle_with_members(adapter_client):
    device_id = await seed_device(nso_device_name="lag-config-full", netbox_device_id=1102)
    await seed_lag_config(
        device_id,
        bundles=[
            {
                "name": "Port-channel1",
                "lag_id": 1,
                "min_links": 2,
                "system_priority": 100,
                "timer": "fast",
                "members": [
                    {"interface_name": "GigabitEthernet0/1", "mode": "active", "port_priority": 200},
                    {"interface_name": "GigabitEthernet0/2", "mode": "active"},
                ],
            }
        ],
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["refresh_source"] == "test"
    assert len(body["bundles"]) == 1

    bundle = body["bundles"][0]
    assert bundle["name"] == "Port-channel1"
    assert bundle["lag_id"] == 1
    assert bundle["min_links"] == 2
    assert bundle["system_priority"] == 100
    assert bundle["timer"] == "fast"
    assert len(bundle["members"]) == 2
    m1 = next(m for m in bundle["members"] if m["interface_name"] == "GigabitEthernet0/1")
    assert m1["mode"] == "active"
    assert m1["port_priority"] == 200
    m2 = next(m for m in bundle["members"] if m["interface_name"] == "GigabitEthernet0/2")
    assert m2.get("port_priority") is None


_APPLY_BODY = {
    "bundles": [
        {
            "name": "Port-channel1",
            "lag_id": 1,
            "min_links": 2,
            "system_priority": 100,
            "timer": "fast",
            "members": [
                {"interface_name": "GigabitEthernet0/1", "mode": "active", "port_priority": 200},
                {"interface_name": "GigabitEthernet0/2", "mode": "active"},
            ],
        }
    ]
}


@pytest.mark.anyio
async def test_apply_lag_config_builds_service_payload(adapter_client):
    device_id = await seed_device(nso_device_name="lag-apply-ok", netbox_device_id=1110)
    nso_write = AsyncMock()
    with (
        patch("nso_adapter.api.lag_config.get_nso_client", return_value=AsyncMock()),
        patch("nso_adapter.core.lag_intent._nso_apply_lag_config", nso_write),
    ):
        resp = await adapter_client.post(
            f"/api/v1/devices/{device_id}/lag-config/apply", json=_APPLY_BODY, headers=AUTH
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "deployed"
    assert body["device"] == "lag-apply-ok"
    assert body["bundle_count"] == 1

    nso_write.assert_awaited_once()
    _client, device_name, bundles = nso_write.await_args.args
    assert device_name == "lag-apply-ok"
    assert len(bundles) == 1
    bundle = bundles[0]
    assert bundle["name"] == "Port-channel1"
    assert bundle["lag-id"] == 1
    assert bundle["min-links"] == 2
    assert bundle["system-priority"] == 100
    assert bundle["timer"] == "fast"
    assert len(bundle["member"]) == 2
    m1 = next(m for m in bundle["member"] if m["interface-name"] == "GigabitEthernet0/1")
    assert m1["mode"] == "active"
    assert m1["port-priority"] == 200
    m2 = next(m for m in bundle["member"] if m["interface-name"] == "GigabitEthernet0/2")
    assert "port-priority" not in m2


@pytest.mark.anyio
async def test_apply_lag_config_device_not_found(adapter_client):
    resp = await adapter_client.post("/api/v1/devices/99999/lag-config/apply", json=_APPLY_BODY, headers=AUTH)
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_apply_lag_config_requires_auth(adapter_client):
    device_id = await seed_device(nso_device_name="lag-apply-noauth", netbox_device_id=1111)
    resp = await adapter_client.post(f"/api/v1/devices/{device_id}/lag-config/apply", json=_APPLY_BODY)
    assert resp.status_code == 401
