# SPDX-License-Identifier: Apache-2.0
"""Tests for GET /api/v1/devices/by-nso."""

from __future__ import annotations

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def test_get_device_includes_failover_block(adapter_client):
    """GET /devices/{id} carries the failover status block the plugin's NSO tab renders."""
    from datetime import UTC, datetime

    from nso_adapter.store.models import ActiveAddress, DeviceFailover

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="fo-rtr", netbox_device_id=70)
    async with session() as db:
        db.add(
            DeviceFailover(
                device_id=device_id,
                primary_ip="10.0.0.1",
                oob_ip="192.0.2.5",
                active_address=ActiveAddress.oob.value,
                last_probe_result="timeout",
                last_probe_target="oob",
                last_probe_detail="cold connect exceeded probe window",
                oob_healthy=None,
                oob_health_result="timeout",
                oob_health_detail="cold connect exceeded probe window",
                last_switch_at=datetime.now(UTC),
            )
        )
        await db.commit()

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}", headers=AUTH)
    assert resp.status_code == 200
    fo = resp.json()["failover"]
    assert fo["active_address"] == "oob"
    assert fo["primary_ip"] == "10.0.0.1"
    assert fo["oob_ip"] == "192.0.2.5"
    assert fo["last_probe_result"] == "timeout"
    assert fo["last_probe_target"] == "oob"
    assert fo["last_probe_detail"] == "cold connect exceeded probe window"
    assert fo["oob_healthy"] is None
    assert fo["oob_health_result"] == "timeout"
    assert fo["oob_health_detail"] == "cold connect exceeded probe window"
    assert fo["last_switch_at"] is not None
    assert fo["manual_override"] is False


async def test_get_device_failover_null_when_unmanaged(adapter_client):
    """A device with no DeviceFailover row → failover is null (tab shows nothing)."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="nofo-rtr", netbox_device_id=71)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["failover"] is None


async def test_get_device_exposes_partial_sync_and_degraded_surfaces(adapter_client):
    """A partial sync surfaces last_sync_status='partial' + degraded_surfaces so the
    plugin can warn that some routing surfaces are stale (finding s2-2)."""
    from nso_adapter.store.models import Device, LastSyncStatus

    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="degraded-rtr", netbox_device_id=72)
    async with session() as db:
        device = await db.get(Device, device_id)
        device.last_sync_status = LastSyncStatus.partial
        device.degraded_surfaces = ["bgp", "ospf"]
        await db.commit()

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["last_sync_status"] == "partial"
    assert body["degraded_surfaces"] == ["bgp", "ospf"]


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
