# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/isis-interfaces — bound_port surfacing."""

from __future__ import annotations

from datetime import UTC, datetime

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
TS = datetime(2026, 6, 2, 19, 27, 0, tzinfo=UTC)


async def _seed_isis_iface(
    device_id: int,
    *,
    interface_name: str,
    af: str = "ipv4",
    bound_port: str | None = None,
    refresh_source: str = "poll",
) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceIsisInterface

    async for db in get_session():
        db.add(
            DeviceIsisInterface(
                device_id=device_id,
                interface_name=interface_name,
                af=af,
                bound_port=bound_port,
                last_refreshed_at=TS.replace(tzinfo=None),
                refresh_source=refresh_source,
            )
        )
        await db.commit()
        break


async def test_isis_interface_surfaces_bound_port(adapter_client):
    """A Nokia IS-IS row with bound_port surfaces it on the GET payload."""
    device_id = await seed_device(nso_device_name="isis-api-nokia", netbox_device_id=970)
    await _seed_isis_iface(device_id, interface_name="LAG99:10", bound_port="lag-99:10")

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/isis-interfaces", headers=AUTH)
    assert resp.status_code == 200
    ifaces = resp.json()["interfaces"]
    assert len(ifaces) == 1
    assert ifaces[0]["interface_name"] == "LAG99:10"
    assert ifaces[0]["bound_port"] == "lag-99:10"


async def test_isis_interface_omits_bound_port_when_none(adapter_client):
    """A Cisco/loopback row (bound_port None) omits the key entirely."""
    device_id = await seed_device(nso_device_name="isis-api-cisco", netbox_device_id=971)
    await _seed_isis_iface(device_id, interface_name="GigabitEthernet0/1", bound_port=None)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/isis-interfaces", headers=AUTH)
    assert resp.status_code == 200
    ifaces = resp.json()["interfaces"]
    assert len(ifaces) == 1
    assert "bound_port" not in ifaces[0]


async def test_isis_interfaces_device_not_found(adapter_client):
    """GET for a non-existent device → 404."""
    resp = await adapter_client.get("/api/v1/devices/99999/isis-interfaces", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"
