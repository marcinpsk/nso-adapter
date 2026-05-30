# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/interface-ips."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
TS = datetime(2026, 5, 27, 9, 41, 12, 221000, tzinfo=UTC)


async def _seed_ip(
    device_id: int,
    *,
    interface_name: str,
    address: str,
    vrf: str = "",
    family: str = "ipv4",
    secondary: bool = False,
    refresh_source: str = "notification",
    last_refreshed_at: datetime = TS,
) -> None:
    """Seed a single InterfaceIpAddress row for a device."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import InterfaceIpAddress

    async for db in get_session():
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name=interface_name,
                address=address,
                vrf=vrf,
                family=family,
                secondary=secondary,
                last_refreshed_at=last_refreshed_at.replace(tzinfo=None),
                refresh_source=refresh_source,
            )
        )
        await db.commit()
        break


# ── GET /api/v1/devices/{id}/interface-ips ──────────────────────────────────


async def test_interface_ips_no_data_returns_never(adapter_client):
    """Device with no IP rows → 200, refresh_source='never', empty interfaces."""
    device_id = await seed_device(nso_device_name="ip-empty-dev", netbox_device_id=850)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "never"
    assert body["last_refreshed_at"] is None
    assert body["interfaces"] == []


async def test_interface_ips_single_address(adapter_client):
    """Device with one interface, one IPv4 address → correct structure."""
    device_id = await seed_device(nso_device_name="ip-single-dev", netbox_device_id=851)
    await _seed_ip(
        device_id,
        interface_name="GigabitEthernet0/1",
        address="10.0.0.1/24",
        family="ipv4",
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "notification"
    assert body["last_refreshed_at"] is not None

    assert len(body["interfaces"]) == 1
    iface = body["interfaces"][0]
    assert iface["interface"] == "GigabitEthernet0/1"
    assert len(iface["addresses"]) == 1
    addr = iface["addresses"][0]
    assert addr["address"] == "10.0.0.1/24"
    assert addr["prefix_length"] == 24
    assert addr["family"] == "ipv4"
    assert addr["secondary"] is False
    assert addr["vrf"] == ""


async def test_interface_ips_multiple_interfaces_grouped(adapter_client):
    """Multiple interfaces → each grouped separately, sorted by name."""
    device_id = await seed_device(nso_device_name="ip-multi-iface-dev", netbox_device_id=852)
    await _seed_ip(device_id, interface_name="GigabitEthernet0/2", address="10.0.2.1/24")
    await _seed_ip(device_id, interface_name="GigabitEthernet0/1", address="10.0.1.1/24")

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)
    assert resp.status_code == 200
    ifaces = resp.json()["interfaces"]
    assert len(ifaces) == 2
    # Response must be sorted by interface name
    assert ifaces[0]["interface"] == "GigabitEthernet0/1"
    assert ifaces[1]["interface"] == "GigabitEthernet0/2"


async def test_interface_ips_multiple_addresses_on_one_interface(adapter_client):
    """One interface with two addresses (one secondary, one VRF)."""
    device_id = await seed_device(nso_device_name="ip-multi-addr-dev", netbox_device_id=853)
    await _seed_ip(device_id, interface_name="Loopback0", address="192.168.1.1/32", family="ipv4")
    await _seed_ip(device_id, interface_name="Loopback0", address="192.168.1.2/32", secondary=True)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)
    assert resp.status_code == 200
    ifaces = resp.json()["interfaces"]
    assert len(ifaces) == 1
    addrs = ifaces[0]["addresses"]
    assert len(addrs) == 2
    secondary_flags = {a["address"]: a["secondary"] for a in addrs}
    assert secondary_flags["192.168.1.1/32"] is False
    assert secondary_flags["192.168.1.2/32"] is True


async def test_interface_ips_vrf_address(adapter_client):
    """Address in a named VRF is returned with the vrf field set."""
    device_id = await seed_device(nso_device_name="ip-vrf-dev", netbox_device_id=854)
    await _seed_ip(device_id, interface_name="GigabitEthernet0/1", address="172.16.0.1/24", vrf="MGMT")

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)
    assert resp.status_code == 200
    addr = resp.json()["interfaces"][0]["addresses"][0]
    assert addr["vrf"] == "MGMT"


async def test_interface_ips_uses_most_recent_timestamp(adapter_client):
    """When multiple rows exist, last_refreshed_at reflects the most recent."""
    device_id = await seed_device(nso_device_name="ip-ts-dev", netbox_device_id=855)
    older_ts = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
    newer_ts = datetime(2026, 5, 27, 9, 41, 12, tzinfo=UTC)
    await _seed_ip(device_id, interface_name="GigabitEthernet0/1", address="10.0.1.1/24",
                   refresh_source="poll", last_refreshed_at=older_ts)
    await _seed_ip(device_id, interface_name="GigabitEthernet0/2", address="10.0.2.1/24",
                   refresh_source="notification", last_refreshed_at=newer_ts)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["refresh_source"] == "notification"
    assert "2026-05-27" in body["last_refreshed_at"]


async def test_interface_ips_unknown_device_returns_404(adapter_client):
    """Non-existent device_id → 404 not_found."""
    resp = await adapter_client.get("/api/v1/devices/9998/interface-ips", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_interface_ips_requires_auth(adapter_client):
    """Missing Authorization header → 401."""
    resp = await adapter_client.get("/api/v1/devices/1/interface-ips")
    assert resp.status_code == 401


# ── PUT /api/v1/devices/{id}/ip-intent ─────────────────────────────────────


async def _seed_interface(device_id: int, name: str) -> int:
    """Seed a DbInterface row and return its id."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DbInterface

    async for db in get_session():
        iface = DbInterface(device_id=device_id, name=name)
        db.add(iface)
        await db.flush()
        iface_id = iface.id
        await db.commit()
        return iface_id
    raise RuntimeError("unreachable")


async def test_put_ip_intent_creates_rows(adapter_client):
    """PUT /ip-intent inserts rows for known interface + returns address_count."""
    device_id = await seed_device(nso_device_name="ip-intent-dev-01", netbox_device_id=900)
    await _seed_interface(device_id, "GigabitEthernet0/1")

    payload = {
        "addresses": [
            {"interface": "GigabitEthernet0/1", "address": "10.0.0.1/24", "family": "ipv4"},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["address_count"] == 1
    assert "updated_at" in body


async def test_put_ip_intent_full_replace(adapter_client):
    """Second PUT replaces all existing rows."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import InterfaceIpIntent
    from sqlalchemy import select

    device_id = await seed_device(nso_device_name="ip-intent-dev-02", netbox_device_id=901)
    iface_id = await _seed_interface(device_id, "GigabitEthernet0/2")

    # First PUT: insert two addresses
    payload1 = {
        "addresses": [
            {"interface": "GigabitEthernet0/2", "address": "10.0.0.1/24", "family": "ipv4"},
            {"interface": "GigabitEthernet0/2", "address": "10.0.0.2/24", "family": "ipv4"},
        ]
    }
    await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=payload1)

    # Second PUT: only one address
    payload2 = {
        "addresses": [
            {"interface": "GigabitEthernet0/2", "address": "10.0.0.1/24", "family": "ipv4"},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=payload2)
    assert resp.json()["address_count"] == 1

    async for db in get_session():
        result = await db.execute(
            select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)
        )
        rows = result.scalars().all()
        break
    # Old 10.0.0.2 row should be gone
    assert len(rows) == 1
    assert rows[0].address == "10.0.0.1/24"


async def test_put_ip_intent_unknown_interface_skipped(adapter_client):
    """Entries referencing unknown interface names are silently skipped."""
    device_id = await seed_device(nso_device_name="ip-intent-dev-03", netbox_device_id=902)
    # No interfaces seeded

    payload = {
        "addresses": [
            {"interface": "GigabitEthernet99/99", "address": "192.168.1.1/30", "family": "ipv4"},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=payload)
    assert resp.status_code == 200
    assert resp.json()["address_count"] == 0


async def test_put_ip_intent_404_unknown_device(adapter_client):
    """Device not found → 404 not_found."""
    payload = {"addresses": []}
    resp = await adapter_client.put("/api/v1/devices/9999/ip-intent", headers=AUTH, json=payload)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_put_ip_intent_requires_auth(adapter_client):
    """Missing Authorization header → 401."""
    resp = await adapter_client.put("/api/v1/devices/1/ip-intent", json={"addresses": []})
    assert resp.status_code == 401
