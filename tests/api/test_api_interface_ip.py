# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/interface-ips."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select

from tests.conftest import VALID_TOKEN, seed_device, session

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
    from nso_adapter.store.models import InterfaceIpAddress

    async with session() as db:
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name=interface_name,
                address=address,
                vrf=vrf,
                family=family,
                secondary=secondary,
                last_refreshed_at=last_refreshed_at,
                refresh_source=refresh_source,
            )
        )
        await db.commit()


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
    await _seed_ip(
        device_id,
        interface_name="GigabitEthernet0/1",
        address="10.0.1.1/24",
        refresh_source="poll",
        last_refreshed_at=older_ts,
    )
    await _seed_ip(
        device_id,
        interface_name="GigabitEthernet0/2",
        address="10.0.2.1/24",
        refresh_source="notification",
        last_refreshed_at=newer_ts,
    )

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
    from nso_adapter.store.models import DbInterface

    async with session() as db:
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
    from sqlalchemy import select

    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await seed_device(nso_device_name="ip-intent-dev-02", netbox_device_id=901)
    iface_id = await _seed_interface(device_id, "GigabitEthernet0/2")

    # First PUT: insert two addresses
    payload1 = {
        "addresses": [
            {"interface": "GigabitEthernet0/2", "address": "10.0.0.1/24", "family": "ipv4"},
            {"interface": "GigabitEthernet0/2", "address": "10.0.0.2/24", "family": "ipv4"},
        ]
    }
    seed = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=payload1)
    assert seed.status_code == 200, seed.text

    # Second PUT: only one address
    payload2 = {
        "addresses": [
            {"interface": "GigabitEthernet0/2", "address": "10.0.0.1/24", "family": "ipv4"},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=payload2)
    assert resp.json()["address_count"] == 1

    async with session() as db:
        result = await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id))
        rows = result.scalars().all()
    # Old 10.0.0.2 row should be gone
    assert len(rows) == 1
    assert rows[0].address == "10.0.0.1/24"


async def test_put_ip_intent_removal_enqueues_interface_config_job(adapter_client):
    """Dropping an address on a re-PUT enqueues an interface_config removal (scoped to the
    affected interface) so the device address is actually removed — a merge-PATCH apply can
    never drop it (#5)."""
    from sqlalchemy import select

    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="ip-rm-dev", netbox_device_id=905)
    await _seed_interface(device_id, "Gi0/3")

    p1 = {
        "addresses": [
            {"interface": "Gi0/3", "address": "10.0.0.1/24", "family": "ipv4"},
            {"interface": "Gi0/3", "address": "10.0.0.2/24", "family": "ipv4"},
        ]
    }
    seed = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=p1)
    assert seed.status_code == 200, seed.text

    p2 = {"addresses": [{"interface": "Gi0/3", "address": "10.0.0.1/24", "family": "ipv4"}]}
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=p2)
    assert resp.json()["removed_interfaces"] == 1
    assert resp.json()["replaced"] is True

    async with session() as db:
        removals = [j for j in (await db.execute(select(Job))).scalars().all() if j.job_type == JobType.removal]
        assert len(removals) == 1
        assert removals[0].context == {
            "scope": "interface_config",
            "interfaces": ["Gi0/3"],
            # #104 phase-3: the removed VALUES ride along so run_removal can do the
            # value-grain residue check after the per-instance replace/delete.
            "removed": {"address": [["Gi0/3", "10.0.0.2/24", ""]]},
            "detach": True,
        }


async def test_store_only_ip_shrink_reports_no_device_replacement(adapter_client):
    """A store-only shrink removes the row but does not enqueue a device replacement."""
    from nso_adapter.store.models import InterfaceIpIntent, Job

    device_id = await seed_device(nso_device_name="ip-store-only-shrink", netbox_device_id=907)
    iface_id = await _seed_interface(device_id, "Gi0/4")
    initial = {
        "addresses": [
            {"interface": "Gi0/4", "address": "198.18.0.1/32", "family": "ipv4"},
            {"interface": "Gi0/4", "address": "198.18.0.2/32", "family": "ipv4"},
        ]
    }
    seed = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=initial)
    assert seed.status_code == 200, seed.text

    response = await adapter_client.put(
        f"/api/v1/devices/{device_id}/ip-intent?store_only=true",
        headers=AUTH,
        json={"addresses": [initial["addresses"][0]]},
    )

    assert response.status_code == 200
    assert response.json()["replaced"] is False
    async with session() as db:
        rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        assert [row.address for row in rows] == ["198.18.0.1/32"]
        assert (await db.execute(select(Job).where(Job.device_id == device_id))).scalars().all() == []


async def test_put_ip_intent_removal_captures_values_per_interface(adapter_client):
    """The removal context carries every removed (interface, address, vrf) triple,
    sorted, across interfaces — the #104 phase-3 value-grain residue input."""
    from sqlalchemy import select

    from nso_adapter.store.models import Job, JobType

    device_id = await seed_device(nso_device_name="ip-rm-vals-dev", netbox_device_id=906)
    await _seed_interface(device_id, "Gi0/5")
    await _seed_interface(device_id, "Gi0/6")

    p1 = {
        "addresses": [
            {"interface": "Gi0/5", "address": "10.0.2.1/30", "family": "ipv4"},
            {"interface": "Gi0/5", "address": "10.0.1.1/30", "family": "ipv4", "vrf": "CUST"},
            {"interface": "Gi0/6", "address": "10.0.3.1/30", "family": "ipv4"},
        ]
    }
    seed = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=p1)
    assert seed.status_code == 200, seed.text

    p2 = {"addresses": [{"interface": "Gi0/6", "address": "10.0.3.1/30", "family": "ipv4"}]}
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=p2)
    assert resp.json()["removed_interfaces"] == 1

    async with session() as db:
        removals = [j for j in (await db.execute(select(Job))).scalars().all() if j.job_type == JobType.removal]
        assert len(removals) == 1
        assert removals[0].context["interfaces"] == ["Gi0/5"]
        assert removals[0].context["removed"] == {
            "address": [["Gi0/5", "10.0.1.1/30", "CUST"], ["Gi0/5", "10.0.2.1/30", ""]]
        }


async def test_put_ip_intent_unknown_interface_lands(adapter_client):
    """Intent for an unknown (greenfield) interface must LAND, not be silently dropped.

    Silently skipping was a disaster waiting to happen: the operator accepts an IP, it
    looks applied, but it vanished at the adapter with no trace. The intent must be stored
    (a minimal interface row materialised) so it's visible; the *apply* then reports whether
    the interface can be realised on the device.
    """
    from sqlalchemy import select

    from nso_adapter.store.models import DbInterface, InterfaceIpIntent

    device_id = await seed_device(nso_device_name="ip-intent-dev-03", netbox_device_id=902)
    # No interfaces seeded — GigabitEthernet99/99 is unknown to the adapter.

    payload = {
        "addresses": [
            {"interface": "GigabitEthernet99/99", "address": "192.168.1.1/30", "family": "ipv4"},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=payload)
    assert resp.status_code == 200
    assert resp.json()["address_count"] == 1  # landed, not dropped

    async with session() as db:
        iface = (
            await db.execute(
                select(DbInterface).where(
                    DbInterface.device_id == device_id, DbInterface.name == "GigabitEthernet99/99"
                )
            )
        ).scalar_one()
        rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1 and rows[0].address == "192.168.1.1/30"


async def test_put_ip_intent_greenfield_routed_creates_interface(adapter_client):
    """An unknown interface carrying Nokia routed binding → DbInterface is materialised."""
    from sqlalchemy import select

    from nso_adapter.store.models import DbInterface, InterfaceIpIntent

    device_id = await seed_device(nso_device_name="ip-intent-gf", netbox_device_id=910)
    payload = {
        "addresses": [
            {
                "interface": "LAG99:99",
                "address": "198.18.249.160/31",
                "family": "ipv4",
                "routed": True,
                "parent_binding": "lag-99",
                "encap_tag": "99",
            }
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=payload)
    assert resp.status_code == 200
    assert resp.json()["address_count"] == 1

    async with session() as db:
        iface = (
            await db.execute(
                select(DbInterface).where(DbInterface.device_id == device_id, DbInterface.name == "LAG99:99")
            )
        ).scalar_one()
        assert iface.kind == "logical"
        assert iface.parent_binding == "lag-99"
        assert iface.encap_tag == "99"
        rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].address == "198.18.249.160/31"


async def test_put_ip_intent_routed_backfills_binding_only_when_missing(adapter_client):
    """Existing imported interface: routed entry backfills null binding, never clobbers."""
    from sqlalchemy import select

    from nso_adapter.store.models import DbInterface

    device_id = await seed_device(nso_device_name="ip-intent-bf", netbox_device_id=911)
    await _seed_interface(device_id, "LAG99:10")  # imported, no binding columns set
    payload = {
        "addresses": [
            {
                "interface": "LAG99:10",
                "address": "10.0.0.0/31",
                "family": "ipv4",
                "routed": True,
                "parent_binding": "lag-99",
                "encap_tag": "10",
            }
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", headers=AUTH, json=payload)
    assert resp.status_code == 200
    async with session() as db:
        iface = (
            await db.execute(
                select(DbInterface).where(DbInterface.device_id == device_id, DbInterface.name == "LAG99:10")
            )
        ).scalar_one()
        assert iface.parent_binding == "lag-99"
        assert iface.encap_tag == "10"


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
