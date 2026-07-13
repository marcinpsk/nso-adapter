# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/snmp-config."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
TS = datetime(2026, 6, 10, 9, 0, 0, tzinfo=UTC)


async def _seed_snmp(
    device_id: int,
    *,
    communities: list[dict] | None = None,
    v3_users: list[dict] | None = None,
    hosts: list[dict] | None = None,
    location: str | None = None,
    contact: str | None = None,
    refresh_source: str = "poll",
    last_refreshed_at: datetime = TS,
) -> None:
    """Seed SNMP rows for a device."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import SnmpCommunity, SnmpHost, SnmpSystemInfo, SnmpV3User

    ts = last_refreshed_at.replace(tzinfo=None)
    async for db in get_session():
        for comm in communities or []:
            db.add(
                SnmpCommunity(
                    device_id=device_id,
                    community_hash=comm["community_hash"],
                    access=comm.get("access", "RO"),
                    acl=comm.get("acl"),
                    last_refreshed_at=ts,
                    refresh_source=refresh_source,
                )
            )
        for user in v3_users or []:
            db.add(
                SnmpV3User(
                    device_id=device_id,
                    username=user["username"],
                    has_auth_secret=user.get("has_auth_secret", False),
                    has_priv_secret=user.get("has_priv_secret", False),
                    last_refreshed_at=ts,
                    refresh_source=refresh_source,
                )
            )
        for host in hosts or []:
            db.add(
                SnmpHost(
                    device_id=device_id,
                    address=host["address"],
                    version=host.get("version"),
                    notify_type=host.get("notify_type"),
                    port=host.get("port"),
                    username=host.get("username"),
                    last_refreshed_at=ts,
                    refresh_source=refresh_source,
                )
            )
        if location or contact:
            db.add(
                SnmpSystemInfo(
                    device_id=device_id,
                    location=location,
                    contact=contact,
                    last_refreshed_at=ts,
                    refresh_source=refresh_source,
                )
            )
        await db.commit()
        break


# ── GET /api/v1/devices/{id}/snmp-config ────────────────────────────────────


@pytest.mark.anyio
async def test_snmp_config_no_data_returns_never(adapter_client):
    """Device with no SNMP rows → 200 with refresh_source='never'."""
    device_id = await seed_device(nso_device_name="snmp-empty-dev", netbox_device_id=950)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["refresh_source"] == "never"
    assert body["last_refreshed_at"] is None
    assert body["communities"] == []
    assert body["v3_users"] == []
    assert body["hosts"] == []
    assert body["system_info"] is None


@pytest.mark.anyio
async def test_snmp_config_communities_returned(adapter_client):
    """Communities are returned with hash, access, acl."""
    device_id = await seed_device(nso_device_name="snmp-comm-dev", netbox_device_id=951)
    await _seed_snmp(
        device_id,
        communities=[
            {"community_hash": "abc123def456abcd", "access": "RO", "acl": "20"},
            {"community_hash": "def456abc123def4", "access": "RW", "acl": None},
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    hashes = {c["community_hash"] for c in body["communities"]}
    assert hashes == {"abc123def456abcd", "def456abc123def4"}
    ro = next(c for c in body["communities"] if c["community_hash"] == "abc123def456abcd")
    assert ro["access"] == "RO"
    assert ro["acl"] == "20"
    rw = next(c for c in body["communities"] if c["community_hash"] == "def456abc123def4")
    assert rw["access"] == "RW"
    assert rw["acl"] is None


@pytest.mark.anyio
async def test_snmp_config_v3_users_returned(adapter_client):
    """v3 users are returned with boolean secret flags."""
    device_id = await seed_device(nso_device_name="snmp-v3-dev", netbox_device_id=952)
    await _seed_snmp(
        device_id,
        v3_users=[
            {"username": "monitor", "has_auth_secret": True, "has_priv_secret": False},
            {"username": "admin", "has_auth_secret": True, "has_priv_secret": True},
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    by_name = {u["username"]: u for u in body["v3_users"]}
    assert by_name["monitor"]["has_auth_secret"] is True
    assert by_name["monitor"]["has_priv_secret"] is False
    assert by_name["admin"]["has_priv_secret"] is True


@pytest.mark.anyio
async def test_snmp_config_hosts_returned(adapter_client):
    """Trap hosts are returned with address, version, notify_type, port."""
    device_id = await seed_device(nso_device_name="snmp-hosts-dev", netbox_device_id=953)
    await _seed_snmp(
        device_id,
        hosts=[
            {"address": "10.0.1.100", "version": "2c", "notify_type": "trap", "port": 162},
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)
    assert resp.status_code == 200
    host = resp.json()["hosts"][0]
    assert host["address"] == "10.0.1.100"
    assert host["version"] == "2c"
    assert host["notify_type"] == "trap"
    assert host["port"] == 162
    assert host["username"] is None  # v1/v2c: the corresponding NED field is the COMMUNITY (secret)


@pytest.mark.anyio
async def test_snmp_config_v3_host_returns_its_SECURITY_USER_NAME(adapter_client):
    """CR-P16: the plugin cannot push a v3 trap host it cannot see the user name of — both NSO host
    writers key the receiver on exactly that field.
    """
    device_id = await seed_device(nso_device_name="snmp-v3host-dev", netbox_device_id=958)
    await _seed_snmp(
        device_id,
        hosts=[{"address": "10.0.1.101", "version": "3", "notify_type": "inform", "username": "netmon-v3"}],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)
    host = resp.json()["hosts"][0]
    assert host["username"] == "netmon-v3"
    assert host["version"] == "3"


@pytest.mark.anyio
async def test_snmp_config_system_info_returned(adapter_client):
    """Location and contact are returned in system_info."""
    device_id = await seed_device(nso_device_name="snmp-sysinfo-dev", netbox_device_id=954)
    await _seed_snmp(device_id, location="ITC-Lab", contact="noc@example.com")
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)
    assert resp.status_code == 200
    info = resp.json()["system_info"]
    assert info["location"] == "ITC-Lab"
    assert info["contact"] == "noc@example.com"


@pytest.mark.anyio
async def test_snmp_config_unknown_device_returns_404(adapter_client):
    """Non-existent device_id → 404 not_found."""
    resp = await adapter_client.get("/api/v1/devices/9999/snmp-config", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_snmp_config_requires_auth(adapter_client):
    """Missing Authorization header → 401."""
    resp = await adapter_client.get("/api/v1/devices/1/snmp-config")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_snmp_config_refresh_source_is_most_recent(adapter_client):
    """When communities and hosts exist, last_refreshed_at is the most recent."""
    device_id = await seed_device(nso_device_name="snmp-ts-dev", netbox_device_id=955)
    older = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
    newer = datetime(2026, 6, 10, 9, 0, 0, tzinfo=UTC)
    await _seed_snmp(
        device_id,
        communities=[{"community_hash": "aaabbbcccddd1111", "access": "RO"}],
        refresh_source="polled-sync",
        last_refreshed_at=older,
    )
    await _seed_snmp(
        device_id,
        hosts=[{"address": "10.0.1.100"}],
        refresh_source="notification",
        last_refreshed_at=newer,
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["refresh_source"] == "notification"
    assert "2026-06-10" in body["last_refreshed_at"]
