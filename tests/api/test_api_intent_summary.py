# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/intent-summary."""

from __future__ import annotations

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def test_intent_summary_empty(adapter_client):
    device_id = await seed_device(nso_device_name="isum-empty", netbox_device_id=940)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent-summary", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["scopes"] == {}
    assert body["pending_clear"] == {}


async def test_intent_summary_counts_device_scope(adapter_client):
    """An OSPF instance intent row shows up in the per-scope summary with apply state."""
    from datetime import UTC, datetime

    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await seed_device(nso_device_name="isum-ospf", netbox_device_id=941)
    async with session() as db:
        db.add(
            OspfInstanceIntent(
                device_id=device_id,
                process_id="1",
                router_id="1.1.1.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent-summary", headers=AUTH)
    assert resp.status_code == 200
    scopes = resp.json()["scopes"]
    assert "ospf_instance_intent" in scopes
    assert scopes["ospf_instance_intent"]["count"] == 1
    assert scopes["ospf_instance_intent"]["applied"] == 0  # last_apply_at is NULL


async def test_intent_summary_counts_interface_scope(adapter_client):
    """interface_id-keyed intent (interface_ip) is joined through interfaces correctly."""
    from datetime import UTC, datetime

    from nso_adapter.store.models import DbInterface, InterfaceIpIntent

    device_id = await seed_device(nso_device_name="isum-ip", netbox_device_id=942)
    async with session() as db:
        iface = DbInterface(device_id=device_id, name="Gi0/1")
        db.add(iface)
        await db.flush()
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id,
                address="10.0.0.1/31",
                vrf="",
                family="ipv4",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent-summary", headers=AUTH)
    assert resp.status_code == 200
    scopes = resp.json()["scopes"]
    assert scopes.get("interface_ip_intent", {}).get("count") == 1


async def test_intent_summary_surfaces_pending_clear_provenance_and_since(adapter_client):
    """Pending clears are device-level counts, never leaf or path disclosure."""
    from datetime import UTC, datetime

    from nso_adapter.store.models import StreamPendingClear

    device_id = await seed_device(nso_device_name="isum-pending", netbox_device_id=943)
    async with session() as db:
        db.add_all(
            [
                StreamPendingClear(
                    device_id=device_id,
                    stream="isis",
                    provenance="authorized",
                    revision=4,
                    recorded_at=datetime(2026, 8, 25, 10, 30, tzinfo=UTC),
                ),
                StreamPendingClear(
                    device_id=device_id,
                    stream="ospf",
                    provenance="store_only",
                    revision=7,
                    recorded_at=datetime(2026, 8, 25, 11, 45, tzinfo=UTC),
                ),
            ]
        )
        await db.commit()

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent-summary", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["pending_clear"] == {
        "isis": {"provenance": "authorized", "since": "2026-08-25T10:30:00Z"},
        "ospf": {"provenance": "store_only", "since": "2026-08-25T11:45:00Z"},
    }


async def test_intent_summary_404_unknown_device(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/99999/intent-summary", headers=AUTH)
    assert resp.status_code == 404


async def test_intent_summary_requires_auth(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/1/intent-summary")
    assert resp.status_code == 401
