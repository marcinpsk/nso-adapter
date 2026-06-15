# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/intent-summary."""

from __future__ import annotations

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def test_intent_summary_empty(adapter_client):
    device_id = await seed_device(nso_device_name="isum-empty", netbox_device_id=940)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent-summary", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    assert body["scopes"] == {}


async def test_intent_summary_counts_device_scope(adapter_client):
    """An OSPF instance intent row shows up in the per-scope summary with apply state."""
    from datetime import UTC, datetime

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await seed_device(nso_device_name="isum-ospf", netbox_device_id=941)
    async for db in get_session():
        db.add(
            OspfInstanceIntent(
                device_id=device_id,
                process_id="1",
                router_id="1.1.1.1",
                accepted_at=datetime.now(UTC).replace(tzinfo=None),
            )
        )
        await db.commit()
        break

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent-summary", headers=AUTH)
    assert resp.status_code == 200
    scopes = resp.json()["scopes"]
    assert "ospf_instance_intent" in scopes
    assert scopes["ospf_instance_intent"]["count"] == 1
    assert scopes["ospf_instance_intent"]["applied"] == 0  # last_apply_at is NULL


async def test_intent_summary_counts_interface_scope(adapter_client):
    """interface_id-keyed intent (interface_ip) is joined through interfaces correctly."""
    from datetime import UTC, datetime

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DbInterface, InterfaceIpIntent

    device_id = await seed_device(nso_device_name="isum-ip", netbox_device_id=942)
    async for db in get_session():
        iface = DbInterface(device_id=device_id, name="Gi0/1")
        db.add(iface)
        await db.flush()
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id,
                address="10.0.0.1/31",
                vrf="",
                family="ipv4",
                accepted_at=datetime.now(UTC).replace(tzinfo=None),
            )
        )
        await db.commit()
        break

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/intent-summary", headers=AUTH)
    assert resp.status_code == 200
    scopes = resp.json()["scopes"]
    assert scopes.get("interface_ip_intent", {}).get("count") == 1


async def test_intent_summary_404_unknown_device(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/99999/intent-summary", headers=AUTH)
    assert resp.status_code == 404


async def test_intent_summary_requires_auth(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/1/intent-summary")
    assert resp.status_code == 401
