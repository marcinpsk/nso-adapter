# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for GET /api/v1/devices/{id}/l2-services."""

from __future__ import annotations

from tests.conftest import VALID_TOKEN, seed_device, seed_l2_saps, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def test_l2_services_no_data_returns_empty(adapter_client):
    device_id = await seed_device(nso_device_name="l2-empty", netbox_device_id=860)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/l2-services", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    # read_state rides every family GET (S4); byte-level pins live in the golden test.
    assert body.pop("read_state")["outcome"] == "unavailable"
    assert body == {"device_id": device_id, "services": []}


async def test_l2_services_grouped_by_service(adapter_client):
    device_id = await seed_device(nso_device_name="l2-ra1", netbox_device_id=861)
    await seed_l2_saps(
        device_id,
        [
            {
                "service_name": "TL",
                "service_type": "epipe",
                "service_id": 4022,
                "saps": [
                    {"sap_id": "lag-60:3999", "port": "lag-60", "outer_tag": 3999},
                    {"sap_id": "lag-60:4022", "port": "lag-60", "outer_tag": 4022},
                ],
            },
            {
                "service_name": "701",
                "service_type": "vpls",
                "saps": [{"sap_id": "1/1/c31/3:701", "port": "1/1/c31/3", "outer_tag": 701}],
            },
        ],
    )
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/l2-services", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["device_id"] == device_id
    services = {s["service_name"]: s for s in body["services"]}
    assert services["TL"]["service_type"] == "epipe"
    assert services["TL"]["service_id"] == 4022
    assert len(services["TL"]["saps"]) == 2
    assert services["701"]["service_type"] == "vpls"
    assert services["701"]["service_id"] is None
    assert services["701"]["saps"][0] == {
        "sap_id": "1/1/c31/3:701",
        "port": "1/1/c31/3",
        "outer_tag": 701,
        "inner_tag": None,
    }


async def test_l2_services_unknown_device_is_404(adapter_client):
    resp = await adapter_client.get("/api/v1/devices/999999/l2-services", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


# ── PUT /api/v1/devices/{id}/l2-sap-intent ────────────────────────


async def _get_intents(device_id: int):
    from sqlalchemy import select

    from nso_adapter.store.models import L2SapIntent

    async with session() as db:
        rows = (await db.execute(select(L2SapIntent).where(L2SapIntent.device_id == device_id))).scalars().all()
        return {(r.service_name, r.sap_id): r for r in rows}
    return {}


async def test_put_l2_sap_intent_upserts(adapter_client):
    device_id = await seed_device(nso_device_name="l2-intent-up", netbox_device_id=880)
    body = {
        "saps": [
            {
                "service_name": "TL",
                "service_type": "epipe",
                "sap_id": "lag-60:3999",
                "port": "lag-60",
                "outer_tag": 3999,
            },
            {
                "service_name": "701",
                "service_type": "vpls",
                "sap_id": "1/1/c31/3:701",
                "port": "1/1/c31/3",
                "outer_tag": 701,
            },
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/l2-sap-intent", json=body, headers=AUTH)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["device_id"] == device_id
    assert payload["count"] == 2
    assert payload["removed"] == 0  # nothing dropped on a fresh upsert
    rows = await _get_intents(device_id)
    assert set(rows) == {("TL", "lag-60:3999"), ("701", "1/1/c31/3:701")}
    assert rows[("TL", "lag-60:3999")].accepted_at is not None
    assert rows[("701", "1/1/c31/3:701")].service_type == "vpls"


async def test_put_l2_sap_intent_full_replace_prunes(adapter_client):
    device_id = await seed_device(nso_device_name="l2-intent-prune", netbox_device_id=881)
    first = {
        "saps": [
            {"service_name": "TL", "service_type": "epipe", "sap_id": "lag-60:3999", "port": "lag-60"},
            {"service_name": "TL", "service_type": "epipe", "sap_id": "lag-60:4022", "port": "lag-60"},
        ]
    }
    await adapter_client.put(f"/api/v1/devices/{device_id}/l2-sap-intent", json=first, headers=AUTH)
    # Second PUT drops the second SAP.
    second = {
        "saps": [
            {"service_name": "TL", "service_type": "epipe", "sap_id": "lag-60:3999", "port": "lag-60"},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/l2-sap-intent", json=second, headers=AUTH)
    assert resp.json()["count"] == 1
    rows = await _get_intents(device_id)
    assert set(rows) == {("TL", "lag-60:3999")}


async def test_put_l2_sap_intent_unknown_device_404(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/999999/l2-sap-intent", json={"saps": []}, headers=AUTH)
    assert resp.status_code == 404


async def test_put_l2_sap_intent_requires_auth(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/1/l2-sap-intent", json={"saps": []})
    assert resp.status_code == 401
