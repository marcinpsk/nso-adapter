# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for PUT /api/v1/devices/{id}/logging-intent (remote-syslog write path)."""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _count_intent(device_id: int) -> int:
    from sqlalchemy import select

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import LoggingHostIntent

    async for db in get_session():
        rows = (
            (await db.execute(select(LoggingHostIntent).where(LoggingHostIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        return len(rows)
    return 0


@pytest.mark.anyio
async def test_put_logging_intent_stores_rows(adapter_client):
    device_id = await seed_device()
    body = {
        "hosts": [
            {"address": "10.0.0.1", "severity": "informational", "source": "Loopback0"},
            {"address": "10.0.0.2", "port": 6514, "vrf": "MGMT"},
        ]
    }
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/logging-intent", json=body, headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["count"] == 2
    assert await _count_intent(device_id) == 2


@pytest.mark.anyio
async def test_put_logging_intent_full_replace(adapter_client):
    device_id = await seed_device()
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.0.0.1"}, {"address": "10.0.0.2"}]},
        headers=AUTH,
    )
    # Second PUT with only one host → the other is deleted (full-replace).
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.0.0.2", "severity": "debugging"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["count"] == 1
    assert await _count_intent(device_id) == 1


@pytest.mark.anyio
async def test_put_logging_intent_clears_on_empty(adapter_client):
    device_id = await seed_device()
    await adapter_client.put(
        f"/api/v1/devices/{device_id}/logging-intent",
        json={"hosts": [{"address": "10.0.0.1"}]},
        headers=AUTH,
    )
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/logging-intent", json={"hosts": []}, headers=AUTH)
    assert resp.status_code == 200
    assert await _count_intent(device_id) == 0


@pytest.mark.anyio
async def test_put_logging_intent_unknown_device_404(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/9999/logging-intent", json={"hosts": []}, headers=AUTH)
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_put_logging_intent_requires_auth(adapter_client):
    resp = await adapter_client.put("/api/v1/devices/1/logging-intent", json={"hosts": []})
    assert resp.status_code in (401, 403)
