# SPDX-License-Identifier: Apache-2.0
"""Tests for the failover config endpoints: GET/PUT /api/v1/config/failover.

End-to-end through the real FastAPI app + the real SQLAlchemy store — only the HTTP client
is the test harness. Proves the singleton round-trips and the GET reflects the live values.
"""

from __future__ import annotations

from tests.conftest import VALID_TOKEN

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def test_get_failover_config_defaults_when_unset(adapter_client):
    """With no FailoverConfig row, GET returns the static SchedulerConfig fallbacks."""
    resp = await adapter_client.get("/api/v1/config/failover", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    # SchedulerConfig test defaults (no DB row yet): primary 2m, oob 15m, thresholds 3/5.
    assert body["primary_probe_interval"] == 2
    assert body["oob_probe_interval"] == 15
    assert body["failure_threshold"] == 3
    assert body["success_threshold"] == 5
    assert body["probe_timeout"] == 10.0
    assert body["probe_concurrency"] == 8
    assert body["max_flips_per_tick"] == 8


async def test_put_failover_config_persists_and_get_reflects(adapter_client):
    """PUT writes the singleton; the response and a subsequent GET both show the new values."""
    payload = {
        "enabled": False,
        "primary_probe_interval": 15,
        "oob_probe_interval": 360,
        "failure_threshold": 4,
        "success_threshold": 6,
        "probe_timeout": 7.5,
        "probe_concurrency": 12,
        "max_flips_per_tick": 10,
        "sync_from_after_switch": False,
    }
    put = await adapter_client.put("/api/v1/config/failover", json=payload, headers=AUTH)
    assert put.status_code == 200
    assert put.json() == payload

    got = await adapter_client.get("/api/v1/config/failover", headers=AUTH)
    assert got.status_code == 200
    assert got.json() == payload


async def test_put_failover_config_is_partial(adapter_client):
    """A second PUT with a subset changes only those fields (singleton upsert, not replace)."""
    await adapter_client.put(
        "/api/v1/config/failover",
        json={"primary_probe_interval": 20, "enabled": True},
        headers=AUTH,
    )
    # Now change just the timeout — the interval must survive.
    resp = await adapter_client.put("/api/v1/config/failover", json={"probe_timeout": 5.0}, headers=AUTH)
    body = resp.json()
    assert body["probe_timeout"] == 5.0
    assert body["primary_probe_interval"] == 20  # untouched
    assert body["enabled"] is True


async def test_put_failover_config_rejects_out_of_range(adapter_client):
    """Validation guards an operator typo (e.g. a zero interval) from wedging the loop."""
    resp = await adapter_client.put("/api/v1/config/failover", json={"primary_probe_interval": 0}, headers=AUTH)
    assert resp.status_code == 422


async def test_put_failover_config_caps_concurrency_to_pool(adapter_client):
    """probe_concurrency is capped (≤16) so it can't exceed the DB pool and starve API/sync."""
    assert (
        await adapter_client.put("/api/v1/config/failover", json={"probe_concurrency": 17}, headers=AUTH)
    ).status_code == 422
    assert (
        await adapter_client.put("/api/v1/config/failover", json={"probe_concurrency": 16}, headers=AUTH)
    ).status_code == 200


async def test_failover_config_requires_auth(adapter_client):
    """Both verbs require the adapter token."""
    assert (await adapter_client.get("/api/v1/config/failover")).status_code == 401
    assert (await adapter_client.put("/api/v1/config/failover", json={})).status_code == 401
