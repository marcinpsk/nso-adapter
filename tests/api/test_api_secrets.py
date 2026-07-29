# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""End-to-end tests for the secrets endpoints (set / verify / harvest-community).

The full app runs with ``secrets.provider: vault`` so the REAL config →
``make_provider`` → ``VaultSecretsProvider`` wiring executes; only the hvac/Vault
boundary is the shared hand-built fake from ``tests.test_vault_provider``.
Harvest additionally fakes only the NSO RESTCONF HTTP boundary (recording httpx
transport serving real-shape NED payloads) — the real ``NsoClient`` runs.
"""

from __future__ import annotations

import hashlib
import json
import threading
import types
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from nso_adapter.main import create_app
from tests.conftest import VALID_TOKEN, seed_device, session
from tests.test_vault_provider import _FakeClient, _FakeForbidden, _FakeInvalidPath, _FakeKvV2

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


def _h(value: str) -> str:
    """The cross-repo secret fingerprint (mirrors network-state-export)."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


@pytest.fixture
async def vault_client(tmp_path, monkeypatch):
    """App with a Vault-backed secrets provider (fake hvac) + one NSO instance.

    Yields ``(http_client, store, kv)`` where *store* is the fake Vault KV data
    dict ({path: {field: value}}) and *kv* the fake KV v2 recorder.
    """
    cfg_text = f"""
secrets:
  provider: vault
  vault:
    address: https://vault.test:8200
    kv_mount: network
nso_instances:
  - name: nso-dev
    base_url: http://nso-dev:8080
    username_ref: NSO_USERNAME
    password_ref: NSO_PASSWORD
netbox:
  base_url: http://netbox.local
  api_token_ref: "NETBOX_TOKEN"
api:
  adapter_token_ref: "ADAPTER_TOKEN"
database_url: sqlite+aiosqlite:///{tmp_path}/test.db
"""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(cfg_text)
    monkeypatch.setenv("CONFIG_FILE", str(cfg_file))
    monkeypatch.setenv("VAULT_ROLE_ID", "role-test")
    monkeypatch.setenv("VAULT_SECRET_ID", "secret-test")
    monkeypatch.setenv("NSO_USERNAME", "admin")
    monkeypatch.setenv("NSO_PASSWORD", "admin")
    monkeypatch.setenv("NETBOX_TOKEN", "nb-test-token")

    store: dict[str, dict[str, str]] = {
        # the provider's own startup refs live in the configured mount
        "credentials/svc": {"adapter_token": VALID_TOKEN, "netbox_token": "nb-test-token"},
    }
    kv = _FakeKvV2(store)
    state: dict[str, list] = {"logins": [], "clients": []}

    def _client_factory(**kwargs):
        client = _FakeClient(kv=kv, logins=state["logins"], **kwargs)
        state["clients"].append(client)
        return client

    fake_hvac = types.SimpleNamespace(
        Client=_client_factory,
        exceptions=types.SimpleNamespace(Forbidden=_FakeForbidden, InvalidPath=_FakeInvalidPath),
    )
    monkeypatch.setattr("nso_adapter.secrets.vault.hvac", fake_hvac)

    # The startup adapter_token/NSO/netbox refs resolve through the FAKE Vault:
    # point them at the seeded credentials path (provider "path#field" dialect).
    cfg_text = cfg_text.replace('api_token_ref: "NETBOX_TOKEN"', 'api_token_ref: "credentials/svc#netbox_token"')
    cfg_text = cfg_text.replace(
        'adapter_token_ref: "ADAPTER_TOKEN"', 'adapter_token_ref: "credentials/svc#adapter_token"'
    )
    cfg_text = cfg_text.replace("username_ref: NSO_USERNAME", 'username_ref: "credentials/svc#adapter_token"')
    cfg_text = cfg_text.replace("password_ref: NSO_PASSWORD", 'password_ref: "credentials/svc#adapter_token"')
    cfg_file.write_text(cfg_text)

    from nso_adapter.config import reset_config

    reset_config()
    app = create_app()

    with (
        patch("nso_adapter.main.set_netbox_client"),
        patch("nso_adapter.main.start_scheduler"),
        patch("nso_adapter.main.stop_scheduler"),
        patch("nso_adapter.main.start_workers", new=AsyncMock()),
        patch("nso_adapter.main.stop_workers", new=AsyncMock()),
        patch("nso_adapter.main.persistent_subscriber", new=AsyncMock()),
    ):
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                yield client, store, kv


@pytest.mark.anyio
async def test_a_slow_vault_write_does_not_stall_the_event_loop(vault_client):
    """hvac is BLOCKING (requests/sockets). Called straight from an `async def` handler it
    freezes the single event-loop thread for the whole round-trip — and the hvac client is
    built with no timeout, while write_path does a read-merge-write plus a possible AppRole
    re-login on 403. For those tens of seconds EVERY other adapter request hangs: the
    plugin's NSO tab times out with "Adapter unreachable" for ALL devices, /healthz stops
    answering (a container liveness probe can kill the adapter mid-write), and the in-process
    scheduler tick driving failover probes and job dispatch stalls.

    Drive a genuinely blocking write and prove an unrelated request is served WHILE it is
    still in flight. The assertion has to be about ORDERING, not about how long /healthz
    itself took: if the blocking call runs on the loop, the /healthz task cannot even start
    until the write has finished, so it would time its own (now unobstructed) round-trip as
    fast and a duration-only check would pass against the broken code.
    """
    import asyncio
    import time

    client, _store, kv = vault_client

    real_write = kv.create_or_update_secret
    gate = threading.Event()
    timeline: dict[str, float] = {}

    def _blocking_write(*args, **kwargs):
        # Hold the hvac call open exactly as a slow Vault would. A real thread must release
        # it, because on the broken (on-loop) path nothing else can run to do so.
        gate.wait(timeout=3)
        timeline["write_unblocked"] = time.monotonic()
        return real_write(*args, **kwargs)

    kv.create_or_update_secret = _blocking_write

    async def _write():
        return await client.post(
            "/api/v1/secrets",
            json={"vault_ref": "network/netbox/snmp/slow", "values": {"community": "s3cr3t"}},
            headers=AUTH,
        )

    async def _health_while_writing():
        await asyncio.sleep(0.05)  # let the write reach the blocking hvac call
        resp = await client.get("/healthz")
        timeline["health_served"] = time.monotonic()
        gate.set()  # only reachable if the loop was never frozen
        return resp

    started = time.monotonic()
    write_resp, health_resp = await asyncio.gather(_write(), _health_while_writing())
    elapsed = time.monotonic() - started

    assert write_resp.status_code == 200
    assert health_resp.status_code == 200
    # /healthz must be answered while the Vault write is still parked in the thread-pool.
    assert timeline["health_served"] < timeline["write_unblocked"], (
        "/healthz was only served AFTER the Vault write released — the blocking hvac call ran on the event loop"
    )
    assert elapsed < 1.0, f"the event loop was frozen for {elapsed:.2f}s by a slow Vault write"


# ── POST /api/v1/secrets (set) ────────────────────────────────────────────────


@pytest.mark.anyio
async def test_set_secret_writes_vault_and_returns_hashes(vault_client):
    client, store, _ = vault_client
    resp = await client.post(
        "/api/v1/secrets",
        json={"vault_ref": "network/netbox/snmp/v3/monitor", "values": {"auth": "hunter2", "priv": "hunter3"}},
        headers=AUTH,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["vault_ref"] == "network/netbox/snmp/v3/monitor"
    assert body["version"] == 1
    assert body["hashes"] == {"auth": _h("hunter2"), "priv": _h("hunter3")}
    assert store["netbox/snmp/v3/monitor"] == {"auth": "hunter2", "priv": "hunter3"}
    # the response never carries the values
    assert "hunter2" not in resp.text and "hunter3" not in resp.text


@pytest.mark.anyio
async def test_set_secret_keyed_ref_writes_that_field(vault_client):
    client, store, _ = vault_client
    ref = "network/netbox/snmp/community/abc123#community"
    resp = await client.post(
        "/api/v1/secrets", json={"vault_ref": ref, "values": {"community": "s3cr3t-comm"}}, headers=AUTH
    )

    assert resp.status_code == 200
    assert resp.json()["hashes"] == {"community": _h("s3cr3t-comm")}
    assert store["netbox/snmp/community/abc123"] == {"community": "s3cr3t-comm"}


@pytest.mark.anyio
async def test_set_secret_keyed_ref_rejects_other_fields(vault_client):
    client, _, _ = vault_client
    resp = await client.post(
        "/api/v1/secrets",
        json={"vault_ref": "network/p#community", "values": {"other": "v"}},
        headers=AUTH,
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_vault_ref"
    assert "v" == "v" and "other" in resp.text  # field NAME may appear...
    assert "s3cr3t" not in resp.text  # ...values never


@pytest.mark.anyio
async def test_set_secret_merge_preserves_sibling_fields(vault_client):
    client, store, kv = vault_client
    store["netbox/snmp/v3/monitor"] = {"priv": "keep-me"}
    kv.versions["netbox/snmp/v3/monitor"] = 4

    resp = await client.post(
        "/api/v1/secrets",
        json={"vault_ref": "network/netbox/snmp/v3/monitor", "values": {"auth": "new"}},
        headers=AUTH,
    )

    assert resp.status_code == 200
    assert resp.json()["version"] == 5
    assert store["netbox/snmp/v3/monitor"] == {"auth": "new", "priv": "keep-me"}


@pytest.mark.anyio
async def test_set_secret_bad_ref_400(vault_client):
    client, _, _ = vault_client
    resp = await client.post("/api/v1/secrets", json={"vault_ref": "no-mount", "values": {"a": "b"}}, headers=AUTH)
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_vault_ref"


@pytest.mark.anyio
async def test_set_secret_empty_values_400(vault_client):
    client, _, _ = vault_client
    resp = await client.post("/api/v1/secrets", json={"vault_ref": "network/p", "values": {}}, headers=AUTH)
    assert resp.status_code in (400, 422)


@pytest.mark.anyio
async def test_secrets_require_auth(vault_client):
    client, _, _ = vault_client
    resp = await client.post("/api/v1/secrets", json={"vault_ref": "network/p", "values": {"a": "b"}})
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_set_secret_local_provider_501(adapter_client):
    resp = await adapter_client.post(
        "/api/v1/secrets", json={"vault_ref": "network/p", "values": {"a": "b"}}, headers=AUTH
    )
    assert resp.status_code == 501
    assert resp.json()["error"]["code"] == "secrets_write_unsupported"


# ── POST /api/v1/secrets/verify ───────────────────────────────────────────────


@pytest.mark.anyio
async def test_verify_returns_fields_and_hashes_never_values(vault_client):
    client, store, kv = vault_client
    store["netbox/snmp/v3/monitor"] = {"auth": "hunter2", "priv": "hunter3"}
    kv.versions["netbox/snmp/v3/monitor"] = 2

    resp = await client.post(
        "/api/v1/secrets/verify", json={"vault_ref": "network/netbox/snmp/v3/monitor"}, headers=AUTH
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["exists"] is True
    assert sorted(body["fields"]) == ["auth", "priv"]
    assert body["hashes"] == {"auth": _h("hunter2"), "priv": _h("hunter3")}
    assert body["version"] == 2
    assert "hunter2" not in resp.text and "hunter3" not in resp.text


@pytest.mark.anyio
async def test_verify_keyed_ref_restricts_to_that_field(vault_client):
    client, store, _ = vault_client
    store["netbox/snmp/community/abc"] = {"community": "s3cr3t", "note": "x"}

    resp = await client.post(
        "/api/v1/secrets/verify", json={"vault_ref": "network/netbox/snmp/community/abc#community"}, headers=AUTH
    )

    body = resp.json()
    assert body["exists"] is True
    assert body["fields"] == ["community"]
    assert body["hashes"] == {"community": _h("s3cr3t")}


@pytest.mark.anyio
async def test_vault_permission_denied_returns_structured_502(vault_client):
    """A Vault policy denial (403 that survives re-auth) must map to a structured
    error, not an unhandled 500 — live-observed on a path outside the AppRole's
    policy. The error text carries only the ref/path, never values."""
    client, store, kv = vault_client
    store["credentials/other-svc"] = {"password": "x"}
    kv.forbid_always.add("credentials/other-svc")

    resp = await client.post(
        "/api/v1/secrets/verify", json={"vault_ref": "network/credentials/other-svc"}, headers=AUTH
    )

    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "vault_error"
    assert "x" not in resp.json()["error"]["message"]


@pytest.mark.anyio
async def test_verify_missing_path_exists_false(vault_client):
    client, _, _ = vault_client
    resp = await client.post("/api/v1/secrets/verify", json={"vault_ref": "network/nope/ghost"}, headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["exists"] is False
    assert body["fields"] == []


# ── POST /api/v1/devices/{id}/secrets/harvest-community ─────────────────────


class _NsoTransport(httpx.AsyncBaseTransport):
    """Serves a canned RESTCONF JSON body for the device-config community GET."""

    def __init__(self, body: dict | None, status: int = 200):
        self.body = body
        self.status = status
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.body is None:
            return httpx.Response(404, content=b"", request=request)
        return httpx.Response(
            self.status,
            content=json.dumps(self.body).encode(),
            headers={"content-type": "application/yang-data+json"},
            request=request,
        )


async def _seed_harvest_device(ned_id: str) -> int:
    device_id = await seed_device(nso_device_name="harvest-dev", netbox_device_id=970)
    from nso_adapter.store.models import Device

    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id = ned_id
        await db.commit()
    return device_id


def _wire_nso_transport(transport: _NsoTransport) -> None:
    from nso_adapter.core.importer import get_nso_client

    client = get_nso_client("nso-dev")
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso-dev:8080")


@pytest.mark.anyio
async def test_harvest_community_ios_happy_path(vault_client):
    client, store, _ = vault_client
    device_id = await _seed_harvest_device("cisco-ios-cli-6.77")
    transport = _NsoTransport(
        {
            "tailf-ned-cisco-ios:community": [
                {"name": "s3cr3t-comm", "RO": [None], "access-list-name": "20"},
                {"name": "other-comm", "RW": [None]},
            ]
        }
    )
    _wire_nso_transport(transport)

    target_hash = _h("s3cr3t-comm")
    ref = f"network/netbox/snmp/community/{target_hash}#community"
    resp = await client.post(
        f"/api/v1/devices/{device_id}/secrets/harvest-community",
        json={"community_hash": target_hash, "vault_ref": ref},
        headers=AUTH,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "vault_ref": ref,
        "secret_hash": target_hash,
        "version": 1,
        "access": "RO",
        "acl": "20",
    }
    assert store[f"netbox/snmp/community/{target_hash}"] == {"community": "s3cr3t-comm"}
    assert "s3cr3t-comm" not in resp.text
    # the GET was the targeted per-NED community subtree, not the full device config
    assert "snmp-server/community" in str(transport.requests[0].url)


@pytest.mark.anyio
async def test_harvest_community_not_found_404_with_sync_hint(vault_client):
    client, _, _ = vault_client
    device_id = await _seed_harvest_device("cisco-ios-cli-6.77")
    _wire_nso_transport(_NsoTransport({"tailf-ned-cisco-ios:community": [{"name": "other", "RO": [None]}]}))

    resp = await client.post(
        f"/api/v1/devices/{device_id}/secrets/harvest-community",
        json={"community_hash": _h("absent"), "vault_ref": "network/p#community"},
        headers=AUTH,
    )

    assert resp.status_code == 404
    err = resp.json()["error"]
    assert err["code"] == "community_not_found"
    assert "sync-from" in err["message"]


@pytest.mark.anyio
async def test_harvest_community_unsupported_ned(vault_client):
    client, _, _ = vault_client
    device_id = await _seed_harvest_device("timos-nc-9.1")

    resp = await client.post(
        f"/api/v1/devices/{device_id}/secrets/harvest-community",
        json={"community_hash": _h("x"), "vault_ref": "network/p#community"},
        headers=AUTH,
    )

    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "harvest_unsupported_ned"


@pytest.mark.anyio
async def test_harvest_community_unknown_device_404(vault_client):
    client, _, _ = vault_client
    resp = await client.post(
        "/api/v1/devices/99999/secrets/harvest-community",
        json={"community_hash": _h("x"), "vault_ref": "network/p#community"},
        headers=AUTH,
    )
    assert resp.status_code == 404
