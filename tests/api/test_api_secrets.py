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
from typing import cast
from unittest.mock import AsyncMock, patch
from uuid import UUID

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from nso_adapter.main import create_app
from tests._secret_discipline import assert_text_free_of
from tests.conftest import VALID_TOKEN, seed_device, session
from tests.test_vault_provider import _FakeClient, _FakeForbidden, _FakeInvalidPath, _FakeKvV2

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


def _h(value: str) -> str:
    """The cross-repo secret fingerprint (mirrors network-state-export)."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


@pytest.fixture
async def vault_client(store_engine, pg_url, tmp_path, monkeypatch):
    """App with a Vault-backed secrets provider (fake hvac) + one NSO instance.

    Yields ``(http_client, store, kv)`` where *store* is the fake Vault KV data
    dict ({path: {field: value}}) and *kv* the fake KV v2 recorder.

    Same engine ownership as ``adapter_client``: ``store_engine`` binds and disposes
    the store globals, so the lifespan's own init/dispose are patched out.
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
database_url: {pg_url}
"""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(cfg_text)
    monkeypatch.setenv("CONFIG_FILE", str(cfg_file))
    monkeypatch.setenv("VAULT_ROLE_ID", "role-test")
    monkeypatch.setenv("VAULT_SECRET_ID", "secret-test")
    monkeypatch.setenv("NSO_USERNAME", "placeholder-user")
    monkeypatch.setenv("NSO_PASSWORD", "placeholder-password")
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
        patch("nso_adapter.main.init_db"),
        patch("nso_adapter.main._dispose_engine", new=AsyncMock()),
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
    entered = threading.Event()
    timeline: dict[str, float] = {}
    released_by_health: list[bool] = []
    # Escape hatch, NOT the measurement: /healthz releases the gate in milliseconds whenever
    # the loop is free, so the bound only has to outlast a loaded runner's scheduling delay —
    # and it is what makes the broken (on-loop) path terminate instead of hanging the suite.
    escape_s = 30.0

    def _blocking_write(*args, **kwargs):
        entered.set()
        # Hold the hvac call open exactly as a slow Vault would. A real thread must release
        # it, because on the broken (on-loop) path nothing else can run to do so.
        released_by_health.append(gate.wait(timeout=escape_s))
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
        assert await asyncio.to_thread(entered.wait, escape_s)
        resp = await client.get("/healthz")
        timeline["health_served"] = time.monotonic()
        gate.set()  # only reachable if the loop was never frozen
        return resp

    write_resp, health_resp = await asyncio.gather(_write(), _health_while_writing())

    assert write_resp.status_code == 200
    assert health_resp.status_code == 200
    assert released_by_health == [True], (
        "the Vault write was released by its escape timeout, not by /healthz — the loop never ran while it was parked"
    )
    # /healthz must be answered while the Vault write is still parked in the thread-pool.
    assert timeline["health_served"] < timeline["write_unblocked"], (
        "/healthz was only served AFTER the Vault write released — the blocking hvac call ran on the event loop"
    )


# ── POST /api/v1/secrets (set) ────────────────────────────────────────────────


@pytest.mark.anyio
async def test_set_secret_writes_vault_and_reports_the_version(vault_client):
    client, store, _ = vault_client
    resp = await client.post(
        "/api/v1/secrets",
        json={"vault_ref": "network/netbox/snmp/v3/monitor", "values": {"auth": "hunter2", "priv": "hunter3"}},
        headers=AUTH,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == 1
    assert body["operation_id"], "the answer must still be joinable to its log record"
    assert store["netbox/snmp/v3/monitor"] == {"auth": "hunter2", "priv": "hunter3"}
    # the response never carries the values, the ref, or the field names the caller chose
    assert_text_free_of(resp.text, ["hunter2", "hunter3", "network/netbox/snmp/v3/monitor", "auth", "priv"])


@pytest.mark.anyio
async def test_set_secret_keyed_ref_writes_that_field(vault_client):
    client, store, _ = vault_client
    ref = "network/netbox/snmp/community/abc123#community"
    resp = await client.post(
        "/api/v1/secrets", json={"vault_ref": ref, "values": {"community": "s3cr3t-comm"}}, headers=AUTH
    )

    assert resp.status_code == 200
    assert resp.json()["version"] == 1
    assert store["netbox/snmp/community/abc123"] == {"community": "s3cr3t-comm"}
    assert_text_free_of(resp.text, ["s3cr3t-comm", ref, "abc123", "community"])


@pytest.mark.anyio
async def test_set_secret_keyed_ref_rejects_other_fields(vault_client):
    client, _, _ = vault_client
    resp = await client.post(
        "/api/v1/secrets",
        json={"vault_ref": "network/p#community", "values": {"other": "placeholder-secret-value"}},
        headers=AUTH,
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_vault_ref"
    # Both halves of the mismatch are the caller's own strings: the refusal states the rule.
    assert_text_free_of(resp.text, ["other", "network/p#community", "placeholder-secret-value"])
    assert "exactly that one field" in resp.json()["error"]["message"]


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
async def test_verify_v3_returns_only_fixed_role_presence(vault_client):
    client, store, kv = vault_client
    store["netbox/snmp/v3/monitor"] = {"auth": "hunter2", "priv": "hunter3"}
    kv.versions["netbox/snmp/v3/monitor"] = 2

    resp = await client.post(
        "/api/v1/secrets/verify", json={"vault_ref": "network/netbox/snmp/v3/monitor"}, headers=AUTH
    )

    assert resp.status_code == 200
    body = resp.json()
    operation_id = body.pop("operation_id")
    assert UUID(operation_id).hex == operation_id
    assert body == {
        "status": "present",
        "fingerprint": None,
        "has_auth": True,
        "has_priv": True,
        "version": 2,
    }
    assert_text_free_of(resp.text, ["hunter2", "hunter3"])


@pytest.mark.anyio
async def test_verify_keyed_ref_returns_a_scalar_fingerprint_without_the_field_name(vault_client):
    client, store, kv = vault_client
    path = "netbox/snmp/community/abc"
    key = "placeholder-selected-field"
    store[path] = {key: "placeholder-selected-value", "placeholder-sibling-field": "placeholder-sibling-value"}
    kv.versions[path] = 4

    resp = await client.post("/api/v1/secrets/verify", json={"vault_ref": f"network/{path}#{key}"}, headers=AUTH)

    body = resp.json()
    assert body["status"] == "present"
    assert body["fingerprint"] == _h("placeholder-selected-value")
    assert body["has_auth"] is False
    assert body["has_priv"] is False
    assert body["version"] == 4
    assert_text_free_of(
        resp.text,
        [key, "placeholder-selected-value", "placeholder-sibling-field", "placeholder-sibling-value"],
    )


@pytest.mark.anyio
async def test_verify_rejects_a_non_string_vault_field_as_a_sanitized_502(vault_client):
    """Vault is an external boundary, so its response cannot rely on type annotations."""
    client, store, _ = vault_client
    path = "netbox/snmp/community/malformed"
    key = "placeholder-selected-field"
    store[path] = cast(dict[str, str], {key: 42})

    resp = await client.post("/api/v1/secrets/verify", json={"vault_ref": f"network/{path}#{key}"}, headers=AUTH)

    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "vault_error"
    assert_text_free_of(resp.text, [path, key])


@pytest.mark.anyio
async def test_verify_ignores_a_non_string_sibling_when_the_selected_field_is_valid(vault_client):
    client, store, _ = vault_client
    path = "netbox/snmp/community/with-metadata"
    key = "placeholder-selected-field"
    store[path] = cast(
        dict[str, str],
        {key: "placeholder-selected-value", "placeholder-metadata": 42},
    )

    resp = await client.post("/api/v1/secrets/verify", json={"vault_ref": f"network/{path}#{key}"}, headers=AUTH)

    assert resp.status_code == 200
    assert resp.json()["fingerprint"] == _h("placeholder-selected-value")
    assert_text_free_of(resp.text, [path, key, "placeholder-metadata", "placeholder-selected-value"])


@pytest.mark.anyio
async def test_vault_permission_denied_returns_structured_502(vault_client):
    """A Vault policy denial (403 that survives re-auth) must map to a structured
    error, not an unhandled 500 - live-observed on a path outside the AppRole's
    policy. The response names the failure and nothing the caller sent."""
    client, store, kv = vault_client
    store["credentials/other-svc"] = {"password": "placeholder-denied-value"}
    kv.forbid_always.add("credentials/other-svc")

    resp = await client.post(
        "/api/v1/secrets/verify", json={"vault_ref": "network/credentials/other-svc"}, headers=AUTH
    )

    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "vault_error"
    assert_text_free_of(
        resp.text,
        ["placeholder-denied-value", "credentials/other-svc", "network/credentials/other-svc"],
    )


@pytest.mark.anyio
async def test_verify_distinguishes_a_missing_path_from_a_missing_selected_field(vault_client):
    client, store, kv = vault_client
    store["present/path"] = {"another-field": "placeholder-value"}
    kv.versions["present/path"] = 7

    missing_field = await client.post(
        "/api/v1/secrets/verify",
        json={"vault_ref": "network/present/path#missing-selected-field"},
        headers=AUTH,
    )
    resp = await client.post("/api/v1/secrets/verify", json={"vault_ref": "network/nope/ghost"}, headers=AUTH)

    assert missing_field.status_code == 200
    assert missing_field.json()["status"] == "missing_field"
    assert missing_field.json()["version"] == 7
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "missing_path"
    assert body["fingerprint"] is None
    assert body["version"] is None


@pytest.mark.anyio
async def test_verify_treats_successful_empty_and_unversioned_paths_as_present(vault_client):
    client, store, kv = vault_client
    store["empty/path"] = {}
    kv.omit_metadata.add("empty/path")
    store["unrelated/path"] = {"placeholder-field": "placeholder-value"}

    empty = await client.post("/api/v1/secrets/verify", json={"vault_ref": "network/empty/path"}, headers=AUTH)
    unrelated = await client.post("/api/v1/secrets/verify", json={"vault_ref": "network/unrelated/path"}, headers=AUTH)

    assert empty.json()["status"] == "present"
    assert empty.json()["version"] is None
    assert empty.json()["has_auth"] is False
    assert empty.json()["has_priv"] is False
    assert unrelated.json()["status"] == "present"
    assert unrelated.json()["has_auth"] is False
    assert unrelated.json()["has_priv"] is False
    assert_text_free_of(unrelated.text, ["placeholder-field", "placeholder-value"])


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
    assert body.pop("operation_id"), "the answer must still be joinable to its log record"
    assert body == {
        "secret_hash": target_hash,
        "version": 1,
        "access": "RO",
        "acl": "20",
    }
    assert_text_free_of(resp.text, [ref, "s3cr3t-comm"])
    assert store[f"netbox/snmp/community/{target_hash}"] == {"community": "s3cr3t-comm"}
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
async def test_a_harvest_hash_THAT_IS_NOT_A_FINGERPRINT_is_refused_at_the_boundary(vault_client):
    """``community_hash`` took any string, and the missing-community 404 echoed it back.

    A caller that pastes the community itself into the field would read its own secret out
    of the refusal body and out of whatever recorded that answer. The field is a sha256[:16]
    fingerprint, so the boundary refuses anything else and nothing of the value travels."""
    from structlog.testing import capture_logs

    from tests._secret_discipline import assert_records_free_of

    client, _, _ = vault_client
    device_id = await _seed_harvest_device("cisco-ios-cli-6.77")
    _wire_nso_transport(_NsoTransport({"tailf-ned-cisco-ios:community": [{"name": "other", "RO": [None]}]}))

    with capture_logs() as logs:
        resp = await client.post(
            f"/api/v1/devices/{device_id}/secrets/harvest-community",
            json={"community_hash": "placeholder-secret-pasted-here", "vault_ref": "network/p#community"},
            headers=AUTH,
        )

    assert resp.status_code == 422, "a value that is not a fingerprint is not a request we can serve"
    assert_text_free_of(resp.text, ["placeholder-secret-pasted-here"])
    assert_records_free_of(logs, ["placeholder-secret-pasted-here"])


@pytest.mark.anyio
async def test_a_missing_community_404_repeats_no_part_of_the_request(vault_client):
    """The refusal states WHICH device could not serve it; the caller holds what it sent."""
    client, _, _ = vault_client
    device_id = await _seed_harvest_device("cisco-ios-cli-6.77")
    _wire_nso_transport(_NsoTransport({"tailf-ned-cisco-ios:community": [{"name": "other", "RO": [None]}]}))

    asked = _h("placeholder-absent-community")
    resp = await client.post(
        f"/api/v1/devices/{device_id}/secrets/harvest-community",
        json={"community_hash": asked, "vault_ref": "network/p#community"},
        headers=AUTH,
    )

    assert resp.status_code == 404
    err = resp.json()["error"]
    assert err["code"] == "community_not_found"
    assert_text_free_of(resp.text, [asked, "harvest-dev"])
    assert f"device {device_id}" in err["message"], "the operator must still learn WHICH device could not serve it"
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
    # The NED id is provider-derived text; the closed code already names the refusal.
    assert_text_free_of(resp.text, ["timos-nc-9.1"])


@pytest.mark.anyio
async def test_harvest_community_unknown_device_404(vault_client):
    client, _, _ = vault_client
    resp = await client.post(
        "/api/v1/devices/99999/secrets/harvest-community",
        json={"community_hash": _h("x"), "vault_ref": "network/p#community"},
        headers=AUTH,
    )
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_a_vault_failure_puts_no_reference_or_provider_text_in_the_502(vault_client, monkeypatch):
    """The 502 names the failure TYPE and nothing the provider or the ref said.

    hvac fails with the request URL in the message and a decode failure can repeat the
    payload, and the ref itself names a Vault mount, path and key. Both went into the
    response body, and the provider's exception stayed reachable on the raised error.
    """
    from nso_adapter.api.errors import ApiError
    from nso_adapter.api.secrets import _vault_op
    from tests._secret_discipline import assert_chain_free_of

    client, _store, kv = vault_client
    ref = "placeholder-mount/placeholder-path#placeholder-key"
    leaked = [ref, "placeholder-mount", "placeholder-path", "placeholder-key", "placeholder-secret"]

    def boom(**_kwargs):
        raise RuntimeError(f"vault: read of {ref} failed holding placeholder-secret")

    monkeypatch.setattr(kv, "read_secret_version", boom)

    resp = await client.post("/api/v1/secrets/verify", json={"vault_ref": ref}, headers=AUTH)

    assert resp.status_code == 502
    assert_text_free_of(resp.text, leaked)
    message = resp.json()["error"]["message"]
    assert resp.json()["error"]["code"] == "vault_error"
    assert "RuntimeError" in message, "the failure type is the half the operator needs"

    # The same failure through the real helper: `from None` would leave the provider's
    # exception on __context__, where a formatted traceback still prints it.
    with pytest.raises(ApiError) as caught:
        await _vault_op(boom)
    assert_chain_free_of(caught.value, leaked)


# ── no reference component is a log field or a response field ────────────────

_REF_MOUNT = "network"
_REF_KEY = "placeholder-key"
_REF_PATH = "placeholder-path/placeholder-leaf"
_REF = f"{_REF_MOUNT}/{_REF_PATH}#{_REF_KEY}"
#: Every component of the reference, plus the plaintext. The caller chose all of them, so a
#: caller that pastes a secret into any of them would read it back out of the answer or out
#: of whatever recorded the answer. Allowing the mount and key would leak both.
_REF_LOCATORS = [_REF, _REF_PATH, _REF_MOUNT, _REF_KEY, "placeholder-path", "placeholder-leaf", "placeholder-secret"]


@pytest.mark.anyio
async def test_a_SUCCESSFUL_set_echoes_no_REFERENCE_COMPONENT_anywhere(vault_client):
    """The success path still logged the mount and the field names and returned the whole ref.

    A ref names a Vault mount, a path and a key, and the caller chose all three. Whoever reads
    the adapter log, or the answer, then knows exactly where every secret the adapter writes
    lives. A caller that pastes a secret into the ref reads it straight back.
    """
    from structlog.testing import capture_logs

    from tests._secret_discipline import assert_records_free_of

    client, store, _ = vault_client
    with capture_logs() as logs:
        resp = await client.post(
            "/api/v1/secrets",
            json={"vault_ref": _REF, "values": {_REF_KEY: "placeholder-secret"}},
            headers=AUTH,
        )

    assert resp.status_code == 200
    assert store[_REF_PATH] == {_REF_KEY: "placeholder-secret"}, "the write must still land"
    written = [record for record in logs if record["event"] == "secrets.set"]
    assert written, "the write was not reported at all"
    assert_records_free_of(logs, _REF_LOCATORS)
    assert_text_free_of(resp.text, _REF_LOCATORS)
    assert written[0]["version"] == 1
    assert written[0]["operation_id"] == resp.json()["operation_id"], "the record must join to the answer"


@pytest.mark.anyio
async def test_a_SUCCESSFUL_harvest_echoes_no_REFERENCE_COMPONENT_anywhere(vault_client):
    """Same sinks on the harvest side, where the ref points at an adopted community."""
    from structlog.testing import capture_logs

    from tests._secret_discipline import assert_records_free_of

    client, store, _ = vault_client
    device_id = await _seed_harvest_device("cisco-ios-cli-6.77")
    _wire_nso_transport(
        _NsoTransport({"tailf-ned-cisco-ios:community": [{"name": "placeholder-secret", "RO": [None]}]})
    )

    target_hash = _h("placeholder-secret")
    with capture_logs() as logs:
        resp = await client.post(
            f"/api/v1/devices/{device_id}/secrets/harvest-community",
            json={"community_hash": target_hash, "vault_ref": _REF},
            headers=AUTH,
        )

    assert resp.status_code == 200
    assert store[_REF_PATH] == {_REF_KEY: "placeholder-secret"}, "the harvest must still land"
    harvested = [record for record in logs if record["event"] == "secrets.harvest_community"]
    assert harvested, "the harvest was not reported at all"
    assert_records_free_of(logs, _REF_LOCATORS)
    assert_text_free_of(resp.text, _REF_LOCATORS)
    assert harvested[0]["device_id"] == device_id, "the adapter's own device id, never the name in NSO"
    assert_records_free_of(logs, ["harvest-dev"])
    assert harvested[0]["community_hash"] == target_hash
    assert harvested[0]["operation_id"] == resp.json()["operation_id"], "the record must join to the answer"


@pytest.mark.anyio
async def test_the_operation_id_is_a_WHOLE_uuid_so_two_operations_cannot_share_one(vault_client):
    """The id was minted as ``uuid4().hex[:12]``: 48 bits, ~1% collision at 2.4M operations.

    The id exists only to join one answer to its own log record, so a collision joins an
    answer to a DIFFERENT operation's record: the one thing the handle promises. Nothing
    constrains its width (the three response schemas type it as a plain string), so the
    whole value costs nothing.
    """
    from structlog.testing import capture_logs

    client, _, _ = vault_client
    seen: set[str] = set()
    for _ in range(3):
        with capture_logs() as logs:
            resp = await client.post(
                "/api/v1/secrets",
                json={"vault_ref": _REF, "values": {_REF_KEY: "placeholder-secret"}},
                headers=AUTH,
            )
        assert resp.status_code == 200
        answered = resp.json()["operation_id"]
        minted = UUID(answered)  # a truncated hex string does not parse
        assert minted.version == 4, "the handle must stay a random uuid4"
        assert answered == minted.hex, "the answer must carry the whole value, unseparated"
        written = [record for record in logs if record["event"] == "secrets.set"]
        assert written[0]["operation_id"] == answered, "the record must still join to the answer"
        seen.add(answered)

    assert len(seen) == 3, "every operation must get its own handle"


@pytest.mark.anyio
async def test_the_secrets_write_description_promises_exactly_what_it_answers(vault_client):
    """The write's description still promised "version + fingerprints".

    SecretWriteOut carries neither field names nor hashes: it answers the operation_id and
    the KV v2 version. The stale promise reached the live OpenAPI document and the committed
    snapshot, so a client author reads a contract the adapter does not serve."""
    from nso_adapter.api.secrets import SecretWriteOut

    client, _, _ = vault_client
    resp = await client.post(
        "/api/v1/secrets", json={"vault_ref": _REF, "values": {_REF_KEY: "placeholder-secret"}}, headers=AUTH
    )

    assert resp.status_code == 200
    answered = set(resp.json())
    assert answered == set(SecretWriteOut.model_fields), "the answer must be the declared model"

    description = create_app().openapi()["paths"]["/api/v1/secrets"]["post"]["description"]
    assert "fingerprint" not in description.lower(), (
        "the description promises a fingerprint the write does not answer; it answers " + ", ".join(sorted(answered))
    )
    missing = {field for field in answered if field not in description}
    assert not missing, f"the description omits response fields: {sorted(missing)}"
    assert "Vault path" in description, "the description no longer says where the merge-write occurs"


@pytest.mark.anyio
async def test_a_SUCCESSFUL_verify_echoes_no_REFERENCE_COMPONENT_anywhere(vault_client):
    """The fixed result carries no submitted ref component, field name, or secret value."""
    from structlog.testing import capture_logs

    from tests._secret_discipline import assert_records_free_of

    client, store, _ = vault_client
    store[_REF_PATH] = {_REF_KEY: "placeholder-secret"}

    with capture_logs() as logs:
        resp = await client.post("/api/v1/secrets/verify", json={"vault_ref": _REF}, headers=AUTH)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "present"
    assert body["fingerprint"] == _h("placeholder-secret"), "the selected value is verified without its name"
    assert_records_free_of(logs, _REF_LOCATORS)
    assert_text_free_of(resp.text, _REF_LOCATORS)
    verified = [record for record in logs if record["event"] == "secrets.verify"]
    assert verified, "the verify was not reported at all"
    assert verified[0]["operation_id"] == body["operation_id"], "the record must join to the answer"


def test_secret_verify_openapi_exposes_only_the_fixed_result_shape():
    schema = create_app().openapi()["components"]["schemas"]["SecretVerifyOut"]
    expected = {"operation_id", "status", "fingerprint", "has_auth", "has_priv", "version"}

    assert set(schema["properties"]) == expected
    assert set(schema["required"]) == expected
    assert not {"exists", "fields", "hashes"} & set(schema["properties"])
    assert all("additionalProperties" not in property_schema for property_schema in schema["properties"].values())
    assert all(property_schema.get("type") != "array" for property_schema in schema["properties"].values())


@pytest.mark.anyio
async def test_an_INVALID_values_ENTRY_keeps_the_callers_key_out_of_the_422(vault_client):
    """Pydantic reports a bad map entry at ``("body", "values", <key>)``.

    The key is the caller's own string. A caller that named the entry after the secret read
    it straight back out of the validation location.
    """
    client, _store, _ = vault_client

    resp = await client.post(
        "/api/v1/secrets",
        json={"vault_ref": _REF, "values": {"placeholder-secret": {"nested": 1}}},
        headers=AUTH,
    )

    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"
    assert_text_free_of(resp.text, ["placeholder-secret"])
    locations = [error["loc"] for error in resp.json()["error"]["detail"]["errors"]]
    assert ["body", "values", "[redacted]"] in locations, "the operator must still learn WHERE it broke"


# ── a malformed reference is never echoed back ───────────────────────────────


@pytest.mark.anyio
async def test_a_MALFORMED_ref_is_answered_with_the_broken_RULE_not_the_input(vault_client):
    """The 400 named the rule AND repeated the caller's own text, secret included.

    ``vault_ref`` is a free-form string on the wire. A caller that pastes a community or a
    password into it had that value written straight back into the error body, and the
    parser exception stayed on ``__cause__`` where a formatted traceback still prints it.
    """
    from nso_adapter.api.errors import ApiError
    from nso_adapter.api.secrets import _parse_ref
    from tests._secret_discipline import assert_chain_free_of, exception_chain

    client, _store, _ = vault_client
    malformed = "network/placeholder-path placeholder-secret#placeholder-key"

    resp = await client.post("/api/v1/secrets", json={"vault_ref": malformed, "values": {"a": "b"}}, headers=AUTH)

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_vault_ref"
    assert_text_free_of(resp.text, [malformed, "placeholder-path", "placeholder-secret"])
    assert "whitespace" in resp.json()["error"]["message"], "the caller must still learn WHAT is malformed"

    # The same input through the real helper: `from exc` kept the parser exception, whose
    # own text repeats the reference verbatim.
    with pytest.raises(ApiError) as caught:
        _parse_ref(malformed)
    assert_chain_free_of(caught.value, [malformed, "placeholder-path", "placeholder-secret"])
    assert exception_chain(caught.value) == [caught.value], "the parser exception must not stay attached"


@pytest.mark.anyio
async def test_an_UNREGISTERED_instance_answers_502_with_nothing_attached(vault_client, monkeypatch):
    """The 502 uses fixed text and is raised after the handler with no attached exception."""
    from nso_adapter.api import secrets as secrets_api
    from nso_adapter.api.errors import ApiError, api_error
    from nso_adapter.store.models import Device
    from tests._secret_discipline import exception_chain

    client, _store, _ = vault_client
    device_id = await _seed_harvest_device("cisco-ios-cli-6.77")
    async with session() as db:
        device = await db.get(Device, device_id)
        device.nso_instance = "nso-not-registered"
        await db.commit()

    built: list[ApiError] = []

    def _spy(*args, **kwargs):
        error = api_error(*args, **kwargs)
        built.append(error)
        return error

    monkeypatch.setattr(secrets_api, "api_error", _spy)

    resp = await client.post(
        f"/api/v1/devices/{device_id}/secrets/harvest-community",
        json={"community_hash": _h("x"), "vault_ref": "network/p#community"},
        headers=AUTH,
    )

    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "nso_unavailable"
    assert resp.json()["error"]["message"] == "No NSO client is registered"
    assert_text_free_of(resp.text, ["nso-not-registered"])
    assert built, "the refusal never went through api_error"
    refusal = built[-1]
    assert exception_chain(refusal) == [refusal], "the registry exception must not stay attached to the 502"
