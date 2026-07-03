# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <mazieba@libertyglobal.com>
"""VaultSecretsProvider — real provider logic against a hand-built fake hvac client.

hvac/Vault is a true external boundary (a live Vault server + AppRole login), so it is
replaced by an explicit fake that records calls and serves canned KV data — NOT a MagicMock,
which would fabricate any attribute and let a broken read path stay green.
"""

from __future__ import annotations

import types

import pytest

from nso_adapter.secrets.vault import VaultSecretsProvider


class _FakeForbidden(Exception):
    """Stands in for hvac.exceptions.Forbidden (a 403 from Vault)."""


class _FakeKvV2:
    def __init__(self, store: dict[str, dict[str, str]]):
        self._store = store
        self.forbid_once: set[str] = set()
        self.read_paths: list[str] = []

    def read_secret_version(self, *, mount_point, path, raise_on_deleted_version):
        self.read_paths.append(path)
        if path in self.forbid_once:
            self.forbid_once.discard(path)  # token "expired" exactly once
            raise _FakeForbidden()
        return {"data": {"data": dict(self._store[path])}}


class _FakeApprole:
    def __init__(self, logins: list[tuple[str, str]]):
        self._logins = logins

    def login(self, *, role_id, secret_id):
        self._logins.append((role_id, secret_id))
        return {"auth": {"client_token": f"tok-{len(self._logins)}"}}


class _FakeClient:
    def __init__(self, *, url, verify, kv, logins, namespace=None):
        self.url = url
        self.verify = verify
        self.namespace = namespace
        self.token: str | None = None
        self.auth = types.SimpleNamespace(approle=_FakeApprole(logins))
        self.secrets = types.SimpleNamespace(kv=types.SimpleNamespace(v2=kv))


@pytest.fixture
def fake_hvac(monkeypatch):
    """Patch nso_adapter.secrets.vault.hvac with a fake; return (state, store, kv)."""
    store: dict[str, dict[str, str]] = {}
    kv = _FakeKvV2(store)
    state: dict[str, list] = {"logins": [], "clients": []}

    def _client_factory(**kwargs):
        client = _FakeClient(kv=kv, logins=state["logins"], **kwargs)
        state["clients"].append(client)
        return client

    fake = types.SimpleNamespace(
        Client=_client_factory,
        exceptions=types.SimpleNamespace(Forbidden=_FakeForbidden),
    )
    monkeypatch.setattr("nso_adapter.secrets.vault.hvac", fake)
    return state, store, kv


def _provider(**overrides) -> VaultSecretsProvider:
    kwargs = {
        "addr": "https://vault.example.com",
        "role_id": "role-abc",
        "secret_id": "secret-abc",
        "mount": "secret",
    }
    kwargs.update(overrides)
    return VaultSecretsProvider(**kwargs)


def test_get_resolves_path_field_and_logs_in_once(fake_hvac):
    state, store, kv = fake_hvac
    store["credentials/svc"] = {"netbox_token": "s3cr3t"}

    provider = _provider()
    assert provider.get("credentials/svc#netbox_token") == "s3cr3t"
    assert state["logins"] == [("role-abc", "secret-abc")]
    assert kv.read_paths == ["credentials/svc"]


def test_get_without_hash_raises_value_error(fake_hvac):
    with pytest.raises(ValueError, match="expected 'path#field'"):
        _provider().get("no-hash-here")


def test_get_caches_path_serving_multiple_fields_with_one_read(fake_hvac):
    state, store, kv = fake_hvac
    store["credentials/svc"] = {"user": "svc-netbox", "password": "pw"}

    provider = _provider()
    assert provider.get("credentials/svc#user") == "svc-netbox"
    assert provider.get("credentials/svc#password") == "pw"
    assert kv.read_paths == ["credentials/svc"]  # second field served from cache
    assert len(state["logins"]) == 1


def test_get_unknown_field_raises_key_error(fake_hvac):
    _, store, _ = fake_hvac
    store["credentials/svc"] = {"user": "svc-netbox"}
    with pytest.raises(KeyError, match="missing"):
        _provider().get("credentials/svc#missing")


def test_get_unknown_field_on_cached_path_raises_key_error(fake_hvac):
    _, store, kv = fake_hvac
    store["credentials/svc"] = {"user": "svc-netbox"}
    provider = _provider()
    provider.get("credentials/svc#user")  # primes the cache
    with pytest.raises(KeyError, match="missing"):
        provider.get("credentials/svc#missing")
    assert kv.read_paths == ["credentials/svc"]  # no second read for the cached path


def test_get_reauthenticates_on_forbidden(fake_hvac):
    state, store, kv = fake_hvac
    store["credentials/svc"] = {"netbox_token": "s3cr3t"}
    kv.forbid_once.add("credentials/svc")  # first read 403s, retry succeeds

    provider = _provider()
    assert provider.get("credentials/svc#netbox_token") == "s3cr3t"
    assert len(state["logins"]) == 2  # initial login + re-auth after the 403
    assert kv.read_paths == ["credentials/svc", "credentials/svc"]


def test_namespace_forwarded_to_client(fake_hvac):
    state, store, _ = fake_hvac
    store["credentials/svc"] = {"netbox_token": "s3cr3t"}
    _provider(namespace="prod").get("credentials/svc#netbox_token")
    assert state["clients"][0].namespace == "prod"
