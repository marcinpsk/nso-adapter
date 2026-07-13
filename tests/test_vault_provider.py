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


class _FakeInvalidPath(Exception):
    """Stands in for hvac.exceptions.InvalidPath (KV v2 read of a missing path)."""


class _FakeKvV2:
    def __init__(self, store: dict[str, dict[str, str]]):
        self._store = store
        self.forbid_once: set[str] = set()
        self.forbid_always: set[str] = set()  # persistent policy denial (survives re-auth)
        self.read_paths: list[str] = []
        self.read_mounts: list[str] = []
        self.write_calls: list[tuple[str, str, dict]] = []
        self.versions: dict[str, int] = {}

    def read_secret_version(self, *, mount_point, path, raise_on_deleted_version):
        self.read_paths.append(path)
        self.read_mounts.append(mount_point)
        if path in self.forbid_always:
            raise _FakeForbidden("permission denied")
        if path in self.forbid_once:
            self.forbid_once.discard(path)  # token "expired" exactly once
            raise _FakeForbidden()
        if path not in self._store:
            raise _FakeInvalidPath(path)
        return {
            "data": {
                "data": dict(self._store[path]),
                "metadata": {"version": self.versions.get(path, 1)},
            }
        }

    def create_or_update_secret(self, *, mount_point, path, secret):
        # Mirrors real KV v2 semantics: the write REPLACES the whole data dict.
        if path in self.forbid_once:
            self.forbid_once.discard(path)
            raise _FakeForbidden()
        self.write_calls.append((mount_point, path, dict(secret)))
        self._store[path] = dict(secret)
        self.versions[path] = self.versions.get(path, 0) + 1
        return {"data": {"version": self.versions[path]}}


class _FakeApprole:
    def __init__(self, logins: list[tuple[str, str]]):
        self._logins = logins

    def login(self, *, role_id, secret_id):
        self._logins.append((role_id, secret_id))
        return {"auth": {"client_token": f"tok-{len(self._logins)}"}}


class _FakeClient:
    def __init__(self, *, url, verify, kv, logins, namespace=None, timeout=None):
        self.url = url
        self.verify = verify
        self.namespace = namespace
        # hvac defaults to NO timeout; the provider must always pass one, or an unreachable
        # Vault holds the calling thread forever (and these run in the request thread-pool).
        self.timeout = timeout
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
        exceptions=types.SimpleNamespace(Forbidden=_FakeForbidden, InvalidPath=_FakeInvalidPath),
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


def test_client_always_carries_a_request_timeout(fake_hvac):
    """hvac defaults to NO timeout. These calls run in the request thread-pool (the API
    offloads them off the event loop), so an unreachable Vault would pin a pool slot
    forever and eventually starve every other blocking offload."""
    from nso_adapter.secrets.vault import VaultSecretsProvider

    state, store, _ = fake_hvac
    store["credentials/svc"] = {"netbox_token": "s3cr3t"}
    _provider().get("credentials/svc#netbox_token")
    assert state["clients"][0].timeout == VaultSecretsProvider.REQUEST_TIMEOUT_S


# ── mount-explicit read_path / write_path (SNMP secrets endpoints) ─────────────


def test_write_path_merges_existing_fields(fake_hvac):
    # KV v2 create_or_update REPLACES the whole path — a v3 user setting only a
    # new auth password must not silently delete the priv field.
    _, store, kv = fake_hvac
    store["netbox/snmp/v3/nms"] = {"auth": "old-auth", "priv": "old-priv"}
    kv.versions["netbox/snmp/v3/nms"] = 1

    version = _provider().write_path("network", "netbox/snmp/v3/nms", {"auth": "new-auth"})

    assert store["netbox/snmp/v3/nms"] == {"auth": "new-auth", "priv": "old-priv"}
    assert version == 2
    assert kv.write_calls[-1][0] == "network"  # mount-explicit, not the configured mount


def test_write_path_replace_mode_drops_siblings(fake_hvac):
    _, store, kv = fake_hvac
    store["netbox/snmp/v3/nms"] = {"auth": "a", "stale": "x"}
    kv.versions["netbox/snmp/v3/nms"] = 3

    version = _provider().write_path("network", "netbox/snmp/v3/nms", {"auth": "b"}, merge=False)

    assert store["netbox/snmp/v3/nms"] == {"auth": "b"}
    assert version == 4


def test_write_path_creates_missing_path(fake_hvac):
    _, store, _ = fake_hvac
    version = _provider().write_path("network", "netbox/snmp/community/abc", {"community": "s3cr3t"})
    assert store["netbox/snmp/community/abc"] == {"community": "s3cr3t"}
    assert version == 1


def test_read_path_returns_fields_and_empty_for_missing(fake_hvac):
    _, store, kv = fake_hvac
    store["netbox/snmp/v3/nms"] = {"auth": "a", "priv": "p"}

    provider = _provider()
    assert provider.read_path("network", "netbox/snmp/v3/nms") == {"auth": "a", "priv": "p"}
    assert provider.read_path("network", "netbox/snmp/v3/ghost") == {}
    assert kv.read_mounts[-2:] == ["network", "network"]


def test_write_path_invalidates_get_cache_on_configured_mount(fake_hvac):
    # get() serves from a per-path cache within the CONFIGURED mount; a write to
    # the same mount/path must not leave get() returning the pre-write value.
    _, store, _ = fake_hvac
    store["credentials/svc"] = {"tok": "old"}

    provider = _provider(mount="network")
    assert provider.get("credentials/svc#tok") == "old"
    provider.write_path("network", "credentials/svc", {"tok": "new"})
    assert provider.get("credentials/svc#tok") == "new"


def test_write_path_reauthenticates_on_forbidden(fake_hvac):
    state, store, kv = fake_hvac
    store["p/q"] = {}
    kv.forbid_once.add("p/q")  # merge pre-read 403s once → re-auth → retry

    version = _provider().write_path("network", "p/q", {"k": "v"})

    assert version == 1
    assert len(state["logins"]) == 2
