# SPDX-License-Identifier: Apache-2.0
"""Tests for SecretsProvider implementations."""

import pytest

from nso_adapter.secrets.base import SecretResolutionError, SecretsProvider, resolve_secret
from nso_adapter.secrets.local import LocalSecretsProvider
from tests._secret_discipline import assert_chain_free_of


def test_local_provider_from_env(monkeypatch):
    monkeypatch.setenv("MY_SECRET", "hunter2")
    p = LocalSecretsProvider()
    assert p.get("MY_SECRET") == "hunter2"


def test_local_provider_returns_empty_env_value(monkeypatch):
    """s3-26: an intentionally empty secret ("") is a set value, not 'unset' — return it
    verbatim instead of falling through to the _FILE variant and a confusing KeyError."""
    monkeypatch.setenv("EMPTY_SECRET", "")
    p = LocalSecretsProvider()
    assert p.get("EMPTY_SECRET") == ""


def test_local_provider_from_file(tmp_path, monkeypatch):
    secret_file = tmp_path / "token"
    secret_file.write_text("  mytoken  \n")
    monkeypatch.setenv("MY_TOKEN_FILE", str(secret_file))
    p = LocalSecretsProvider()
    assert p.get("MY_TOKEN") == "mytoken"


def test_local_provider_missing_refuses_without_the_reference():
    """The protocol's refusal: a KeyError quoting the reference met neither half of it.

    ``resolve_secret`` masked it for CONFIGURED lookups, so a direct caller got a KeyError
    it could not catch as SecretResolutionError, carrying the reference it did not need.
    """
    p = LocalSecretsProvider()

    with pytest.raises(SecretResolutionError) as caught:
        p.get("PLACEHOLDER_MISSING_REF")

    assert caught.value.reason == "the referenced environment variable is not set"
    assert caught.value.slot is None, "only the caller knows the configuration slot"
    assert "PLACEHOLDER_MISSING_REF" not in str(caught.value)
    assert_chain_free_of(caught.value, ["PLACEHOLDER_MISSING_REF"])


def test_resolve_secret_stamps_the_slot_on_the_local_refusal():
    """The provider classifies, the caller addresses: the slot is the caller's to add."""
    with pytest.raises(SecretResolutionError) as caught:
        resolve_secret(LocalSecretsProvider(), "PLACEHOLDER_MISSING_REF", slot="netbox.api_token_ref")

    assert str(caught.value) == "netbox.api_token_ref: the referenced environment variable is not set"
    assert "PLACEHOLDER_MISSING_REF" not in str(caught.value)
    assert_chain_free_of(caught.value, ["PLACEHOLDER_MISSING_REF"])


def test_local_provider_satisfies_protocol():
    p = LocalSecretsProvider()
    assert isinstance(p, SecretsProvider)


def test_local_provider_classifies_an_unreadable_file_without_the_path(tmp_path, monkeypatch):
    """A directory answers ``exists()``, so ``read_text`` raises an OSError naming the path.

    The protocol says the provider refuses with a SecretResolutionError that repeats no part
    of the reference, and the path in ``<REFERENCE>_FILE`` is the deployment's own layout.
    """
    monkeypatch.setenv("MY_TOKEN_FILE", str(tmp_path / "secret-dir"))
    (tmp_path / "secret-dir").mkdir()

    with pytest.raises(SecretResolutionError) as caught:
        LocalSecretsProvider().get("MY_TOKEN")

    assert caught.value.reason == "the referenced file could not be read (IsADirectoryError)"
    assert "secret-dir" not in str(caught.value)
    assert_chain_free_of(caught.value, ["secret-dir", str(tmp_path)])


def test_local_provider_classifies_undecodable_file_bytes(tmp_path, monkeypatch):
    """A decode failure quotes the bytes it choked on; neither they nor the path travel."""
    secret_file = tmp_path / "binary-token"
    secret_file.write_bytes(b"\xff\xfe secret-bytes")
    monkeypatch.setenv("MY_TOKEN_FILE", str(secret_file))

    with pytest.raises(SecretResolutionError) as caught:
        LocalSecretsProvider().get("MY_TOKEN")

    assert caught.value.reason == "the referenced file could not be read (UnicodeDecodeError)"
    assert_chain_free_of(caught.value, ["binary-token", str(tmp_path), "secret-bytes"])


def test_local_provider_falls_through_when_the_referenced_file_is_gone(tmp_path, monkeypatch):
    """An absent path is 'unset', not a read failure: the refusal stays the missing-var one."""
    monkeypatch.setenv("MY_TOKEN_FILE", str(tmp_path / "never-written"))

    with pytest.raises(SecretResolutionError) as caught:
        LocalSecretsProvider().get("MY_TOKEN")

    assert caught.value.reason == "the referenced environment variable is not set"
    assert_chain_free_of(caught.value, ["never-written", str(tmp_path)])


def test_resolve_secret_stamps_the_slot_on_an_unreadable_file(tmp_path, monkeypatch):
    """The configured path: the provider classifies, the caller addresses, nothing leaks."""
    monkeypatch.setenv("NETBOX_TOKEN_FILE", str(tmp_path / "token-dir"))
    (tmp_path / "token-dir").mkdir()

    with pytest.raises(SecretResolutionError) as caught:
        resolve_secret(LocalSecretsProvider(), "NETBOX_TOKEN", slot="netbox.api_token_ref")

    assert str(caught.value) == "netbox.api_token_ref: the referenced file could not be read (IsADirectoryError)"
    assert_chain_free_of(caught.value, ["token-dir", str(tmp_path)])
