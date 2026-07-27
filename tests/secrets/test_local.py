# SPDX-License-Identifier: Apache-2.0
"""Tests for SecretsProvider implementations."""

import pytest

from nso_adapter.secrets.base import SecretsProvider
from nso_adapter.secrets.local import LocalSecretsProvider


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


def test_local_provider_missing_raises():
    p = LocalSecretsProvider()
    with pytest.raises(KeyError, match="NONEXISTENT_KEY_XYZ"):
        p.get("NONEXISTENT_KEY_XYZ")


def test_local_provider_satisfies_protocol():
    p = LocalSecretsProvider()
    assert isinstance(p, SecretsProvider)
