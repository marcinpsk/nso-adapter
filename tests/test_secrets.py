# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for secrets factory — make_provider()."""

from __future__ import annotations

import pytest

from nso_adapter.config import ApiConfig, AppConfig, EnvSettings, NetboxConfig, SecretsConfig, VaultConfig
from nso_adapter.secrets import make_provider
from nso_adapter.secrets.local import LocalSecretsProvider
from nso_adapter.secrets.vault import VaultSecretsProvider


def _app_config(provider: str, vault: VaultConfig | None = None) -> AppConfig:
    return AppConfig(
        secrets=SecretsConfig(provider=provider, vault=vault),
        nso_instances=[],
        netbox=NetboxConfig(base_url="http://netbox.local", api_token_ref="NB_TOKEN"),
        api=ApiConfig(adapter_token_ref="ADAPTER_TOKEN"),
    )


def _env() -> EnvSettings:
    return EnvSettings(
        config_file="config.yaml",
        vault_role_id="role-id-abc",
        vault_secret_id="secret-id-abc",
    )


def test_make_provider_local_returns_local_provider():
    """provider=local → LocalSecretsProvider instance."""
    provider = make_provider(_app_config("local"), _env())
    assert isinstance(provider, LocalSecretsProvider)


def test_make_provider_vault_no_vault_block_raises():
    """provider=vault but vault config block missing → ValueError."""
    with pytest.raises(ValueError, match="secrets.vault config block is required"):
        make_provider(_app_config("vault", vault=None), _env())


def test_make_provider_vault_with_config_returns_vault_provider():
    """provider=vault with vault block → a REAL VaultSecretsProvider with the mapped config.

    __init__ is pure (lazy client, no Vault I/O until get()), so we build the real provider
    and assert the config landed — covering both make_provider's kwarg mapping AND the
    provider's own field assignment (e.g. an empty namespace collapses to None).
    """
    vault_cfg = VaultConfig(address="https://vault.example.com", kv_mount="secret")
    cfg = _app_config("vault", vault=vault_cfg)

    result = make_provider(cfg, _env())

    assert isinstance(result, VaultSecretsProvider)
    assert result._addr == "https://vault.example.com"
    assert result._role_id == "role-id-abc"
    assert result._secret_id == "secret-id-abc"
    assert result._mount == "secret"
    assert result._namespace is None  # config namespace=None → "" → None in __init__
    assert result._verify_ssl is True


def test_make_provider_vault_namespace_passed_through():
    """provider=vault with namespace → namespace forwarded into the real provider."""
    vault_cfg = VaultConfig(address="https://vault.example.com", kv_mount="secret", namespace="prod")
    cfg = _app_config("vault", vault=vault_cfg)

    result = make_provider(cfg, _env())

    assert isinstance(result, VaultSecretsProvider)
    assert result._namespace == "prod"


def test_make_provider_unknown_provider_raises():
    """Unknown provider string → ValueError."""
    with pytest.raises(ValueError, match="Unknown secrets provider"):
        make_provider(_app_config("s3"), _env())
