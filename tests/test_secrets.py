# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for secrets factory — make_provider()."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nso_adapter.config import ApiConfig, AppConfig, EnvSettings, NetboxConfig, SecretsConfig, VaultConfig
from nso_adapter.secrets import make_provider
from nso_adapter.secrets.local import LocalSecretsProvider


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
    """provider=vault with vault block → VaultSecretsProvider is returned."""
    vault_cfg = VaultConfig(address="https://vault.example.com", kv_mount="secret")
    cfg = _app_config("vault", vault=vault_cfg)

    with patch("nso_adapter.secrets.VaultSecretsProvider") as MockVault:
        mock_instance = MagicMock()
        MockVault.return_value = mock_instance

        result = make_provider(cfg, _env())

        MockVault.assert_called_once_with(
            addr="https://vault.example.com",
            role_id="role-id-abc",
            secret_id="secret-id-abc",
            mount="secret",
            namespace="",
            verify_ssl=True,
        )
        assert result is mock_instance


def test_make_provider_vault_namespace_passed_through():
    """provider=vault with namespace → namespace forwarded to VaultSecretsProvider."""
    vault_cfg = VaultConfig(address="https://vault.example.com", kv_mount="secret", namespace="prod")
    cfg = _app_config("vault", vault=vault_cfg)

    with patch("nso_adapter.secrets.VaultSecretsProvider") as MockVault:
        MockVault.return_value = MagicMock()
        make_provider(cfg, _env())
        _, kwargs = MockVault.call_args
        assert kwargs["namespace"] == "prod"


def test_make_provider_unknown_provider_raises():
    """Unknown provider string → ValueError."""
    with pytest.raises(ValueError, match="Unknown secrets provider"):
        make_provider(_app_config("s3"), _env())
