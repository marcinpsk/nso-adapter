# SPDX-License-Identifier: Apache-2.0
"""SecretsProvider protocol and factory."""

from __future__ import annotations

from nso_adapter.config import AppConfig, EnvSettings, SecretsConfig

from .base import SecretsProvider
from .local import LocalSecretsProvider
from .vault import VaultSecretsProvider

__all__ = ["SecretsProvider", "LocalSecretsProvider", "VaultSecretsProvider", "make_provider"]


def make_provider(cfg: AppConfig, env: EnvSettings) -> SecretsProvider:
    """Build and return a SecretsProvider from application config."""
    secrets_cfg = cfg.secrets
    if secrets_cfg.provider == "vault":
        if secrets_cfg.vault is None:
            raise ValueError("secrets.vault config block is required when provider=vault")
        vault_cfg = secrets_cfg.vault
        return VaultSecretsProvider(
            addr=vault_cfg.address,
            role_id=env.vault_role_id,
            secret_id=env.vault_secret_id,
            mount=vault_cfg.kv_mount,
            namespace=vault_cfg.namespace or "",
            verify_ssl=vault_cfg.verify_ssl,
        )
    if secrets_cfg.provider == "local":
        return LocalSecretsProvider()
    raise ValueError(f"Unknown secrets provider: {secrets_cfg.provider!r}")
