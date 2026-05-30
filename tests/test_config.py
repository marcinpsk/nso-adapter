# SPDX-License-Identifier: Apache-2.0
"""Tests for config loading."""

import pytest

from nso_adapter.config import get_config, get_env_settings, reset_config


def test_get_config_loads_yaml(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("""
secrets:
  provider: local
nso_instances:
  - name: test-nso
    base_url: http://10.0.0.1:8080
    host_header: nso.example.com
    username_ref: "NSO_USERNAME"
    password_ref: "NSO_PASSWORD"
netbox:
  base_url: http://netbox.local
  api_token_ref: "NETBOX_TOKEN"
api:
  adapter_token_ref: "ADAPTER_TOKEN"
database_url: sqlite+aiosqlite:///./test.db
""")
    monkeypatch.setenv("CONFIG_FILE", str(cfg_file))
    reset_config()
    cfg = get_config()
    assert len(cfg.nso_instances) == 1
    assert cfg.nso_instances[0].name == "test-nso"
    assert cfg.nso_instances[0].username_ref == "NSO_USERNAME"
    assert cfg.netbox.base_url == "http://netbox.local"
    assert cfg.api.adapter_token_ref == "ADAPTER_TOKEN"
    assert cfg.secrets.provider == "local"


def test_get_config_missing_file(tmp_path, monkeypatch):
    monkeypatch.setenv("CONFIG_FILE", str(tmp_path / "nonexistent.yaml"))
    reset_config()
    with pytest.raises(FileNotFoundError):
        get_config()


def test_reset_config_clears_singleton(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("""
secrets:
  provider: local
nso_instances: []
netbox:
  base_url: http://netbox.local
  api_token_ref: "NETBOX_TOKEN"
api:
  adapter_token_ref: "ADAPTER_TOKEN"
""")
    monkeypatch.setenv("CONFIG_FILE", str(cfg_file))
    reset_config()
    cfg1 = get_config()
    reset_config()
    cfg2 = get_config()
    assert cfg1 is not cfg2


def test_env_settings_reads_role_id(monkeypatch):
    monkeypatch.setenv("VAULT_ROLE_ID", "test-role")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret")
    reset_config()
    env = get_env_settings()
    assert env.vault_role_id == "test-role"
    assert env.vault_secret_id == "test-secret"

