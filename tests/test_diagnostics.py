# SPDX-License-Identifier: Apache-2.0
"""Tests for diagnostic device references."""

from __future__ import annotations

import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

from nso_adapter.config import ApiConfig, AppConfig, NetboxConfig, SecretsConfig
from nso_adapter.domain.diagnostics import device_fields, device_ref, register_device_ref_key
from nso_adapter.main import _init_secrets
from nso_adapter.secrets.base import SecretResolutionError
from tests._secret_discipline import assert_chain_free_of


def _config() -> AppConfig:
    return AppConfig(
        secrets=SecretsConfig(provider="local"),
        nso_instances=[],
        netbox=NetboxConfig(base_url="http://netbox.invalid", api_token_ref="NETBOX_TOKEN"),
        api=ApiConfig(adapter_token_ref="ADAPTER_TOKEN"),
        diagnostic_key_ref="DIAGNOSTIC_KEY",
        database_url="postgresql+asyncpg://adapter:placeholder@db/adapter",
    )


def _cli_environment(tmp_path) -> dict[str, str]:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
secrets:
  provider: local
nso_instances: []
netbox:
  base_url: https://netbox.invalid
  api_token_ref: NETBOX_TOKEN
api:
  adapter_token_ref: ADAPTER_TOKEN
diagnostic_key_ref: DIAGNOSTIC_KEY
database_url: postgresql+asyncpg://adapter:placeholder@database.invalid/adapter
""",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["CONFIG_FILE"] = str(config_path)
    environment["DIAGNOSTIC_KEY"] = "placeholder-diagnostic-key"
    environment.pop("DATABASE_URL", None)
    environment.pop("DIAGNOSTIC_KEY_FILE", None)
    return environment


def test_startup_rejects_a_missing_diagnostic_key(monkeypatch) -> None:
    monkeypatch.setenv("ADAPTER_TOKEN", "placeholder-adapter-token")
    monkeypatch.delenv("DIAGNOSTIC_KEY", raising=False)

    with pytest.raises(SecretResolutionError) as caught:
        _init_secrets(SimpleNamespace(state=SimpleNamespace()), _config(), SimpleNamespace())

    assert str(caught.value) == "diagnostic_key_ref: the referenced environment variable is not set"
    assert_chain_free_of(caught.value, ["DIAGNOSTIC_KEY"])


def test_startup_rejects_an_empty_diagnostic_key(monkeypatch) -> None:
    monkeypatch.setenv("ADAPTER_TOKEN", "placeholder-adapter-token")
    monkeypatch.setenv("DIAGNOSTIC_KEY", "")

    with pytest.raises(ValueError) as caught:
        _init_secrets(SimpleNamespace(state=SimpleNamespace()), _config(), SimpleNamespace())

    assert str(caught.value) == "diagnostic reference key must not be empty"
    assert_chain_free_of(caught.value, ["DIAGNOSTIC_KEY", "placeholder-adapter-token"])


def test_startup_registers_the_resolved_diagnostic_key(monkeypatch) -> None:
    register_device_ref_key("stale-placeholder-key")
    monkeypatch.setenv("ADAPTER_TOKEN", "placeholder-adapter-token")
    monkeypatch.setenv("DIAGNOSTIC_KEY", "placeholder-diagnostic-key")

    app = SimpleNamespace(state=SimpleNamespace())
    _init_secrets(app, _config(), SimpleNamespace())

    assert device_ref("nso-a", "edge-1") == "5682a11cb050d759"


def test_device_ref_uses_the_keyed_domain_separated_pair() -> None:
    register_device_ref_key("placeholder-diagnostic-key")

    assert device_ref("nso-a", "edge-1") == "5682a11cb050d759"


def test_device_ref_distinguishes_instances_and_pair_boundaries() -> None:
    register_device_ref_key("placeholder-diagnostic-key")

    assert device_ref("nso-a", "edge-1") != device_ref("nso-b", "edge-1")
    assert device_ref("a", "bc") != device_ref("ab", "c")


def test_device_fields_selects_each_available_identity() -> None:
    register_device_ref_key("placeholder-diagnostic-key")
    reference = device_ref("nso-a", "edge-1")

    assert device_fields(device_id=0) == {"device_id": 0}
    assert device_fields(nso_instance="nso-a", nso_device_name="edge-1") == {"device_ref": reference}
    assert device_fields(device_id=42, nso_instance="nso-a", nso_device_name="edge-1") == {
        "device_id": 42,
        "device_ref": reference,
    }
    assert device_fields(nso_instance="nso-a") == {}
    assert device_fields() == {}


def test_device_fields_rejects_a_device_name_without_an_nso_instance() -> None:
    with pytest.raises(ValueError, match="nso_instance is required when nso_device_name is provided"):
        device_fields(nso_device_name="edge-1")


@pytest.mark.parametrize("invalid_id", [True, "42", 42.0])
def test_device_fields_rejects_non_integer_ids(invalid_id) -> None:
    with pytest.raises(TypeError, match="device_id must be an int"):
        device_fields(device_id=invalid_id)


def test_correlate_matches_the_runtime_helper_without_inventory(tmp_path) -> None:
    register_device_ref_key("placeholder-diagnostic-key")
    expected = device_ref("nso-a", "edge-1")

    result = subprocess.run(
        [sys.executable, "-m", "nso_adapter.domain.diagnostics", "correlate"],
        input="nso-a\nedge-1\n",
        check=False,
        capture_output=True,
        text=True,
        env=_cli_environment(tmp_path),
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout == f"{expected}\n"


def test_device_references_are_stable_across_processes(tmp_path) -> None:
    command = [sys.executable, "-m", "nso_adapter.domain.diagnostics", "correlate"]
    environment = _cli_environment(tmp_path)

    first = subprocess.run(
        command,
        input="nso-a\nedge-1\n",
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=10,
    )
    second = subprocess.run(
        command,
        input="nso-a\nedge-1\n",
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=10,
    )

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert first.stdout == second.stdout == "5682a11cb050d759\n"
