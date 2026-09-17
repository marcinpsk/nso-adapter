# SPDX-License-Identifier: Apache-2.0
"""Tests for diagnostic device references."""

from __future__ import annotations

import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

from nso_adapter.config import ApiConfig, AppConfig, NetboxConfig, SecretsConfig
from nso_adapter.domain.diagnostics import (
    DEVICE_REF_PATTERN,
    device_fields,
    device_ref,
    register_device_ref_key,
)
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


@pytest.mark.parametrize("blank", ["", " ", "   ", "\t", "\n", " \t\n "])
def test_startup_rejects_a_BLANK_diagnostic_key(monkeypatch, blank) -> None:
    """A blank key is an unset key wearing a space: it keys every pseudonym in the fleet with a
    value an attacker guesses first, which is what the keyed digest exists to prevent.

    The refusal comes from `resolve_secret`, so it names the configuration slot the operator has
    to fix. `register_device_ref_key` keeps its own check for callers that do not come through
    the resolver.
    """
    monkeypatch.setenv("ADAPTER_TOKEN", "placeholder-adapter-token")
    monkeypatch.setenv("DIAGNOSTIC_KEY", blank)

    with pytest.raises(SecretResolutionError) as caught:
        _init_secrets(SimpleNamespace(state=SimpleNamespace()), _config(), SimpleNamespace())

    assert str(caught.value) == "diagnostic_key_ref: the configured secret is blank"
    assert_chain_free_of(caught.value, ["DIAGNOSTIC_KEY", "placeholder-adapter-token"])


@pytest.mark.parametrize("blank", ["", " ", "\t\n"])
def test_register_device_ref_key_refuses_a_blank_on_its_own(blank) -> None:
    """The module's own contract, independent of where the key came from."""
    with pytest.raises(ValueError, match="must not be empty"):
        register_device_ref_key(blank)


@pytest.mark.parametrize("blank", ["", " ", "\t"])
def test_startup_rejects_a_BLANK_ADAPTER_TOKEN(monkeypatch, blank) -> None:
    """Same class as the diagnostic key: the provider serves a blank as a SET value, so nothing
    below rejects it. A blank bearer token is guessed on the first try, and it gates every
    write, apply and push endpoint."""
    monkeypatch.setenv("ADAPTER_TOKEN", blank)
    monkeypatch.setenv("DIAGNOSTIC_KEY", "placeholder-diagnostic-key")

    with pytest.raises(SecretResolutionError) as caught:
        _init_secrets(SimpleNamespace(state=SimpleNamespace()), _config(), SimpleNamespace())

    assert str(caught.value) == "api.adapter_token_ref: the configured secret is blank"
    assert_chain_free_of(caught.value, ["ADAPTER_TOKEN", "placeholder-diagnostic-key"])


@pytest.mark.parametrize(
    ("variable", "slot"),
    [
        ("NETBOX_TOKEN", "netbox.api_token_ref"),
        ("NSO_USERNAME", "nso_instances[nso-a].username_ref"),
        ("NSO_PASSWORD", "nso_instances[nso-a].password_ref"),
    ],
)
def test_EVERY_configured_secret_slot_refuses_a_blank(monkeypatch, variable, slot) -> None:
    """The rule sits on `resolve_secret`, the one boundary every configured reference crosses, so
    a slot added later cannot forget it. Without that, a blank NetBox token reaches the wire as
    `Authorization: Token ` and only fails per-request, long after startup said the config was
    fine."""
    from nso_adapter.secrets import LocalSecretsProvider, resolve_secret

    monkeypatch.setenv(variable, "   ")

    with pytest.raises(SecretResolutionError) as caught:
        resolve_secret(LocalSecretsProvider(), variable, slot=slot)

    assert str(caught.value) == f"{slot}: the configured secret is blank"
    assert_chain_free_of(caught.value, [variable])


def test_a_NONBLANK_key_keeps_its_own_whitespace() -> None:
    """Rejecting blanks must not silently strip: the key is secret material, used verbatim, and
    trimming it would make two different configured keys produce the same references."""
    register_device_ref_key(" placeholder-diagnostic-key ")
    padded = device_ref("nso-a", "edge-1")
    register_device_ref_key("placeholder-diagnostic-key")

    assert padded != device_ref("nso-a", "edge-1")


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


@pytest.mark.parametrize("bad", [12345, ["a"], {"n": 1}, b"bytes", 3.5])
def test_device_ref_refuses_a_non_string_component(bad):
    """The reference is a keyed identity, not a renderer: a non-string component is a contract error.

    Without this the failure is an AttributeError from `.encode()` deep inside the digest, raised
    from whatever log call happened to build the fields. `device_fields` already refuses a non-int
    `device_id` the same way.
    """
    register_device_ref_key("placeholder-diagnostic-key")

    with pytest.raises(TypeError, match="must be a str"):
        device_ref("nso-dev", bad)
    with pytest.raises(TypeError, match="must be a str"):
        device_ref(bad, "rtr01")


def test_the_PUBLISHED_pattern_is_what_device_ref_actually_produces() -> None:
    """The response models declare this pattern, so a width change must break here rather than
    500 on a live response when pydantic validates the outgoing field."""
    import re

    register_device_ref_key("placeholder-diagnostic-key")

    for instance, name in (("nso-a", "edge-1"), ("nso-b", "edge-1"), ("nso-a", "a" * 200)):
        assert re.fullmatch(DEVICE_REF_PATTERN, device_ref(instance, name))


def test_a_response_model_REFUSES_a_reference_that_is_not_one() -> None:
    """The declaration has to be enforced, not decorative."""
    import pytest as _pytest
    from pydantic import ValidationError

    from nso_adapter.api.nso_instances import InstanceDeviceOut

    fields = {
        "name": "placeholder-device",
        "address": None,
        "ned_id": None,
        "platform": None,
        "auth_group": None,
        "admin_state": None,
        "onboarded": False,
        "onboarded_device_id": None,
        "onboarded_netbox_device_id": None,
    }

    assert InstanceDeviceOut(device_ref="0123456789abcdef", **fields).device_ref == "0123456789abcdef"
    for rejected in ("placeholder-device", "0123456789ABCDEF", "0123456789abcde", "0123456789abcdef0"):
        with _pytest.raises(ValidationError):
            InstanceDeviceOut(device_ref=rejected, **fields)
