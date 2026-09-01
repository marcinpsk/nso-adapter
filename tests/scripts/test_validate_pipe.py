# SPDX-License-Identifier: Apache-2.0
"""Tests for the connectivity smoke-test script."""

from __future__ import annotations

import runpy
from pathlib import Path

import httpx
import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "validate_pipe.py"


def test_validate_pipe_does_not_print_vault_key_names(monkeypatch, capsys):
    monkeypatch.setenv("NSO_URL", "http://nso.example.test:8080")
    monkeypatch.setenv("NSO_HOST_HEADER", "nso.example.test")
    monkeypatch.setenv("VAULT_ADDR", "https://vault.example.test:8200")
    monkeypatch.setenv("VAULT_ROLE_ID", "test-role-id")
    monkeypatch.setenv("VAULT_SECRET_ID", "test-secret-id")
    monkeypatch.setenv("VAULT_MOUNT", "network")
    monkeypatch.setenv("VAULT_PATH", "credentials/test-service")
    monkeypatch.setenv("SECRETS_BACKEND", "vault")
    monkeypatch.delenv("ADAPTER_URL", raising=False)

    def fake_get(url: str, **kwargs) -> httpx.Response:
        request = httpx.Request("GET", url)
        if "/v1/network/data/credentials/test-service" in url:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "data": {
                            "username": "test-user",
                            "password": "test-password",
                            "netbox_token": "test-netbox-token",
                            "adapter_token": "test-adapter-token",
                            "unexpected-sensitive-label": "test-value",
                        }
                    }
                },
                request=request,
            )
        if "/restconf/data/tailf-ncs:devices" in url:
            if kwargs.get("auth") is None:
                return httpx.Response(401, request=request)
            return httpx.Response(200, json={"tailf-ncs:devices": {"device": []}}, request=request)
        raise AssertionError(f"unexpected GET {url}")

    def fake_post(url: str, **_kwargs) -> httpx.Response:
        request = httpx.Request("POST", url)
        return httpx.Response(200, json={"auth": {"client_token": "test-token"}}, request=request)

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(httpx, "post", fake_post)

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(SCRIPT), run_name="__main__")

    assert exit_info.value.code == 0
    assert "unexpected-sensitive-label" not in capsys.readouterr().out
