# SPDX-License-Identifier: Apache-2.0
"""Tests for nso/client.py — NsoClient constructor and _client() helper."""
from __future__ import annotations

from unittest.mock import MagicMock

import httpx

from nso_adapter.nso.client import NsoClient


def _make_cfg(base_url="http://nso:8080/", ca_cert=None, host_header=None):
    cfg = MagicMock()
    cfg.base_url = base_url
    cfg.ca_cert = ca_cert
    cfg.host_header = host_header
    return cfg


class TestNsoClientInit:
    def test_strips_trailing_slash_from_base(self):
        client = NsoClient(_make_cfg("http://nso:8080/"), "admin", "pass")
        assert client._base == "http://nso:8080"

    def test_stores_auth(self):
        client = NsoClient(_make_cfg(), "user", "secret")
        assert client._auth == ("user", "secret")

    def test_verify_true_when_no_ca_cert(self):
        client = NsoClient(_make_cfg(ca_cert=None), "u", "p")
        assert client._verify is True

    def test_verify_path_when_ca_cert_given(self):
        client = NsoClient(_make_cfg(ca_cert="/etc/ssl/ca.pem"), "u", "p")
        assert client._verify == "/etc/ssl/ca.pem"

    def test_default_timeouts_set(self):
        client = NsoClient(_make_cfg(), "u", "p")
        assert client._timeout == 30.0
        assert client._action_timeout == 120.0


class TestNsoClientFactory:
    def test_returns_async_client(self):
        client = NsoClient(_make_cfg(), "u", "p")
        http = client._client()
        assert isinstance(http, httpx.AsyncClient)

    def test_custom_timeout_overrides_default(self):
        client = NsoClient(_make_cfg(), "u", "p")
        http = client._client(timeout=5.0)
        # httpx.AsyncClient exposes timeout via .timeout
        assert http.timeout.read == 5.0

    def test_host_header_injected(self):
        client = NsoClient(_make_cfg(host_header="nso.example.com"), "u", "p")
        http = client._client()
        assert http.headers.get("host") == "nso.example.com"

    def test_no_host_header_when_not_configured(self):
        client = NsoClient(_make_cfg(host_header=None), "u", "p")
        http = client._client()
        # httpx sets a default Host header; verify it's not overridden
        assert "nso.example.com" not in http.headers.get("host", "")
