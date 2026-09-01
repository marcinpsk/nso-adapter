#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""validate_pipe.py — Phase 0 connectivity smoke test.

Tests:
  1. NSO RESTCONF reachable and returns device list
  2. Vault AppRole login succeeds and expected secrets exist
  3. Adapter /healthz returns 200 (if ADAPTER_URL is set)

Usage:
  # On host or in container:
  python scripts/validate_pipe.py

Environment variables (from .env or container env):
  VAULT_ADDR       — Vault URL (default https://vault.example.com:8200)
  VAULT_ROLE_ID    — AppRole role ID
  VAULT_SECRET_ID  — AppRole secret ID
  VAULT_MOUNT      — KV v2 mount (default network)
  VAULT_PATH       — secret path (default credentials/example-svc)
  NSO_URL          — NSO RESTCONF base URL (default http://nso-host:8080)
  NSO_USER         — NSO admin username (default admin)
  NSO_HOST_HEADER  — Host header value (default nso.example.com — REQUIRED by NSO webui)
  NSO_PASSWORD     — NSO admin password (used if SECRETS_BACKEND=local)
  ADAPTER_URL      — Adapter base URL to test /healthz (optional)
  ADAPTER_TOKEN    — Bearer token for adapter (optional)
  SECRETS_BACKEND  — "vault" (default) or "local"
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Load .env if present
env_file = Path(__file__).parent.parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

# Ensure httpx bypasses proxy for internal hosts (httpx reads lowercase no_proxy).
# Derive the NSO/Vault/adapter hosts from their configured URLs (loaded from .env
# above) instead of hardcoding site addresses.
from urllib.parse import urlsplit  # noqa: E402

_required_no_proxy = {"localhost", "127.0.0.1"}
for _url_var in ("NSO_URL", "VAULT_ADDR", "ADAPTER_URL"):
    _host = urlsplit(os.environ.get(_url_var, "")).hostname
    if _host:
        _required_no_proxy.add(_host)
_existing_no_proxy = set(os.environ.get("no_proxy", "").split(",")) | set(os.environ.get("NO_PROXY", "").split(","))
_combined = ",".join(filter(None, _existing_no_proxy | _required_no_proxy))
os.environ["no_proxy"] = _combined
os.environ["NO_PROXY"] = _combined

import httpx  # noqa: E402

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

results: list[tuple[str, bool, str]] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    results.append((label, ok, detail))
    icon = PASS if ok else FAIL
    print(f"  {icon}  {label}" + (f"\n       {detail}" if detail else ""))


# ── 1. NSO RESTCONF ──────────────────────────────────────────────────────────
print("\n[1] NSO RESTCONF")
nso_url = os.environ.get("NSO_URL", "http://nso-host:8080")
nso_user = os.environ.get("NSO_USER", "admin")
nso_host_header = os.environ.get("NSO_HOST_HEADER", "nso.example.com")

secrets_backend = os.environ.get("SECRETS_BACKEND", "vault")
nso_password: str | None = None

if secrets_backend == "local":
    nso_password = os.environ.get("NSO_PASSWORD")
    if not nso_password:
        check("NSO_PASSWORD env var", False, "Set NSO_PASSWORD when SECRETS_BACKEND=local")
    else:
        check("NSO_PASSWORD present", True)

if nso_password or secrets_backend == "vault":
    _pw = nso_password or "placeholder-will-be-replaced-by-vault"
    try:
        resp = httpx.get(
            f"{nso_url}/restconf/data/tailf-ncs:devices",
            auth=(nso_user, _pw) if nso_password else None,
            headers={"Accept": "application/yang-data+json", "Host": nso_host_header},
            timeout=10,
        )
        if resp.status_code == 401 and secrets_backend == "vault":
            check("NSO RESTCONF reachable (auth skipped — needs Vault pw)", True, f"HTTP {resp.status_code}")
        elif resp.status_code < 300:
            data = resp.json()
            devices = data.get("tailf-ncs:devices", {}).get("device", [])
            check("NSO RESTCONF GET /devices", True, f"{len(devices)} device(s) found")
        else:
            check("NSO RESTCONF GET /devices", False, f"HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as exc:
        check("NSO RESTCONF reachable", False, str(exc))

# ── 2. Vault AppRole ─────────────────────────────────────────────────────────
print("\n[2] HashiCorp Vault")
vault_addr = os.environ.get("VAULT_ADDR", "https://vault.example.com:8200")
vault_role_id = os.environ.get("VAULT_ROLE_ID", "")
vault_secret_id = os.environ.get("VAULT_SECRET_ID", "")
vault_mount = os.environ.get("VAULT_MOUNT", "network")
vault_path = os.environ.get("VAULT_PATH", "credentials/example-svc")

if not vault_role_id or not vault_secret_id:
    check("Vault credentials in env", False, "VAULT_ROLE_ID / VAULT_SECRET_ID not set")
else:
    check("Vault credentials in env", True)
    try:
        login_resp = httpx.post(
            f"{vault_addr}/v1/auth/approle/login",
            json={"role_id": vault_role_id, "secret_id": vault_secret_id},
            verify=False,
            timeout=10,
        )
        if login_resp.status_code == 200:
            token = login_resp.json()["auth"]["client_token"]
            check("Vault AppRole login", True)

            # Read secret
            secret_resp = httpx.get(
                f"{vault_addr}/v1/{vault_mount}/data/{vault_path}",
                headers={"X-Vault-Token": token},
                verify=False,
                timeout=10,
            )
            if secret_resp.status_code == 200:
                secret_data: dict = secret_resp.json().get("data", {}).get("data", {})
                check("Vault KV read", True)
                for key in ("username", "password", "netbox_token", "adapter_token"):
                    check(f"  secret key '{key}' present", key in secret_data)

                # Now re-test NSO with real credentials from Vault
                if "username" in secret_data and "password" in secret_data:
                    nso_password = secret_data["password"]
                    nso_user_vault = secret_data["username"]
                    try:
                        resp2 = httpx.get(
                            f"{nso_url}/restconf/data/tailf-ncs:devices",
                            auth=(nso_user_vault, nso_password),
                            headers={"Accept": "application/yang-data+json", "Host": nso_host_header},
                            timeout=10,
                        )
                        if resp2.status_code < 300:
                            devices2 = resp2.json().get("tailf-ncs:devices", {}).get("device", [])
                            check("NSO RESTCONF with Vault credentials", True, f"{len(devices2)} device(s)")
                        else:
                            check("NSO RESTCONF with Vault credentials", False, f"HTTP {resp2.status_code}")
                    except Exception as exc2:
                        check("NSO RESTCONF with Vault credentials", False, str(exc2))
            else:
                check("Vault KV read", False, f"HTTP {secret_resp.status_code}")
        else:
            check("Vault AppRole login", False, f"HTTP {login_resp.status_code}")
    except Exception as exc:
        check("Vault reachable", False, str(exc))

# ── 3. Adapter health (optional) ─────────────────────────────────────────────
adapter_url = os.environ.get("ADAPTER_URL", "")
if adapter_url:
    print("\n[3] Adapter health")
    try:
        resp3 = httpx.get(f"{adapter_url}/healthz", timeout=5)
        check("/healthz", resp3.status_code == 200, f"HTTP {resp3.status_code}")
    except Exception as exc:
        check("/healthz", False, str(exc))

# ── Summary ──────────────────────────────────────────────────────────────────
print()
failed = [r for r in results if not r[1]]
if failed:
    print(f"RESULT: {FAIL}  {len(failed)} check(s) failed")
    sys.exit(1)
else:
    print(f"RESULT: {PASS}  All {len(results)} checks passed")
    sys.exit(0)
