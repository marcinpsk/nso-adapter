# SPDX-License-Identifier: Apache-2.0
"""NSO Adapter — configuration loading.

Config is split into two layers:
- EnvSettings  — bootstrap env vars only (AppRole creds, config file path)
- AppConfig    — loaded from config.yaml; holds all non-secret settings and
                 secret *references* (never secret values).
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings

# ── YAML-based config models ─────────────────────────────────────────────────


class VaultConfig(BaseModel):
    address: str
    kv_mount: str
    auth_method: str = "approle"
    namespace: str | None = None
    verify_ssl: bool = True


class SecretsConfig(BaseModel):
    provider: str  # "vault" | "local"
    vault: VaultConfig | None = None


class NsoInstanceConfig(BaseModel):
    """One NSO instance. Secret fields hold references, not values."""

    name: str
    base_url: str
    ca_cert: str | None = None
    username_ref: str  # e.g. "credentials/nso#username" (vault) or "NSO_USERNAME" (local)
    password_ref: str  # e.g. "credentials/nso#password"
    host_header: str | None = None  # required by some NSO webui configs (server-name)


class NetboxConfig(BaseModel):
    base_url: str
    ca_cert: str | None = None
    api_token_ref: str  # e.g. "credentials/example-svc#netbox_token"


class ApiConfig(BaseModel):
    adapter_token_ref: str  # e.g. "credentials/nso-adapter#adapter_token"


class SchedulerConfig(BaseModel):
    poll_interval: int = 15
    scope_reconcile_interval: int = 5
    # Layer B durable worker: number of in-process worker tasks draining the Job
    # table.  Default 1 (serial) — per-device dedup means cross-device parallelism
    # is the only thing >1 buys, at the cost of more concurrent NSO/NetBox load.
    worker_concurrency: int = 1
    enable_nso_streams: bool = True
    lag_topology_poll_interval: int = 60
    enable_interface_ip_sync: bool = True
    interface_ip_poll_interval: int = 60
    enable_snmp_sync: bool = True
    snmp_poll_interval: int = 300
    enable_static_routing_sync: bool = True
    static_route_poll_interval: int = 300
    enable_isis_sync: bool = True
    isis_poll_interval: int = 300
    enable_bgp_sync: bool = True
    bgp_poll_interval: int = 300
    enable_ospf_sync: bool = True
    ospf_poll_interval: int = 300
    enable_redistribution_sync: bool = True
    redistribution_poll_interval: int = 300
    enable_route_policy_sync: bool = True
    route_policy_poll_interval: int = 300
    # Topology interface reconcile: ensure NetBox holds the LAG parents, logical
    # channels/SAPs and loopback/system interfaces that bound_port correlation
    # needs. Runs after the IS-IS/IP/LAG refreshes have populated the mirror.
    enable_topology_interface_sync: bool = True
    topology_interface_poll_interval: int = 120


class AppConfig(BaseModel):
    """Full application config — loaded from config.yaml."""

    secrets: SecretsConfig
    nso_instances: list[NsoInstanceConfig] = []
    netbox: NetboxConfig
    api: ApiConfig
    scheduler: SchedulerConfig = SchedulerConfig()
    database_url: str = "sqlite+aiosqlite:///./nso_adapter.db"
    log_level: str = "INFO"
    log_format: str = "json"


# ── Env-only bootstrap settings ──────────────────────────────────────────────


class EnvSettings(BaseSettings):
    """Minimal env-only settings — only the AppRole secrets needed to bootstrap Vault,
    plus the path to the config file."""

    config_file: str = "config.yaml"
    vault_role_id: str = ""
    vault_secret_id: str = ""

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "populate_by_name": True,
        "extra": "ignore",
    }


# ── Singletons ────────────────────────────────────────────────────────────────

_app_config: AppConfig | None = None
_env_settings: EnvSettings | None = None


def get_env_settings() -> EnvSettings:
    global _env_settings
    if _env_settings is None:
        _env_settings = EnvSettings()
    return _env_settings


def get_config() -> AppConfig:
    global _app_config
    if _app_config is None:
        env = get_env_settings()
        path = Path(env.config_file)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}. Copy config.yaml.example to {path} and edit it.")
        raw = yaml.safe_load(path.read_text()) or {}
        # Allow the deployment environment to override the DB URL without editing
        # config.yaml — keeps dev/prod DB credentials with the infra (compose/.env).
        db_url_override = os.environ.get("DATABASE_URL")
        if db_url_override:
            raw["database_url"] = db_url_override
        _app_config = AppConfig(**raw)
    return _app_config


def reset_config() -> None:
    """For testing only."""
    global _app_config, _env_settings
    _app_config = None
    _env_settings = None
