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
    lag_config_poll_interval: int = 60
    enable_interface_ip_sync: bool = True
    interface_ip_poll_interval: int = 60
    enable_snmp_sync: bool = True
    enable_logging_sync: bool = True
    enable_bfd_sync: bool = True
    snmp_poll_interval: int = 300
    logging_poll_interval: int = 300
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
    # Route-policy capability matrix probe — refreshes the representable half per device
    # on a slow cadence (daily by default; the apply-failed hook keeps the accepted half
    # current between probes). Operators can also force a refresh via the capability API.
    enable_capability_refresh: bool = True
    capability_refresh_interval: int = 1440  # minutes (daily)
    enable_l2_service_sync: bool = True
    l2_service_poll_interval: int = 300
    # L2/L3 interface family (M34 VLAN-db + switchport, M35 SVI/IRB, M36 dot1q
    # subinterface). These otherwise refresh ONLY on an SSE config-change event, so
    # without a periodic poll their mirror never populates / self-heals on a device
    # that hasn't changed since the adapter started.
    enable_vlan_sync: bool = True
    vlan_poll_interval: int = 300
    enable_switchport_sync: bool = True
    switchport_poll_interval: int = 300
    enable_svi_sync: bool = True
    svi_poll_interval: int = 300
    enable_subinterface_sync: bool = True
    subinterface_poll_interval: int = 300
    enable_interface_mtu_sync: bool = True
    interface_mtu_poll_interval: int = 300
    # Topology interface reconcile: ensured NetBox held the LAG parents, logical
    # channels/SAPs and loopback/system interfaces that the OLD bound_port-named
    # correlation needed. M27R supersedes this: interface-attributes now exports
    # the Nokia logical interfaces with parent-binding/kind, and sync_device's
    # bulk_ensure_interfaces creates them by their FAITHFUL (logical) name with
    # the right parent — so this job would now create bound_port-named DUPLICATES.
    # Default off. (Follow-up: fold members-only LAG creation into the new path.)
    enable_topology_interface_sync: bool = False
    topology_interface_poll_interval: int = 120
    # ── Management-IP failover (Phase 0: OFF by default; hardcoded fast test cadence).
    # NSO probes reachability; the adapter switches the device address primary↔OOB with
    # hysteresis. A frequent base tick processes only devices whose per-address probe is due.
    # Prod cadence (set after the perf spike): primary ~15m, OOB ~6–12h.
    enable_failover: bool = False
    failover_base_tick: int = 1  # minutes — how often the loop wakes to process due probes
    failover_primary_probe_interval: int = 2  # minutes — probe the primary IP
    failover_oob_probe_interval: int = 15  # minutes — probe/verify the OOB fallback IP
    failover_failure_threshold: int = 3  # consecutive failures before primary→OOB
    failover_success_threshold: int = 5  # consecutive successes before OOB→primary
    failover_probe_timeout: float = 10.0  # seconds — short, so an unreachable connect can't hang
    failover_sync_from_after_switch: bool = True


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
    """Minimal env-only settings.

    Only the AppRole secrets needed to bootstrap Vault, plus the path to the config file.
    """

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
