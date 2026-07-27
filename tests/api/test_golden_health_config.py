# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — health (/healthz) and config (/config/failover) routers.

healthz is unauthenticated and lists reachability per configured NSO instance (none
in the hermetic fixture → empty). config/failover round-trips the failover tuning
singleton: PUT known values, GET echoes them back plus the static deployment switch.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_healthz_golden(adapter_client):
    from nso_adapter import __version__

    body = (await adapter_client.get("/healthz")).json()
    assert body == {"status": "ok", "version": __version__, "nso_instances": []}


@pytest.mark.anyio
async def test_failover_config_roundtrip_golden(adapter_client):
    from nso_adapter.config import get_config

    payload = {
        "enabled": True,
        "primary_probe_interval": 5,
        "oob_probe_interval": 10,
        "failure_threshold": 3,
        "success_threshold": 2,
        "probe_timeout": 4.0,
        "active_probe_timeout": 2.0,
        "probe_concurrency": 8,
        "max_flips_per_tick": 16,
        "sync_from_after_switch": True,
    }
    deployment_enabled = get_config().scheduler.enable_failover
    expected = {**payload, "deployment_enabled": deployment_enabled}

    put_body = (await adapter_client.put("/api/v1/config/failover", json=payload, headers=AUTH)).json()
    assert put_body == expected

    get_body = (await adapter_client.get("/api/v1/config/failover", headers=AUTH)).json()
    assert get_body == expected
