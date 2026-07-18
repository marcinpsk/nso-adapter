# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — the two union-shaped apply POSTs (switchport / lag-config).

Each returns a result envelope that is a union:
  * success → {status:"deployed", device, interface_count|bundle_count}
  * failure → {status:"error", error, message[, detail]}
These are documented in OpenAPI via ``responses={200: {"model": <Union>}}`` with
``response_model=None`` — the handler dict passes through untouched (zero wire risk),
so the golden is trivially neutral but pins both branch shapes. The core apply
delegates reach NSO (a true external boundary) and are patched to canned envelopes;
the deployed/error logic itself is covered by the core apply tests.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_switchport_apply_deployed_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="sw-ok", netbox_device_id=701)
    canned = {"status": "deployed", "device": "sw-ok", "interface_count": 2}
    with patch("nso_adapter.api.vlan.apply_switchport_core", new=AsyncMock(return_value=canned)):
        resp = await adapter_client_with_nso.post(
            f"/api/v1/devices/{device_id}/switchport/apply",
            json={"interfaces": []},
            headers=AUTH,
        )
    assert resp.status_code == 200
    assert resp.json() == canned


@pytest.mark.anyio
async def test_switchport_apply_error_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="sw-err", netbox_device_id=702)
    canned = {"status": "error", "error": "nso_commit_failed", "message": "boom", "detail": {"node": "x"}}
    with patch("nso_adapter.api.vlan.apply_switchport_core", new=AsyncMock(return_value=canned)):
        resp = await adapter_client_with_nso.post(
            f"/api/v1/devices/{device_id}/switchport/apply",
            json={"interfaces": []},
            headers=AUTH,
        )
    assert resp.status_code == 200
    assert resp.json() == canned


@pytest.mark.anyio
async def test_lag_config_apply_deployed_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="lag-ok", netbox_device_id=703)
    canned = {"status": "deployed", "device": "lag-ok", "bundle_count": 1}
    with patch("nso_adapter.api.lag_config.apply_lag_config_core", new=AsyncMock(return_value=canned)):
        resp = await adapter_client_with_nso.post(
            f"/api/v1/devices/{device_id}/lag-config/apply",
            json={"bundles": []},
            headers=AUTH,
        )
    assert resp.status_code == 200
    assert resp.json() == canned


@pytest.mark.anyio
async def test_lag_config_apply_error_golden(adapter_client_with_nso):
    device_id = await seed_device(nso_device_name="lag-err", netbox_device_id=704)
    canned = {"status": "error", "error": "no_nso_device_name", "message": "no name"}
    with patch("nso_adapter.api.lag_config.apply_lag_config_core", new=AsyncMock(return_value=canned)):
        resp = await adapter_client_with_nso.post(
            f"/api/v1/devices/{device_id}/lag-config/apply",
            json={"bundles": []},
            headers=AUTH,
        )
    assert resp.status_code == 200
    assert resp.json() == canned
