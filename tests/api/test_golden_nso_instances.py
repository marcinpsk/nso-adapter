# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — the nso_instances router (list / instance devices / instance neds).

The NSO RESTCONF client is a true external boundary (HTTP to NSO we can't run in a unit
test); spec=NsoClient bounds the fakes to the real interface, only the payload is canned.
  * list           → {id, name, base_url, reachable} (unreachable in the hermetic fixture);
  * {id}/devices   → EMIT-NULL enriched device rows (nullables as null), sorted by name;
  * {id}/neds      → NED packages with a derived ``platform`` label.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nso_adapter.nso.client import NsoClient
from tests.conftest import VALID_TOKEN

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


def _fake_nso(*, devices=None, neds=None):
    m = MagicMock(spec=NsoClient)
    m.list_devices = AsyncMock(return_value=devices or [])
    m.list_ned_packages = AsyncMock(return_value=neds or [])
    return m


@pytest.mark.anyio
async def test_list_nso_instances_golden(adapter_client_with_nso):
    """One configured instance; list_devices raises (real client, no NSO) → reachable False."""
    body = (await adapter_client_with_nso.get("/api/v1/nso-instances", headers=AUTH)).json()
    assert body == [{"id": "nso-dev", "name": "nso-dev", "base_url": "http://nso-dev:8080", "reachable": False}]


@pytest.mark.anyio
async def test_list_instance_devices_golden(adapter_client_with_nso):
    devices = [
        {
            "name": "core-rtr-01",
            "address": "10.0.0.1",
            "authgroup": "default",
            "device-type": {"cli": {"ned-id": "cisco-ios-cli-6.95"}},
            "state": {"admin-state": "unlocked"},
        },
        {"name": "aaa-switch-03"},  # minimal → every nullable null
    ]
    with patch("nso_adapter.api.nso_instances.get_nso_client", return_value=_fake_nso(devices=devices)):
        body = (await adapter_client_with_nso.get("/api/v1/nso-instances/nso-dev/devices", headers=AUTH)).json()

    assert body == [
        {
            "name": "aaa-switch-03",
            "address": None,
            "ned_id": None,
            "platform": None,
            "auth_group": None,
            "admin_state": None,
            "onboarded": False,
            "onboarded_device_id": None,
            "onboarded_netbox_device_id": None,
        },
        {
            "name": "core-rtr-01",
            "address": "10.0.0.1",
            "ned_id": "cisco-ios-cli-6.95",
            "platform": "ios",
            "auth_group": "default",
            "admin_state": "unlocked",
            "onboarded": False,
            "onboarded_device_id": None,
            "onboarded_netbox_device_id": None,
        },
    ]


@pytest.mark.anyio
async def test_list_instance_neds_golden(adapter_client_with_nso):
    neds = [
        {
            "ned_id": "cisco-ios-cli-6.95",
            "package": "cisco-ios-cli-6.95",
            "version": "6.95.5",
            "oper_status": "up",
            "vendor": "Cisco",
            "operating_systems": ["ios"],
            "product_families": ["catalyst"],
        }
    ]
    with patch("nso_adapter.api.nso_instances.get_nso_client", return_value=_fake_nso(neds=neds)):
        body = (await adapter_client_with_nso.get("/api/v1/nso-instances/nso-dev/neds", headers=AUTH)).json()

    assert body == [
        {
            "ned_id": "cisco-ios-cli-6.95",
            "package": "cisco-ios-cli-6.95",
            "version": "6.95.5",
            "oper_status": "up",
            "vendor": "Cisco",
            "operating_systems": ["ios"],
            "product_families": ["catalyst"],
            "platform": "ios",
        }
    ]
