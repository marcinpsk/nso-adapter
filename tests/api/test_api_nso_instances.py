# SPDX-License-Identifier: Apache-2.0
"""Tests for GET /api/v1/nso-instances/{id}/devices — enriched device list."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

# Realistic NSO payload covering all nullable fields
_NSO_DEVICES = [
    {
        "name": "core-rtr-01",
        "address": "10.0.0.1",
        "authgroup": "default",
        "device-type": {"cli": {"ned-id": "cisco-ios-cli-6.95"}},
        "state": {"admin-state": "unlocked"},
    },
    {
        "name": "edge-rtr-02",
        "address": "10.0.0.2",
        "authgroup": "lab",
        "device-type": {"netconf": {"ned-id": "juniper-junos-nc-4.1"}},
        "state": {"admin-state": "locked"},
    },
    {
        "name": "aaa-switch-03",
        # no address, no device-type, no state — tests null handling
    },
]


def _fake_nso(devices=_NSO_DEVICES):
    m = MagicMock()
    m.list_devices = AsyncMock(return_value=devices)
    return m


def _fake_cfg(instance_name="nso-dev"):
    inst = MagicMock()
    inst.name = instance_name
    cfg = MagicMock()
    cfg.nso_instances = [inst]
    return cfg


async def test_list_devices_unknown_instance_returns_404(adapter_client):
    """Instance ID not in config → 404."""
    with patch("nso_adapter.api.nso_instances.get_config", return_value=_fake_cfg("other")):
        resp = await adapter_client.get("/api/v1/nso-instances/nso-dev/devices", headers=AUTH)
    assert resp.status_code == 404


async def test_list_devices_enriched_fields_present(adapter_client):
    """Response includes all nullable fields; no key omitted."""
    with patch("nso_adapter.api.nso_instances.get_config", return_value=_fake_cfg()), \
         patch("nso_adapter.api.nso_instances.get_nso_client", return_value=_fake_nso()):
        resp = await adapter_client.get("/api/v1/nso-instances/nso-dev/devices", headers=AUTH)
    assert resp.status_code == 200
    items = resp.json()
    # All items must carry every key (nullable fields must be present, not omitted)
    for item in items:
        for key in ("name", "address", "ned_id", "platform", "auth_group",
                    "admin_state", "onboarded", "onboarded_device_id",
                    "onboarded_netbox_device_id"):
            assert key in item, f"Key '{key}' missing in {item}"


async def test_list_devices_cisco_ned_derives_platform(adapter_client):
    """ned_id from Cisco IOS NED → platform='ios'."""
    with patch("nso_adapter.api.nso_instances.get_config", return_value=_fake_cfg()), \
         patch("nso_adapter.api.nso_instances.get_nso_client", return_value=_fake_nso()):
        resp = await adapter_client.get("/api/v1/nso-instances/nso-dev/devices", headers=AUTH)
    items = {d["name"]: d for d in resp.json()}
    assert items["core-rtr-01"]["ned_id"] == "cisco-ios-cli-6.95"
    assert items["core-rtr-01"]["platform"] == "ios"
    assert items["edge-rtr-02"]["ned_id"] == "juniper-junos-nc-4.1"
    assert items["edge-rtr-02"]["platform"] == "junos"


async def test_list_devices_null_fields_for_minimal_payload(adapter_client):
    """Device without address/ned/state → all nullable fields are null, not omitted."""
    with patch("nso_adapter.api.nso_instances.get_config", return_value=_fake_cfg()), \
         patch("nso_adapter.api.nso_instances.get_nso_client", return_value=_fake_nso()):
        resp = await adapter_client.get("/api/v1/nso-instances/nso-dev/devices", headers=AUTH)
    items = {d["name"]: d for d in resp.json()}
    minimal = items["aaa-switch-03"]
    assert minimal["address"] is None
    assert minimal["ned_id"] is None
    assert minimal["platform"] is None
    assert minimal["auth_group"] is None
    assert minimal["admin_state"] is None


async def test_list_devices_onboarded_cross_reference(adapter_client):
    """Device already in adapter Device table → onboarded=True with correct IDs."""
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="core-rtr-01",
        netbox_device_id=42,
    )
    with patch("nso_adapter.api.nso_instances.get_config", return_value=_fake_cfg()), \
         patch("nso_adapter.api.nso_instances.get_nso_client", return_value=_fake_nso()):
        resp = await adapter_client.get("/api/v1/nso-instances/nso-dev/devices", headers=AUTH)
    items = {d["name"]: d for d in resp.json()}
    assert items["core-rtr-01"]["onboarded"] is True
    assert items["core-rtr-01"]["onboarded_device_id"] == device_id
    assert items["core-rtr-01"]["onboarded_netbox_device_id"] == 42
    # edge-rtr-02 is NOT onboarded
    assert items["edge-rtr-02"]["onboarded"] is False
    assert items["edge-rtr-02"]["onboarded_device_id"] is None
    assert items["edge-rtr-02"]["onboarded_netbox_device_id"] is None


async def test_list_devices_sorted_by_name(adapter_client):
    """Response is sorted by name ascending."""
    with patch("nso_adapter.api.nso_instances.get_config", return_value=_fake_cfg()), \
         patch("nso_adapter.api.nso_instances.get_nso_client", return_value=_fake_nso()):
        resp = await adapter_client.get("/api/v1/nso-instances/nso-dev/devices", headers=AUTH)
    names = [d["name"] for d in resp.json()]
    assert names == sorted(names)


async def test_list_devices_onboarded_no_netbox_match(adapter_client):
    """Device in DB with netbox_device_id=None → onboarded=True, netbox ids correct."""
    from tests.conftest import seed_device
    device_id = await seed_device(
        nso_instance="nso-dev",
        nso_device_name="core-rtr-01",
        netbox_device_id=None,
    )
    nso_devices = [{"name": "core-rtr-01"}]
    with patch("nso_adapter.api.nso_instances.get_config", return_value=_fake_cfg()), \
         patch(
             "nso_adapter.api.nso_instances.get_nso_client",
             return_value=AsyncMock(**{"list_devices.return_value": nso_devices}),
         ):
        resp = await adapter_client.get(
            "/api/v1/nso-instances/nso-dev/devices",
            headers={"Authorization": f"Bearer {VALID_TOKEN}"},
        )
    assert resp.status_code == 200
    by_name = {d["name"]: d for d in resp.json()}
    item = by_name["core-rtr-01"]
    assert item["onboarded"] is True
    assert item["onboarded_device_id"] == device_id
    assert item["onboarded_netbox_device_id"] is None


async def test_list_devices_cross_instance_isolation(adapter_client):
    """Device in a different NSO instance must not appear as onboarded for nso-dev."""
    from tests.conftest import seed_device
    await seed_device(
        nso_instance="nso-prod",
        nso_device_name="core-rtr-01",
        netbox_device_id=99,
    )
    nso_devices = [{"name": "core-rtr-01"}]
    with patch("nso_adapter.api.nso_instances.get_config", return_value=_fake_cfg()), \
         patch(
             "nso_adapter.api.nso_instances.get_nso_client",
             return_value=AsyncMock(**{"list_devices.return_value": nso_devices}),
         ):
        resp = await adapter_client.get(
            "/api/v1/nso-instances/nso-dev/devices",
            headers={"Authorization": f"Bearer {VALID_TOKEN}"},
        )
    assert resp.status_code == 200
    by_name = {d["name"]: d for d in resp.json()}
    item = by_name["core-rtr-01"]
    assert item["onboarded"] is False
    assert item["onboarded_device_id"] is None
