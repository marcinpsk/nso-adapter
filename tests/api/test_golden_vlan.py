# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — GET /vlan-database and /switchport.

Both are fixed-shape responses with NO top-level timestamp: every key is always
present (untagged_vlan null / tagged_vlans [] when empty; name/mode coerced to
""), so neither uses exclude_unset. Deep-equality pins the exact bytes.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device, seed_switchport, seed_vlan_database

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_vlan_database_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="vlan-golden", netbox_device_id=7980)
    await seed_vlan_database(device_id, [{"vlan_id": 100, "name": "DATA"}, {"vlan_id": 200}])

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/vlan-database", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "vlans": [
            {"vlan_id": 100, "name": "DATA", "source": "vlan-database"},
            {"vlan_id": 200, "name": "", "source": "vlan-database"},
        ],
    }


@pytest.mark.anyio
async def test_vlan_database_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="vlan-golden-empty", netbox_device_id=7981)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/vlan-database", headers=AUTH)).json()
    assert body == {"device_id": device_id, "vlans": []}


@pytest.mark.anyio
async def test_switchport_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="sw-golden", netbox_device_id=7982)
    await seed_switchport(
        device_id,
        [
            {"interface_name": "GE0/1", "mode": "access", "untagged_vlan": 100, "tagged_vlans": []},
            {"interface_name": "GE0/2", "mode": "trunk", "tagged_vlans": [300, 200]},
            {"interface_name": "GE0/3"},  # no mode -> "", no vlans
        ],
    )

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/switchport", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "interfaces": [
            {
                "interface_name": "GE0/1",
                "mode": "access",
                "untagged_vlan": 100,
                "tagged_vlans": [],
                "source": "switchport",
            },
            {
                "interface_name": "GE0/2",
                "mode": "trunk",
                "untagged_vlan": None,
                "tagged_vlans": [200, 300],
                "source": "switchport",
            },
            {"interface_name": "GE0/3", "mode": "", "untagged_vlan": None, "tagged_vlans": [], "source": "switchport"},
        ],
    }


@pytest.mark.anyio
async def test_switchport_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="sw-golden-empty", netbox_device_id=7983)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/switchport", headers=AUTH)).json()
    assert body == {"device_id": device_id, "interfaces": []}
