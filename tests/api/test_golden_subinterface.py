# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/subinterface.

Fixed-shape response, NO top-level timestamp; every key always present
(parent_interface/dot1q_vlan null when unset; vrf coerced to ""). Deep-equality
pins the exact bytes.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device, seed_subinterface

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_subinterface_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="subif-golden", netbox_device_id=7986)
    await seed_subinterface(
        device_id,
        [
            {"interface_name": "GE0/0.100", "parent_interface": "GE0/0", "dot1q_vlan": 100, "vrf": "RED"},
            {"interface_name": "GE0/1.200", "dot1q_vlan": 200},  # no parent_interface (null), no vrf ("")
        ],
    )

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/subinterface", headers=AUTH)).json()
    # Ordered by interface_name.
    assert body == {
        "device_id": device_id,
        "interfaces": [
            {
                "interface_name": "GE0/0.100",
                "parent_interface": "GE0/0",
                "dot1q_vlan": 100,
                "type": "subinterface",
                "vrf": "RED",
                "source": "subinterface",
            },
            {
                "interface_name": "GE0/1.200",
                "parent_interface": None,
                "dot1q_vlan": 200,
                "type": "subinterface",
                "vrf": "",
                "source": "subinterface",
            },
        ],
    }


@pytest.mark.anyio
async def test_subinterface_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="subif-golden-empty", netbox_device_id=7987)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/subinterface", headers=AUTH)).json()
    assert body == {"device_id": device_id, "interfaces": []}
