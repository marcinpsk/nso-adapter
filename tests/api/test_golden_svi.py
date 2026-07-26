# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/svi.

Fixed-shape response, NO top-level timestamp; every key always present (vrf
coerced to ""). Deep-equality pins the exact bytes.
"""

from __future__ import annotations

import pytest

from tests.conftest import (
    GOLDEN_BORN_ISO,
    GOLDEN_INCARNATION,
    VALID_TOKEN,
    pin_store_incarnation,
    seed_device,
    seed_svi,
)

_SYNTH_READ_STATE = {
    "outcome": "unavailable",
    "reason": "not_ready",
    "freshness": None,
    "result": None,
    "succeeded": None,
    "read_at": None,
    "attempt_id": None,
    "source_epoch": 1,
    "payload_revision": None,
    "incarnation": GOLDEN_INCARNATION,
    "incarnation_born": GOLDEN_BORN_ISO,
}

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


@pytest.mark.anyio
async def test_svi_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="svi-golden", netbox_device_id=7984)
    await pin_store_incarnation()
    await seed_svi(
        device_id,
        [
            {"interface_name": "Vlan100", "vlan_id": 100, "type": "svi", "vrf": "MGMT"},
            {"interface_name": "Vlan200", "vlan_id": 200},  # no vrf -> ""
        ],
    )

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/svi", headers=AUTH)).json()
    # Ordered by vlan_id.
    assert body == {
        "device_id": device_id,
        "read_state": _SYNTH_READ_STATE,
        "interfaces": [
            {"interface_name": "Vlan100", "vlan_id": 100, "type": "svi", "vrf": "MGMT", "source": "svi"},
            {"interface_name": "Vlan200", "vlan_id": 200, "type": "svi", "vrf": "", "source": "svi"},
        ],
    }


@pytest.mark.anyio
async def test_svi_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="svi-golden-empty", netbox_device_id=7985)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/svi", headers=AUTH)).json()
    assert body == {"device_id": device_id, "read_state": _SYNTH_READ_STATE, "interfaces": []}
