# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/interface-mtu.

S1b: this read GET had no test_contract_*.py before now. Fixed/EMIT-NULL shape
with NO top-level timestamp: mtu/ip_mtu/mpls_mtu are always present (null when
unset), bound_port is coerced to "". No exclude_unset. Deep-equality pins the bytes.
"""

from __future__ import annotations

import pytest

from tests.conftest import (
    GOLDEN_BORN_ISO,
    GOLDEN_INCARNATION,
    VALID_TOKEN,
    pin_store_incarnation,
    seed_device,
    session,
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


async def _seed_mtu(device_id: int) -> None:
    from nso_adapter.store.models import DeviceInterfaceMtu

    async with session() as db:
        db.add(
            DeviceInterfaceMtu(
                device_id=device_id,
                interface_name="GE0/0",
                mtu=9000,
                ip_mtu=8986,
                mpls_mtu=9000,
                bound_port="1/1/1",
                refresh_source="poll",
            )
        )
        # Minimal: all MTU values null, bound_port unset -> "".
        db.add(DeviceInterfaceMtu(device_id=device_id, interface_name="GE0/1", refresh_source="poll"))
        await db.commit()


@pytest.mark.anyio
async def test_interface_mtu_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="mtu-golden", netbox_device_id=7957)
    await pin_store_incarnation()
    await _seed_mtu(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/interface-mtu", headers=AUTH)).json()
    # Ordered by interface_name.
    assert body == {
        "device_id": device_id,
        "read_state": _SYNTH_READ_STATE,
        "interfaces": [
            {"interface_name": "GE0/0", "mtu": 9000, "ip_mtu": 8986, "mpls_mtu": 9000, "bound_port": "1/1/1"},
            {"interface_name": "GE0/1", "mtu": None, "ip_mtu": None, "mpls_mtu": None, "bound_port": ""},
        ],
    }


@pytest.mark.anyio
async def test_interface_mtu_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="mtu-golden-empty", netbox_device_id=7958)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/interface-mtu", headers=AUTH)).json()
    assert body == {"device_id": device_id, "read_state": _SYNTH_READ_STATE, "interfaces": []}
