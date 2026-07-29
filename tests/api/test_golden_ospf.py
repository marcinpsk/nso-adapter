# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/ospf.

Deep-equality proof of the exact OSPF read-mirror JSON. ``last_refreshed_at`` goes
through ``iso_z`` like every other family, so it serialises as ``"<iso>Z"``.

Covers a maximal instance (router_id + enabled + areas), a minimal instance
(``areas`` still present as []), a maximal + minimal interface, and the empty
device.
"""

from __future__ import annotations

from datetime import datetime

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

TS = datetime(2026, 6, 1, 10, 0, 0)


async def _seed_ospf(device_id: int) -> None:
    from nso_adapter.store.models import DeviceOspfInstance, DeviceOspfInterface

    async with session() as db:
        db.add(
            DeviceOspfInstance(
                device_id=device_id,
                process_id="1",
                vrf="",
                areas=["0.0.0.0"],
                router_id="10.0.0.1",
                enabled=False,  # explicit admin-state → emitted
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        # Minimal instance: no router_id/enabled; areas unset → emitted as [] (row.areas or []).
        db.add(
            DeviceOspfInstance(device_id=device_id, process_id="2", vrf="", last_refreshed_at=TS, refresh_source="poll")
        )
        db.add(
            DeviceOspfInterface(
                device_id=device_id,
                interface_name="GE0/0",
                process_id="1",
                area_id="0.0.0.0",
                passive=True,
                priority=10,
                cost=100,
                network_type="point-to-point",
                auth_type="md5",
                auth_present=True,
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        db.add(
            DeviceOspfInterface(
                device_id=device_id,
                interface_name="GE0/1",
                passive=False,
                auth_present=False,
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        await db.commit()


@pytest.mark.anyio
async def test_ospf_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="ospf-golden", netbox_device_id=7935)
    await pin_store_incarnation()
    await _seed_ospf(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/ospf", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "read_state": _SYNTH_READ_STATE,
        "instances": [
            {"process_id": "1", "vrf": "", "areas": ["0.0.0.0"], "router_id": "10.0.0.1", "enabled": False},
            {"process_id": "2", "vrf": "", "areas": []},
        ],
        "interfaces": [
            {
                "interface_name": "GE0/0",
                "passive": True,
                "auth_present": True,
                "process_id": "1",
                "area_id": "0.0.0.0",
                "priority": 10,
                "cost": 100,
                "network_type": "point-to-point",
                "auth_type": "md5",
            },
            {"interface_name": "GE0/1", "passive": False, "auth_present": False},
        ],
    }


@pytest.mark.anyio
async def test_ospf_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="ospf-golden-empty", netbox_device_id=7936)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/ospf", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "read_state": _SYNTH_READ_STATE,
        "instances": [],
        "interfaces": [],
    }
