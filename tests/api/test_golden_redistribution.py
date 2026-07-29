# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/redistribution.

Deep-equality proof of the exact redistribution JSON. Optional keys (route_map/
metric/metric_type) are OMITTED when unset. Like OSPF (and unlike static/bgp/isis)
the reader returns the RAW naive datetime, so ``last_refreshed_at`` serialises with
NO trailing ``Z`` and the model field must be ``datetime`` (not ``str``).
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


async def _seed_redistribution(device_id: int) -> None:
    from nso_adapter.store.models import DeviceRedistribution

    async with session() as db:
        db.add(
            DeviceRedistribution(
                device_id=device_id,
                dest_protocol="ospf",
                dest_ref="1",
                source_protocol="bgp",
                source_ref="65100",
                route_map="RM-REDIST",
                metric=100,
                metric_type="type-1",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        db.add(
            DeviceRedistribution(
                device_id=device_id,
                dest_protocol="isis",
                dest_ref="",
                source_protocol="connected",
                source_ref="",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        await db.commit()


@pytest.mark.anyio
async def test_redistribution_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="rd-golden", netbox_device_id=7925)
    await pin_store_incarnation()
    await _seed_redistribution(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)).json()

    # Ordered by (dest_protocol, dest_ref, source_protocol): "isis" < "ospf".
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00",  # RAW datetime — no trailing "Z"
        "refresh_source": "poll",
        "read_state": _SYNTH_READ_STATE,
        "entries": [
            {"dest_protocol": "isis", "dest_ref": "", "source_protocol": "connected", "source_ref": ""},
            {
                "dest_protocol": "ospf",
                "dest_ref": "1",
                "source_protocol": "bgp",
                "source_ref": "65100",
                "route_map": "RM-REDIST",
                "metric": 100,
                "metric_type": "type-1",
            },
        ],
    }


@pytest.mark.anyio
async def test_redistribution_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="rd-golden-empty", netbox_device_id=7926)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "read_state": _SYNTH_READ_STATE,
        "entries": [],
    }
