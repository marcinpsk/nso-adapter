# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/logging-config.

Deep-equality proof of the remote-syslog read mirror. Host optional keys (port/
severity/facility/transport/vrf/source) are OMITTED when unset. ``last_refreshed_at``
is a "<iso>Z" string.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0)


async def _seed_logging(device_id: int) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceLoggingHost

    async for db in get_session():
        db.add(
            DeviceLoggingHost(
                device_id=device_id,
                address="10.0.0.5",
                port=514,
                severity="informational",
                facility="local7",
                transport="udp",
                vrf="MGMT",
                source="Loopback0",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        db.add(DeviceLoggingHost(device_id=device_id, address="10.0.0.6", last_refreshed_at=TS, refresh_source="poll"))
        await db.commit()
        break


@pytest.mark.anyio
async def test_logging_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="log-golden", netbox_device_id=7968)
    await _seed_logging(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/logging-config", headers=AUTH)).json()

    # Ordered by address: "10.0.0.5" < "10.0.0.6".
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "hosts": [
            {
                "address": "10.0.0.5",
                "port": 514,
                "severity": "informational",
                "facility": "local7",
                "transport": "udp",
                "vrf": "MGMT",
                "source": "Loopback0",
            },
            {"address": "10.0.0.6"},
        ],
    }


@pytest.mark.anyio
async def test_logging_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="log-golden-empty", netbox_device_id=7969)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/logging-config", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "hosts": [],
    }
