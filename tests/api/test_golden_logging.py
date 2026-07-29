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


async def _seed_logging(device_id: int) -> None:
    from nso_adapter.store.models import DeviceLoggingHost

    async with session() as db:
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


@pytest.mark.anyio
async def test_logging_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="log-golden", netbox_device_id=7968)
    await pin_store_incarnation()
    await _seed_logging(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/logging-config", headers=AUTH)).json()

    # Ordered by address: "10.0.0.5" < "10.0.0.6".
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "read_state": _SYNTH_READ_STATE,
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
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/logging-config", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "read_state": _SYNTH_READ_STATE,
        "hosts": [],
    }


async def _seed_levels(device_id: int, **severities) -> None:
    from nso_adapter.store.models import DeviceLoggingLevels

    async with session() as db:
        db.add(DeviceLoggingLevels(device_id=device_id, last_refreshed_at=TS, refresh_source="poll", **severities))
        await db.commit()


@pytest.mark.anyio
async def test_logging_golden_local_levels(adapter_client):
    """local_levels rides the body when the mirror row exists; unset severities are OMITTED."""
    device_id = await seed_device(nso_device_name="log-golden-lvl", netbox_device_id=7970)
    await pin_store_incarnation()
    await _seed_levels(device_id, console_severity="CRITICAL", monitor_severity="NOTICE")

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/logging-config", headers=AUTH)).json()

    # A levels-only device: freshness comes from the levels row; hosts stay [].
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "read_state": _SYNTH_READ_STATE,
        "hosts": [],
        "local_levels": {"console_severity": "CRITICAL", "monitor_severity": "NOTICE"},
    }


@pytest.mark.anyio
async def test_logging_golden_hosts_and_levels(adapter_client):
    device_id = await seed_device(nso_device_name="log-golden-both", netbox_device_id=7971)
    await pin_store_incarnation()
    await _seed_logging(device_id)
    await _seed_levels(device_id, console_severity="CRITICAL", monitor_severity="NOTICE", module_severity="NOTICE")

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/logging-config", headers=AUTH)).json()

    assert body["local_levels"] == {
        "console_severity": "CRITICAL",
        "monitor_severity": "NOTICE",
        "module_severity": "NOTICE",
    }
    assert [h["address"] for h in body["hosts"]] == ["10.0.0.5", "10.0.0.6"]
