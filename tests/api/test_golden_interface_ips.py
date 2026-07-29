# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/interface-ips.

Fixed/EMIT-NULL shape: ``bound_port`` and each address's ``prefix_length`` are
always present (null when absent), so the response model must NOT use
exclude_unset. ``last_refreshed_at`` is a "<iso>Z" string. Interfaces are sorted
by name; addresses keep insertion order within an interface.
"""

from __future__ import annotations

from datetime import UTC, datetime

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

TS = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)


async def _seed_ips(device_id: int) -> None:
    from nso_adapter.store.models import InterfaceIpAddress

    async with session() as db:
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name="GE0/0",
                address="10.0.0.1/24",
                vrf="",
                family="ipv4",
                secondary=False,
                bound_port="lag-99:100",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name="GE0/0",
                address="10.0.0.2/24",
                vrf="",
                family="ipv4",
                secondary=True,
                bound_port="lag-99:100",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        # Second interface: no bound_port (null), an address with no prefix length.
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name="Loopback0",
                address="192.0.2.1",
                vrf="MGMT",
                family="ipv4",
                secondary=False,
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        await db.commit()


@pytest.mark.anyio
async def test_interface_ips_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="ip-golden", netbox_device_id=7950)
    await pin_store_incarnation()
    await _seed_ips(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)).json()

    # interfaces sorted by name: "GE0/0" < "Loopback0".
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "read_state": _SYNTH_READ_STATE,
        "interfaces": [
            {
                "interface": "GE0/0",
                "bound_port": "lag-99:100",
                "addresses": [
                    {"address": "10.0.0.1/24", "prefix_length": 24, "family": "ipv4", "secondary": False, "vrf": ""},
                    {"address": "10.0.0.2/24", "prefix_length": 24, "family": "ipv4", "secondary": True, "vrf": ""},
                ],
            },
            {
                "interface": "Loopback0",
                "bound_port": None,
                "addresses": [
                    {
                        "address": "192.0.2.1",
                        "prefix_length": None,
                        "family": "ipv4",
                        "secondary": False,
                        "vrf": "MGMT",
                    }
                ],
            },
        ],
    }


@pytest.mark.anyio
async def test_interface_ips_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="ip-golden-empty", netbox_device_id=7951)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "read_state": _SYNTH_READ_STATE,
        "interfaces": [],
    }
