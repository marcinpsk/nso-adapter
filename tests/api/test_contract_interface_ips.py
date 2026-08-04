# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/interface-ips.

Addresses are grouped per interface; every level emits a fixed key set. Consumed by the
plugin in ``template_content`` (interface-IP reconcile).

Canonical contract: ``docs/api-contract.md`` (interface-ips §).
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_interface_ips.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TOP_KEYS = {"device_id", "last_refreshed_at", "refresh_source", "read_state", "interfaces"}
IFACE_KEYS = {"interface", "bound_port", "addresses"}
ADDR_KEYS = {"address", "prefix_length", "family", "secondary", "vrf"}


@pytest.mark.anyio
async def test_interface_ips_contract(adapter_client):
    from nso_adapter.store.models import InterfaceIpAddress

    device_id = await seed_device(nso_device_name="ip-ct", netbox_device_id=7980)
    ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
    async with session() as db:
        db.add(
            InterfaceIpAddress(
                device_id=device_id,
                interface_name="GE0/0",
                address="10.0.0.1/24",
                vrf="",
                family="ipv4",
                secondary=False,
                bound_port="GE0/0",
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        await db.commit()

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS
    iface = body["interfaces"][0]
    assert set(iface.keys()) == IFACE_KEYS
    addr = iface["addresses"][0]
    assert set(addr.keys()) == ADDR_KEYS
    assert isinstance(addr["secondary"], bool)
    assert isinstance(addr["prefix_length"], int)


@pytest.mark.anyio
async def test_interface_ips_no_data_shape(adapter_client):
    device_id = await seed_device(nso_device_name="ip-ct-empty", netbox_device_id=7981)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/interface-ips", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS
    assert body["interfaces"] == [] and body["refresh_source"] == "never"
