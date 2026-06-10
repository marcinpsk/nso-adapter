# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract tests — producer side of the L2/L3-interface read-mirrors (M34–M36).

Covers GET /vlan-database, /switchport, /svi, /subinterface — the four read-only
endpoints the plugin consumes in vlan_reconciler / svi_reconciler /
subinterface_reconciler. These four share a trait the routing endpoints don't: their
response has **no** top-level ``last_refreshed_at``/``refresh_source`` and every level
emits a **fixed** key set (no optional/omitted keys).

Canonical contract: ``docs/api-contract.md`` (the M34–M36 sections).
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_l2_read.py`` — the
``*_KEYS`` sets MUST stay identical across both files.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

VLAN_TOP_KEYS = {"device_id", "vlans"}
VLAN_KEYS = {"vlan_id", "name", "source"}
SWITCHPORT_TOP_KEYS = {"device_id", "interfaces"}
SWITCHPORT_IFACE_KEYS = {"interface_name", "mode", "untagged_vlan", "tagged_vlans", "source"}
SVI_TOP_KEYS = {"device_id", "interfaces"}
SVI_IFACE_KEYS = {"interface_name", "vlan_id", "type", "vrf", "source"}
SUBIF_TOP_KEYS = {"device_id", "interfaces"}
SUBIF_IFACE_KEYS = {"interface_name", "parent_interface", "dot1q_vlan", "type", "vrf", "source"}


@pytest.mark.anyio
async def test_vlan_database_contract(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceVlan

    device_id = await seed_device(nso_device_name="l2-vlan", netbox_device_id=7950)
    async for db in get_session():
        db.add(DeviceVlan(device_id=device_id, vlan_id=100, name="DATA", refresh_source="poll"))
        await db.commit()
        break

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/vlan-database", headers=AUTH)).json()
    assert set(body.keys()) == VLAN_TOP_KEYS
    assert set(body["vlans"][0].keys()) == VLAN_KEYS
    assert body["vlans"][0]["source"] == "vlan-database"


@pytest.mark.anyio
async def test_switchport_contract(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSwitchport

    device_id = await seed_device(nso_device_name="l2-sw", netbox_device_id=7951)
    async for db in get_session():
        db.add(DeviceSwitchport(device_id=device_id, interface_name="GE0/1", mode="access", refresh_source="poll"))
        await db.commit()
        break

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/switchport", headers=AUTH)).json()
    assert set(body.keys()) == SWITCHPORT_TOP_KEYS
    iface = body["interfaces"][0]
    assert set(iface.keys()) == SWITCHPORT_IFACE_KEYS
    assert iface["untagged_vlan"] is None and iface["tagged_vlans"] == []  # always present even when empty


@pytest.mark.anyio
async def test_svi_contract(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSvi

    device_id = await seed_device(nso_device_name="l2-svi", netbox_device_id=7952)
    async for db in get_session():
        db.add(DeviceSvi(device_id=device_id, interface_name="Vlan100", vlan_id=100, svi_type="svi",
                         vrf="MGMT", refresh_source="poll"))
        await db.commit()
        break

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/svi", headers=AUTH)).json()
    assert set(body.keys()) == SVI_TOP_KEYS
    iface = body["interfaces"][0]
    assert set(iface.keys()) == SVI_IFACE_KEYS
    assert iface["type"] == "svi" and iface["source"] == "svi"


@pytest.mark.anyio
async def test_subinterface_contract(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceSubinterface

    device_id = await seed_device(nso_device_name="l2-subif", netbox_device_id=7953)
    async for db in get_session():
        db.add(DeviceSubinterface(device_id=device_id, interface_name="GE0/0.100", parent_interface="GE0/0",
                                  dot1q_vlan=100, sub_type="subinterface", vrf="", refresh_source="poll"))
        await db.commit()
        break

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/subinterface", headers=AUTH)).json()
    assert set(body.keys()) == SUBIF_TOP_KEYS
    iface = body["interfaces"][0]
    assert set(iface.keys()) == SUBIF_IFACE_KEYS
    assert iface["source"] == "subinterface"
