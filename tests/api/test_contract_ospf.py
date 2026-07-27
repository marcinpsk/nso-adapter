# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/ospf.

Pins the JSON shape the adapter emits for OSPF, which the plugin consumes in
``template_content._reconcile_ospf``. Optional keys are OMITTED when unset (not null).

Canonical contract: ``docs/api-contract.md`` § "GET .../ospf".
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_ospf.py`` — the
``*_KEYS`` sets MUST stay identical across both files.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

REQUIRED_TOP_KEYS = {"device_id", "last_refreshed_at", "refresh_source", "read_state", "instances", "interfaces"}
REQUIRED_INSTANCE_KEYS = {"process_id", "vrf", "areas"}
OPTIONAL_INSTANCE_KEYS = {"router_id"}
REQUIRED_IFACE_KEYS = {"interface_name", "passive", "auth_present"}
OPTIONAL_IFACE_KEYS = {"process_id", "area_id", "priority", "cost", "network_type", "auth_type"}


async def _seed_ospf(device_id: int) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceOspfInstance, DeviceOspfInterface

    ts = datetime(2026, 6, 1, 10, 0, 0)
    async for db in get_session():
        # Maximal instance (router_id set) + minimal instance (omitted).
        db.add(
            DeviceOspfInstance(
                device_id=device_id,
                process_id="1",
                vrf="",
                areas=["0.0.0.0"],
                router_id="10.0.0.1",
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        db.add(
            DeviceOspfInstance(
                device_id=device_id, process_id="2", vrf="", areas=[], last_refreshed_at=ts, refresh_source="poll"
            )
        )
        # Maximal interface (every optional) + minimal interface (only required).
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
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        db.add(
            DeviceOspfInterface(
                device_id=device_id,
                interface_name="GE0/1",
                passive=False,
                auth_present=False,
                last_refreshed_at=ts,
                refresh_source="poll",
            )
        )
        await db.commit()
        break


@pytest.mark.anyio
async def test_ospf_payload_matches_contract_exactly(adapter_client):
    """Instances + interfaces expose required keys; extras are only documented optionals."""
    device_id = await seed_device(nso_device_name="ospf-contract", netbox_device_id=7930)
    await _seed_ospf(device_id)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/ospf", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()

    assert set(body.keys()) == REQUIRED_TOP_KEYS

    insts = {i["process_id"]: i for i in body["instances"]}
    assert set(insts["1"].keys()) == REQUIRED_INSTANCE_KEYS | OPTIONAL_INSTANCE_KEYS
    assert set(insts["2"].keys()) == REQUIRED_INSTANCE_KEYS  # router_id omitted
    assert insts["1"]["areas"] == ["0.0.0.0"]

    ifaces = {i["interface_name"]: i for i in body["interfaces"]}
    assert set(ifaces["GE0/0"].keys()) == REQUIRED_IFACE_KEYS | OPTIONAL_IFACE_KEYS
    assert set(ifaces["GE0/1"].keys()) == REQUIRED_IFACE_KEYS  # optionals omitted
    assert isinstance(ifaces["GE0/0"]["passive"], bool)


@pytest.mark.anyio
async def test_ospf_no_data_shape(adapter_client):
    """Empty shape keeps the top-level keys (refresh_source='never')."""
    device_id = await seed_device(nso_device_name="ospf-contract-empty", netbox_device_id=7931)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/ospf", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == REQUIRED_TOP_KEYS
    assert body["instances"] == [] and body["interfaces"] == []
    assert body["refresh_source"] == "never"


# ── GET endpoint behavior edges ───────────────────────────────────────────────


@pytest.mark.anyio
async def test_ospf_get_device_not_found(adapter_client):
    """GET for a non-existent device → 404."""
    resp = await adapter_client.get("/api/v1/devices/99999/ospf", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.anyio
async def test_ospf_instance_enabled_emitted_when_set(adapter_client):
    """An instance with an explicit admin-state emits ``enabled`` in the response."""
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceOspfInstance

    device_id = await seed_device(nso_device_name="ospf-enabled-dev", netbox_device_id=7932)
    async for db in get_session():
        db.add(
            DeviceOspfInstance(
                device_id=device_id,
                process_id="1",
                vrf="",
                areas=[],
                enabled=False,  # explicit admin-state down → must appear (not omitted)
                last_refreshed_at=datetime(2026, 6, 1, 10, 0, 0),
                refresh_source="poll",
            )
        )
        await db.commit()
        break

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/ospf", headers=AUTH)
    assert resp.status_code == 200
    inst = resp.json()["instances"][0]
    assert inst["enabled"] is False
