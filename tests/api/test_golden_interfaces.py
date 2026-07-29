# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — GET /api/v1/devices/{id}/interfaces and /state.

``/interfaces`` returns a BARE LIST of interface dicts; each has a dynamic
``attrs`` map keyed by attribute name, and every key is always present
(EMIT-NULL — no exclude_unset). ``/state`` returns a fixed dict whose
``by_status`` is a ``dict[str, int]`` over the SyncState values. Deep-equality
pins both so response-model typing cannot drop a key or reshape the attr map.
``last_apply_at`` / ``last_checked_at`` are "<iso>Z" strings.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 5, 20, 10, 0, 0)


async def _seed_iface_full(device_id: int) -> None:
    """One logical interface with a with-intent attr and a state-only attr."""
    from nso_adapter.store.models import DbInterface, InterfaceAttrState, InterfaceIntent, SyncState

    async with session() as db:
        iface = DbInterface(
            device_id=device_id,
            name="GE0/0.100",
            netbox_interface_id=1000,
            parent_binding="lag-99",
            kind="logical",
            encap_tag="100",
            vrf="BLUE",
            service="EPIPE-1",
        )
        db.add(iface)
        await db.flush()
        # attr WITH an intent row → last_apply_at/last_apply_error populated.
        db.add(
            InterfaceAttrState(
                interface_id=iface.id,
                attribute="description",
                nso_value="uplink",
                netbox_value="uplink",
                sync_state=SyncState.apply_failed,
            )
        )
        db.add(
            InterfaceIntent(
                interface_id=iface.id,
                attribute="description",
                intent_value="uplink",
                last_apply_at=TS,
                last_apply_error={"code": "nso_error", "message": "boom"},
            )
        )
        # attr WITHOUT an intent row → intent_value/last_apply_at/last_apply_error null.
        db.add(
            InterfaceAttrState(
                interface_id=iface.id,
                attribute="mtu",
                nso_value="1500",
                netbox_value=None,
                sync_state=SyncState.changed,
            )
        )
        await db.commit()


@pytest.mark.anyio
async def test_interfaces_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="if-golden", netbox_device_id=7940)
    await _seed_iface_full(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/interfaces", headers=AUTH)).json()

    assert body == [
        {
            "name": "GE0/0.100",
            "netbox_interface_id": 1000,
            "attrs": {
                "description": {
                    "nso_value": "uplink",
                    "netbox_value": "uplink",
                    "intent_value": "uplink",
                    "status": "apply_failed",
                    "last_apply_at": "2026-05-20T10:00:00Z",
                    "last_apply_error": {"code": "nso_error", "message": "boom"},
                },
                "mtu": {
                    "nso_value": "1500",
                    "netbox_value": None,
                    "intent_value": None,
                    "status": "changed",
                    "last_apply_at": None,
                    "last_apply_error": None,
                },
            },
            "parent_binding": "lag-99",
            "kind": "logical",
            "encap_tag": "100",
            "vrf": "BLUE",
            "service": "EPIPE-1",
        }
    ]


@pytest.mark.anyio
async def test_interfaces_golden_physical_nulls(adapter_client):
    """A physical port: netbox_interface_id + all logical-modeling keys are null."""
    from nso_adapter.store.models import DbInterface

    device_id = await seed_device(nso_device_name="if-golden-phys", netbox_device_id=7941)
    async with session() as db:
        db.add(DbInterface(device_id=device_id, name="GE0/1"))
        await db.commit()

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/interfaces", headers=AUTH)).json()
    assert body == [
        {
            "name": "GE0/1",
            "netbox_interface_id": None,
            "attrs": {},
            "parent_binding": None,
            "kind": None,
            "encap_tag": None,
            "vrf": None,
            "service": None,
        }
    ]


@pytest.mark.anyio
async def test_interfaces_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="if-golden-empty", netbox_device_id=7942)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/interfaces", headers=AUTH)).json()
    assert body == []


# ── GET /state ────────────────────────────────────────────────────────────────


async def _seed_state(device_id: int) -> None:
    from nso_adapter.store.models import DbInterface, InterfaceAttrState, SyncState

    async with session() as db:
        for name, state in (("GE0/1", SyncState.imported), ("GE0/2", SyncState.changed)):
            iface = DbInterface(device_id=device_id, name=name)
            db.add(iface)
            await db.flush()
            db.add(
                InterfaceAttrState(
                    interface_id=iface.id,
                    attribute="description",
                    nso_value="v",
                    netbox_value="v",
                    sync_state=state,
                    last_checked_at=TS,
                )
            )
        await db.commit()


@pytest.mark.anyio
async def test_state_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="state-golden", netbox_device_id=7943)
    await _seed_state(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/state", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "managed_interfaces": 2,
        "by_status": {
            "unknown": 0,
            "imported": 1,
            "changed": 1,
            "error": 0,
            "accepted": 0,
            "deploying": 0,
            "in_sync": 0,
            "apply_failed": 0,
            "drifted": 0,
        },
        "last_checked_at": "2026-05-20T10:00:00Z",
    }


@pytest.mark.anyio
async def test_state_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="state-golden-empty", netbox_device_id=7944)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/state", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "managed_interfaces": 0,
        "by_status": {
            "unknown": 0,
            "imported": 0,
            "changed": 0,
            "error": 0,
            "accepted": 0,
            "deploying": 0,
            "in_sync": 0,
            "apply_failed": 0,
            "drifted": 0,
        },
        "last_checked_at": None,
    }
