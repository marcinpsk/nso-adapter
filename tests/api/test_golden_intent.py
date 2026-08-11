# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — the intent router (GET intent / PUT intent / GET intent-summary).

GET intent and PUT intent both mint a read/write-time ``updated_at`` via
``datetime.now()`` (intent.py) — the module clock is frozen so the body is
deterministic, exactly like the S1b snmp/ip write goldens. intent-summary is a
``{scope_name: {count, applied, failed}}`` map driven by the ``*_intent`` catalogue.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from nso_adapter.api.timestamps import iso_z
from tests.conftest import VALID_TOKEN, push_seq, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
TS_Z = "2026-06-01T10:00:00Z"
FROZEN_Z = "2026-06-01T10:00:00Z"


class _FrozenDatetime(datetime):
    """datetime whose .now() is fixed, so the read/write-time updated_at is deterministic."""

    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 6, 1, 10, 0, 0, tzinfo=tz)


@pytest.mark.anyio
async def test_get_intent_golden(adapter_client, monkeypatch):
    import nso_adapter.api.intent as intent_mod

    monkeypatch.setattr(intent_mod, "datetime", _FrozenDatetime)

    from nso_adapter.store.models import DbInterface, InterfaceIntent

    device_id = await seed_device(nso_device_name="intent-get", netbox_device_id=401)
    async with session() as db:
        iface = DbInterface(device_id=device_id, name="GigabitEthernet0/0", kind="physical")
        db.add(iface)
        await db.flush()
        db.add(
            InterfaceIntent(
                interface_id=iface.id,
                attribute="description",
                intent_value="hello world",
                accepted_at=TS,
                last_apply_at=TS,
                last_apply_error={"code": "x"},
            )
        )
        await db.commit()

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/intent", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "attributes": [
            {
                "interface": "GigabitEthernet0/0",
                "attribute": "description",
                "intent_value": "hello world",
                "accepted_at": TS_Z,
                "last_apply_at": TS_Z,
                "last_apply_error": {"code": "x"},
            }
        ],
        "updated_at": FROZEN_Z,
    }


@pytest.mark.anyio
async def test_get_intent_empty_golden(adapter_client, monkeypatch):
    import nso_adapter.api.intent as intent_mod

    monkeypatch.setattr(intent_mod, "datetime", _FrozenDatetime)

    device_id = await seed_device(nso_device_name="intent-empty", netbox_device_id=402)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/intent", headers=AUTH)).json()
    assert body == {"device_id": device_id, "attributes": [], "updated_at": FROZEN_Z}


@pytest.mark.anyio
async def test_put_intent_result_golden(adapter_client, monkeypatch):
    import nso_adapter.api.intent as intent_mod

    monkeypatch.setattr(intent_mod, "datetime", _FrozenDatetime)

    device_id = await seed_device(nso_device_name="intent-put", netbox_device_id=403, attributes=["description"])
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={"attributes": [{"interface": "GigabitEthernet0/0", "attribute": "description", "intent_value": "x"}]},
        headers=AUTH | push_seq(),
    )
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, "attribute_count": 1, "updated_at": FROZEN_Z}


@pytest.mark.anyio
async def test_get_intent_summary_golden(adapter_client):
    from nso_adapter.store.models import VlanIntent

    device_id = await seed_device(nso_device_name="intent-sum", netbox_device_id=404)
    async with session() as db:
        db.add(
            VlanIntent(
                device_id=device_id, vlan_id=10, name="v10", accepted_at=TS, last_apply_at=TS, last_apply_error={"e": 1}
            )
        )
        await db.commit()

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/intent-summary", headers=AUTH)).json()
    assert body == {"device_id": device_id, "scopes": {"vlan_intent": {"count": 1, "applied": 1, "failed": 1}}}


def test_frozen_now_is_fixed():
    assert iso_z(_FrozenDatetime.now(UTC)) == FROZEN_Z
