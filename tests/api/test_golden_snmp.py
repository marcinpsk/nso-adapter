# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/snmp-config.

SNMP is an EMIT-NULL shape: every key is ALWAYS present, nullable values are
emitted as ``null`` (not omitted), and ``system_info`` is either a fixed dict or
``null``. The response model therefore must NOT use ``response_model_exclude_unset``
— the null-variant golden pins that acl/version/notify_type/port/username/
location/contact stay present as null. ``last_refreshed_at`` is a "<iso>Z" string.
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


@pytest.mark.anyio
async def test_snmp_golden_body(adapter_client):
    from nso_adapter.store.models import SnmpCommunity, SnmpHost, SnmpSystemInfo, SnmpV3User

    device_id = await seed_device(nso_device_name="snmp-golden", netbox_device_id=7965)
    await pin_store_incarnation()
    async with session() as db:
        db.add(
            SnmpCommunity(
                device_id=device_id,
                community_hash="abc",
                access="RO",
                acl="ACL-1",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        db.add(
            SnmpV3User(
                device_id=device_id,
                username="v3-test-group",
                has_auth_secret=True,
                has_priv_secret=False,
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        db.add(
            SnmpHost(
                device_id=device_id,
                address="10.0.0.9",
                version="3",
                notify_type="inform",
                port=162,
                username="v3-test-group",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        db.add(
            SnmpSystemInfo(
                device_id=device_id, location="DC1", contact="noc@x", last_refreshed_at=TS, refresh_source="poll"
            )
        )
        await db.commit()

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "read_state": _SYNTH_READ_STATE,
        "communities": [{"community_hash": "abc", "access": "RO", "acl": "ACL-1"}],
        "v3_users": [{"username": "v3-test-group", "has_auth_secret": True, "has_priv_secret": False}],
        "hosts": [
            {"address": "10.0.0.9", "version": "3", "notify_type": "inform", "port": 162, "username": "v3-test-group"}
        ],
        "system_info": {"location": "DC1", "contact": "noc@x"},
    }


@pytest.mark.anyio
async def test_snmp_golden_nulls(adapter_client):
    """acl/version/notify_type/port/username/location/contact all present as null (emit-null)."""
    from nso_adapter.store.models import SnmpCommunity, SnmpHost, SnmpSystemInfo

    device_id = await seed_device(nso_device_name="snmp-golden-nulls", netbox_device_id=7966)
    await pin_store_incarnation()
    async with session() as db:
        db.add(
            SnmpCommunity(
                device_id=device_id,
                community_hash="def",
                access="RW",
                acl=None,
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        # v1/v2c host with no port/username; version/notify_type left unset.
        db.add(SnmpHost(device_id=device_id, address="10.0.0.1", last_refreshed_at=TS, refresh_source="poll"))
        db.add(SnmpSystemInfo(device_id=device_id, last_refreshed_at=TS, refresh_source="poll"))
        await db.commit()

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)).json()

    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "read_state": _SYNTH_READ_STATE,
        "communities": [{"community_hash": "def", "access": "RW", "acl": None}],
        "v3_users": [],
        "hosts": [{"address": "10.0.0.1", "version": None, "notify_type": None, "port": None, "username": None}],
        "system_info": {"location": None, "contact": None},
    }


@pytest.mark.anyio
async def test_snmp_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="snmp-golden-empty", netbox_device_id=7967)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/snmp-config", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        "read_state": _SYNTH_READ_STATE,
        "communities": [],
        "v3_users": [],
        "hosts": [],
        "system_info": None,
    }
