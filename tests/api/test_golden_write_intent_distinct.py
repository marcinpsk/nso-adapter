# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body tests — the intent PUTs with distinct (non-shared) result shapes.

S1b write path. bgp/ospf/isis-interface/isis-flex-algo return deterministic
summaries. snmp/ip additionally stamp a read-time ``updated_at`` via
``datetime.now()`` — the module clock is frozen so the body is deterministic.
Each runs against a fresh device with a one-item body; deep-equality pins the shape.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from nso_adapter.api.timestamps import iso_z
from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

FROZEN_ISO_Z = "2026-06-01T10:00:00Z"


class _FrozenDatetime(datetime):
    """datetime whose .now() is fixed, so write-time ``updated_at`` is deterministic."""

    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 6, 1, 10, 0, 0, tzinfo=tz)


# (url-suffix, request body, expected fields besides device_id) — deterministic PUTs.
DETERMINISTIC = [
    ("bgp-intent", {"routers": [{"asn": "65100"}]}, {"router_count": 1}),
    (
        "ospf-intent",
        {"instances": [{"process_id": "1"}], "interfaces": []},
        {"instance_count": 1, "interface_count": 0},
    ),
    (
        "isis-interface-intent",
        {"interfaces": [{"interface_name": "GE0/0", "af": "ipv4"}], "processes": []},
        {"interface_count": 1, "process_count": 0},
    ),
    ("isis-flex-algo-intent", {"flex_algos": [{"algo_id": 128}]}, {"flex_algo_count": 1, "removal_queued": False}),
]


@pytest.mark.anyio
@pytest.mark.parametrize("suffix,body,expected", DETERMINISTIC)
async def test_distinct_intent_result_golden(adapter_client, suffix, body, expected):
    device_id = await seed_device(nso_device_name=f"wr-{suffix}", netbox_device_id=8010)
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/{suffix}", json=body, headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, **expected}


@pytest.mark.anyio
async def test_snmp_intent_result_golden(adapter_client, monkeypatch):
    import nso_adapter.api.snmp as snmp_mod

    monkeypatch.setattr(snmp_mod, "datetime", _FrozenDatetime)
    device_id = await seed_device(nso_device_name="wr-snmp", netbox_device_id=8011)
    body = {"hosts": [{"address": "10.0.0.9", "version": "2c", "notify_type": "trap", "community_or_user": "public"}]}
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/snmp-intent", json=body, headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == {
        "device_id": device_id,
        "community_count": 0,
        "v3_user_count": 0,
        "host_count": 1,
        "has_system_info": False,
        "updated_at": FROZEN_ISO_Z,
    }


@pytest.mark.anyio
async def test_ip_intent_result_golden(adapter_client, monkeypatch):
    import nso_adapter.api.interface_ip as ip_mod

    monkeypatch.setattr(ip_mod, "datetime", _FrozenDatetime)
    device_id = await seed_device(nso_device_name="wr-ip", netbox_device_id=8012)
    body = {"addresses": [{"interface": "GE0/0", "address": "10.0.0.1/24", "family": "ipv4"}]}
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/ip-intent", json=body, headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == {
        "device_id": device_id,
        "address_count": 1,
        "removed_interfaces": 0,
        "replaced": False,
        "updated_at": FROZEN_ISO_Z,
    }


def test_frozen_datetime_is_fixed():
    """Guard the freeze helper itself (used by the two clock-stamped goldens)."""
    assert iso_z(_FrozenDatetime.now(UTC)) == FROZEN_ISO_Z
