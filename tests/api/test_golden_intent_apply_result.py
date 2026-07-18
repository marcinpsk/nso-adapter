# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — the eight intent PUTs that share IntentApplyResult.

S1b write path. Each of these full-replace PUTs returns exactly
``{device_id, count, removed, replaced}``. A fresh device + a one-item body
gives a deterministic ``{count: 1, removed: 0, replaced: False}`` (no existing
rows to drop, no auto_apply configured). Deep-equality across all eight pins the
shared shape; run before AND after wiring response_model=IntentApplyResult.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

# (url-suffix, minimal valid request body) — one item each.
CASES = [
    ("static-route-intent", {"routes": [{"prefix": "10.0.0.0/8", "next_hop": "192.0.2.1"}]}),
    ("vlan-intent", {"vlans": [{"vlan_id": 100, "name": "DATA"}]}),
    ("svi-intent", {"interfaces": [{"interface_name": "Vlan100", "vlan_id": 100}]}),
    ("subinterface-intent", {"interfaces": [{"interface_name": "GE0/0.100"}]}),
    ("l2-sap-intent", {"saps": [{"service_name": "EPIPE-1", "service_type": "epipe", "sap_id": "1/1/1:200"}]}),
    ("logging-intent", {"hosts": [{"address": "10.0.0.5"}]}),
    ("bfd-intent", {"interfaces": [{"interface_name": "GE0/0"}]}),
    ("interface-mtu-intent", {"interfaces": [{"interface_name": "GE0/0", "mtu": 9000}]}),
]


@pytest.mark.anyio
@pytest.mark.parametrize("suffix,body", CASES)
async def test_intent_apply_result_golden(adapter_client, suffix, body):
    device_id = await seed_device(nso_device_name=f"wr-{suffix}", netbox_device_id=8000)
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/{suffix}", json=body, headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, "count": 1, "removed": 0, "replaced": False}
