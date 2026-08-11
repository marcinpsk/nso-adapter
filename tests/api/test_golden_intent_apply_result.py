# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — the intent PUTs that share IntentApplyResult.

S1b write path. Each of these full-replace PUTs returns exactly
``{device_id, count, removed, replaced}``. A fresh device + a one-item body
gives a deterministic ``{count: 1, removed: 0, replaced: False}`` (no existing
rows to drop, no auto_apply configured). Deep-equality across all seven pins the
shared shape; run before AND after wiring response_model=IntentApplyResult.

Static routes have their own case below: #1396 R3 P0 EXTENDS the shared summary with a
per-route ``routes`` echo, and the deep-equality assertion is what keeps that from silently
spreading to the other seven.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, push_seq, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

# (url-suffix, minimal valid request body) — one item each.
CASES = [
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
    resp = await adapter_client.put(f"/api/v1/devices/{device_id}/{suffix}", json=body, headers=AUTH | push_seq())
    assert resp.status_code == 200
    assert resp.json() == {"device_id": device_id, "count": 1, "removed": 0, "replaced": False}


@pytest.mark.anyio
async def test_static_route_intent_result_golden(adapter_client):
    """The static-route PUT is the shared summary, the settlement echo AND the deletion ack.

    Deep equality, not a subset check: the echo is what the pusher records as its
    expectation, so a key that quietly disappears from it is a settlement that silently
    stops correlating. The four acknowledgement fields (#1503 §4.4) are emitted on EVERY
    response, including one that carried no deletion authority at all — the pusher validates
    the partition unconditionally, so a missing list is not the same as an empty one.
    """
    device_id = await seed_device(nso_device_name="wr-static-route-intent", netbox_device_id=8001)
    body = {"routes": [{"route_id": 41, "generation": 7, "prefix": "10.0.0.0/8", "next_hop": "192.0.2.1"}]}
    resp = await adapter_client.put(
        f"/api/v1/devices/{device_id}/static-route-intent", json=body, headers=AUTH | push_seq()
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload) == {
        "device_id",
        "count",
        "removed",
        "replaced",
        "routes",
        "deleted_executed_ids",
        "deleted_degraded_ids",
        "deleted_moot_ids",
        "removed_uncorrelated",
    }
    assert (payload["device_id"], payload["count"], payload["removed"], payload["replaced"]) == (device_id, 1, 0, False)
    assert [(r["route_id"], r["generation"]) for r in payload["routes"]] == [(41, 7)]
    assert set(payload["routes"][0]) == {"route_id", "generation", "fingerprint"}
    assert payload["deleted_executed_ids"] == []
    assert payload["deleted_degraded_ids"] == []
    assert payload["deleted_moot_ids"] == []
    assert payload["removed_uncorrelated"] == []
