# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/interfaces.

This pins the EXACT JSON shape the adapter emits for the interfaces endpoint, which
the netbox-nso-plugin consumes in ``template_content._upsert_interface_states``. A
rename/removal on this side (e.g. ``status`` -> ``sync_state``) is silently absorbed
by the plugin (it defaults the missing key to ``"unknown"``), so single-repo coverage
never catches it. This test fails loudly instead.

Canonical contract: ``docs/api-contract.md`` § "GET /api/v1/devices/{id}/interfaces".
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_interfaces.py`` —
the two ``EXPECTED_*_KEYS`` sets MUST stay identical; if you change one, change the
doc and the other.
"""

from __future__ import annotations

from nso_adapter.store.models import DbInterface, InterfaceAttrState, InterfaceIntent, SyncState
from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

# The contract, as documented in docs/api-contract.md. Keep in lockstep with the
# plugin mirror.
# M27R added the logical-interface modeling keys (NULL for physical ports / Cisco / Junos).
EXPECTED_IFACE_KEYS = {
    "name",
    "netbox_interface_id",
    "attrs",
    "parent_binding",
    "kind",
    "encap_tag",
    "vrf",
    "service",
}
EXPECTED_ATTR_KEYS = {
    "nso_value",
    "netbox_value",
    "intent_value",
    "status",
    "last_apply_at",
    "last_apply_error",
}


async def _seed_iface_with_intent(device_id: int):
    """Seed an interface whose description attr has BOTH a state and an intent row,
    so every attr key (including last_apply_at / last_apply_error) is exercised."""
    from datetime import datetime

    from nso_adapter.store.db import get_session

    async for db in get_session():
        iface = DbInterface(device_id=device_id, name="GE0/0", netbox_interface_id=1000)
        db.add(iface)
        await db.flush()
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
                last_apply_at=datetime(2026, 5, 20, 10, 0, 0),
                last_apply_error={"code": "nso_error", "message": "boom"},
            )
        )
        await db.commit()
        break


async def test_interfaces_payload_matches_contract_exactly(adapter_client):
    """The interface dict and each attr dict expose EXACTLY the documented keys."""
    device_id = await seed_device(nso_instance="nso-dev", nso_device_name="contract-dev", netbox_device_id=7900)
    await _seed_iface_with_intent(device_id)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interfaces", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    iface = body[0]

    # Exact key sets — extra OR missing keys both fail (catches renames/additions).
    assert set(iface.keys()) == EXPECTED_IFACE_KEYS
    attr = iface["attrs"]["description"]
    assert set(attr.keys()) == EXPECTED_ATTR_KEYS

    # Types/format the consumer relies on.
    assert isinstance(iface["name"], str)
    assert attr["status"] == "apply_failed"  # str enum value, not an int
    assert attr["last_apply_at"].endswith("Z")  # ISO-8601 + Z, parseable by the plugin
    assert isinstance(attr["last_apply_error"], dict)
