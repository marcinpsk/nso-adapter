# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/bgp._upsert_bgp_data — robustness to duplicate peers."""

from __future__ import annotations

from sqlalchemy import select

from tests.conftest import seed_device


async def _scope_with_peers(*peer_addrs: str) -> list[dict]:
    return [
        {
            "asn": "65100",
            "scope": [
                {
                    "vrf": "",
                    "address-family": [{"af": "ipv4-unicast"}],
                    "peer": [{"peer-address": a, "peer-group": f"G-{i}"} for i, a in enumerate(peer_addrs)],
                }
            ],
        }
    ]


async def test_duplicate_peer_in_scope_does_not_crash(adapter_client):
    """Same neighbor IP under two groups (same scope) → one row, no IntegrityError."""
    from nso_adapter.core.bgp import _upsert_bgp_data
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, DeviceBgpPeer

    device_id = await seed_device(nso_device_name="bgp-dup", netbox_device_id=880)

    async for db in get_session():
        device = await db.get(Device, device_id)
        # 10.0.0.1 appears twice (two groups) — must not roll back the whole refresh.
        await _upsert_bgp_data(db, device, await _scope_with_peers("10.0.0.1", "10.0.0.2", "10.0.0.1"), "test")

        peers = (await db.execute(select(DeviceBgpPeer))).scalars().all()
        addrs = sorted(p.peer_address for p in peers)
        assert addrs == ["10.0.0.1", "10.0.0.2"]  # dup collapsed, both real peers kept
        break
