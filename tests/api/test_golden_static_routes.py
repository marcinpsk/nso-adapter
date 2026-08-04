# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/static-routes.

Deep-equality proof of the EXACT JSON the adapter emits, field by field. The
contract test (``test_contract_static_routes.py``) pins key *sets*; this pins
key *values* — the guard that response-model typing does not silently drop a
field, reformat a timestamp, or coerce a value. Every optional route key
(including ``next_hop_vrf``, which the contract test does not exercise) appears
on the maximal route so a model missing it would fail here.

``last_refreshed_at`` is a formatted string (``.isoformat() + "Z"``) — pinned
literally so a switch to a raw datetime would be caught.
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

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TS = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)


async def _seed_pinned_outcome(device_id: int) -> int:
    """Terminalize one static_route attempt and pin its timestamps to TS — the golden
    body byte-pins the full REAL read_state block (attempt_id 1 in a fresh test DB)."""
    from sqlalchemy import update

    from nso_adapter.nso.read_outcome import Freshness, Present
    from nso_adapter.store import outcome_store
    from nso_adapter.store.models import RefreshOutcome

    async with session() as db:
        attempt_id = await outcome_store.record_read_outcome(
            db, device_id, "static_route", Present({"r": []}, Freshness.fresh), refresh_source="poll"
        )
        await outcome_store.record_result(db, attempt_id, result="replaced", succeeded=True, row_count=2)
        await db.execute(
            update(RefreshOutcome).where(RefreshOutcome.id == attempt_id).values(started_at=TS, completed_at=TS)
        )
        await db.commit()
        return attempt_id


async def _seed_static_routes(device_id: int) -> None:
    from nso_adapter.store.models import DeviceStaticRoute

    async with session() as db:
        # MAXIMAL route — every optional key set, incl. next_hop_vrf.
        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="BLUE",
                prefix="10.0.0.0/8",
                next_hop="192.0.2.1",
                interface_next_hop="GE0/0",
                next_hop_vrf="RED",
                metric=10,
                permanent=True,
                tag=99,
                name="RT-1",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        # MINIMAL route — only the required identity keys.
        db.add(
            DeviceStaticRoute(
                device_id=device_id,
                vrf="",
                prefix="0.0.0.0/0",
                next_hop="192.0.2.254",
                last_refreshed_at=TS,
                refresh_source="poll",
            )
        )
        await db.commit()


@pytest.mark.anyio
async def test_static_routes_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="sr-golden", netbox_device_id=7975)
    await pin_store_incarnation()
    attempt_id = await _seed_pinned_outcome(device_id)
    await _seed_static_routes(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)).json()

    # Routes ordered by (vrf, prefix, next_hop): "" < "BLUE".
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": "2026-06-01T10:00:00Z",
        "refresh_source": "poll",
        "read_state": {
            "outcome": "present",
            "reason": None,
            "freshness": "fresh",
            "result": "replaced",
            "succeeded": True,
            "read_at": "2026-06-01T10:00:00Z",
            "attempt_id": attempt_id,
            "source_epoch": 1,
            "payload_revision": attempt_id,
            "incarnation": GOLDEN_INCARNATION,
            "incarnation_born": GOLDEN_BORN_ISO,
        },
        "routes": [
            {"vrf": "", "prefix": "0.0.0.0/0", "next_hop": "192.0.2.254"},
            {
                "vrf": "BLUE",
                "prefix": "10.0.0.0/8",
                "next_hop": "192.0.2.1",
                "interface_next_hop": "GE0/0",
                "next_hop_vrf": "RED",
                "metric": 10,
                "permanent": True,
                "tag": 99,
                "name": "RT-1",
            },
        ],
    }


@pytest.mark.anyio
async def test_static_routes_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="sr-golden-empty", netbox_device_id=7976)
    await pin_store_incarnation()
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/static-routes", headers=AUTH)).json()
    assert body == {
        "device_id": device_id,
        "last_refreshed_at": None,
        "refresh_source": "never",
        # Pointerless → the SYNTHESIZED block, byte-pinned: never read_state: null (D3).
        "read_state": {
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
        },
        "routes": [],
    }
