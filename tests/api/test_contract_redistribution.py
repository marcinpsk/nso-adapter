# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/redistribution.

Pins the JSON shape the adapter emits for redistribution, which the plugin consumes
in ``redistribution_reconciler.reconcile_redistribution``. Optional keys (route_map,
metric, metric_type) are OMITTED when unset (not null).

Canonical contract: ``docs/api-contract.md`` § "GET .../redistribution".
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_redistribution.py`` —
the ``*_KEYS`` sets MUST stay identical across both files.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

REQUIRED_TOP_KEYS = {"device_id", "last_refreshed_at", "refresh_source", "read_state", "entries"}
REQUIRED_ENTRY_KEYS = {"dest_protocol", "dest_ref", "source_protocol", "source_ref"}
OPTIONAL_ENTRY_KEYS = {"route_map", "metric", "metric_type"}


async def _seed_redistribution(device_id: int, entries: list[dict]) -> None:
    from nso_adapter.store.models import DeviceRedistribution

    ts = datetime(2026, 6, 1, 10, 0, 0)
    async with session() as db:
        for entry in entries:
            db.add(
                DeviceRedistribution(
                    device_id=device_id,
                    dest_protocol=entry["dest_protocol"],
                    dest_ref=entry.get("dest_ref", ""),
                    source_protocol=entry["source_protocol"],
                    source_ref=entry.get("source_ref", ""),
                    route_map=entry.get("route_map"),
                    metric=entry.get("metric"),
                    metric_type=entry.get("metric_type"),
                    last_refreshed_at=ts,
                    refresh_source="poll",
                )
            )
        await db.commit()


@pytest.mark.anyio
async def test_redistribution_payload_matches_contract_exactly(adapter_client):
    """Each entry exposes its required keys; extras are only documented optionals."""
    device_id = await seed_device(nso_device_name="rd-contract", netbox_device_id=7920)
    await _seed_redistribution(
        device_id,
        [
            # MAXIMAL entry — every optional set.
            {
                "dest_protocol": "ospf",
                "dest_ref": "1",
                "source_protocol": "bgp",
                "source_ref": "65100",
                "route_map": "RM-REDIST",
                "metric": 100,
                "metric_type": "type-1",
            },
            # MINIMAL entry — optionals omitted, not null.
            {"dest_protocol": "isis", "dest_ref": "", "source_protocol": "connected", "source_ref": ""},
        ],
    )

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()

    assert set(body.keys()) == REQUIRED_TOP_KEYS
    entries = {(e["dest_protocol"], e["source_protocol"]): e for e in body["entries"]}
    maximal = entries[("ospf", "bgp")]
    minimal = entries[("isis", "connected")]

    assert set(maximal.keys()) == REQUIRED_ENTRY_KEYS | OPTIONAL_ENTRY_KEYS
    assert set(minimal.keys()) == REQUIRED_ENTRY_KEYS  # optionals omitted
    assert isinstance(maximal["metric"], int)


@pytest.mark.anyio
async def test_redistribution_no_data_shape(adapter_client):
    """Empty shape keeps the top-level keys (refresh_source='never')."""
    device_id = await seed_device(nso_device_name="rd-contract-empty", netbox_device_id=7921)
    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/redistribution", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == REQUIRED_TOP_KEYS
    assert body["entries"] == []
    assert body["refresh_source"] == "never"
