# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/lag-config.

Bundles and members OMIT optional keys when unset (not null). Consumed by the plugin in
``lacp_reconciler.reconcile_lag_config``.

Canonical contract: ``docs/api-contract.md`` (LACP/LAG §).
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_lag.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TOP_KEYS = {"device_id", "last_refreshed_at", "refresh_source", "read_state", "bundles"}
BUNDLE_REQUIRED_KEYS = {"name", "lag_id", "members"}
BUNDLE_OPTIONAL_KEYS = {"min_links", "system_priority", "system_id", "timer", "admin_key"}
MEMBER_REQUIRED_KEYS = {"interface_name"}
MEMBER_OPTIONAL_KEYS = {"mode", "port_priority"}


@pytest.mark.anyio
async def test_lag_config_contract(adapter_client):
    from nso_adapter.store.models import LagBundleConfig, LagMemberConfig

    device_id = await seed_device(nso_device_name="lag-ct", netbox_device_id=7990)
    ts = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
    async with session() as db:
        # Maximal bundle (every optional) + minimal bundle (only required).
        b1 = LagBundleConfig(
            device_id=device_id,
            name="Bundle-Ether1",
            lag_id=1,
            min_links=2,
            system_priority=100,
            system_id="00:11:22:33:44:55",
            timer="fast",
            admin_key=10,
            last_refreshed_at=ts,
            refresh_source="poll",
        )
        db.add(b1)
        await db.flush()
        db.add(LagMemberConfig(lag_bundle_id=b1.id, interface_name="GE0/1", mode="active", port_priority=32))
        db.add(LagMemberConfig(lag_bundle_id=b1.id, interface_name="GE0/2"))  # minimal member
        b2 = LagBundleConfig(
            device_id=device_id, name="Bundle-Ether2", lag_id=2, last_refreshed_at=ts, refresh_source="poll"
        )
        db.add(b2)
        await db.commit()

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS
    bundles = {b["lag_id"]: b for b in body["bundles"]}
    assert set(bundles[1].keys()) == BUNDLE_REQUIRED_KEYS | BUNDLE_OPTIONAL_KEYS
    assert set(bundles[2].keys()) == BUNDLE_REQUIRED_KEYS  # optionals omitted
    members = {m["interface_name"]: m for m in bundles[1]["members"]}
    assert set(members["GE0/1"].keys()) == MEMBER_REQUIRED_KEYS | MEMBER_OPTIONAL_KEYS
    assert set(members["GE0/2"].keys()) == MEMBER_REQUIRED_KEYS  # optionals omitted


@pytest.mark.anyio
async def test_lag_config_no_data_shape(adapter_client):
    device_id = await seed_device(nso_device_name="lag-ct-empty", netbox_device_id=7991)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/lag-config", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS
    assert body["bundles"] == [] and body["refresh_source"] == "never"
