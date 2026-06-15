# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Contract test — producer side of GET /api/v1/devices/{id}/l2-services.

Nokia epipe/vpls services with their SAPs; every level emits a fixed key set. Like the
M34-36 read endpoints, the response has NO top-level last_refreshed_at/refresh_source.
Consumed by the plugin in ``l2_service_reconciler.reconcile_l2_services``.

Canonical contract: ``docs/api-contract.md`` (M37 L2 services §).
Mirror (consumer side): ``netbox-nso-plugin/.../tests/test_contract_l2_services.py``.
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

TOP_KEYS = {"device_id", "services"}
SERVICE_KEYS = {"service_name", "service_type", "service_id", "saps"}
SAP_KEYS = {"sap_id", "port", "outer_tag", "inner_tag"}


@pytest.mark.anyio
async def test_l2_services_contract(adapter_client):
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceL2Sap

    device_id = await seed_device(nso_device_name="l2svc-ct", netbox_device_id=7995)
    async for db in get_session():
        db.add(
            DeviceL2Sap(
                device_id=device_id,
                service_name="EPIPE-1",
                service_type="epipe",
                service_id=100,
                sap_id="1/1/1:200",
                port="1/1/1",
                outer_tag=200,
                inner_tag=None,
                refresh_source="poll",
            )
        )
        await db.commit()
        break

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/l2-services", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS
    svc = body["services"][0]
    assert set(svc.keys()) == SERVICE_KEYS
    assert svc["service_type"] == "epipe"
    sap = svc["saps"][0]
    assert set(sap.keys()) == SAP_KEYS
    assert sap["inner_tag"] is None  # always present even when unset


@pytest.mark.anyio
async def test_l2_services_no_data_shape(adapter_client):
    device_id = await seed_device(nso_device_name="l2svc-ct-empty", netbox_device_id=7996)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/l2-services", headers=AUTH)).json()
    assert set(body.keys()) == TOP_KEYS
    assert body["services"] == []
