# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Golden-body test — GET /api/v1/devices/{id}/l2-services.

Nokia epipe/vpls services + SAPs. The response has NO top-level
last_refreshed_at/refresh_source (only device_id + services). Every key is
always present (EMIT-NULL: service_id/outer_tag/inner_tag null when unset), so
the response model must NOT use exclude_unset. Rows are ordered by
(service_name, sap_id).
"""

from __future__ import annotations

import pytest

from tests.conftest import VALID_TOKEN, seed_device

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


async def _seed_l2(device_id: int) -> None:
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import DeviceL2Sap

    async for db in get_session():
        # Service with two SAPs (one with inner_tag, one without).
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
        db.add(
            DeviceL2Sap(
                device_id=device_id,
                service_name="EPIPE-1",
                service_type="epipe",
                service_id=100,
                sap_id="1/1/2:200.10",
                port="1/1/2",
                outer_tag=200,
                inner_tag=10,
                refresh_source="poll",
            )
        )
        # Second service with a null service_id.
        db.add(
            DeviceL2Sap(
                device_id=device_id,
                service_name="VPLS-2",
                service_type="vpls",
                service_id=None,
                sap_id="1/1/3:300",
                port="1/1/3",
                outer_tag=300,
                inner_tag=None,
                refresh_source="poll",
            )
        )
        await db.commit()
        break


@pytest.mark.anyio
async def test_l2_services_golden_body(adapter_client):
    device_id = await seed_device(nso_device_name="l2-golden", netbox_device_id=7997)
    await _seed_l2(device_id)

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/l2-services", headers=AUTH)).json()

    # Rows ordered by (service_name, sap_id) → services grouped in first-seen order.
    assert body == {
        "device_id": device_id,
        "services": [
            {
                "service_name": "EPIPE-1",
                "service_type": "epipe",
                "service_id": 100,
                "saps": [
                    {"sap_id": "1/1/1:200", "port": "1/1/1", "outer_tag": 200, "inner_tag": None},
                    {"sap_id": "1/1/2:200.10", "port": "1/1/2", "outer_tag": 200, "inner_tag": 10},
                ],
            },
            {
                "service_name": "VPLS-2",
                "service_type": "vpls",
                "service_id": None,
                "saps": [{"sap_id": "1/1/3:300", "port": "1/1/3", "outer_tag": 300, "inner_tag": None}],
            },
        ],
    }


@pytest.mark.anyio
async def test_l2_services_golden_empty(adapter_client):
    device_id = await seed_device(nso_device_name="l2-golden-empty", netbox_device_id=7998)
    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/l2-services", headers=AUTH)).json()
    assert body == {"device_id": device_id, "services": []}
