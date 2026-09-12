# SPDX-License-Identifier: Apache-2.0
"""Boundary validation against the real store."""

import sqlalchemy as sa

from nso_adapter.store.models import DeploymentGeneration, InterfaceIpIntent
from tests.conftest import seed_device, session


async def test_malformed_stored_address_refuses_removal_atomically(adapter_client):
    from tests.core.test_execution_context import _put_addresses, _seed_interface

    device_id = await seed_device(nso_device_name="invalid-address")
    interface_id = await _seed_interface(device_id, "GigabitEthernet0/1")
    async with session() as db:
        row = InterfaceIpIntent(interface_id=interface_id, address="invalid", family="ipv4", vrf="")
        db.add(row)
        await db.commit()
        row_id = row.id
    response = await _put_addresses(adapter_client, device_id, [], seq=1)
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "apply_unexecutable"
    assert response.json()["error"]["detail"]["streams"] == {"ip": "invalid_stored_address"}
    async with session() as db:
        assert await db.get(InterfaceIpIntent, row_id) is not None
        assert await db.scalar(sa.select(sa.func.count()).select_from(DeploymentGeneration)) == 0
