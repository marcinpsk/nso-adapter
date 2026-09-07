# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Aggregate collateral protection for the interface section (#1522 design §3).

The interface family is exempt from the post-apply key COMPARISON, not from device-wide
collateral protection: one PUT retracts every interface the outgoing document omits, so a
live root or address nobody authorized dropping must block the write.
"""

import pytest

from tests.conftest import seed_device, session
from tests.core.test_action_apply_promotion import _apply, _put_vlans
from tests.core.test_execution_context import _execute, _put_addresses, _put_attrs, _seed_interface
from tests.core.test_generation_protocol import job_row, recorded_client, run_head, seed_settings

pytestmark = pytest.mark.anyio

_IFACE = "GigabitEthernet0/1"
_ATTR = [{"interface": _IFACE, "attribute": "description", "intent_value": "core link"}]


async def _blocked_orphans(device_id: int) -> dict:
    """The orphan report the blocked write recorded on the family it did carry."""
    import sqlalchemy as sa

    from nso_adapter.store.models import VlanIntent

    async with session() as db:
        row = (await db.execute(sa.select(VlanIntent).where(VlanIntent.device_id == device_id))).scalar_one()
    assert row.last_apply_error["code"] == "removal_blocked_collateral"
    return row.last_apply_error["detail"]["orphans"]


def _live(*, description: str | None = None, addresses: list[dict] | None = None) -> dict:
    """The live aggregate instance carrying one interface entry."""
    entry: dict = {"interface-name": _IFACE}
    if description is not None:
        entry["description"] = description
    if addresses is not None:
        entry["ipv4-address"] = addresses
    return {"interface": {"interface": [entry]}}


async def test_unasserted_interface_root_blocks_an_unrelated_vlan_put(adapter_client):
    """A VLAN-only document carries no interface authority, so it may not retract one."""
    device_id = await seed_device(nso_device_name="interface-collateral", netbox_device_id=17331)
    await seed_settings(device_id, auto_apply=True)
    assert (await _put_vlans(adapter_client, device_id, [100], seq=1)).status_code == 200
    client, rec = recorded_client("interface-collateral")
    client.get_service_config.return_value = _live(description="core link")
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "failed", job.result
    assert await _blocked_orphans(device_id) == {"interface_config/interface": [[_IFACE]]}
    assert not rec.documents


async def test_unasserted_interface_address_blocks_an_unrelated_vlan_put(adapter_client):
    """The address child grain is guarded too: an omitted address is a retracted address."""
    device_id = await seed_device(nso_device_name="interface-address-collateral", netbox_device_id=17332)
    await seed_settings(device_id, auto_apply=True)
    assert (await _put_vlans(adapter_client, device_id, [100], seq=1)).status_code == 200
    client, rec = recorded_client("interface-address-collateral")
    client.get_service_config.return_value = _live(addresses=[{"address": "192.0.2.5", "prefix-length": 24}])
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "failed", job.result
    assert await _blocked_orphans(device_id) == {
        "interface_config/interface": [[_IFACE]],
        "interface_config/ipv4-address": [[_IFACE, "192.0.2.5"]],
    }
    assert not rec.documents


async def test_an_authorized_interface_removal_drops_the_root(adapter_client):
    """The operator's own deletion carries authority for the interface it empties."""
    device_id = await seed_device(nso_device_name="interface-authorized", netbox_device_id=17333)
    await seed_settings(device_id, auto_apply=False)
    assert (await _put_attrs(adapter_client, device_id, _ATTR, seq=1760)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"interface_config": 1760})).status_code == 202
    live = (await _execute(device_id, "interface-authorized")).documents[-1]
    assert live["interface"]["interface"][0]["description"] == "core link"

    # delete_origin: an un-own commits no-networking and the guard has nothing to protect.
    assert (await _put_attrs(adapter_client, device_id, [], seq=1761, query="?delete_origin=true")).status_code == 200
    assert (await _apply(adapter_client, device_id, {"interface_config": 1761})).status_code == 202
    client, rec = recorded_client("interface-authorized")
    client.get_service_config.return_value = live
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "succeeded", job.error
    assert rec.documents[-1]["interface"] == {"interface": []}


async def test_an_authorized_address_removal_drops_the_address(adapter_client):
    """An address deletion authorizes its own wire key, in the grain the guard compares."""
    device_id = await seed_device(nso_device_name="interface-address-authorized", netbox_device_id=17334)
    await seed_settings(device_id, auto_apply=False)
    await _seed_interface(device_id, _IFACE)
    addresses = [{"interface": _IFACE, "address": "192.0.2.5/24", "family": "ipv4"}]
    assert (await _put_addresses(adapter_client, device_id, addresses, seq=1770)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"ip": 1770})).status_code == 202
    live = (await _execute(device_id, "interface-address-authorized")).documents[-1]
    assert live["interface"]["interface"][0]["ipv4-address"][0]["address"] == "192.0.2.5"

    assert (
        await _put_addresses(adapter_client, device_id, [], seq=1771, query="?delete_origin=true")
    ).status_code == 200
    client, rec = recorded_client("interface-address-authorized")
    client.get_service_config.return_value = live
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "succeeded", job.error
    assert rec.documents[-1]["interface"] == {"interface": []}
