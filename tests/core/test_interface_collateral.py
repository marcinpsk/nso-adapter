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
from tests.core.test_generation_protocol import (
    generations,
    job_row,
    recorded_client,
    run_head,
    seed_settings,
    stream_row,
)

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
    assert await _blocked_orphans(device_id) == {
        "interface_config/interface": [[_IFACE]],
        "interface_config/description": [[_IFACE]],
    }
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


@pytest.mark.parametrize("attribute,value", [("description", "keep"), ("enabled", False)])
async def test_unasserted_interface_attribute_blocks_an_ip_only_put(adapter_client, attribute, value):
    """Keeping the root and address does not authorize retracting an attribute."""
    from sqlalchemy import select

    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await seed_device(nso_device_name="attribute-collateral", netbox_device_id=17335)
    await seed_settings(device_id, auto_apply=False)
    await _seed_interface(device_id, _IFACE)
    addresses = [{"interface": _IFACE, "address": "192.0.2.5/24", "family": "ipv4"}]
    assert (await _put_addresses(adapter_client, device_id, addresses, seq=1780)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"ip": 1780})).status_code == 202
    client, rec = recorded_client("attribute-collateral")
    live = _live(addresses=[{"address": "192.0.2.5", "prefix-length": 24}])
    live["interface"]["interface"][0][attribute] = value
    client.get_service_config.return_value = live
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "failed", job.result
    async with session() as db:
        row = (await db.execute(select(InterfaceIpIntent))).scalar_one()
        assert row.last_apply_error["code"] == "removal_blocked_collateral"
        assert row.last_apply_error["detail"]["orphans"] == {f"interface_config/{attribute}": [[_IFACE]]}
    assert not rec.documents


@pytest.mark.parametrize("attribute,value", [("description", "keep"), ("enabled", False)])
async def test_authorized_attribute_removal_keeps_the_address(adapter_client, attribute, value):
    """An attribute deletion authorizes its leaf while the IP stream keeps the root."""
    device_id = await seed_device(nso_device_name="attribute-removal", netbox_device_id=17336, attributes=[attribute])
    await seed_settings(device_id, auto_apply=False)
    attrs = [{"interface": _IFACE, "attribute": attribute, "intent_value": value}]
    assert (await _put_attrs(adapter_client, device_id, attrs, seq=1790)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"interface_config": 1790})).status_code == 202
    await _execute(device_id, "attribute-removal")
    addresses = [{"interface": _IFACE, "address": "192.0.2.5/24", "family": "ipv4"}]
    assert (await _put_addresses(adapter_client, device_id, addresses, seq=1791)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"ip": 1791})).status_code == 202
    live = (await _execute(device_id, "attribute-removal")).documents[-1]
    assert live["interface"]["interface"][0][attribute] == value

    assert (await _put_attrs(adapter_client, device_id, [], seq=1792, query="?delete_origin=true")).status_code == 200
    assert (await _apply(adapter_client, device_id, {"interface_config": 1792})).status_code == 202
    client, rec = recorded_client("attribute-removal")
    client.get_service_config.return_value = live
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "succeeded", job.error
    entry = rec.documents[-1]["interface"]["interface"][0]
    assert attribute not in entry
    assert entry["ipv4-address"] == live["interface"]["interface"][0]["ipv4-address"]


async def test_ip_only_put_without_attribute_changes_succeeds(adapter_client):
    """An unchanged address-only section carries no attribute omission."""
    device_id = await seed_device(nso_device_name="ip-collateral-clean", netbox_device_id=17337)
    await seed_settings(device_id, auto_apply=False)
    await _seed_interface(device_id, _IFACE)
    addresses = [{"interface": _IFACE, "address": "192.0.2.5/24", "family": "ipv4"}]
    assert (await _put_addresses(adapter_client, device_id, addresses, seq=1800)).status_code == 200
    assert (await _apply(adapter_client, device_id, {"ip": 1800})).status_code == 202
    client, rec = recorded_client("ip-collateral-clean")
    live = _live(addresses=[{"address": "192.0.2.5", "prefix-length": 24}])
    client.get_service_config.return_value = live
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "succeeded", job.error
    entry = rec.documents[-1]["interface"]["interface"][0]
    assert entry["interface-name"] == _IFACE
    assert entry["ipv4-address"][0]["address"] == "192.0.2.5"
    assert entry["ipv4-address"][0]["prefix-length"] == 24
    assert "description" not in entry
    assert "enabled" not in entry


async def test_automatic_attribute_removal_keeps_enabled(adapter_client):
    """An automatic marked omission removes the description through the real worker."""
    device_id = await seed_device(
        nso_device_name="attribute-auto-removal", netbox_device_id=17338, attributes=["description", "enabled"]
    )
    await seed_settings(device_id, auto_apply=True)
    enabled = {"interface": _IFACE, "attribute": "enabled", "intent_value": True}
    assert (await _put_attrs(adapter_client, device_id, [*_ATTR, enabled], seq=1810)).status_code == 200
    live = (await _execute(device_id, "attribute-auto-removal")).documents[-1]
    assert live["interface"]["interface"][0]["description"] == "core link"
    assert live["interface"]["interface"][0]["enabled"] is True

    response = await _put_attrs(adapter_client, device_id, [enabled], seq=1811, query="?delete_origin=true")
    assert response.status_code == 200, response.text
    client, rec = recorded_client("attribute-auto-removal")
    client.get_service_config.return_value = live
    job = await job_row(await run_head(device_id, client))
    assert job.status.value == "succeeded", (job.error, job.result)
    (entry,) = rec.documents[-1]["interface"]["interface"]
    assert entry["interface-name"] == _IFACE
    assert entry["enabled"] is True
    assert "description" not in entry


@pytest.mark.parametrize("queued", [False, True], ids=["idle", "queued"])
@pytest.mark.parametrize("reject", [False, True], ids=["success", "rejected"])
async def test_automatic_attribute_edit_with_detach(adapter_client, reject, queued):
    """A positive edit lands before detach, and both links must succeed to settle."""
    name = "attribute-auto-mixed"
    device_id = await seed_device(nso_device_name=name, netbox_device_id=17339, attributes=["description", "enabled"])
    await seed_settings(device_id, auto_apply=True)
    enabled = {"interface": _IFACE, "attribute": "enabled", "intent_value": True}
    assert (await _put_attrs(adapter_client, device_id, [*_ATTR, enabled], seq=1820)).status_code == 200
    live = (await _execute(device_id, name)).documents[-1]
    before = await stream_row(device_id, "interface_config")
    assert before.applied_revision == before.desired_revision

    if queued:
        assert (await _put_vlans(adapter_client, device_id, [100], seq=1)).status_code == 200
        predecessor = (await generations(device_id))[-1]

    response = await _put_attrs(
        adapter_client, device_id, [{**enabled, "intent_value": False}], seq=1821, query="?delete_origin=false"
    )
    assert response.status_code == 200, response.text
    pending = await stream_row(device_id, "interface_config")
    assert pending.applied_revision == before.applied_revision < pending.desired_revision

    async def check_unsettled():
        current = await stream_row(device_id, "interface_config")
        assert current.applied_revision == before.applied_revision

    client, rec = recorded_client(name, on_sync_from=check_unsettled)
    client.get_service_config.return_value = live
    if queued:
        companion, detach = (await generations(device_id))[-2:]
        assert companion.settlement_cohort is not None
        assert companion.settlement_cohort == detach.settlement_cohort
        assert companion.job_id != predecessor.job_id
        assert not (await job_row(companion.job_id)).coalescible
        first = await job_row(await run_head(device_id, client))
        assert first.id == predecessor.job_id
        assert first.status.value == "succeeded", (first.error, first.result)
        assert len(rec.documents) == 1
        assert rec.vlan_ids() == [[100]]
        assert rec.documents[0]["interface"] == live["interface"]
        assert not rec.fake.writes[0]["no_networking"]
        await check_unsettled()
        client.get_service_config.return_value = rec.documents[-1]
    offset = int(queued)
    if reject:
        rec.fake.reject_containers.add("interface")
    job = await job_row(await run_head(device_id, client))
    assert rec.fake.writes and not rec.fake.writes[offset]["no_networking"], (
        "the positive edit needs a networked transmission"
    )
    (entry,) = rec.documents[offset]["interface"]["interface"]
    assert entry["enabled"] is False
    assert entry["description"] == "core link"
    await check_unsettled()

    if reject:
        assert job.status.value == "failed", (job.error, job.result)
        assert job.error["code"] == "nso_commit_failed"
        assert await run_head(device_id, client) is None
        assert len(rec.documents) == offset + 1
        await check_unsettled()
        return

    assert job.status.value == "succeeded", (job.error, job.result)
    client.get_service_config.return_value = rec.documents[-1]
    detach = await job_row(await run_head(device_id, client))
    assert detach.status.value == "succeeded", (detach.error, detach.result)
    assert len(rec.documents) == offset + 2
    assert rec.fake.writes[-1]["no_networking"]
    (final,) = rec.documents[-1]["interface"]["interface"]
    assert final["enabled"] is False
    assert "description" not in final
    settled = await stream_row(device_id, "interface_config")
    assert settled.applied_revision == pending.desired_revision


@pytest.mark.parametrize("auto_apply", [False, True], ids=["manual", "store-only-auto"])
async def test_manual_attribute_edit_with_detach_refuses_queued_apply(adapter_client, auto_apply):
    """Manual promotion refuses the incumbent without authorizing the prepared edit."""
    name = "attribute-manual-queued"
    device_id = await seed_device(nso_device_name=name, netbox_device_id=17340, attributes=["description", "enabled"])
    await seed_settings(device_id, auto_apply=True)
    enabled = {"interface": _IFACE, "attribute": "enabled", "intent_value": True}
    assert (await _put_attrs(adapter_client, device_id, [*_ATTR, enabled], seq=1830)).status_code == 200
    live = (await _execute(device_id, name)).documents[-1]
    before = await stream_row(device_id, "interface_config")
    assert (await _put_vlans(adapter_client, device_id, [100], seq=1)).status_code == 200
    incumbent = (await generations(device_id))[-1]
    if not auto_apply:
        from sqlalchemy import select

        from nso_adapter.store.models import DeviceSettings

        async with session() as db:
            settings = await db.scalar(select(DeviceSettings).where(DeviceSettings.device_id == device_id))
            settings.auto_apply = False
            await db.commit()
    response = await _put_attrs(
        adapter_client,
        device_id,
        [{**enabled, "intent_value": False}],
        seq=1831,
        query="?store_only=true&delete_origin=false",
    )
    assert response.status_code == 200, response.text
    response = await _apply(adapter_client, device_id, {"interface_config": 1831})
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "conflict"
    assert response.json()["error"]["detail"]["job_id"] == incumbent.job_id
    assert len(await generations(device_id)) == 2
    current = await stream_row(device_id, "interface_config")
    assert current.authorized_revision == before.authorized_revision < current.desired_revision
    assert current.applied_revision == before.applied_revision
    client, rec = recorded_client(name)
    client.get_service_config.return_value = live
    job = await job_row(await run_head(device_id, client))
    assert job.id == incumbent.job_id
    assert job.status.value == "succeeded", (job.error, job.result)
    assert len(rec.documents) == 1
    assert rec.documents[0]["interface"] == live["interface"]
    assert rec.vlan_ids() == [[100]]
    assert await run_head(device_id, client) is None
