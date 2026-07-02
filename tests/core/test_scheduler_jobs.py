# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <mazieba@libertyglobal.com>
"""Coverage paydown for the grandfathered scheduler omit.

Exercises the inner ``_scheduled_*`` jobs DIRECTLY (per testing-strategy.md §3.6), not the
APScheduler lifecycle wrapper (covered separately in test_lag_topology_scheduler.py). The
NSO/NetBox clients and per-family refresh functions are the true integration edges and are
monkeypatched; the DB is real (the ``adapter_client`` fixture wires ``get_session``).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select

from nso_adapter.bindings.netbox.scope import PluginScopeRecord
from nso_adapter.core import scheduler as sched
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.db import get_session
from nso_adapter.store.models import DbInterface, Device, InterfaceIntent, ManagedScope


def _nso_client():
    # The NSO client is a real external HTTP boundary; bind the stand-in to NsoClient via
    # spec=. The scheduled jobs thread it straight to the (stubbed) per-family refresh, so it
    # is never dereferenced — spec= keeps a renamed member from being fabricated.
    return MagicMock(spec=NsoClient)


async def _seed_devices(*specs: tuple[str, int]) -> dict[str, int]:
    """Seed (nso_device_name, netbox_device_id) devices; return {name: device_id}."""
    ids: dict[str, int] = {}
    async for db in get_session():
        for name, nb_id in specs:
            dev = Device(nso_instance="nso-dev", nso_device_name=name, netbox_device_id=nb_id)
            db.add(dev)
        await db.commit()
        result = await db.execute(select(Device).where(Device.nso_instance == "nso-dev"))
        for dev in result.scalars().all():
            ids[dev.nso_device_name] = dev.id
        break
    return ids


# ── Part A: per-family refresh wrappers (iterate every device, refresh_source=poll) ──

_WRAPPERS = [
    ("_scheduled_lag_config_refresh", "nso_adapter.core.lag_config.refresh_lag_config_for_device"),
    ("_scheduled_interface_ip_refresh", "nso_adapter.core.interface_ip.refresh_interface_ips_for_device"),
    ("_scheduled_static_route_refresh", "nso_adapter.core.static_route.refresh_static_routes_for_device"),
    ("_scheduled_isis_refresh", "nso_adapter.core.isis.refresh_isis_interfaces_for_device"),
    ("_scheduled_bgp_refresh", "nso_adapter.core.bgp.refresh_bgp_config_for_device"),
    ("_scheduled_ospf_refresh", "nso_adapter.core.ospf.refresh_ospf_for_device"),
    ("_scheduled_redistribution_refresh", "nso_adapter.core.redistribution.refresh_redistribution_for_device"),
    ("_scheduled_snmp_refresh", "nso_adapter.core.snmp.refresh_snmp_config_for_device"),
    ("_scheduled_logging_refresh", "nso_adapter.core.logging_config.refresh_logging_config_for_device"),
    ("_scheduled_route_policy_refresh", "nso_adapter.core.route_policy.refresh_route_policy_for_device"),
    ("_scheduled_interface_mtu_refresh", "nso_adapter.core.interface_mtu.refresh_interface_mtu_for_device"),
]


@pytest.mark.anyio
@pytest.mark.parametrize(("fn_name", "refresh_target"), _WRAPPERS)
async def test_family_refresh_wrapper_refreshes_all_devices(adapter_client, monkeypatch, fn_name, refresh_target):
    await _seed_devices(("d1", 4001), ("d2", 4002))
    refresh = AsyncMock()
    nso_client = _nso_client()
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: nso_client)
    monkeypatch.setattr(refresh_target, refresh)

    await getattr(sched, fn_name)()

    assert refresh.await_count == 2
    assert all(c.kwargs["refresh_source"] == "poll" for c in refresh.await_args_list)
    # The resolved client must reach refresh as the 3rd positional arg (db, device, nso_client, ...).
    assert all(c.args[2] is nso_client for c in refresh.await_args_list)


@pytest.mark.anyio
async def test_family_refresh_skips_device_without_nso_client(adapter_client, monkeypatch):
    await _seed_devices(("d1", 5001))
    refresh = AsyncMock()

    def _raise(*_):
        raise RuntimeError("NSO client not registered")

    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", _raise)
    monkeypatch.setattr("nso_adapter.core.bgp.refresh_bgp_config_for_device", refresh)

    await sched._scheduled_bgp_refresh()
    refresh.assert_not_called()


# ── Part B: sync_all / scope_reconcile / intent_reconcile / capability / topology ──


@pytest.mark.anyio
async def test_scheduled_sync_all_enqueues_each_scoped_device(adapter_client, monkeypatch):
    async for db in get_session():
        dev = Device(nso_instance="nso-dev", nso_device_name="sw01", netbox_device_id=6001)
        db.add(dev)
        await db.commit()
        db.add(ManagedScope(device_id=dev.id, attribute="description"))
        await db.commit()
        dev_id = dev.id
        break

    enqueue = AsyncMock(return_value=(SimpleNamespace(id=99), True))
    monkeypatch.setattr("nso_adapter.core.jobs.enqueue_job", enqueue)

    await sched._scheduled_sync_all()

    assert enqueue.await_count == 1
    assert enqueue.await_args.args[0] == dev_id


@pytest.mark.anyio
async def test_scheduled_sync_all_handles_skip_and_error(adapter_client, monkeypatch):
    async for db in get_session():
        for name, nb in (("sw01", 6101), ("sw02", 6102)):
            dev = Device(nso_instance="nso-dev", nso_device_name=name, netbox_device_id=nb)
            db.add(dev)
            await db.commit()
            db.add(ManagedScope(device_id=dev.id, attribute="description"))
            await db.commit()
        break

    # first device: job already active (created=False, skip branch); second: enqueue raises (error branch)
    enqueue = AsyncMock(side_effect=[(SimpleNamespace(id=1), False), RuntimeError("boom")])
    monkeypatch.setattr("nso_adapter.core.jobs.enqueue_job", enqueue)

    await sched._scheduled_sync_all()  # must not raise
    assert enqueue.await_count == 2


@pytest.mark.anyio
async def test_scope_reconcile_skips_without_netbox_client(adapter_client, monkeypatch):
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: None)
    await sched._scheduled_scope_reconcile()  # returns cleanly


@pytest.mark.anyio
async def test_scope_reconcile_aborts_on_fetch_error(adapter_client, monkeypatch):
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: object())
    monkeypatch.setattr(
        "nso_adapter.bindings.netbox.scope.fetch_all_scope", AsyncMock(side_effect=RuntimeError("NetBox down"))
    )
    await sched._scheduled_scope_reconcile()  # aborts wholesale, no raise


@pytest.mark.anyio
async def test_scope_reconcile_offboards_absent_and_sets_present(adapter_client, monkeypatch):
    await _seed_devices(("present", 7001), ("absent", 7002))
    set_scope = AsyncMock()
    offboard = AsyncMock()
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: object())
    monkeypatch.setattr(
        "nso_adapter.bindings.netbox.scope.fetch_all_scope",
        AsyncMock(return_value=[PluginScopeRecord(netbox_device_id=7001, attributes=["description"])]),
    )
    monkeypatch.setattr("nso_adapter.core.onboarding.set_scope", set_scope)
    monkeypatch.setattr("nso_adapter.core.onboarding.offboard_device", offboard)

    await sched._scheduled_scope_reconcile()

    set_scope.assert_awaited_once()  # the device still in the plugin
    offboard.assert_awaited_once()  # the device dropped from the plugin


@pytest.mark.anyio
async def test_scope_reconcile_persists_failover_ips(adapter_client, monkeypatch):
    """The reconcile must COMMIT its session — otherwise the plugin-sourced primary/OOB IPs
    (and scope) are silently discarded when get_session closes uncommitted (s3-3).

    Runs the real set_scope + upsert_failover_ips, then reads back in a FRESH session so the
    assertion only passes if the change was actually committed, not just left pending.
    """
    from nso_adapter.store.models import DeviceFailover

    ids = await _seed_devices(("fo-persist", 7100))
    device_id = ids["fo-persist"]
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: object())
    monkeypatch.setattr(
        "nso_adapter.bindings.netbox.scope.fetch_all_scope",
        AsyncMock(
            return_value=[
                PluginScopeRecord(
                    netbox_device_id=7100,
                    attributes=["description"],
                    primary_ip="10.0.0.1",
                    oob_ip="192.0.2.5",
                )
            ]
        ),
    )

    await sched._scheduled_scope_reconcile()

    async for db in get_session():
        fo = (
            await db.execute(select(DeviceFailover).where(DeviceFailover.device_id == device_id))
        ).scalar_one_or_none()
        assert fo is not None, "failover row not persisted — reconcile never committed"
        assert fo.primary_ip == "10.0.0.1"
        assert fo.oob_ip == "192.0.2.5"
        break


@pytest.mark.anyio
async def test_intent_reconcile_skips_without_netbox_client(adapter_client, monkeypatch):
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: None)
    await sched._scheduled_intent_reconcile()


@pytest.mark.anyio
async def test_intent_reconcile_aborts_on_fetch_error(adapter_client, monkeypatch):
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: object())
    monkeypatch.setattr(
        "nso_adapter.bindings.netbox.intent.fetch_all_intent", AsyncMock(side_effect=RuntimeError("NetBox down"))
    )
    await sched._scheduled_intent_reconcile()


@pytest.mark.anyio
async def test_intent_reconcile_replaces_intent_and_skips_unknown_interface(adapter_client, monkeypatch):
    ids = await _seed_devices(("sw01", 8001))
    async for db in get_session():
        db.add(DbInterface(device_id=ids["sw01"], name="GigabitEthernet0/1"))
        await db.commit()
        break

    records = [
        SimpleNamespace(
            netbox_device_id=8001,
            interface_name="GigabitEthernet0/1",
            attribute="description",
            intent_value="uplink",
            accepted_at=None,
        ),
        SimpleNamespace(  # unknown interface — must be skipped, not crash
            netbox_device_id=8001,
            interface_name="GigabitEthernet9/9",
            attribute="description",
            intent_value="x",
            accepted_at=None,
        ),
    ]
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: object())
    monkeypatch.setattr("nso_adapter.bindings.netbox.intent.fetch_all_intent", AsyncMock(return_value=records))

    await sched._scheduled_intent_reconcile()

    async for db in get_session():
        rows = (await db.execute(select(InterfaceIntent))).scalars().all()
        break
    assert [(r.attribute, r.intent_value) for r in rows] == [("description", "uplink")]


@pytest.mark.anyio
async def test_intent_reconcile_deletes_existing_when_no_records(adapter_client, monkeypatch):
    """A device with a pre-existing intent but no incoming records → row deleted, no re-add."""
    ids = await _seed_devices(("sw01", 8101))
    async for db in get_session():
        iface = DbInterface(device_id=ids["sw01"], name="GigabitEthernet0/1")
        db.add(iface)
        await db.flush()
        db.add(InterfaceIntent(interface_id=iface.id, attribute="description", intent_value="stale"))
        await db.commit()
        break

    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: object())
    monkeypatch.setattr("nso_adapter.bindings.netbox.intent.fetch_all_intent", AsyncMock(return_value=[]))

    await sched._scheduled_intent_reconcile()

    async for db in get_session():
        rows = (await db.execute(select(InterfaceIntent))).scalars().all()
        break
    assert rows == []  # the stale intent was deleted; nothing re-added (count==0)


@pytest.mark.anyio
async def test_capability_refresh_probes_each_device(adapter_client, monkeypatch):
    await _seed_devices(("c1", 9001), ("c2", 9002))
    refresh = AsyncMock()
    nso_client = _nso_client()
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: nso_client)
    monkeypatch.setattr("nso_adapter.core.capability.refresh_device_capability", refresh)

    await sched._scheduled_capability_refresh()
    assert refresh.await_count == 2
    # refresh_device_capability(db, nso_client, device_name, device) — assert the client lands.
    assert all(c.args[1] is nso_client for c in refresh.await_args_list)


@pytest.mark.anyio
async def test_capability_refresh_isolates_per_device_failures(adapter_client, monkeypatch):
    await _seed_devices(("c1", 9101), ("c2", 9102))
    refresh = AsyncMock(side_effect=RuntimeError("probe failed"))
    nso_client = _nso_client()
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: nso_client)
    monkeypatch.setattr("nso_adapter.core.capability.refresh_device_capability", refresh)

    await sched._scheduled_capability_refresh()  # one failure must not abort the fleet
    assert refresh.await_count == 2


@pytest.mark.anyio
async def test_topology_interfaces_refresh_skips_without_netbox_client(adapter_client, monkeypatch):
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: None)
    await sched._scheduled_topology_interfaces_refresh()


@pytest.mark.anyio
async def test_topology_interfaces_refresh_ensures_each_device(adapter_client, monkeypatch):
    await _seed_devices(("t1", 9201), ("t2", 9202))
    ensure = AsyncMock()
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: object())
    monkeypatch.setattr("nso_adapter.core.topology_interfaces.ensure_topology_interfaces", ensure)

    await sched._scheduled_topology_interfaces_refresh()
    assert ensure.await_count == 2


@pytest.mark.anyio
async def test_topology_interfaces_refresh_isolates_per_device_failure(adapter_client, monkeypatch):
    """One device's ensure error is logged, not raised, and the fleet refresh continues."""
    await _seed_devices(("t1", 9301), ("t2", 9302))
    ensure = AsyncMock(side_effect=RuntimeError("netbox 500"))
    monkeypatch.setattr("nso_adapter.core.importer.get_netbox_client", lambda: object())
    monkeypatch.setattr("nso_adapter.core.topology_interfaces.ensure_topology_interfaces", ensure)

    await sched._scheduled_topology_interfaces_refresh()  # must not raise
    assert ensure.await_count == 2  # both devices attempted despite the error
