# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nso_adapter.config import SchedulerConfig
from nso_adapter.core import scheduler as scheduler_module
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device


class _FakeScheduler:
    def __init__(self) -> None:
        self.jobs: list[dict] = []
        self.started = False
        self.stopped = False

    def add_job(self, func, trigger, **kwargs):
        self.jobs.append({"func": func, "trigger": trigger, **kwargs})

    def start(self):
        self.started = True

    def shutdown(self, wait=True):
        self.stopped = True


def test_scheduler_config_defaults():
    cfg = SchedulerConfig()
    assert cfg.enable_nso_streams is True
    assert cfg.lag_topology_poll_interval == 60
    assert cfg.lag_config_poll_interval == 60
    assert cfg.enable_interface_ip_sync is True
    assert cfg.interface_ip_poll_interval == 60
    assert cfg.orphan_reap_interval == 5


def test_scheduler_config_from_yaml():
    cfg = SchedulerConfig(enable_nso_streams=False, lag_topology_poll_interval=0)
    assert cfg.enable_nso_streams is False
    assert cfg.lag_topology_poll_interval == 0


def test_start_scheduler_registers_lag_refresh_job(monkeypatch: pytest.MonkeyPatch):
    fake_scheduler = _FakeScheduler()
    monkeypatch.setattr(scheduler_module, "AsyncIOScheduler", lambda **_kw: fake_scheduler)
    monkeypatch.setattr(
        scheduler_module,
        "get_config",
        lambda: SimpleNamespace(
            scheduler=SimpleNamespace(
                poll_interval=15,
                scope_reconcile_interval=5,
                lag_topology_poll_interval=60,
                lag_config_poll_interval=60,
                enable_bfd_sync=True,
                bfd_poll_interval=300,
                enable_interface_ip_sync=True,
                interface_ip_poll_interval=60,
                enable_interface_mtu_sync=False,
                interface_mtu_poll_interval=300,
                enable_snmp_sync=False,
                snmp_poll_interval=300,
                enable_static_routing_sync=False,
                static_route_poll_interval=60,
                enable_isis_sync=False,
                isis_poll_interval=300,
                enable_bgp_sync=False,
                bgp_poll_interval=300,
                enable_ospf_sync=False,
                ospf_poll_interval=300,
                enable_redistribution_sync=False,
                redistribution_poll_interval=300,
                enable_route_policy_sync=False,
                route_policy_poll_interval=300,
                enable_capability_refresh=False,
                capability_refresh_interval=1440,
                enable_logging_sync=False,
                logging_poll_interval=300,
                enable_l2_service_sync=True,
                l2_service_poll_interval=300,
                enable_vlan_sync=True,
                vlan_poll_interval=300,
                enable_switchport_sync=True,
                switchport_poll_interval=300,
                enable_svi_sync=True,
                svi_poll_interval=300,
                enable_subinterface_sync=True,
                subinterface_poll_interval=300,
                enable_topology_interface_sync=False,
                topology_interface_poll_interval=120,
                enable_failover=False,
                failover_base_tick=1,
                orphan_reap_interval=5,
            )
        ),
    )

    scheduler_module.start_scheduler()

    assert fake_scheduler.started is True
    assert {job["id"] for job in fake_scheduler.jobs} == {
        "sync_all_devices",
        "scope_reconcile",
        "intent_reconcile",
        "orphan_reap",
        "lag_topology_refresh",
        "lag_config_refresh",
        "bfd_refresh",
        "interface_ip_refresh",
        "l2_service_refresh",
        "vlan_refresh",
        "switchport_refresh",
        "svi_refresh",
        "subinterface_refresh",
        "startup_sync_kick",  # A4: one-shot restart repopulation
    }
    scheduler_module.stop_scheduler()
    assert fake_scheduler.stopped is True


def test_start_scheduler_skips_lag_refresh_when_disabled(monkeypatch: pytest.MonkeyPatch):
    fake_scheduler = _FakeScheduler()
    monkeypatch.setattr(scheduler_module, "AsyncIOScheduler", lambda **_kw: fake_scheduler)
    monkeypatch.setattr(
        scheduler_module,
        "get_config",
        lambda: SimpleNamespace(
            scheduler=SimpleNamespace(
                poll_interval=15,
                scope_reconcile_interval=5,
                lag_topology_poll_interval=0,
                lag_config_poll_interval=0,
                enable_bfd_sync=False,
                bfd_poll_interval=300,
                enable_interface_ip_sync=False,
                interface_ip_poll_interval=60,
                enable_interface_mtu_sync=False,
                interface_mtu_poll_interval=300,
                enable_snmp_sync=False,
                snmp_poll_interval=300,
                enable_static_routing_sync=False,
                static_route_poll_interval=60,
                enable_isis_sync=False,
                isis_poll_interval=300,
                enable_bgp_sync=False,
                bgp_poll_interval=300,
                enable_ospf_sync=False,
                ospf_poll_interval=300,
                enable_redistribution_sync=False,
                redistribution_poll_interval=300,
                enable_route_policy_sync=False,
                route_policy_poll_interval=300,
                enable_capability_refresh=False,
                capability_refresh_interval=1440,
                enable_logging_sync=False,
                logging_poll_interval=300,
                enable_l2_service_sync=False,
                l2_service_poll_interval=300,
                enable_vlan_sync=False,
                vlan_poll_interval=300,
                enable_switchport_sync=False,
                switchport_poll_interval=300,
                enable_svi_sync=False,
                svi_poll_interval=300,
                enable_subinterface_sync=False,
                subinterface_poll_interval=300,
                enable_topology_interface_sync=False,
                topology_interface_poll_interval=120,
                enable_failover=False,
                failover_base_tick=1,
                orphan_reap_interval=5,
            )
        ),
    )

    scheduler_module.start_scheduler()

    assert fake_scheduler.started is True
    # orphan_reap is a safety-net with no feature-enable flag: disabling every feature sync must
    # NOT disable self-healing of stranded jobs — it stays registered (only interval=0 drops it).
    assert {job["id"] for job in fake_scheduler.jobs} == {
        "sync_all_devices",
        "scope_reconcile",
        "intent_reconcile",
        "orphan_reap",
        "startup_sync_kick",  # A4: one-shot restart repopulation (always registered)
    }
    scheduler_module.stop_scheduler()


def _full_scheduler_config(**overrides) -> SimpleNamespace:
    """A scheduler config with a UNIQUE interval per job so id→minutes is checkable."""
    base = dict(
        poll_interval=1,
        scope_reconcile_interval=2,
        lag_topology_poll_interval=3,
        lag_config_poll_interval=4,
        enable_bfd_sync=True,
        bfd_poll_interval=24,
        enable_interface_ip_sync=True,
        interface_ip_poll_interval=5,
        enable_static_routing_sync=True,
        static_route_poll_interval=6,
        enable_isis_sync=True,
        isis_poll_interval=7,
        enable_bgp_sync=True,
        bgp_poll_interval=8,
        enable_ospf_sync=True,
        ospf_poll_interval=9,
        enable_redistribution_sync=True,
        redistribution_poll_interval=10,
        enable_snmp_sync=True,
        snmp_poll_interval=11,
        enable_logging_sync=True,
        logging_poll_interval=12,
        enable_l2_service_sync=True,
        l2_service_poll_interval=13,
        enable_vlan_sync=True,
        vlan_poll_interval=14,
        enable_switchport_sync=True,
        switchport_poll_interval=15,
        enable_svi_sync=True,
        svi_poll_interval=16,
        enable_subinterface_sync=True,
        subinterface_poll_interval=17,
        enable_interface_mtu_sync=True,
        interface_mtu_poll_interval=18,
        enable_route_policy_sync=True,
        route_policy_poll_interval=19,
        enable_capability_refresh=True,
        capability_refresh_interval=20,
        enable_topology_interface_sync=True,
        topology_interface_poll_interval=21,
        enable_failover=True,
        failover_base_tick=22,
        orphan_reap_interval=23,
    )
    base.update(overrides)
    return SimpleNamespace(scheduler=SimpleNamespace(**base))


def test_start_scheduler_registers_every_job_with_correct_interval(monkeypatch: pytest.MonkeyPatch):
    """All flags on → every job registers, each mapped to its own config interval (minutes)."""
    fake_scheduler = _FakeScheduler()
    monkeypatch.setattr(scheduler_module, "AsyncIOScheduler", lambda **_kw: fake_scheduler)
    monkeypatch.setattr(scheduler_module, "get_config", _full_scheduler_config)

    scheduler_module.start_scheduler()

    assert fake_scheduler.started is True
    # Interval jobs only: the A4 startup_sync_kick is a one-shot "date" job with no `minutes`.
    assert {job["id"]: job["minutes"] for job in fake_scheduler.jobs if job["trigger"] == "interval"} == {
        "sync_all_devices": 1,
        "scope_reconcile": 2,
        "intent_reconcile": 2,
        "orphan_reap": 23,
        "lag_topology_refresh": 3,
        "lag_config_refresh": 4,
        "bfd_refresh": 24,
        "interface_ip_refresh": 5,
        "static_route_refresh": 6,
        "isis_refresh": 7,
        "bgp_refresh": 8,
        "ospf_refresh": 9,
        "redistribution_refresh": 10,
        "snmp_refresh": 11,
        "logging_refresh": 12,
        "l2_service_refresh": 13,
        "vlan_refresh": 14,
        "switchport_refresh": 15,
        "svi_refresh": 16,
        "subinterface_refresh": 17,
        "interface_mtu_refresh": 18,
        "route_policy_refresh": 19,
        "capability_refresh": 20,
        "topology_interfaces_refresh": 21,
        "failover_probe": 22,
    }
    scheduler_module.stop_scheduler()


def test_start_scheduler_interval_only_gate_skips_lag_when_zero(monkeypatch: pytest.MonkeyPatch):
    """The lag jobs have no enable flag — a 0 interval alone must skip them."""
    fake_scheduler = _FakeScheduler()
    monkeypatch.setattr(scheduler_module, "AsyncIOScheduler", lambda **_kw: fake_scheduler)
    monkeypatch.setattr(
        scheduler_module,
        "get_config",
        lambda: _full_scheduler_config(lag_topology_poll_interval=0, lag_config_poll_interval=0),
    )

    scheduler_module.start_scheduler()

    ids = {job["id"] for job in fake_scheduler.jobs}
    assert "lag_topology_refresh" not in ids
    assert "lag_config_refresh" not in ids
    # the 3 unconditional jobs are always present
    assert {"sync_all_devices", "scope_reconcile", "intent_reconcile"} <= ids


def test_stop_scheduler_is_noop_when_not_started():
    """stop_scheduler with no live scheduler must not raise."""
    scheduler_module._scheduler = None
    scheduler_module.stop_scheduler()  # no error


# ── no-NSO-client skip: every poll job tolerates an unregistered instance ──────


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("scheduled_fn", "refresh_target"),
    [
        ("_scheduled_lag_topology_refresh", "nso_adapter.core.lag_topology.refresh_lag_topology_for_device"),
        ("_scheduled_lag_config_refresh", "nso_adapter.core.lag_config.refresh_lag_config_for_device"),
        ("_scheduled_l2_service_refresh", "nso_adapter.core.l2_service.refresh_l2_services_for_device"),
        ("_scheduled_interface_ip_refresh", "nso_adapter.core.interface_ip.refresh_interface_ips_for_device"),
        ("_scheduled_static_route_refresh", "nso_adapter.core.static_route.refresh_static_routes_for_device"),
        ("_scheduled_isis_refresh", "nso_adapter.core.isis.refresh_isis_interfaces_for_device"),
        ("_scheduled_bgp_refresh", "nso_adapter.core.bgp.refresh_bgp_config_for_device"),
        ("_scheduled_ospf_refresh", "nso_adapter.core.ospf.refresh_ospf_for_device"),
        ("_scheduled_redistribution_refresh", "nso_adapter.core.redistribution.refresh_redistribution_for_device"),
        ("_scheduled_snmp_refresh", "nso_adapter.core.snmp.refresh_snmp_config_for_device"),
        ("_scheduled_logging_refresh", "nso_adapter.core.logging_config.refresh_logging_config_for_device"),
        ("_scheduled_route_policy_refresh", "nso_adapter.core.route_policy.refresh_route_policy_for_device"),
        ("_scheduled_capability_refresh", "nso_adapter.core.capability.refresh_device_capability"),
        # _refresh_all_devices-backed job → exercises the shared helper's skip branch.
        ("_scheduled_vlan_refresh", "nso_adapter.core.vlan.refresh_vlan_database_for_device"),
    ],
)
async def test_poll_job_skips_device_without_nso_client(adapter_client, monkeypatch, scheduled_fn, refresh_target):
    """A device whose NSO instance isn't registered is skipped, not refreshed, without raising."""
    async for db in get_session():
        db.add(Device(nso_instance="ghost", nso_device_name="d1", netbox_device_id=4001))
        await db.commit()
        break

    def _raise(*_):
        raise RuntimeError("NSO client for 'ghost' not registered")

    refresh = AsyncMock()
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", _raise)
    monkeypatch.setattr(refresh_target, refresh)

    await getattr(scheduler_module, scheduled_fn)()  # must not raise

    assert refresh.await_count == 0  # skipped, never refreshed


@pytest.mark.anyio
async def test_scheduled_lag_topology_refresh_refreshes_all_devices(adapter_client, monkeypatch):
    async for db in get_session():
        db.add_all(
            [
                Device(nso_instance="nso-dev", nso_device_name="sw01", netbox_device_id=1001),
                Device(nso_instance="nso-dev", nso_device_name="sw02", netbox_device_id=1002),
            ]
        )
        await db.commit()
        break

    refresh = AsyncMock()
    nso_client = MagicMock(spec=NsoClient)  # boundary stand-in, bound to the real client interface
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: nso_client)
    monkeypatch.setattr("nso_adapter.core.lag_topology.refresh_lag_topology_for_device", refresh)

    await scheduler_module._scheduled_lag_topology_refresh()

    assert refresh.await_count == 2
    assert all(call.kwargs["refresh_source"] == "poll" for call in refresh.await_args_list)
    # The resolved NSO client must actually reach refresh (guards against a copy-paste bug
    # passing the wrong positional arg) — call shape is (db, device, nso_client, ...).
    assert all(call.args[2] is nso_client for call in refresh.await_args_list)
    assert {call.args[1].nso_device_name for call in refresh.await_args_list} == {"sw01", "sw02"}


@pytest.mark.anyio
async def test_scheduled_l2_service_refresh_refreshes_all_devices(adapter_client, monkeypatch):
    async for db in get_session():
        db.add_all(
            [
                Device(nso_instance="nso-dev", nso_device_name="ra1", netbox_device_id=2001),
                Device(nso_instance="nso-dev", nso_device_name="ra2", netbox_device_id=2002),
            ]
        )
        await db.commit()
        break

    refresh = AsyncMock()
    nso_client = MagicMock(spec=NsoClient)  # boundary stand-in, bound to the real client interface
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: nso_client)
    monkeypatch.setattr("nso_adapter.core.l2_service.refresh_l2_services_for_device", refresh)

    await scheduler_module._scheduled_l2_service_refresh()

    assert refresh.await_count == 2
    assert all(call.kwargs["refresh_source"] == "poll" for call in refresh.await_args_list)
    assert all(call.args[2] is nso_client for call in refresh.await_args_list)
    assert {call.args[1].nso_device_name for call in refresh.await_args_list} == {"ra1", "ra2"}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("scheduled_fn", "refresh_target"),
    [
        ("_scheduled_vlan_refresh", "nso_adapter.core.vlan.refresh_vlan_database_for_device"),
        ("_scheduled_switchport_refresh", "nso_adapter.core.vlan.refresh_switchport_for_device"),
        ("_scheduled_svi_refresh", "nso_adapter.core.svi.refresh_svi_for_device"),
        ("_scheduled_subinterface_refresh", "nso_adapter.core.subinterface.refresh_subinterface_for_device"),
    ],
)
async def test_l2l3_family_refresh_refreshes_all_devices(adapter_client, monkeypatch, scheduled_fn, refresh_target):
    """//: the L2/L3 interface-family poll jobs refresh every managed device."""
    async for db in get_session():
        db.add_all(
            [
                Device(nso_instance="nso-dev", nso_device_name="d1", netbox_device_id=3001),
                Device(nso_instance="nso-dev", nso_device_name="d2", netbox_device_id=3002),
            ]
        )
        await db.commit()
        break

    refresh = AsyncMock()
    nso_client = MagicMock(spec=NsoClient)  # boundary stand-in, bound to the real client interface
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: nso_client)
    monkeypatch.setattr(refresh_target, refresh)

    await getattr(scheduler_module, scheduled_fn)()

    assert refresh.await_count == 2
    assert all(call.kwargs["refresh_source"] == "poll" for call in refresh.await_args_list)
    # Routes through the shared _refresh_all_devices helper — the resolved client must still
    # reach refresh as the 3rd positional arg (db, device, nso_client, ...).
    assert all(call.args[2] is nso_client for call in refresh.await_args_list)
    assert {call.args[1].nso_device_name for call in refresh.await_args_list} == {"d1", "d2"}


def test_start_scheduler_kicks_startup_sync(monkeypatch: pytest.MonkeyPatch):
    """A4: start_scheduler schedules ONE immediate sync sweep (routing + interface_ip) so a
    restart repopulates the visible mirror promptly — not all 17 per-family polls at once."""
    fake_scheduler = _FakeScheduler()
    monkeypatch.setattr(scheduler_module, "AsyncIOScheduler", lambda **_kw: fake_scheduler)
    monkeypatch.setattr(scheduler_module, "get_config", lambda: SimpleNamespace(scheduler=SchedulerConfig()))

    scheduler_module.start_scheduler()

    kick = [j for j in fake_scheduler.jobs if j.get("id") == "startup_sync_kick"]
    assert len(kick) == 1
    assert kick[0]["func"] is scheduler_module._scheduled_sync_all
    assert kick[0]["trigger"] == "date"
    # It must NOT schedule the per-family poll wrappers as startup one-shots (no 17-read burst):
    family_oneshots = [j for j in fake_scheduler.jobs if j["trigger"] == "date" and j.get("id") != "startup_sync_kick"]
    assert family_oneshots == []
