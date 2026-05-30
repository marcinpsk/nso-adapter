# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nso_adapter.config import SchedulerConfig
from nso_adapter.core import scheduler as scheduler_module
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
    assert cfg.enable_interface_ip_sync is True
    assert cfg.interface_ip_poll_interval == 60


def test_scheduler_config_from_yaml():
    cfg = SchedulerConfig(enable_nso_streams=False, lag_topology_poll_interval=0)
    assert cfg.enable_nso_streams is False
    assert cfg.lag_topology_poll_interval == 0


def test_start_scheduler_registers_lag_refresh_job(monkeypatch: pytest.MonkeyPatch):
    fake_scheduler = _FakeScheduler()
    monkeypatch.setattr(scheduler_module, "AsyncIOScheduler", lambda: fake_scheduler)
    monkeypatch.setattr(
        scheduler_module,
        "get_config",
        lambda: SimpleNamespace(
            scheduler=SimpleNamespace(
                poll_interval=15,
                scope_reconcile_interval=5,
                lag_topology_poll_interval=60,
                enable_interface_ip_sync=True,
                interface_ip_poll_interval=60,
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
            )
        ),
    )

    scheduler_module.start_scheduler()

    assert fake_scheduler.started is True
    assert {job["id"] for job in fake_scheduler.jobs} == {
        "sync_all_devices",
        "scope_reconcile",
        "intent_reconcile",
        "lag_topology_refresh",
        "interface_ip_refresh",
    }
    scheduler_module.stop_scheduler()
    assert fake_scheduler.stopped is True


def test_start_scheduler_skips_lag_refresh_when_disabled(monkeypatch: pytest.MonkeyPatch):
    fake_scheduler = _FakeScheduler()
    monkeypatch.setattr(scheduler_module, "AsyncIOScheduler", lambda: fake_scheduler)
    monkeypatch.setattr(
        scheduler_module,
        "get_config",
        lambda: SimpleNamespace(
            scheduler=SimpleNamespace(
                poll_interval=15,
                scope_reconcile_interval=5,
                lag_topology_poll_interval=0,
                enable_interface_ip_sync=False,
                interface_ip_poll_interval=60,
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
            )
        ),
    )

    scheduler_module.start_scheduler()

    assert fake_scheduler.started is True
    assert {job["id"] for job in fake_scheduler.jobs} == {
        "sync_all_devices",
        "scope_reconcile",
        "intent_reconcile",
    }
    scheduler_module.stop_scheduler()


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
    nso_client = MagicMock()
    monkeypatch.setattr("nso_adapter.core.importer.get_nso_client", lambda *_: nso_client)
    monkeypatch.setattr("nso_adapter.core.lag_topology.refresh_lag_topology_for_device", refresh)

    await scheduler_module._scheduled_lag_topology_refresh()

    assert refresh.await_count == 2
    assert all(call.kwargs["refresh_source"] == "poll" for call in refresh.await_args_list)
