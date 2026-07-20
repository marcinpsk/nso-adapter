# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""A1: the comprehensive per-device refresh covers all 18 families + aggregates a failed list."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from nso_adapter.config import SchedulerConfig
from nso_adapter.core.importer import (
    _config_surfaces,
    _extra_mirror_surfaces,
    _routing_surfaces,
    _run_surfaces,
    refresh_all_surfaces_for_device,
)
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device
from tests.conftest import seed_device

# The complete read-mirror family set the comprehensive refresh must cover.
_EXPECTED_18 = {
    # routing fan-out (also drives sync_device / Sync Now) — interface_ip folded in (A3)
    "static_route",
    "isis",
    "bgp",
    "ospf",
    "redistribution",
    "route_policy",
    "snmp",
    "logging",
    "bfd",
    "interface_ip",
    # L2 / interface config surfaces
    "vlan",
    "svi",
    "subinterface",
    "interface_mtu",
    # extra device-mirror surfaces
    "lag_topology",
    "lag_config",
    "l2_service",
    "switchport",
}


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


def test_registry_covers_exactly_the_18_families():
    """With every enable flag on (defaults), the combined registry is exactly the 18 families,
    with no duplicates — so a family can never silently fall out of the comprehensive refresh."""
    cfg = SchedulerConfig()
    names = [n for n, _ in (_routing_surfaces(cfg) + _config_surfaces(cfg) + _extra_mirror_surfaces(cfg))]
    assert len(names) == 18, names
    assert set(names) == _EXPECTED_18
    assert len(names) == len(set(names)), "duplicate surface in the registry"


def test_interface_ip_rides_the_routing_fanout():
    """A3: interface_ip must be in the routing fan-out (what sync_device / Sync Now runs),
    not only in the comprehensive set — that is how the 15-min sync keeps the IP mirror fresh."""
    cfg = SchedulerConfig()
    assert "interface_ip" in {n for n, _ in _routing_surfaces(cfg)}


@pytest.mark.anyio
async def test_run_surfaces_aggregates_failures_for_partial():
    """The failed-surface list must include a surface that returns False AND one that raises,
    while a True surface is omitted — this is the ``partial`` contract the fan-out relies on."""

    async def ok(db, device, client, *, refresh_source):
        return True

    async def swallowed_failure(db, device, client, *, refresh_source):
        return False  # NSO read failed, rows left stale

    async def raised(db, device, client, *, refresh_source):
        raise RuntimeError("nso blew up")

    surfaces = [("good", ok), ("degraded", swallowed_failure), ("crashed", raised)]
    failed = await _run_surfaces(None, _FakeDevice(), None, surfaces, "test")
    assert failed == ["degraded", "crashed"]


class _FakeDevice:
    id = 1


@pytest.mark.anyio
async def test_refresh_all_surfaces_reports_failed_family(adapter_client):
    """End-to-end through the real registry + store: a family whose NSO read fails lands in
    the returned failed list; interface_ip is exercised as the operator-visible pain family."""
    device_id = await seed_device(nso_device_name="all-surfaces")
    async with _device_session(device_id) as (db, device):
        client = _RecordingClient(fail={"interface-ip"})  # interface_ip is envelope-flipped (S3)
        failed = await refresh_all_surfaces_for_device(db, device, client, refresh_source="test")
        assert "interface_ip" in failed
        # every family was attempted (each family reads at least one getter)
        assert len(client.called) >= 18


class _RecordingClient:
    """Fake NsoClient: every ``get_*`` returns an empty-but-valid oper-data dict and is
    recorded; names (legacy getters) or wire families (envelope sections) in *fail* raise
    to exercise the degraded path. S3-flipped families arrive as
    ``get_device_state_section(device, wire_family)`` — the fake answers ok-empty
    sections and records the WIRE family, keeping the one-call-per-family count."""

    def __init__(self, fail: frozenset[str] = frozenset()):
        self.called: list[str] = []
        self._fail = fail

    def __getattr__(self, name):
        import httpx

        async def _getter(device_name, *args):
            called_as = args[0] if name == "get_device_state_section" else name
            self.called.append(called_as)
            if called_as in self._fail:
                raise httpx.ConnectError("boom")
            if name == "get_device_state_section":
                return {"status": "ok"}
            return {}

        return _getter
