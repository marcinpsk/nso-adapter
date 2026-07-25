# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM 1331B — the refresh engine owns every FamilySpec commit boundary."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete, select

from nso_adapter.core.families import ENGINE_FAMILY_KEYS
from nso_adapter.core.importer import refresh_routing_surfaces_for_device
from nso_adapter.core.refresh_engine import FamilySpec, run_family_refresh_from_outcome
from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import AbsentAuthoritative, Freshness, Present
from nso_adapter.store import outcome_store
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceStaticRoute
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


async def _fresh_prefixes(device_id: int) -> list[str]:
    async for db in get_session():
        rows = (
            (await db.execute(select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device_id)))
            .scalars()
            .all()
        )
        return [row.prefix for row in rows]
    raise RuntimeError("no session")


async def _seed_route(device_id: int, prefix: str) -> None:
    async for db in get_session():
        db.add(DeviceStaticRoute(device_id=device_id, vrf="", prefix=prefix, next_hop="198.18.0.1"))
        await db.commit()
        return
    raise RuntimeError("no session")


@pytest.mark.parametrize(
    ("outcome", "expected_before_phase_two", "expected_result"),
    [
        (
            Present(
                {"route": [{"vrf": "", "prefix": "198.18.10.0/24", "next-hop": "198.18.0.2"}]},
                Freshness.fresh,
            ),
            ["198.18.10.0/24"],
            "replaced",
        ),
        (AbsentAuthoritative(), [], "cleared"),
    ],
    ids=("present", "authoritative-clear"),
)
async def test_engine_commits_staged_mirror_before_phase_two(
    adapter_client,
    monkeypatch,
    outcome,
    expected_before_phase_two,
    expected_result,
):
    """A transaction-neutral materializer is durable before best-effort phase two."""
    device_id = await seed_device(nso_device_name=f"engine-commit-{expected_result}")
    await _seed_route(device_id, "198.18.9.0/24")

    async def _stage_routes(db, device, routes, refresh_source):
        await db.execute(delete(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id))
        for route in routes:
            db.add(
                DeviceStaticRoute(
                    device_id=device.id,
                    vrf=route.get("vrf", ""),
                    prefix=route["prefix"],
                    next_hop=route.get("next-hop", ""),
                    refresh_source=refresh_source,
                )
            )

    spec = FamilySpec(
        name="transaction_probe",
        extract=lambda data: data.get("route", []),
        materialize=_stage_routes,
        wire_name="transaction-probe",
    )
    real_record_result = outcome_store.record_result
    visible_before_phase_two: list[list[str]] = []

    async def _observe_then_record(db, attempt_id, *, result, succeeded, row_count=None):
        visible_before_phase_two.append(await _fresh_prefixes(device_id))
        await real_record_result(
            db,
            attempt_id,
            result=result,
            succeeded=succeeded,
            row_count=row_count,
        )

    monkeypatch.setattr(outcome_store, "record_result", _observe_then_record)

    async with _device_session(device_id) as (db, device):
        ok = await run_family_refresh_from_outcome(db, device, spec, outcome)

    assert ok is True
    assert visible_before_phase_two == [expected_before_phase_two]
    assert await _fresh_prefixes(device_id) == expected_before_phase_two
    async for db in get_session():
        current = await outcome_store.get_current_outcome(db, device_id, spec.name)
        assert current is not None
        assert (current.result, current.succeeded) == (expected_result, True)
        break


class _CommitForbiddenSession:
    """Real-session forwarding proxy with one forbidden transaction operation."""

    def __init__(self, db, family: str, payload_kind: str) -> None:
        self._db = db
        self._label = f"{family}/{payload_kind}"

    def __getattr__(self, name):
        return getattr(self._db, name)

    async def commit(self) -> None:
        raise AssertionError(f"{self._label} materializer committed the caller session")


def _family_specs() -> tuple[FamilySpec, ...]:
    from nso_adapter.core.bfd import BFD_SPEC
    from nso_adapter.core.bgp import BGP_SPEC
    from nso_adapter.core.interface_ip import INTERFACE_IP_SPEC
    from nso_adapter.core.interface_mtu import INTERFACE_MTU_SPEC
    from nso_adapter.core.isis import ISIS_SPEC
    from nso_adapter.core.l2_service import L2_SERVICE_SPEC
    from nso_adapter.core.lag_config import LAG_CONFIG_SPEC
    from nso_adapter.core.lag_topology import LAG_TOPOLOGY_SPEC
    from nso_adapter.core.logging_config import LOGGING_CONFIG_SPEC
    from nso_adapter.core.ospf import OSPF_SPEC
    from nso_adapter.core.route_policy import ROUTE_POLICY_SPEC
    from nso_adapter.core.snmp import SNMP_SPEC
    from nso_adapter.core.static_route import STATIC_ROUTE_SPEC
    from nso_adapter.core.subinterface import SUBINTERFACE_SPEC
    from nso_adapter.core.svi import SVI_SPEC
    from nso_adapter.core.vlan import SWITCHPORT_SPEC, VLAN_DATABASE_SPEC

    return (
        LAG_TOPOLOGY_SPEC,
        LOGGING_CONFIG_SPEC,
        SNMP_SPEC,
        BGP_SPEC,
        SVI_SPEC,
        SUBINTERFACE_SPEC,
        INTERFACE_IP_SPEC,
        ISIS_SPEC,
        VLAN_DATABASE_SPEC,
        SWITCHPORT_SPEC,
        BFD_SPEC,
        L2_SERVICE_SPEC,
        STATIC_ROUTE_SPEC,
        INTERFACE_MTU_SPEC,
        LAG_CONFIG_SPEC,
        OSPF_SPEC,
        ROUTE_POLICY_SPEC,
    )


_PRESENT_DATA = {
    "lag": {"lag": [{"name": "ae1", "lag-id": 1}]},
    "logging": {"host": [{"address": "198.18.0.10"}]},
    "snmp": {"community": [{"name": "sha256:placeholder", "access": "RO"}]},
    "bgp": {"router": [{"asn": "64512"}]},
    "svi": {"interface": [{"interface-name": "Vlan100", "vlan-id": 100}]},
    "subinterface": {
        "interface": [{"interface-name": "xe-0/0/0.100", "parent-interface": "xe-0/0/0", "dot1q-vlan": 100}]
    },
    "interface_ip": {
        "interface": [
            {
                "interface-name": "Loopback0",
                "address": [{"address": "198.18.0.1/32", "family": "ipv4"}],
            }
        ]
    },
    "isis": {"process": [{"process-tag": "CORE"}]},
    "vlan": {"vlan": [{"vlan-id": 100, "name": "TEST"}]},
    "switchport": {"interface": [{"interface-name": "xe-0/0/0", "mode": "access"}]},
    "bfd": {"interface": [{"interface-name": "ae1", "enabled": True}]},
    "l2_service": {
        "service": [
            {
                "service-name": "test-vpls",
                "service-type": "vpls",
                "sap": [{"sap-id": "1/1/1:100", "port": "1/1/1", "outer-tag": 100}],
            }
        ]
    },
    "static_route": {"route": [{"prefix": "198.18.20.0/24", "next-hop": "198.18.0.2"}]},
    "interface_mtu": {"interface": [{"interface-name": "xe-0/0/0", "mtu": 1500}]},
    "lag_config": {"lag": [{"name": "ae1", "lag-id": 1}]},
    "ospf": {"instance": [{"process-id": "1", "vrf": ""}]},
    "route_policy": {"prefix-list": [{"name": "TEST-PREFIXES", "family": 4}]},
}


def _materializer_cases() -> Iterator[tuple[FamilySpec, str, dict]]:
    specs = _family_specs()
    assert tuple(spec.name for spec in specs) == ENGINE_FAMILY_KEYS
    assert set(_PRESENT_DATA) == set(ENGINE_FAMILY_KEYS)
    for spec in specs:
        yield spec, "clear", {}
        yield spec, "present", _PRESENT_DATA[spec.name]


async def test_all_family_materializers_leave_commit_to_the_engine(adapter_client):
    """The 17 canonical specs stage both clear and Present paths without committing."""
    device_id = await seed_device(nso_device_name="transaction-neutral-materializers")
    async with _device_session(device_id) as (db, _device):
        for spec, payload_kind, data in _materializer_cases():
            device = await db.get(Device, device_id)
            assert device is not None
            proxy = _CommitForbiddenSession(db, spec.name, payload_kind)
            await spec.materialize(proxy, device, spec.extract(data), "test")
            await db.rollback()


async def test_projected_fanout_keeps_an_earlier_family_durable_after_later_failure(adapter_client):
    """A later materializer failure cannot roll back an earlier projected family."""
    device_id = await seed_device(nso_device_name="projected-family-commits")
    nso_client = AsyncMock(spec=NsoClient)
    nso_client.get_device_state_doc.return_value = {
        "device-name": "projected-family-commits",
        "static-route": {
            "status": "ok",
            "route": [{"vrf": "", "prefix": "198.18.30.0/24", "next-hop": "198.18.0.1"}],
        },
        "isis-interface": {"status": "ok", "process": 42},
        "bgp-config": {"status": "error", "error-reason": "test failure"},
        "ospf-config": {"status": "ok"},
        "route-policy": {"status": "ok"},
        "snmp-config": {"status": "ok"},
        "logging-config": {"status": "ok"},
        "bfd-config": {"status": "ok"},
        "interface-ip": {"status": "ok"},
    }

    async with _device_session(device_id) as (db, device):
        failed = await refresh_routing_surfaces_for_device(
            db,
            device,
            nso_client,
            refresh_source="sync",
        )

    assert "isis" in failed
    assert await _fresh_prefixes(device_id) == ["198.18.30.0/24"]
    async for fresh_db in get_session():
        static_outcome = await outcome_store.get_current_outcome(fresh_db, device_id, "static_route")
        isis_outcome = await outcome_store.get_current_outcome(fresh_db, device_id, "isis")
        assert static_outcome is not None
        assert (static_outcome.result, static_outcome.succeeded) == ("replaced", True)
        assert isis_outcome is not None
        assert (isis_outcome.result, isis_outcome.succeeded) == ("error", False)
        break
