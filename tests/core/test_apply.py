# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/apply.py — enqueue_apply and run_apply."""

from __future__ import annotations

from contextlib import ExitStack
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from nso_adapter.core.apply import _nokia_routed_kind, enqueue_apply, run_apply
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.db import get_session
from nso_adapter.store.models import (
    DbInterface,
    Device,
    DeviceSettings,
    InterfaceAttrState,
    InterfaceIntent,
    Job,
    JobStatus,
    JobType,
    SyncState,
)

# ── _nokia_routed_kind (pure: derives SR OS router context from kind/service/vrf) ──


def _iface(kind, service="", vrf=""):
    return SimpleNamespace(kind=kind, service=service, vrf=vrf)


def test_nokia_routed_kind_none_for_non_routed_interfaces():
    assert _nokia_routed_kind(_iface("physical")) is None
    assert _nokia_routed_kind(_iface("lag")) is None


def test_nokia_routed_kind_base_when_no_service():
    assert _nokia_routed_kind(_iface("loopback")) == "base"
    assert _nokia_routed_kind(_iface("logical")) == "base"


def test_nokia_routed_kind_vprn_when_vrf_equals_service():
    assert _nokia_routed_kind(_iface("logical", service="VPRN-A", vrf="VPRN-A")) == "vprn"


def test_nokia_routed_kind_ies_when_service_global_table_or_mismatched_vrf():
    assert _nokia_routed_kind(_iface("logical", service="IES-1", vrf="")) == "ies"
    assert _nokia_routed_kind(_iface("logical", service="SVC", vrf="other")) == "ies"


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _seed_device(name: str = "test-rtr", netbox_id: int = 1) -> int:
    async for db in get_session():
        d = Device(nso_instance="nso-dev", nso_device_name=name, netbox_device_id=netbox_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id
    raise RuntimeError("no session")


async def _seed_apply_job(device_id: int, status: JobStatus = JobStatus.queued) -> int:
    async for db in get_session():
        j = Job(job_type=JobType.apply, device_id=device_id, status=status)
        db.add(j)
        await db.commit()
        await db.refresh(j)
        return j.id
    raise RuntimeError("no session")


async def _seed_interface_with_intent(
    device_id: int,
    iface_name: str,
    attribute: str,
    intent_value: str,
    sync_state: SyncState,
    netbox_id: int = 100,
) -> tuple[int, int]:
    """Create DbInterface + InterfaceAttrState + InterfaceIntent, return (iface_id, attr_id)."""
    async for db in get_session():
        iface = DbInterface(
            device_id=device_id,
            netbox_interface_id=netbox_id,
            name=iface_name,
        )
        db.add(iface)
        await db.flush()

        attr_state = InterfaceAttrState(
            interface_id=iface.id,
            attribute=attribute,
            sync_state=sync_state,
        )
        db.add(attr_state)

        intent = InterfaceIntent(
            interface_id=iface.id,
            attribute=attribute,
            intent_value=intent_value,
            accepted_at=datetime.utcnow(),
        )
        db.add(intent)
        await db.commit()
        await db.refresh(iface)
        await db.refresh(attr_state)
        return iface.id, attr_state.id
    raise RuntimeError("no session")


# ── enqueue_apply ─────────────────────────────────────────────────────────────


async def test_enqueue_apply_creates_job(adapter_client):
    """enqueue_apply creates an apply job when no active job exists."""
    device_id = await _seed_device("rtr-a01", 101)
    async for db in get_session():
        job = await enqueue_apply(db, device_id=device_id)
        assert job is not None
        assert job.job_type == JobType.apply
        assert job.status == JobStatus.queued
        break


async def test_enqueue_apply_blocked_by_active_job(adapter_client):
    """enqueue_apply returns None when an active job exists."""
    device_id = await _seed_device("rtr-a02", 102)
    await _seed_apply_job(device_id, JobStatus.running)

    async for db in get_session():
        result = await enqueue_apply(db, device_id=device_id)
        assert result is None
        break


# ── run_apply ─────────────────────────────────────────────────────────────────


async def test_run_apply_job_not_found(adapter_client):
    """run_apply exits early when job_id doesn't exist in DB."""
    device_id = await _seed_device("rtr-a10", 110)
    # Should not raise — just log and return
    await run_apply(job_id=99999, device_id=device_id)


async def test_run_apply_device_not_found(adapter_client):
    """run_apply marks job failed when device_id doesn't exist."""
    device_id = await _seed_device("rtr-a11", 111)
    job_id = await _seed_apply_job(device_id)

    with patch("nso_adapter.core.importer.get_nso_client", side_effect=KeyError("nso-dev")):
        await run_apply(job_id=job_id, device_id=99998)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        break


async def test_run_apply_nothing_eligible(adapter_client):
    """run_apply marks job succeeded when no interfaces are eligible."""
    device_id = await _seed_device("rtr-a12", 112)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result == {
            "attribute_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "ip_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "snmp_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "static_route_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "subinterface_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "vlan_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "bfd_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "interface_mtu_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "l2_sap_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "isis_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "bgp_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "route_policy_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
            "ospf_count_by_outcome": {"in_sync": 0, "apply_failed": 0},
        }
        break


async def _set_sync_before_apply(device_id: int, value: bool) -> None:
    async for db in get_session():
        db.add(DeviceSettings(device_id=device_id, auto_apply=False, sync_before_apply=value))
        await db.commit()
        return
    raise RuntimeError("no session")


async def test_run_apply_syncs_from_device_by_default(adapter_client):
    """With no DeviceSettings (or sync_before_apply on), run_apply sync-froms the device
    before pushing intent — clears the out-of-sync a prior timed-out commit can leave."""
    device_id = await _seed_device("rtr-sync-on", 130)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_client.sync_from.assert_awaited_once_with("rtr-sync-on")


async def test_run_apply_skips_sync_from_when_disabled(adapter_client):
    """sync_before_apply=False (per-device toggle) skips the pre-apply sync-from — for
    NEDs that already sync on connect."""
    device_id = await _seed_device("rtr-sync-off", 131)
    await _set_sync_before_apply(device_id, False)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_client.sync_from.assert_not_called()


async def test_run_apply_survives_sync_from_failure(adapter_client):
    """A failing pre-apply sync-from is best-effort — it must not fail the apply."""
    device_id = await _seed_device("rtr-sync-err", 132)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    mock_client.sync_from.side_effect = RuntimeError("transport timeout")
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # nothing eligible, sync error swallowed
        break


async def test_collect_apply_diff_returns_scope_deltas(adapter_client):
    """collect_apply_diff dry-runs each scope's intent and returns the native device delta."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await _seed_device("rtr-diff", 199)
    async for db in get_session():
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.utcnow())
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_ospf_config", new_callable=AsyncMock, return_value="OSPF NATIVE DELTA"),
    ):
        async for db in get_session():
            diffs = await collect_apply_diff(db, device_id)
            break
    assert diffs == {"ospf": "OSPF NATIVE DELTA"}


async def test_collect_apply_diff_empty_scope_omitted(adapter_client):
    """A scope whose dry-run shows no change (empty delta) is omitted from the result."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await _seed_device("rtr-diff2", 198)
    async for db in get_session():
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.utcnow())
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_ospf_config", new_callable=AsyncMock, return_value="   "),
    ):
        async for db in get_session():
            diffs = await collect_apply_diff(db, device_id)
            break
    assert diffs == {}


async def test_collect_apply_diff_covers_multiple_scopes(adapter_client):
    """collect_apply_diff dry-runs every scope with accepted intent, keyed by scope name."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent, StaticRouteIntent

    device_id = await _seed_device("rtr-diff3", 197)
    async for db in get_session():
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.utcnow())
        )
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="10.0.0.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.utcnow(),
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_ospf_config", new_callable=AsyncMock, return_value="OSPF DELTA"),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock, return_value="STATIC DELTA"),
    ):
        async for db in get_session():
            diffs = await collect_apply_diff(db, device_id)
            break
    assert diffs == {"ospf": "OSPF DELTA", "static_route": "STATIC DELTA"}


async def test_collect_apply_diff_device_not_found(adapter_client):
    """A dry-run preview for an unknown device returns an empty mapping (no NSO call)."""
    from nso_adapter.core.apply import collect_apply_diff

    async for db in get_session():
        diffs = await collect_apply_diff(db, 999999)
        break
    assert diffs == {}


async def test_collect_apply_diff_covers_every_scope(adapter_client):
    """Every scope with accepted intent gets dry-run and keyed by scope name.

    Locks the per-scope wiring in collect_apply_diff: interface attrs/IPs, OSPF, IS-IS,
    BGP, route-policy, SNMP, static routes, logging, SVI, subinterface, VLAN, BFD, MTU
    and L2 SAP each produce their own delta.
    """
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store import models as m

    device_id = await _seed_device("rtr-diff-all", 196)
    async for db in get_session():
        dev = await db.get(Device, device_id)
        dev.ned_id = "cisco-ios-cli-6.95"
        iface = DbInterface(device_id=device_id, name="GigabitEthernet0/0", netbox_interface_id=900)
        db.add(iface)
        await db.flush()
        db.add(
            m.InterfaceIntent(
                interface_id=iface.id, attribute="description", intent_value="uplink", accepted_at=datetime.utcnow()
            )
        )
        db.add(
            m.InterfaceIpIntent(
                interface_id=iface.id, address="10.0.0.1/24", family="ipv4", accepted_at=datetime.utcnow()
            )
        )
        db.add(
            m.OspfInstanceIntent(
                device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.utcnow()
            )
        )
        db.add(
            m.IsisInterfaceIntent(device_id=device_id, interface_name="Gi0/0", af="ipv4", accepted_at=datetime.utcnow())
        )
        db.add(m.BgpRouterIntent(device_id=device_id, asn="65000", accepted_at=datetime.utcnow()))
        db.add(
            m.RoutePolicyObjectIntent(
                device_id=device_id, family="ipv4", name="RM", entries=[], accepted_at=datetime.utcnow()
            )
        )
        db.add(
            m.SnmpCommunityIntent(
                device_id=device_id, label="ro", vault_ref="vault://ro", access="ro", accepted_at=datetime.utcnow()
            )
        )
        db.add(
            m.StaticRouteIntent(
                device_id=device_id, prefix="10.1.0.0/24", next_hop="10.1.0.1", accepted_at=datetime.utcnow()
            )
        )
        db.add(m.LoggingHostIntent(device_id=device_id, address="10.0.0.99", accepted_at=datetime.utcnow()))
        db.add(m.SviIntent(device_id=device_id, interface_name="Vlan10", vlan_id=10, accepted_at=datetime.utcnow()))
        db.add(m.SubinterfaceIntent(device_id=device_id, interface_name="Gi0/0.10", accepted_at=datetime.utcnow()))
        db.add(m.VlanIntent(device_id=device_id, vlan_id=20, accepted_at=datetime.utcnow()))
        db.add(m.BfdIntent(device_id=device_id, interface_name="Gi0/1", accepted_at=datetime.utcnow()))
        db.add(
            m.InterfaceMtuIntent(device_id=device_id, interface_name="Gi0/2", mtu=9000, accepted_at=datetime.utcnow())
        )
        db.add(
            m.L2SapIntent(
                device_id=device_id,
                service_name="EPIPE-1",
                service_type="epipe",
                sap_id="1/1/1",
                accepted_at=datetime.utcnow(),
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    # Each dry-run returns a distinct, non-empty native delta keyed off its scope.
    patches = {
        "apply_interface_attribute": "ATTR",
        "apply_interface_ips": "IP",
        "apply_ospf_config": "OSPF",
        "apply_isis_interfaces": "ISIS",
        "apply_bgp_config": "BGP",
        "apply_route_policy_config": "RP",
        "apply_snmp_config": "SNMP",
        "apply_static_routes": "SR",
        "apply_logging_config": "LOG",
        "apply_svi_config": "SVI",
        "apply_subinterface_config": "SUBIF",
        "apply_vlan_config": "VLAN",
        "apply_bfd_config": "BFD",
        "apply_mtu_config": "MTU",
        "apply_l2_saps": "L2",
    }
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        with ExitStack() as stack:
            for fn, delta in patches.items():
                stack.enter_context(patch(f"nso_adapter.nso.apply.{fn}", new_callable=AsyncMock, return_value=delta))
            async for db in get_session():
                diffs = await collect_apply_diff(db, device_id)
                break

    assert diffs == {
        "interface_attribute": "ATTR",
        "interface_ip": "IP",
        "ospf": "OSPF",
        "isis": "ISIS",
        "bgp": "BGP",
        "route_policy": "RP",
        "snmp": "SNMP",
        "static_route": "SR",
        "logging": "LOG",
        "svi": "SVI",
        "subinterface": "SUBIF",
        "vlan": "VLAN",
        "bfd": "BFD",
        "interface_mtu": "MTU",
        "l2_sap": "L2",
    }


async def test_collect_apply_diff_scope_failure_is_isolated(adapter_client):
    """A scope whose dry-run raises is logged and skipped; other scopes still report."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent, StaticRouteIntent

    device_id = await _seed_device("rtr-diff-iso", 195)
    async for db in get_session():
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.utcnow())
        )
        db.add(
            StaticRouteIntent(
                device_id=device_id, prefix="10.2.0.0/24", next_hop="10.2.0.1", accepted_at=datetime.utcnow()
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(
            "nso_adapter.nso.apply.apply_ospf_config", new_callable=AsyncMock, side_effect=RuntimeError("dry-run boom")
        ),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock, return_value="STATIC DELTA"),
    ):
        async for db in get_session():
            diffs = await collect_apply_diff(db, device_id)
            break
    # ospf raised → omitted; static_route still present
    assert diffs == {"static_route": "STATIC DELTA"}


async def test_collect_apply_diff_interface_scope_failures_and_skips(adapter_client):
    """The interface attr/IP previews skip non-eligible attrs and swallow per-slice dry-run errors."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-diff-ifaceerr", 194)
    async for db in get_session():
        iface = DbInterface(device_id=device_id, name="GigabitEthernet0/0", netbox_interface_id=910)
        db.add(iface)
        await db.flush()
        # eligible description slice — its dry-run will be made to raise
        db.add(
            InterfaceIntent(
                interface_id=iface.id, attribute="description", intent_value="up", accepted_at=datetime.utcnow()
            )
        )
        # non-eligible attribute (skipped before any dry-run)
        db.add(
            InterfaceIntent(interface_id=iface.id, attribute="mtu", intent_value="9000", accepted_at=datetime.utcnow())
        )
        # eligible attribute but not accepted (also skipped)
        db.add(InterfaceIntent(interface_id=iface.id, attribute="enabled", intent_value="true"))
        # IP intent whose dry-run will be made to raise
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id, address="10.0.0.1/24", family="ipv4", accepted_at=datetime.utcnow()
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(
            "nso_adapter.nso.apply.apply_interface_attribute",
            new_callable=AsyncMock,
            side_effect=RuntimeError("attr dry-run boom"),
        ),
        patch(
            "nso_adapter.nso.apply.apply_interface_ips",
            new_callable=AsyncMock,
            side_effect=RuntimeError("ip dry-run boom"),
        ),
    ):
        async for db in get_session():
            diffs = await collect_apply_diff(db, device_id)
            break

    # both interface scopes failed → omitted; preview never raised
    assert diffs == {}


async def test_collect_apply_diff_interface_in_sync_yields_no_entry(adapter_client):
    """An interface already in sync (empty dry-run delta) contributes no preview entry."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-diff-insync", 193)
    async for db in get_session():
        iface = DbInterface(device_id=device_id, name="GigabitEthernet0/0", netbox_interface_id=911)
        db.add(iface)
        await db.flush()
        db.add(
            InterfaceIntent(
                interface_id=iface.id, attribute="description", intent_value="up", accepted_at=datetime.utcnow()
            )
        )
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id, address="10.0.0.1/24", family="ipv4", accepted_at=datetime.utcnow()
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_interface_attribute", new_callable=AsyncMock, return_value=""),
        patch("nso_adapter.nso.apply.apply_interface_ips", new_callable=AsyncMock, return_value=None),
    ):
        async for db in get_session():
            diffs = await collect_apply_diff(db, device_id)
            break

    assert diffs == {}


async def test_run_apply_all_succeed(adapter_client):
    """run_apply marks job succeeded when all attributes apply successfully."""
    device_id = await _seed_device("rtr-a13", 113)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/0",
        attribute="description",
        intent_value="uplink",
        sync_state=SyncState.accepted,
        netbox_id=200,
    )

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_interface_attribute", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["attribute_count_by_outcome"]["in_sync"] == 1
        assert job.result["attribute_count_by_outcome"]["apply_failed"] == 0
        break


async def test_run_apply_partial_failure(adapter_client):
    """run_apply marks job failed when some attributes fail to apply."""
    from nso_adapter.nso.apply import NsoApplyError

    device_id = await _seed_device("rtr-a14", 114)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/1",
        attribute="description",
        intent_value="downlink",
        sync_state=SyncState.accepted,
        netbox_id=201,
    )

    mock_client = AsyncMock()
    nso_err = NsoApplyError(code="nso_error", message="NSO rejected commit", detail={})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_interface_attribute", new_callable=AsyncMock, side_effect=nso_err),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "nso_commit_failed"
        assert job.result["attribute_count_by_outcome"]["apply_failed"] == 1
        break


async def test_run_apply_unexpected_exception_on_attribute(adapter_client):
    """run_apply handles unexpected (non-NsoApplyError) exceptions per-attribute."""
    device_id = await _seed_device("rtr-a15", 115)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/2",
        attribute="description",
        intent_value="mgmt",
        sync_state=SyncState.drifted,
        netbox_id=202,
    )

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(
            "nso_adapter.nso.apply.apply_interface_attribute",
            new_callable=AsyncMock,
            side_effect=RuntimeError("unexpected internal error"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert (
            "unexpected internal error" in str(job.result)
            or job.result["attribute_count_by_outcome"]["apply_failed"] == 1
        )
        break


async def test_run_apply_no_force_filters_eligible(adapter_client):
    """run_apply with force=False only applies accepted/apply_failed/drifted, not in_sync."""
    device_id = await _seed_device("rtr-a16", 116)
    job_id = await _seed_apply_job(device_id)
    # in_sync is NOT eligible when force=False
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/3",
        attribute="description",
        intent_value="in-sync-iface",
        sync_state=SyncState.in_sync,
        netbox_id=203,
    )

    mock_client = AsyncMock()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=False)

    async for db in get_session():
        job = await db.get(Job, job_id)
        # in_sync is not in _NO_FORCE_ELIGIBLE, so nothing was applied
        assert job.status == JobStatus.succeeded
        assert job.result["attribute_count_by_outcome"]["in_sync"] == 0
        break


async def test_run_apply_outer_exception(adapter_client):
    """run_apply marks job failed on an outer unexpected exception."""
    device_id = await _seed_device("rtr-a17", 117)
    job_id = await _seed_apply_job(device_id)

    with patch("nso_adapter.core.importer.get_nso_client", side_effect=RuntimeError("DB boom")):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "internal"
        break


# ── IP intent apply pass ───────────────────────────────────────────────────


async def _seed_iface(device_id: int, iface_name: str) -> int:
    """Create a bare DbInterface row and return its id."""
    async for db in get_session():
        iface = DbInterface(device_id=device_id, name=iface_name)
        db.add(iface)
        await db.flush()
        iface_id = iface.id
        await db.commit()
        return iface_id
    raise RuntimeError("unreachable")


async def _seed_ip_intent(
    interface_id: int,
    *,
    address: str,
    family: str = "ipv4",
    secondary: bool = False,
    vrf: str = "",
    accepted: bool = True,
) -> None:
    """Seed an InterfaceIpIntent row."""
    from datetime import UTC, datetime

    from nso_adapter.store.models import InterfaceIpIntent

    async for db in get_session():
        row = InterfaceIpIntent(
            interface_id=interface_id,
            address=address,
            vrf=vrf,
            family=family,
            secondary=secondary,
            accepted_at=datetime.now(UTC).replace(tzinfo=None) if accepted else None,
        )
        db.add(row)
        await db.commit()
        break


@pytest.mark.anyio
async def test_run_apply_ip_intent_success(adapter_client):
    """IP intent rows are applied and last_apply_at is set on success."""
    from sqlalchemy import select

    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-ip-01", 201)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/1")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.0.0.1/24", family="ipv4")

    mock_nso = AsyncMock(spec=NsoClient)  # opaque token: apply_interface_ips is patched below
    mock_nso._base = "http://fake-nso"
    mock_nso._action_timeout = 30

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_nso),
        patch("nso_adapter.nso.apply.apply_interface_ips", new_callable=AsyncMock) as mock_ip_apply,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        result = job.result
        assert result["ip_count_by_outcome"]["in_sync"] == 1
        assert result["ip_count_by_outcome"]["apply_failed"] == 0
        # Verify last_apply_at was stamped
        ip_rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        assert ip_rows[0].last_apply_at is not None
        assert ip_rows[0].last_apply_error is None
        break

    mock_ip_apply.assert_awaited_once()


@pytest.mark.anyio
async def test_run_apply_ip_intent_failure_marks_error(adapter_client):
    """When apply_interface_ips raises NsoApplyError, last_apply_error is stored."""
    from sqlalchemy import select

    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-ip-02", 202)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/2")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.0.1.1/30", family="ipv4")

    mock_nso = AsyncMock()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_nso),
        patch(
            "nso_adapter.nso.apply.apply_interface_ips",
            new_callable=AsyncMock,
            side_effect=NsoApplyError("nso_patch_failed", "NSO returned 500"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["ip_count_by_outcome"]["apply_failed"] == 1
        ip_rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        assert ip_rows[0].last_apply_error is not None
        assert ip_rows[0].last_apply_error["code"] == "nso_patch_failed"
        break


@pytest.mark.anyio
async def test_run_apply_ip_intent_not_accepted_skipped(adapter_client):
    """IP rows without accepted_at are not eligible and not applied."""
    device_id = await _seed_device("rtr-ip-03", 203)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/3")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.0.2.1/24", family="ipv4", accepted=False)

    mock_nso = AsyncMock()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_nso),
        patch("nso_adapter.nso.apply.apply_interface_ips", new_callable=AsyncMock) as mock_ip_apply,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_ip_apply.assert_not_awaited()


@pytest.mark.anyio
async def test_run_apply_ip_already_applied_skipped_without_force(adapter_client):
    """IP rows with last_apply_at set and no error are skipped when force=False."""
    from datetime import UTC, datetime

    from sqlalchemy import select

    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-ip-04", 204)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/4")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.0.3.1/24", family="ipv4", accepted=True)

    # Stamp last_apply_at to simulate already-applied
    async for db in get_session():
        rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        rows[0].last_apply_at = datetime.now(UTC).replace(tzinfo=None)
        rows[0].last_apply_error = None
        await db.commit()
        break

    mock_nso = AsyncMock()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_nso),
        patch("nso_adapter.nso.apply.apply_interface_ips", new_callable=AsyncMock) as mock_ip_apply,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=False)

    mock_ip_apply.assert_not_awaited()


async def test_run_apply_bgp_intent_does_not_crash_on_commit(adapter_client):
    """Regression: a dirty BgpRouterIntent must not crash the apply commit.

    The apply manually eager-loads BGP relationships (scopes/peers/afs). It used to write
    raw Python lists into __dict__, which bypasses SQLAlchemy instrumentation — so once the
    row was marked applied (dirty) the commit flush hit
    'list object has no attribute _sa_adapter' and aborted the ENTIRE job. set_committed_value
    instruments the collection, so flush sees committed (empty-history) state.
    """

    from nso_adapter.store.models import BgpRouterIntent

    device_id = await _seed_device("rtr-bgp-crash", 555)
    job_id = await _seed_apply_job(device_id)
    async for db in get_session():
        db.add(BgpRouterIntent(device_id=device_id, asn="65100", accepted_at=datetime.now(UTC)))
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_bgp_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)  # must not raise

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["bgp_count_by_outcome"]["in_sync"] == 1
        break


# ── Per-scope apply passes (SNMP / static-route / logging / SVI / subif / VLAN /
#    BFD / MTU / L2-SAP / IS-IS / route-policy / OSPF) ─────────────────────────
#
# These passes share one shape: collect accepted rows → call the scope's
# nso.apply function → on success stamp last_apply_at and report in_sync; on
# NsoApplyError/other stamp last_apply_error and report apply_failed. The
# parametrized success test locks the wiring (right apply fn, right result key,
# rows stamped) for every single-list scope; dedicated tests below cover the
# multi-list scopes and the failure paths.


# (model_name, row kwargs, patched nso.apply fn, result-dict key)
_SCOPE_CASES = [
    ("StaticRouteIntent", dict(prefix="10.9.0.0/24", next_hop="10.9.0.1"), "apply_static_routes", "static_route"),
    ("LoggingHostIntent", dict(address="10.9.0.99"), "apply_logging_config", "logging"),
    ("SviIntent", dict(interface_name="Vlan10", vlan_id=10), "apply_svi_config", "svi"),
    ("SubinterfaceIntent", dict(interface_name="GigabitEthernet0/0.10"), "apply_subinterface_config", "subinterface"),
    ("VlanIntent", dict(vlan_id=20), "apply_vlan_config", "vlan"),
    ("BfdIntent", dict(interface_name="GigabitEthernet0/1"), "apply_bfd_config", "bfd"),
    ("InterfaceMtuIntent", dict(interface_name="GigabitEthernet0/2", mtu=9000), "apply_mtu_config", "interface_mtu"),
    (
        "L2SapIntent",
        dict(service_name="EPIPE-1", service_type="epipe", sap_id="1/1/1"),
        "apply_l2_saps",
        "l2_sap",
    ),
    (
        "SnmpCommunityIntent",
        dict(label="ro", vault_ref="vault://snmp/ro", access="ro"),
        "apply_snmp_config",
        "snmp",
    ),
    ("OspfInstanceIntent", dict(process_id="1", router_id="9.9.9.9"), "apply_ospf_config", "ospf"),
    ("IsisInterfaceIntent", dict(interface_name="GigabitEthernet0/3", af="ipv4"), "apply_isis_interfaces", "isis"),
]


@pytest.mark.parametrize("model_name, kwargs, apply_fn, result_key", _SCOPE_CASES)
async def test_run_apply_scope_success(adapter_client, model_name, kwargs, apply_fn, result_key):
    """Each single-list scope applies its accepted rows and reports them in_sync."""
    from nso_adapter.store import models as m

    device_id = await _seed_device(f"rtr-{result_key}", 300)
    job_id = await _seed_apply_job(device_id)
    model = getattr(m, model_name)
    async for db in get_session():
        db.add(model(device_id=device_id, accepted_at=datetime.utcnow(), **kwargs))
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(f"nso_adapter.nso.apply.{apply_fn}", new_callable=AsyncMock) as mock_apply,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_apply.assert_awaited_once()
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result[f"{result_key}_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        # the row was stamped applied
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_at is not None
        assert rows[0].last_apply_error is None
        break


async def test_run_apply_scope_failure_marks_error(adapter_client):
    """A scope NsoApplyError fails the job, stamps last_apply_error, and tags the item."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-sr-fail", 310)
    job_id = await _seed_apply_job(device_id)
    async for db in get_session():
        db.add(
            StaticRouteIntent(
                device_id=device_id, prefix="10.8.0.0/24", next_hop="10.8.0.1", accepted_at=datetime.utcnow()
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    nso_err = NsoApplyError(code="nso_error", message="route rejected", detail={"x": 1})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock, side_effect=nso_err),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        assert job.error["code"] == "nso_commit_failed"
        items = job.error["detail"]["items"]
        assert {"type": "static_route", "error": "route rejected"} in items
        rows = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        assert rows[0].last_apply_error == {"code": "nso_error", "message": "route rejected", "detail": {"x": 1}}
        break


async def test_run_apply_scope_unexpected_exception(adapter_client):
    """A non-NsoApplyError from a scope is caught, recorded as 'internal', job failed."""
    from nso_adapter.store.models import VlanIntent

    device_id = await _seed_device("rtr-vlan-boom", 311)
    job_id = await _seed_apply_job(device_id)
    async for db in get_session():
        db.add(VlanIntent(device_id=device_id, vlan_id=42, accepted_at=datetime.utcnow()))
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(
            "nso_adapter.nso.apply.apply_vlan_config",
            new_callable=AsyncMock,
            side_effect=RuntimeError("kaboom"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["vlan_count_by_outcome"]["apply_failed"] == 1
        rows = (await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_error["code"] == "internal"
        assert "kaboom" in rows[0].last_apply_error["message"]
        break


async def test_run_apply_isis_applies_process_redist_and_flexalgo(adapter_client):
    """The IS-IS pass stamps interface + process + redistribute + flex-algo rows together."""
    from nso_adapter.store.models import (
        IsisFlexAlgoIntent,
        IsisInterfaceIntent,
        IsisProcessIntent,
        RedistributionIntent,
    )

    device_id = await _seed_device("rtr-isis-combo", 320)
    job_id = await _seed_apply_job(device_id)
    async for db in get_session():
        db.add(
            IsisInterfaceIntent(device_id=device_id, interface_name="Gi0/3", af="ipv4", accepted_at=datetime.utcnow())
        )
        db.add(IsisProcessIntent(device_id=device_id, accepted_at=datetime.utcnow()))
        db.add(IsisFlexAlgoIntent(device_id=device_id, algo_id=128, accepted_at=datetime.utcnow()))
        db.add(
            RedistributionIntent(
                device_id=device_id,
                dest_protocol="isis",
                source_protocol="connected",
                accepted_at=datetime.utcnow(),
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_isis_interfaces", new_callable=AsyncMock) as mock_isis,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_isis.assert_awaited_once()
    call = mock_isis.await_args.kwargs
    assert len(call["isis_intent_rows"]) == 1
    assert len(call["isis_process_rows"]) == 1
    assert len(call["redistribution_rows"]) == 1
    assert len(call["flex_algo_rows"]) == 1
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        # in_sync counts every row across the four lists
        assert job.result["isis_count_by_outcome"] == {"in_sync": 4, "apply_failed": 0}
        break


async def test_run_apply_ospf_applies_instance_interface_and_redist(adapter_client):
    """The OSPF pass applies process + interface + ospf-destined redistribution together."""
    from nso_adapter.store.models import OspfInstanceIntent, OspfInterfaceIntent, RedistributionIntent

    device_id = await _seed_device("rtr-ospf-combo", 321)
    job_id = await _seed_apply_job(device_id)
    async for db in get_session():
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.utcnow())
        )
        db.add(OspfInterfaceIntent(device_id=device_id, interface_name="Gi0/4", accepted_at=datetime.utcnow()))
        db.add(
            RedistributionIntent(
                device_id=device_id,
                dest_protocol="ospf",
                source_protocol="static",
                accepted_at=datetime.utcnow(),
            )
        )
        # a bgp-destined redist row must NOT be swept into the ospf pass
        db.add(
            RedistributionIntent(
                device_id=device_id, dest_protocol="bgp", source_protocol="static", accepted_at=datetime.utcnow()
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_ospf_config", new_callable=AsyncMock) as mock_ospf,
        patch("nso_adapter.nso.apply.apply_bgp_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_ospf.assert_awaited_once()
    call = mock_ospf.await_args.kwargs
    assert len(call["process_intent_rows"]) == 1
    assert len(call["interface_intent_rows"]) == 1
    assert len(call["redistribution_rows"]) == 1  # only the ospf-destined row
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.result["ospf_count_by_outcome"] == {"in_sync": 3, "apply_failed": 0}
        break


async def test_run_apply_snmp_applies_all_row_types(adapter_client):
    """The SNMP pass stamps communities, v3 users, hosts and the single system-info row."""
    from nso_adapter.store.models import (
        SnmpCommunityIntent,
        SnmpHostIntent,
        SnmpSystemInfoIntent,
        SnmpV3UserIntent,
    )

    device_id = await _seed_device("rtr-snmp-all", 322)
    job_id = await _seed_apply_job(device_id)
    async for db in get_session():
        db.add(
            SnmpCommunityIntent(
                device_id=device_id, label="ro", vault_ref="vault://ro", access="ro", accepted_at=datetime.utcnow()
            )
        )
        db.add(SnmpV3UserIntent(device_id=device_id, username="netops", accepted_at=datetime.utcnow()))
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="10.7.0.5",
                version="v2c",
                notify_type="traps",
                community_or_user="ro",
                accepted_at=datetime.utcnow(),
            )
        )
        db.add(SnmpSystemInfoIntent(device_id=device_id, location="rack-7", accepted_at=datetime.utcnow()))
        await db.commit()
        break

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_snmp_config", new_callable=AsyncMock) as mock_snmp,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_snmp.assert_awaited_once()
    call = mock_snmp.await_args.kwargs
    assert len(call["community_intents"]) == 1
    assert len(call["v3_user_intents"]) == 1
    assert len(call["host_intents"]) == 1
    assert call["system_info_intent"] is not None
    async for db in get_session():
        job = await db.get(Job, job_id)
        # 3 list rows + 1 system-info row
        assert job.result["snmp_count_by_outcome"] == {"in_sync": 4, "apply_failed": 0}
        break


async def test_run_apply_route_policy_failure_records_capability(adapter_client):
    """A route-policy NsoApplyError fails the job AND records a capability rejection.

    The device parser only rejects an unsupported construct on a real commit (dry-run
    renders it), so the accepted-half learns the (ned, sw) limitation here.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_device("rtr-rp-fail", 323)
    # give the device a ned_id so apply_route_policy_config gets one
    async for db in get_session():
        dev = await db.get(Device, device_id)
        dev.ned_id = "cisco-ios-cli-6.95"
        await db.commit()
        break
    job_id = await _seed_apply_job(device_id)
    async for db in get_session():
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="ipv4",
                name="RM-IN",
                entries=[],
                accepted_at=datetime.utcnow(),
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    nso_err = NsoApplyError(code="nso_error", message="unsupported set community RM-IN", detail={})
    rec = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_route_policy_config", new_callable=AsyncMock, side_effect=nso_err),
        patch(
            "nso_adapter.core.capability.refresh_device_capability",
            new_callable=AsyncMock,
            return_value={"ned_id": "cisco-ios-cli-6.95", "sw_version": "15.5"},
        ),
        patch(
            "nso_adapter.core.capability.parse_rejected_construct",
            return_value=("route-policy", "RM-IN"),
        ),
        patch("nso_adapter.core.capability.record_capability_rejection", new=rec),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    rec.assert_awaited_once()
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["route_policy_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        break


async def test_run_apply_route_policy_capability_recording_is_best_effort(adapter_client):
    """If capability recording itself raises, the apply still fails cleanly (swallowed)."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_device("rtr-rp-cap-err", 324)
    job_id = await _seed_apply_job(device_id)
    async for db in get_session():
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id, family="ipv4", name="RM-X", entries=[], accepted_at=datetime.utcnow()
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    nso_err = NsoApplyError(code="nso_error", message="boom", detail={})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_route_policy_config", new_callable=AsyncMock, side_effect=nso_err),
        patch(
            "nso_adapter.core.capability.refresh_device_capability",
            new_callable=AsyncMock,
            side_effect=RuntimeError("capability backend down"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)  # must not raise

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["route_policy_count_by_outcome"]["apply_failed"] == 1
        break


async def test_run_apply_route_policy_capability_skips_record_when_unparseable(adapter_client):
    """When the rejected construct can't be parsed (no name), no capability row is recorded."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_device("rtr-rp-cap-skip", 325)
    job_id = await _seed_apply_job(device_id)
    async for db in get_session():
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id, family="ipv4", name="RM-Y", entries=[], accepted_at=datetime.utcnow()
            )
        )
        await db.commit()
        break

    mock_client = AsyncMock()
    nso_err = NsoApplyError(code="nso_error", message="opaque error", detail={})
    rec = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_route_policy_config", new_callable=AsyncMock, side_effect=nso_err),
        patch(
            "nso_adapter.core.capability.refresh_device_capability",
            new_callable=AsyncMock,
            return_value={"ned_id": "cisco-ios-cli-6.95", "sw_version": "15.5"},
        ),
        patch("nso_adapter.core.capability.parse_rejected_construct", return_value=(None, None)),
        patch("nso_adapter.core.capability.record_capability_rejection", new=rec),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    rec.assert_not_awaited()
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        break


async def test_run_apply_ip_unexpected_exception(adapter_client):
    """A non-NsoApplyError from the IP pass is recorded as 'internal' and fails the job."""
    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-ip-boom", 326)
    iface_id = await _seed_iface(device_id, "GigabitEthernet0/9")
    job_id = await _seed_apply_job(device_id)
    await _seed_ip_intent(iface_id, address="10.6.0.1/24", family="ipv4")

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(
            "nso_adapter.nso.apply.apply_interface_ips",
            new_callable=AsyncMock,
            side_effect=RuntimeError("transport exploded"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["ip_count_by_outcome"]["apply_failed"] == 1
        rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        assert rows[0].last_apply_error["code"] == "internal"
        assert "transport exploded" in rows[0].last_apply_error["message"]
        break


# ── Atomic apply (NSO_ADAPTER_ATOMIC_APPLY): subif + IP in one transaction ─────


async def _seed_subif_and_ip(device_id: int, iface_name: str = "ae99.999") -> int:
    """Seed a DbInterface + accepted SubinterfaceIntent (device-keyed) + accepted
    InterfaceIpIntent (interface-keyed) — the greenfield subif+IP pair. Returns iface_id."""
    from nso_adapter.store.models import InterfaceIpIntent, SubinterfaceIntent

    async for db in get_session():
        iface = DbInterface(device_id=device_id, netbox_interface_id=999, name=iface_name, kind="logical")
        db.add(iface)
        await db.flush()
        db.add(
            SubinterfaceIntent(
                device_id=device_id,
                interface_name=iface_name,
                parent_interface="ae99",
                dot1q_vlan=999,
                sub_type="subinterface",
                accepted_at=datetime.utcnow(),
            )
        )
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id,
                address="33.1.1.1/24",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.utcnow(),
            )
        )
        await db.commit()
        return iface.id
    raise RuntimeError("no session")


async def _ip_and_subif_rows(device_id: int):
    from nso_adapter.store.models import InterfaceIpIntent, SubinterfaceIntent

    async for db in get_session():
        subif = (
            (await db.execute(select(SubinterfaceIntent).where(SubinterfaceIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        ip = (await db.execute(select(InterfaceIpIntent))).scalars().all()
        return subif, ip
    raise RuntimeError("no session")


async def _seed_snmp_and_static_route(device_id: int) -> None:
    from nso_adapter.store.models import SnmpCommunityIntent, StaticRouteIntent

    async for db in get_session():
        db.add(
            SnmpCommunityIntent(
                device_id=device_id, label="public", vault_ref="m/p#k", access="RO", accepted_at=datetime.utcnow()
            )
        )
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="10.9.9.0/24", next_hop="1.1.1.1", accepted_at=datetime.utcnow()
            )
        )
        await db.commit()
        return
    raise RuntimeError("no session")


@pytest.mark.asyncio
async def test_run_apply_atomic_stages_subif_and_ip_in_one_commit(adapter_client, monkeypatch):
    """With NSO_ADAPTER_ATOMIC_APPLY on, subif + IP are staged (by the REAL body-builders)
    and committed via ONE apply_combined call carrying both module bodies in one transaction."""
    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    await _seed_subif_and_ip(device_id)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    combined = AsyncMock(return_value=None)
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        # Only the NSO commit boundary is mocked — the real scope body-builders run, staging
        # their bodies into the combined modules dict that apply_combined receives.
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    combined.assert_awaited_once()
    modules = combined.await_args.args[2]
    assert set(modules) == {"subinterface-reconciler:subif-config", "interface-reconciler:interface-config"}
    # The IP rides ae99.999 in the SAME edit as the subif that defines its unit.
    iface_entry = modules["interface-reconciler:interface-config"][0]
    assert iface_entry["interface-name"] == "ae99.999"
    assert iface_entry["ipv4-address"][0]["address"] == "33.1.1.1"
    assert modules["subinterface-reconciler:subif-config"][0]["interface"][0]["interface-name"] == "ae99.999"

    subif_rows, ip_rows = await _ip_and_subif_rows(device_id)
    assert all(r.last_apply_at is not None and r.last_apply_error is None for r in subif_rows + ip_rows)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        break


@pytest.mark.asyncio
async def test_run_apply_atomic_stages_all_scopes_in_one_commit(adapter_client, monkeypatch):
    """I3b: every scope (not just subif+IP) lands in ONE apply_combined transaction."""
    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    await _seed_subif_and_ip(device_id)
    await _seed_snmp_and_static_route(device_id)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    combined = AsyncMock(return_value=None)
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    combined.assert_awaited_once()
    modules = combined.await_args.args[2]
    assert {
        "subinterface-reconciler:subif-config",
        "interface-reconciler:interface-config",
        "snmp-reconciler:snmp-config",
        "static-route-reconciler:static-route-config",
    } <= set(modules)
    async for db in get_session():
        assert (await db.get(Job, job_id)).status == JobStatus.succeeded
        break


@pytest.mark.asyncio
async def test_run_apply_atomic_merges_attr_and_ip_into_one_interface_entry(adapter_client, monkeypatch):
    """Attribute + IP intent on the SAME interface merge into ONE interface-config entry
    (they share the (device, interface-name) key — two list items would conflict)."""
    from nso_adapter.store.models import InterfaceIpIntent

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    iface_id, _attr_id = await _seed_interface_with_intent(
        device_id, "Gi0/1", "description", "uplink", SyncState.accepted
    )
    async for db in get_session():
        db.add(
            InterfaceIpIntent(
                interface_id=iface_id,
                address="10.0.0.1/30",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.utcnow(),
            )
        )
        await db.commit()
        break
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    combined = AsyncMock(return_value=None)
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    entries = combined.await_args.args[2]["interface-reconciler:interface-config"]
    gi = [e for e in entries if e["interface-name"] == "Gi0/1"]
    assert len(gi) == 1  # ONE merged entry, not two
    assert gi[0]["description"] == "uplink"
    assert gi[0]["ipv4-address"][0]["address"] == "10.0.0.1"


@pytest.mark.asyncio
async def test_run_apply_atomic_failure_localizes_offender_others_pending(adapter_client, monkeypatch):
    """An atomic failure fails only the localised offender scope; non-offenders are pending
    (rolled back, untouched → retried next apply). The job fails."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import SnmpCommunityIntent, StaticRouteIntent

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    await _seed_snmp_and_static_route(device_id)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    boom = AsyncMock(side_effect=NsoApplyError("nso_patch_failed", "static route rejected"))

    async def _fake_localize(*_a, **_k):
        return {"static-route-reconciler:static-route-config"}

    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", boom))
        stack.enter_context(patch("nso_adapter.core.apply._localize_atomic_failure", _fake_localize))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async for db in get_session():
        sr = (await db.execute(select(StaticRouteIntent))).scalars().all()
        sc = (await db.execute(select(SnmpCommunityIntent))).scalars().all()
        assert sr and all(r.last_apply_error is not None for r in sr)  # offender → failed
        assert sc and all(r.last_apply_error is None and r.last_apply_at is None for r in sc)  # pending
        assert (await db.get(Job, job_id)).status == JobStatus.failed
        break


@pytest.mark.asyncio
async def test_run_apply_atomic_off_uses_per_scope(adapter_client, monkeypatch):
    """Flag off (default): apply_combined is never used; the per-scope subif + IP writers run."""
    monkeypatch.delenv("NSO_ADAPTER_ATOMIC_APPLY", raising=False)
    device_id = await _seed_device(name="sw01")
    await _seed_subif_and_ip(device_id)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    combined = AsyncMock(return_value=None)
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", combined))
        per_subif = stack.enter_context(
            patch("nso_adapter.nso.apply.apply_subinterface_config", new_callable=AsyncMock, return_value=None)
        )
        per_ip = stack.enter_context(
            patch("nso_adapter.nso.apply.apply_interface_ips", new_callable=AsyncMock, return_value=None)
        )
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    combined.assert_not_called()
    per_subif.assert_awaited_once()
    per_ip.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_apply_atomic_failure_marks_both_subif_and_ip(adapter_client, monkeypatch):
    """An atomic commit failure is all-or-nothing: every subif AND IP row records the
    error and the job fails (the whole pair rolled back together)."""
    from nso_adapter.nso.apply import NsoApplyError

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    await _seed_subif_and_ip(device_id)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    boom = AsyncMock(side_effect=NsoApplyError("nso_patch_failed", "device said no"))
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", boom))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    subif_rows, ip_rows = await _ip_and_subif_rows(device_id)
    assert all(r.last_apply_error is not None and r.last_apply_at is None for r in subif_rows + ip_rows)
    async for db in get_session():
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        break
