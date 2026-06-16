# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/apply.py — enqueue_apply and run_apply."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from nso_adapter.core.apply import _nokia_routed_kind, enqueue_apply, run_apply
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

    mock_nso = AsyncMock()
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
