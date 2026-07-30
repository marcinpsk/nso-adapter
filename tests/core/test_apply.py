# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for core/apply.py — enqueue_apply and run_apply."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from nso_adapter.core.apply import _nokia_routed_kind, enqueue_apply, run_apply
from nso_adapter.nso.client import NsoClient
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
from tests.conftest import session

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


# ── _nokia_attr_kind (attribute-write context: routed kinds + lag) ────────────────


def test_nokia_attr_kind_lag_for_a_lag_interface():
    """A Nokia LAG's description/admin-state belong on `configure lag`, so the attribute-write
    context is 'lag' — routed-kind returns None for a lag (it never carries an IP)."""
    from nso_adapter.core.apply import _nokia_attr_kind

    assert _nokia_attr_kind(_iface("lag")) == "lag"


def test_nokia_attr_kind_matches_routed_for_l3_and_ports():
    """For everything except a lag, the attribute context is the routed context: base/ies/vprn
    for L3 routed interfaces, None for a physical port (→ the legacy port path)."""
    from nso_adapter.core.apply import _nokia_attr_kind

    assert _nokia_attr_kind(_iface("loopback")) == "base"
    assert _nokia_attr_kind(_iface("logical", service="VPRN-A", vrf="VPRN-A")) == "vprn"
    assert _nokia_attr_kind(_iface("logical", service="IES-1", vrf="")) == "ies"
    assert _nokia_attr_kind(_iface("physical")) is None


# ── _apply_attributes threads the Nokia routed context (Finding C-drift) ──────────


def _capturing_nso_client() -> tuple[NsoClient, list]:
    """A spec'd NsoClient whose HTTP pool records every PATCH content. Only the RESTCONF
    boundary is faked; the real ``_apply_attributes`` → ``apply_interface_attribute`` body
    building runs unchanged. Returns (client, list-of-decoded-PATCH-bodies)."""
    import json
    from unittest.mock import MagicMock

    import httpx

    from nso_adapter.nso.client import NsoClient as _Client

    bodies: list = []

    async def _patch(url, content=None, headers=None):
        bodies.append(json.loads(content))
        return httpx.Response(204, request=httpx.Request("PATCH", url), text="")

    http = AsyncMock()
    http.patch.side_effect = _patch
    client = MagicMock(spec=_Client)
    client._base = "http://nso"
    client._action_timeout = 120.0
    cm = client._client.return_value
    cm.__aenter__.return_value = http
    cm.__aexit__.return_value = False
    return client, bodies


async def test_apply_attributes_threads_nokia_routed_context():
    """The real per-scope apply path derives base|ies|vprn for a Nokia routed interface and
    threads it into the attribute PATCH, so description/enabled land on the router/service
    interface — not a phantom ``configure port <logical-name>`` (Finding C-drift root cause)."""
    from nso_adapter.core.apply import _apply_attributes
    from nso_adapter.nso.apply import apply_interface_attribute

    iface = DbInterface(
        device_id=1,
        netbox_interface_id=7001,
        name="CRPD-VPN:LO7",
        kind="loopback",
        service="CRPD-VPN",
        vrf="CRPD-VPN",  # vrf == service ⇒ vprn
        parent_binding="lag-99",
        encap_tag="10",
    )
    attr_state = InterfaceAttrState(interface_id=1, attribute="description", sync_state=SyncState.accepted)
    intent = InterfaceIntent(interface_id=1, attribute="description", intent_value="loopback for CRPD-VPN")

    client, bodies = _capturing_nso_client()
    ok, failed, failures = await _apply_attributes(
        [(attr_state, intent, iface)],
        apply_interface_attribute,
        client=client,
        device_name="ra1",
        job_id=1,
        now=datetime.now(UTC),
    )

    assert (ok, failed) == (1, 0)
    assert attr_state.sync_state == SyncState.in_sync
    entry = bodies[0]["interface-reconciler:interface-config"][0]
    assert entry["kind"] == "vprn"
    assert entry["service"] == "CRPD-VPN"
    assert entry["parent-binding"] == "lag-99"
    assert entry["encap-tag"] == "10"
    assert entry["description"] == "loopback for CRPD-VPN"


async def test_apply_attributes_threads_lag_kind_for_a_nokia_lag():
    """A Nokia LAG's attribute PATCH carries kind='lag' (no service/binding), so the reconciler
    writes description/admin-state to `configure lag`, not a phantom `configure port lag-N`."""
    from nso_adapter.core.apply import _apply_attributes
    from nso_adapter.nso.apply import apply_interface_attribute

    iface = DbInterface(device_id=1, netbox_interface_id=7003, name="lag-30", kind="lag")
    attr_state = InterfaceAttrState(interface_id=1, attribute="description", sync_state=SyncState.accepted)
    intent = InterfaceIntent(interface_id=1, attribute="description", intent_value="uplink bundle")

    client, bodies = _capturing_nso_client()
    await _apply_attributes(
        [(attr_state, intent, iface)],
        apply_interface_attribute,
        client=client,
        device_name="ra1",
        job_id=1,
        now=datetime.now(UTC),
    )

    entry = bodies[0]["interface-reconciler:interface-config"][0]
    assert entry["kind"] == "lag"
    assert entry["description"] == "uplink bundle"
    assert "service" not in entry  # a LAG is not an IES/VPRN service


async def test_apply_attributes_ios_interface_omits_routed_context():
    """A non-Nokia interface (kind unset) carries no routed context — the per-scope path is
    unchanged for IOS/Junos, guarding against a false kind leaking into every PATCH."""
    from nso_adapter.core.apply import _apply_attributes
    from nso_adapter.nso.apply import apply_interface_attribute

    iface = DbInterface(device_id=1, netbox_interface_id=7002, name="GigabitEthernet0/0")
    attr_state = InterfaceAttrState(interface_id=1, attribute="enabled", sync_state=SyncState.accepted)
    intent = InterfaceIntent(interface_id=1, attribute="enabled", intent_value="true")

    client, bodies = _capturing_nso_client()
    await _apply_attributes(
        [(attr_state, intent, iface)],
        apply_interface_attribute,
        client=client,
        device_name="core-rtr-01",
        job_id=1,
        now=datetime.now(UTC),
    )

    entry = bodies[0]["interface-reconciler:interface-config"][0]
    assert entry["enabled"] is True
    assert "kind" not in entry and "service" not in entry
    assert "parent-binding" not in entry and "encap-tag" not in entry


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _seed_device(name: str = "test-rtr", netbox_id: int = 1) -> int:
    async with session() as db:
        d = Device(nso_instance="nso-dev", nso_device_name=name, netbox_device_id=netbox_id)
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id


async def _seed_apply_job(device_id: int, status: JobStatus = JobStatus.queued) -> int:
    async with session() as db:
        j = Job(job_type=JobType.apply, device_id=device_id, status=status)
        db.add(j)
        await db.commit()
        await db.refresh(j)
        return j.id


async def _seed_interface_with_intent(
    device_id: int,
    iface_name: str,
    attribute: str,
    intent_value: str,
    sync_state: SyncState,
    netbox_id: int = 100,
) -> tuple[int, int]:
    """Create DbInterface + InterfaceAttrState + InterfaceIntent, return (iface_id, attr_id)."""
    async with session() as db:
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
            accepted_at=datetime.now(UTC),
        )
        db.add(intent)
        await db.commit()
        await db.refresh(iface)
        await db.refresh(attr_state)
        return iface.id, attr_state.id


# ── enqueue_apply ─────────────────────────────────────────────────────────────


async def test_enqueue_apply_creates_job(adapter_client):
    """enqueue_apply creates an apply job when no active job exists."""
    device_id = await _seed_device("rtr-a01", 101)
    async with session() as db:
        job = await enqueue_apply(db, device_id=device_id)
        assert job is not None
        assert job.job_type == JobType.apply
        assert job.status == JobStatus.queued


async def test_enqueue_apply_blocked_by_a_queued_apply(adapter_client):
    """enqueue_apply refuses only when a QUEUED apply already exists."""
    device_id = await _seed_device("rtr-a02", 102)
    await _seed_apply_job(device_id, JobStatus.queued)

    async with session() as db:
        assert await enqueue_apply(db, device_id=device_id) is None


async def test_enqueue_apply_admitted_while_an_apply_runs(adapter_client):
    """A running apply does not refuse its successor: the successor carries the newer
    intent, and the device claim is what serializes their execution."""
    device_id = await _seed_device("rtr-a02b", 103)
    await _seed_apply_job(device_id, JobStatus.running)

    async with session() as db:
        assert await enqueue_apply(db, device_id=device_id) is not None


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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed


async def test_run_apply_nothing_eligible(adapter_client):
    """run_apply marks job succeeded when no interfaces are eligible."""
    device_id = await _seed_device("rtr-a12", 112)
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
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


async def _set_sync_before_apply(device_id: int, value: bool) -> None:
    async with session() as db:
        db.add(DeviceSettings(device_id=device_id, auto_apply=False, sync_before_apply=value))
        await db.commit()
        return


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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # nothing eligible, sync error swallowed


async def test_collect_apply_diff_returns_scope_deltas(adapter_client):
    """collect_apply_diff dry-runs each scope's intent and returns the native device delta."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await _seed_device("rtr-diff", 199)
    async with session() as db:
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.now(UTC))
        )
        await db.commit()

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_ospf_config", new_callable=AsyncMock, return_value="OSPF NATIVE DELTA"),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)
    assert diffs == {"ospf": "OSPF NATIVE DELTA"}


async def test_collect_apply_diff_empty_scope_omitted(adapter_client):
    """A scope whose dry-run shows no change (empty delta) is omitted from the result."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await _seed_device("rtr-diff2", 198)
    async with session() as db:
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.now(UTC))
        )
        await db.commit()

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_ospf_config", new_callable=AsyncMock, return_value="   "),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)
    assert diffs == {}


async def test_collect_apply_diff_outformat_cli_threads_format(adapter_client):
    """outformat='cli' threads dry_run='cli' into every scope apply — NSO then renders the
    NED-uniform +/- tree diff (the apply-preview 'diff -u' panel) instead of device-native."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent

    device_id = await _seed_device("rtr-diff-cli", 197)
    async with session() as db:
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.now(UTC))
        )
        await db.commit()

    mock_client = AsyncMock()
    ospf = AsyncMock(return_value="+ router ospf 1")
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_ospf_config", ospf),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id, outformat="cli")
    assert diffs == {"ospf": "+ router ospf 1"}
    assert ospf.await_args.kwargs["dry_run"] == "cli"


async def test_collect_apply_diff_covers_multiple_scopes(adapter_client):
    """collect_apply_diff dry-runs every scope with accepted intent, keyed by scope name."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent, StaticRouteIntent

    device_id = await _seed_device("rtr-diff3", 197)
    async with session() as db:
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.now(UTC))
        )
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="10.0.0.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_ospf_config", new_callable=AsyncMock, return_value="OSPF DELTA"),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock, return_value="STATIC DELTA"),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)
    assert diffs == {"ospf": "OSPF DELTA", "static_route": "STATIC DELTA"}


async def test_collect_apply_diff_device_not_found(adapter_client):
    """A dry-run preview for an unknown device returns an empty mapping (no NSO call)."""
    from nso_adapter.core.apply import collect_apply_diff

    async with session() as db:
        diffs = await collect_apply_diff(db, 999999)
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
    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id = "cisco-ios-cli-6.95"
        iface = DbInterface(device_id=device_id, name="GigabitEthernet0/0", netbox_interface_id=900)
        db.add(iface)
        await db.flush()
        db.add(
            m.InterfaceIntent(
                interface_id=iface.id, attribute="description", intent_value="uplink", accepted_at=datetime.now(UTC)
            )
        )
        db.add(
            m.InterfaceIpIntent(
                interface_id=iface.id, address="10.0.0.1/24", family="ipv4", accepted_at=datetime.now(UTC)
            )
        )
        db.add(
            m.OspfInstanceIntent(
                device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.now(UTC)
            )
        )
        db.add(
            m.IsisInterfaceIntent(device_id=device_id, interface_name="Gi0/0", af="ipv4", accepted_at=datetime.now(UTC))
        )
        db.add(m.BgpRouterIntent(device_id=device_id, asn="65000", accepted_at=datetime.now(UTC)))
        db.add(
            m.RoutePolicyObjectIntent(
                device_id=device_id, family="ipv4", name="RM", entries=[], accepted_at=datetime.now(UTC)
            )
        )
        db.add(
            m.SnmpCommunityIntent(
                device_id=device_id,
                label="ro",
                vault_ref="network/netbox/snmp/community/ro#community",
                access="ro",
                accepted_at=datetime.now(UTC),
            )
        )
        db.add(
            m.StaticRouteIntent(
                device_id=device_id, prefix="10.1.0.0/24", next_hop="10.1.0.1", accepted_at=datetime.now(UTC)
            )
        )
        db.add(m.LoggingHostIntent(device_id=device_id, address="10.0.0.99", accepted_at=datetime.now(UTC)))
        db.add(m.SviIntent(device_id=device_id, interface_name="Vlan10", vlan_id=10, accepted_at=datetime.now(UTC)))
        db.add(m.SubinterfaceIntent(device_id=device_id, interface_name="Gi0/0.10", accepted_at=datetime.now(UTC)))
        db.add(m.VlanIntent(device_id=device_id, vlan_id=20, accepted_at=datetime.now(UTC)))
        db.add(m.BfdIntent(device_id=device_id, interface_name="Gi0/1", accepted_at=datetime.now(UTC)))
        db.add(
            m.InterfaceMtuIntent(device_id=device_id, interface_name="Gi0/2", mtu=9000, accepted_at=datetime.now(UTC))
        )
        db.add(
            m.L2SapIntent(
                device_id=device_id,
                service_name="EPIPE-1",
                service_type="epipe",
                sap_id="1/1/1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

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
            async with session() as db:
                diffs = await collect_apply_diff(db, device_id)

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
    """A scope whose dry-run raises must not block the others — but must not vanish either.

    The operator approves the apply FROM this panel, and an omitted scope reads as "nothing
    to do". A body-builder error (a vault_ref the writer cannot render, an unmappable enum)
    is precisely what will fail the real apply, so it has to be visible here first.
    """
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import OspfInstanceIntent, StaticRouteIntent

    device_id = await _seed_device("rtr-diff-iso", 195)
    async with session() as db:
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.now(UTC))
        )
        db.add(
            StaticRouteIntent(
                device_id=device_id, prefix="10.2.0.0/24", next_hop="10.2.0.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(
            "nso_adapter.nso.apply.apply_ospf_config", new_callable=AsyncMock, side_effect=RuntimeError("dry-run boom")
        ),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock, return_value="STATIC DELTA"),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)
    # static_route still previews normally; ospf's failure is REPORTED, not swallowed
    assert diffs["static_route"] == "STATIC DELTA"
    assert "dry-run boom" in diffs["ospf"]
    assert diffs["ospf"].startswith("!! preview unavailable")


async def test_collect_apply_diff_interface_scope_failures_and_skips(adapter_client):
    """The interface attr/IP previews skip non-eligible attrs and swallow per-slice dry-run errors."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-diff-ifaceerr", 194)
    async with session() as db:
        iface = DbInterface(device_id=device_id, name="GigabitEthernet0/0", netbox_interface_id=910)
        db.add(iface)
        await db.flush()
        # eligible description slice — its dry-run will be made to raise
        db.add(
            InterfaceIntent(
                interface_id=iface.id, attribute="description", intent_value="up", accepted_at=datetime.now(UTC)
            )
        )
        # non-eligible attribute (skipped before any dry-run)
        db.add(
            InterfaceIntent(interface_id=iface.id, attribute="mtu", intent_value="9000", accepted_at=datetime.now(UTC))
        )
        # eligible attribute but not accepted (also skipped)
        db.add(InterfaceIntent(interface_id=iface.id, attribute="enabled", intent_value="true"))
        # IP intent whose dry-run will be made to raise
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id, address="10.0.0.1/24", family="ipv4", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

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
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)

    # both interface scopes failed → omitted; preview never raised
    assert diffs == {}


async def test_collect_apply_diff_interface_in_sync_yields_no_entry(adapter_client):
    """An interface already in sync (empty dry-run delta) contributes no preview entry."""
    from nso_adapter.core.apply import collect_apply_diff
    from nso_adapter.store.models import InterfaceIpIntent

    device_id = await _seed_device("rtr-diff-insync", 193)
    async with session() as db:
        iface = DbInterface(device_id=device_id, name="GigabitEthernet0/0", netbox_interface_id=911)
        db.add(iface)
        await db.flush()
        db.add(
            InterfaceIntent(
                interface_id=iface.id, attribute="description", intent_value="up", accepted_at=datetime.now(UTC)
            )
        )
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id, address="10.0.0.1/24", family="ipv4", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_interface_attribute", new_callable=AsyncMock, return_value=""),
        patch("nso_adapter.nso.apply.apply_interface_ips", new_callable=AsyncMock, return_value=None),
    ):
        async with session() as db:
            diffs = await collect_apply_diff(db, device_id)

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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["attribute_count_by_outcome"]["in_sync"] == 1
        assert job.result["attribute_count_by_outcome"]["apply_failed"] == 0


async def test_run_apply_refreshes_mirror_and_notifies_plugin(adapter_client):
    """After a finalized Apply, run_apply re-reads the applied surfaces into the read-mirror and
    fires the plugin sync-complete callback, so a 'deploying' overlay row settles on the immediate
    post-apply reconcile instead of only on the next periodic sync (the route-policy rg03 race).

    Before this, Apply pushed config to NSO but never refreshed the cache-only GET endpoints or
    notified the plugin, so the plugin's presence-based settle read a stale mirror (applied object
    not yet present) and re-marked the row 'deploying' — settling only on the next 15-min sync."""
    from nso_adapter.bindings.netbox.client import NetboxClient
    from nso_adapter.core import importer as imp

    device_id = await _seed_device("rtr-settle", 321)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/0",
        attribute="description",
        intent_value="uplink",
        sync_state=SyncState.accepted,
        netbox_id=400,
    )

    mock_client = AsyncMock()
    # READSEM grain b: post-apply consumes the projected doc. Serve BOTH families' sections
    # with a row each so their materializers demonstrably run.
    mock_client.get_device_state_doc.return_value = {
        "device-name": "rtr-settle",
        "route-policy": {"status": "ok", "prefix-list": [{"name": "PL-SETTLE", "entry": []}]},
        "svi": {"status": "ok", "interface": [{"interface-name": "Vlan77"}]},
    }
    nb = AsyncMock(spec=NetboxClient)
    nb.notify_sync_complete = AsyncMock()
    imp._netbox_client = nb
    try:
        with (
            patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
            patch("nso_adapter.nso.apply.apply_interface_attribute", new_callable=AsyncMock),
        ):
            await run_apply(job_id=job_id, device_id=device_id, force=True)
    finally:
        imp._netbox_client = None

    # Both post-apply fan-outs (routing AND config) consumed the projected doc — one doc GET
    # each (codex S3-R2 F5's two-fan-out proof under grain b) ...
    assert mock_client.get_device_state_doc.await_count == 2, mock_client.get_device_state_doc.await_count
    # ... their sections actually materialized (the families that back a 'deploying' row) ...
    async with session() as db:
        from nso_adapter.store.models import DeviceRoutePolicyPrefixList, DeviceSvi

        pls = (
            (
                await db.execute(
                    select(DeviceRoutePolicyPrefixList).where(DeviceRoutePolicyPrefixList.device_id == device_id)
                )
            )
            .scalars()
            .all()
        )
        svis = (await db.execute(select(DeviceSvi).where(DeviceSvi.device_id == device_id))).scalars().all()
        assert [x.name for x in pls] == ["PL-SETTLE"]
        assert [x.interface_name for x in svis] == ["Vlan77"]
    # ... and the plugin was notified so its post-apply reconcile settles the deploying row.
    nb.notify_sync_complete.assert_awaited_once_with(321)


async def test_run_apply_post_refresh_failure_does_not_fail_job(adapter_client):
    """The post-apply refresh/notify is best-effort: the Apply job is already finalized, so a
    failure re-reading the mirror or notifying the plugin must NOT flip a succeeded job to failed
    (the periodic sync is the backstop)."""
    from nso_adapter.core import importer as imp

    device_id = await _seed_device("rtr-settle-fail", 322)
    job_id = await _seed_apply_job(device_id)
    await _seed_interface_with_intent(
        device_id=device_id,
        iface_name="GigabitEthernet0/1",
        attribute="description",
        intent_value="uplink",
        sync_state=SyncState.accepted,
        netbox_id=401,
    )

    mock_client = AsyncMock()
    imp._netbox_client = None  # get_netbox_client() -> None; helper must still not raise
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_interface_attribute", new_callable=AsyncMock),
        patch(
            "nso_adapter.core.importer.refresh_routing_surfaces_for_device",
            new_callable=AsyncMock,
            side_effect=RuntimeError("NSO unreachable"),
        ),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)  # must not raise

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # unchanged by the best-effort post-refresh


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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "nso_commit_failed"
        assert job.result["attribute_count_by_outcome"]["apply_failed"] == 1


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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert (
            "unexpected internal error" in str(job.result)
            or job.result["attribute_count_by_outcome"]["apply_failed"] == 1
        )


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

    async with session() as db:
        job = await db.get(Job, job_id)
        # in_sync is not in _NO_FORCE_ELIGIBLE, so nothing was applied
        assert job.status == JobStatus.succeeded
        assert job.result["attribute_count_by_outcome"]["in_sync"] == 0


async def test_run_apply_outer_exception(adapter_client):
    """run_apply marks job failed on an outer unexpected exception."""
    device_id = await _seed_device("rtr-a17", 117)
    job_id = await _seed_apply_job(device_id)

    with patch("nso_adapter.core.importer.get_nso_client", side_effect=RuntimeError("DB boom")):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.error["code"] == "internal"


# ── IP intent apply pass ───────────────────────────────────────────────────


async def _seed_iface(device_id: int, iface_name: str) -> int:
    """Create a bare DbInterface row and return its id."""
    async with session() as db:
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

    async with session() as db:
        row = InterfaceIpIntent(
            interface_id=interface_id,
            address=address,
            vrf=vrf,
            family=family,
            secondary=secondary,
            accepted_at=datetime.now(UTC) if accepted else None,
        )
        db.add(row)
        await db.commit()


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

    async with session() as db:
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

    async with session() as db:
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
    async with session() as db:
        rows = (
            (await db.execute(select(InterfaceIpIntent).where(InterfaceIpIntent.interface_id == iface_id)))
            .scalars()
            .all()
        )
        rows[0].last_apply_at = datetime.now(UTC)
        rows[0].last_apply_error = None
        await db.commit()

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
    async with session() as db:
        db.add(BgpRouterIntent(device_id=device_id, asn="65100", accepted_at=datetime.now(UTC)))
        await db.commit()

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_bgp_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)  # must not raise

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["bgp_count_by_outcome"]["in_sync"] == 1


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
        dict(label="ro", vault_ref="network/netbox/snmp/community/ro#community", access="ro"),
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
    async with session() as db:
        db.add(model(device_id=device_id, accepted_at=datetime.now(UTC), **kwargs))
        await db.commit()

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch(f"nso_adapter.nso.apply.{apply_fn}", new_callable=AsyncMock) as mock_apply,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_apply.assert_awaited_once()
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result[f"{result_key}_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        # the row was stamped applied
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_at is not None
        assert rows[0].last_apply_error is None


async def test_run_apply_logging_threads_and_stamps_levels_intent(adapter_client):
    """The accepted local-levels singleton rides the logging scope: threaded to
    apply_logging_config as levels_intent_row and stamped alongside the host rows."""
    from nso_adapter.store.models import LoggingHostIntent, LoggingLevelsIntent

    device_id = await _seed_device("rtr-logging-lvl", 311)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(LoggingHostIntent(device_id=device_id, address="10.9.0.98", accepted_at=datetime.now(UTC)))
        db.add(LoggingLevelsIntent(device_id=device_id, console_severity="CRITICAL", accepted_at=datetime.now(UTC)))
        await db.commit()

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_logging_config", new_callable=AsyncMock) as mock_apply,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_apply.assert_awaited_once()
    assert mock_apply.await_args.kwargs["levels_intent_row"] is not None
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        # both the host row and the levels singleton count and get stamped
        assert job.result["logging_count_by_outcome"] == {"in_sync": 2, "apply_failed": 0}
        row = (
            await db.execute(select(LoggingLevelsIntent).where(LoggingLevelsIntent.device_id == device_id))
        ).scalar_one()
        assert row.last_apply_at is not None
        assert row.last_apply_error is None


async def test_run_apply_logging_levels_only_is_eligible(adapter_client):
    """A levels-only accept (no host intent at all) must still trigger the logging scope."""
    from nso_adapter.store.models import LoggingLevelsIntent

    device_id = await _seed_device("rtr-logging-lvl2", 312)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(LoggingLevelsIntent(device_id=device_id, monitor_severity="NOTICE", accepted_at=datetime.now(UTC)))
        await db.commit()

    mock_client = AsyncMock()
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_logging_config", new_callable=AsyncMock) as mock_apply,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_apply.assert_awaited_once()
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["logging_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}


async def test_run_apply_scope_failure_marks_error(adapter_client):
    """A scope NsoApplyError fails the job, stamps last_apply_error, and tags the item."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-sr-fail", 310)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, prefix="10.8.0.0/24", next_hop="10.8.0.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = AsyncMock()
    nso_err = NsoApplyError(code="nso_error", message="route rejected", detail={"x": 1})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock, side_effect=nso_err),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
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


async def test_run_apply_scope_unexpected_exception(adapter_client):
    """A non-NsoApplyError from a scope is caught, recorded as 'internal', job failed."""
    from nso_adapter.store.models import VlanIntent

    device_id = await _seed_device("rtr-vlan-boom", 311)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(VlanIntent(device_id=device_id, vlan_id=42, accepted_at=datetime.now(UTC)))
        await db.commit()

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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["vlan_count_by_outcome"]["apply_failed"] == 1
        rows = (await db.execute(select(VlanIntent).where(VlanIntent.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_error["code"] == "internal"
        assert "kaboom" in rows[0].last_apply_error["message"]


# The IS-IS sub-collections (process/level/flex) are eligible on their OWN — a per-level
# knob can be the only accepted row on a device whose interfaces are already in sync. They
# were missing from run_apply's `any_eligible`, so the isis scope still PUSHED (its _Scope
# rows list includes them) while _finalize_job took the "nothing eligible" early-return:
# an all-zero SUCCESS for a commit the device had rejected, which the plugin then settled
# deploying -> in_sync. These lock each sub-collection as independently apply-worthy.
_ISIS_SUBSCOPE_CASES = [
    ("IsisProcessIntent", dict(process_tag="1", net="49.0001.0000.0000.0001.00")),
    ("IsisLevelIntent", dict(process_tag="1", level=2, wide_metrics_only=True)),
    ("IsisFlexAlgoIntent", dict(process_tag="1", algo_id=128)),
]


@pytest.mark.parametrize("model_name, kwargs", _ISIS_SUBSCOPE_CASES)
async def test_run_apply_isis_subscope_failure_fails_the_job(adapter_client, model_name, kwargs):
    """An IS-IS process/level/flex row is the ONLY eligible intent and the device rejects it.

    The job must FAIL. Before the fix it reported succeeded with all-zero counts.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store import models as m

    device_id = await _seed_device(f"rtr-isis-sub-{model_name.lower()}", 330)
    job_id = await _seed_apply_job(device_id)
    model = getattr(m, model_name)
    async with session() as db:
        db.add(model(device_id=device_id, accepted_at=datetime.now(UTC), **kwargs))
        await db.commit()

    nso_err = NsoApplyError(code="nso_error", message="level rejected", detail={})
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=AsyncMock()),
        patch("nso_adapter.nso.apply.apply_isis_interfaces", new_callable=AsyncMock, side_effect=nso_err),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["isis_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        assert job.error["code"] == "nso_commit_failed"
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_error["message"] == "level rejected"


@pytest.mark.parametrize("model_name, kwargs", _ISIS_SUBSCOPE_CASES)
async def test_run_apply_isis_subscope_success_is_counted(adapter_client, model_name, kwargs):
    """The same row applying cleanly must be COUNTED in_sync, not reported as nothing-eligible."""
    from nso_adapter.store import models as m

    device_id = await _seed_device(f"rtr-isis-sub-ok-{model_name.lower()}", 331)
    job_id = await _seed_apply_job(device_id)
    model = getattr(m, model_name)
    async with session() as db:
        db.add(model(device_id=device_id, accepted_at=datetime.now(UTC), **kwargs))
        await db.commit()

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=AsyncMock()),
        patch("nso_adapter.nso.apply.apply_isis_interfaces", new_callable=AsyncMock) as mock_apply,
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_apply.assert_awaited_once()
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["isis_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        rows = (await db.execute(select(model).where(model.device_id == device_id))).scalars().all()
        assert rows[0].last_apply_at is not None


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
    async with session() as db:
        db.add(
            IsisInterfaceIntent(device_id=device_id, interface_name="Gi0/3", af="ipv4", accepted_at=datetime.now(UTC))
        )
        db.add(IsisProcessIntent(device_id=device_id, accepted_at=datetime.now(UTC)))
        db.add(IsisFlexAlgoIntent(device_id=device_id, algo_id=128, accepted_at=datetime.now(UTC)))
        db.add(
            RedistributionIntent(
                device_id=device_id,
                dest_protocol="isis",
                source_protocol="connected",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        # in_sync counts every row across the four lists
        assert job.result["isis_count_by_outcome"] == {"in_sync": 4, "apply_failed": 0}


async def test_run_apply_ospf_applies_instance_interface_and_redist(adapter_client):
    """The OSPF pass applies process + interface + ospf-destined redistribution together."""
    from nso_adapter.store.models import OspfInstanceIntent, OspfInterfaceIntent, RedistributionIntent

    device_id = await _seed_device("rtr-ospf-combo", 321)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            OspfInstanceIntent(device_id=device_id, process_id="1", router_id="1.1.1.1", accepted_at=datetime.now(UTC))
        )
        db.add(OspfInterfaceIntent(device_id=device_id, interface_name="Gi0/4", accepted_at=datetime.now(UTC)))
        db.add(
            RedistributionIntent(
                device_id=device_id,
                dest_protocol="ospf",
                source_protocol="static",
                accepted_at=datetime.now(UTC),
            )
        )
        # a bgp-destined redist row must NOT be swept into the ospf pass
        db.add(
            RedistributionIntent(
                device_id=device_id, dest_protocol="bgp", source_protocol="static", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.result["ospf_count_by_outcome"] == {"in_sync": 3, "apply_failed": 0}


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
    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id,
                label="ro",
                vault_ref="network/netbox/snmp/community/ro#community",
                access="ro",
                accepted_at=datetime.now(UTC),
            )
        )
        db.add(SnmpV3UserIntent(device_id=device_id, username="v3-test-group", accepted_at=datetime.now(UTC)))
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="10.7.0.5",
                version="v2c",
                notify_type="traps",
                community_or_user="ro",
                accepted_at=datetime.now(UTC),
            )
        )
        db.add(SnmpSystemInfoIntent(device_id=device_id, location="rack-7", accepted_at=datetime.now(UTC)))
        await db.commit()

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
    async with session() as db:
        job = await db.get(Job, job_id)
        # 3 list rows + 1 system-info row
        assert job.result["snmp_count_by_outcome"] == {"in_sync": 4, "apply_failed": 0}


async def test_run_apply_route_policy_failure_records_capability(adapter_client):
    """A route-policy NsoApplyError fails the job AND records a capability rejection.

    The device parser only rejects an unsupported construct on a real commit (dry-run
    renders it), so the accepted-half learns the (ned, sw) limitation here.
    """
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_device("rtr-rp-fail", 323)
    # give the device a ned_id so apply_route_policy_config gets one
    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id = "cisco-ios-cli-6.95"
        await db.commit()
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="ipv4",
                name="RM-IN",
                entries=[],
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["route_policy_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}


async def test_run_apply_route_policy_capability_recording_is_best_effort(adapter_client):
    """If capability recording itself raises, the apply still fails cleanly (swallowed)."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_device("rtr-rp-cap-err", 324)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id, family="ipv4", name="RM-X", entries=[], accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

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

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["route_policy_count_by_outcome"]["apply_failed"] == 1


async def test_run_apply_route_policy_capability_skips_record_when_unparseable(adapter_client):
    """When the rejected construct can't be parsed (no name), no capability row is recorded."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_device("rtr-rp-cap-skip", 325)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id, family="ipv4", name="RM-Y", entries=[], accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed


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

    async with session() as db:
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


# ── Atomic apply (NSO_ADAPTER_ATOMIC_APPLY): subif + IP in one transaction ─────


async def _seed_subif_and_ip(device_id: int, iface_name: str = "ae99.999") -> int:
    """Seed a DbInterface + accepted SubinterfaceIntent (device-keyed) + accepted
    InterfaceIpIntent (interface-keyed) — the greenfield subif+IP pair. Returns iface_id."""
    from nso_adapter.store.models import InterfaceIpIntent, SubinterfaceIntent

    async with session() as db:
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
                accepted_at=datetime.now(UTC),
            )
        )
        db.add(
            InterfaceIpIntent(
                interface_id=iface.id,
                address="198.18.1.1/24",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()
        return iface.id


async def _ip_and_subif_rows(device_id: int):
    from nso_adapter.store.models import InterfaceIpIntent, SubinterfaceIntent

    async with session() as db:
        subif = (
            (await db.execute(select(SubinterfaceIntent).where(SubinterfaceIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        ip = (await db.execute(select(InterfaceIpIntent))).scalars().all()
        return subif, ip


async def _seed_snmp_and_static_route(device_id: int) -> None:
    from nso_adapter.store.models import SnmpCommunityIntent, StaticRouteIntent

    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id, label="public", vault_ref="m/p#k", access="RO", accepted_at=datetime.now(UTC)
            )
        )
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="10.9.9.0/24", next_hop="1.1.1.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()
        return


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
    assert iface_entry["ipv4-address"][0]["address"] == "198.18.1.1"
    assert modules["subinterface-reconciler:subif-config"][0]["interface"][0]["interface-name"] == "ae99.999"

    subif_rows, ip_rows = await _ip_and_subif_rows(device_id)
    assert all(r.last_apply_at is not None and r.last_apply_error is None for r in subif_rows + ip_rows)
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded


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
    async with session() as db:
        assert (await db.get(Job, job_id)).status == JobStatus.succeeded


@pytest.mark.asyncio
async def test_run_apply_atomic_unrenderable_scope_does_not_take_down_the_job(adapter_client, monkeypatch):
    """A scope whose BODY cannot be built must fail alone — not the entire apply.

    A legacy SnmpCommunityIntent whose vault_ref predates the mount/path#key contract makes
    apply_snmp_config raise while _stage_atomic_modules is assembling the combined body.
    That raise propagated out of _run_atomic_apply and failed the WHOLE job: interfaces,
    IPs, BGP, IS-IS — every scope, none of which had anything wrong with it. Isolate the
    offender: stamp its rows, drop it from the transaction, and commit the rest.
    """
    from nso_adapter.store.models import SnmpCommunityIntent, StaticRouteIntent

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01-legacy")
    await _seed_snmp_and_static_route(device_id)
    async with session() as db:
        row = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        row.vault_ref = "network/netbox/snmp/legacy"  # pre-#121: no '#key'
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    combined = AsyncMock(return_value=None)
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    # The healthy scopes still committed, without the offender's module.
    combined.assert_awaited_once()
    modules = combined.await_args.args[2]
    assert "static-route-reconciler:static-route-config" in modules
    assert "snmp-reconciler:snmp-config" not in modules

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed  # the snmp scope really did fail
        assert job.result["snmp_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        comm = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert comm.last_apply_error["code"] == "invalid_vault_ref"
        sr = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert sr.last_apply_error is None  # an innocent scope was not punished
        assert sr.last_apply_at is not None


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
    async with session() as db:
        db.add(
            InterfaceIpIntent(
                interface_id=iface_id,
                address="10.0.0.1/30",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()
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
        return {"static-route-reconciler:static-route-config": "static route rejected"}, (None, None)

    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", boom))
        stack.enter_context(patch("nso_adapter.core.apply._localize_atomic_failure", _fake_localize))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        sr = (await db.execute(select(StaticRouteIntent))).scalars().all()
        sc = (await db.execute(select(SnmpCommunityIntent))).scalars().all()
        assert sr and all(r.last_apply_error is not None for r in sr)  # offender → failed
        assert sc and all(r.last_apply_error is None and r.last_apply_at is None for r in sc)  # pending
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_run_apply_atomic_failure_records_scope_capability(adapter_client, monkeypatch):
    """An atomic failure records a capability rejection for the localised offender scope —
    generalised beyond route-policy. The NED rejecting a scope's dry-run is a real capability
    gap, so the matrix learns ``(ned, sw, static_route) = unsupported``; the scope that
    compiled fine (snmp) gets no row."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import Device, DeviceCapability, SnmpCommunityIntent, StaticRouteIntent

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    await _seed_snmp_and_static_route(device_id)
    async with session() as db:  # give the device a known (ned, sw) so no probe is needed
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = "cisco-ios-cli:cisco-ios", "15.7"
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    async def _combined(client, device_name, modules, *, dry_run=False, strict=False):
        if not dry_run:
            raise NsoApplyError("nso_patch_failed", "static route rejected by NED")
        # per-scope dry-run localisation (strict): only static-route conclusively rejects
        if "static-route-reconciler:static-route-config" in modules:
            raise NsoApplyError("dry_run_rejected", "static route cannot compile on this NED")
        return "delta"

    mock_client = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", _combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        caps = (
            (await db.execute(select(DeviceCapability).where(DeviceCapability.ned_id == "cisco-ios-cli:cisco-ios")))
            .scalars()
            .all()
        )
        by_scope = {c.scope: c for c in caps}
        assert "static_route" in by_scope and by_scope["static_route"].status == "unsupported"
        assert "snmp" not in by_scope  # compiled fine → not an offender → no capability row
        # offender failed, snmp pending, job failed
        sr = (await db.execute(select(StaticRouteIntent))).scalars().all()
        sc = (await db.execute(select(SnmpCommunityIntent))).scalars().all()
        assert all(r.last_apply_error is not None for r in sr)
        assert all(r.last_apply_error is None and r.last_apply_at is None for r in sc)
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_run_apply_atomic_iface_rejection_attributed_to_offending_half(adapter_client, monkeypatch):
    """H2: a rejected merged interface-config module whose error names the IP half records
    ONLY (interface_ip, <construct>) — interface_attribute no longer falsely warns (the old
    coarse recording marked BOTH scopes on one rejection)."""
    from datetime import datetime

    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import Device, DeviceCapability, InterfaceIpIntent

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    iface_id, _attr_id = await _seed_interface_with_intent(
        device_id, "Gi0/1", "description", "uplink", SyncState.accepted
    )
    async with session() as db:
        db.add(
            InterfaceIpIntent(
                interface_id=iface_id,
                address="10.0.0.1/30",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.now(UTC),
            )
        )
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = "cisco-ios-cli:cisco-ios", "15.7"
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    # REAL sample shape (captured live on rg03): the 4xx names the ipv4-address node.
    reject_msg = (
        "invalid value for: prefix-length in /ir:interface-config[ir:device='sw01']"
        "[ir:interface-name='Gi0/1']/ir:ipv4-address[ir:address='10.0.0.1']/ir:prefix-length:"
        ' "99" is out of range.'
    )

    async def _combined(client, device_name, modules, *, dry_run=False, strict=False):
        if not dry_run:
            raise NsoApplyError("nso_patch_failed", reject_msg)
        if "interface-reconciler:interface-config" in modules:
            raise NsoApplyError("dry_run_rejected", reject_msg)
        return "delta"

    mock_client = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", _combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        caps = (
            (await db.execute(select(DeviceCapability).where(DeviceCapability.ned_id == "cisco-ios-cli:cisco-ios")))
            .scalars()
            .all()
        )
        by_scope = {c.scope: c for c in caps}
        assert "interface_ip" in by_scope
        assert by_scope["interface_ip"].name == "ipv4-address"  # construct-named, not coarse
        assert "interface_attribute" not in by_scope  # the attribute half no longer falsely warns


@pytest.mark.asyncio
async def test_run_apply_atomic_iface_rejection_unattributable_falls_back_to_both(adapter_client, monkeypatch):
    """When the rejection names no known construct, the fail-safe records BOTH halves coarse —
    losing precision, never losing the record."""
    from datetime import datetime

    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import Device, DeviceCapability, InterfaceIpIntent

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    iface_id, _attr_id = await _seed_interface_with_intent(
        device_id, "Gi0/2", "description", "uplink", SyncState.accepted
    )
    async with session() as db:
        db.add(
            InterfaceIpIntent(
                interface_id=iface_id,
                address="10.0.0.5/30",
                family="ipv4",
                secondary=False,
                accepted_at=datetime.now(UTC),
            )
        )
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = "cisco-ios-cli:cisco-ios", "15.7"
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    async def _combined(client, device_name, modules, *, dry_run=False, strict=False):
        if not dry_run:
            raise NsoApplyError("nso_patch_failed", "opaque NED failure")
        if "interface-reconciler:interface-config" in modules:
            raise NsoApplyError("dry_run_rejected", "opaque NED failure")
        return "delta"

    mock_client = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", _combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        caps = (
            (await db.execute(select(DeviceCapability).where(DeviceCapability.ned_id == "cisco-ios-cli:cisco-ios")))
            .scalars()
            .all()
        )
        by_scope = {c.scope: c for c in caps}
        assert "interface_ip" in by_scope and "interface_attribute" in by_scope  # coarse fallback


@pytest.mark.asyncio
async def test_run_apply_atomic_success_clears_stale_reactive_unsupported(adapter_client, monkeypatch):
    """A clean atomic commit is the strongest positive signal — it clears a prior reactive
    'unsupported' verdict for the applied scopes so the gap does not stick forever (a probe
    cannot downgrade an apply-rejection). A scope that was NOT applied, and any route-policy
    fine-grained construct row, are left untouched."""
    from nso_adapter.store.models import Device, DeviceCapability

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    await _seed_snmp_and_static_route(device_id)
    ned = "cisco-ios-cli:cisco-ios"
    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = ned, "15.7"
        # stale reactive rejections left by an earlier FAILED apply of these scopes
        db.add_all(
            [
                DeviceCapability(
                    ned_id=ned,
                    sw_version="15.7",
                    scope="snmp",
                    name="snmp",
                    status="unsupported",
                    detail="old error",
                    source="apply",
                ),
                DeviceCapability(
                    ned_id=ned,
                    sw_version="15.7",
                    scope="route_policy",
                    name="route_policy",
                    status="unsupported",
                    detail="old error",
                    source="apply",
                ),
                DeviceCapability(
                    ned_id=ned,
                    sw_version="15.7",
                    scope="rm-set",
                    name="set extcommunity color",
                    status="unsupported",
                    detail="fine-grained",
                    source="apply",
                ),
            ]
        )
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    combined = AsyncMock(return_value=None)  # clean commit (snmp + static_route both apply)
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        by_key = {
            (c.scope, c.name): c
            for c in (await db.execute(select(DeviceCapability).where(DeviceCapability.ned_id == ned))).scalars().all()
        }
        assert ("snmp", "snmp") not in by_key  # applied scope → stale rejection cleared
        assert ("route_policy", "route_policy") in by_key  # NOT applied → untouched
        assert ("rm-set", "set extcommunity color") in by_key  # fine-grained → never cleared
        assert (await db.get(Job, job_id)).status == JobStatus.succeeded


async def _seed_route_map_intent(device_id, ned_id):
    from nso_adapter.store.models import Device, RoutePolicyObjectIntent

    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = ned_id, ""
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="route_map",
                name="TEST-RM",
                entries=[{"sequence": 10, "action": "permit"}],
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()


@pytest.mark.asyncio
async def test_run_apply_atomic_misconfig_device_rejection_records_no_capability(adapter_client, monkeypatch):
    """A generic device rejection that no per-scope dry-run localises — e.g. a route-map
    referencing a prefix-list not included in the push (a MISCONFIGURATION, not a NED limit) —
    must NOT record capability; that would be a false 'unsupported' verdict. The job still fails
    and last_apply_error carries the real device error. (The live IOS-XR-route-map→Junos case:
    'prefix-list referenced but not defined'.)"""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import DeviceCapability, RoutePolicyObjectIntent

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    await _seed_route_map_intent(device_id, "juniper-junos-nc-4.19:junos")
    job_id = await _seed_apply_job(device_id)

    device_err = "RPC error towards sw01: Policy error: PL-X prefix-list referenced (in term 10) but not defined"

    async def _combined(client, device_name, modules, *, dry_run=False, strict=False):
        if not dry_run:
            raise NsoApplyError(
                "nso_patch_failed",
                "NSO combined PATCH failed with status 400",
                detail={"nso_error": {"ietf-restconf:errors": {"error": [{"error-message": device_err}]}}},
            )
        return "rendered-delta"  # route-policy renders clean in dry-run → not localised

    mock_client = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", _combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        assert (await db.execute(select(DeviceCapability))).scalars().all() == []  # NOT a capability gap
        rp = (await db.execute(select(RoutePolicyObjectIntent))).scalars().all()
        assert all(r.last_apply_error is not None for r in rp)  # but the apply did fail + recorded the error
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_run_apply_atomic_transient_failure_records_no_capability(adapter_client, monkeypatch):
    """A transport/internal failure (no device rejection) records NO capability — no false verdict."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import DeviceCapability

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    await _seed_route_map_intent(device_id, "juniper-junos-nc-4.19:junos")
    job_id = await _seed_apply_job(device_id)

    async def _combined(client, device_name, modules, *, dry_run=False, strict=False):
        if not dry_run:
            raise NsoApplyError("internal", "connection timed out")  # transport — no nso_error
        return "rendered-delta"

    mock_client = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", _combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        assert (await db.execute(select(DeviceCapability))).scalars().all() == []  # nothing recorded
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_run_apply_atomic_transient_during_localize_records_no_capability(adapter_client, monkeypatch):
    """A transient transport error DURING per-scope localisation must NOT brand the scope
    'unsupported' — only a conclusive rejection is a capability signal (finding #10)."""
    from nso_adapter.nso.apply import NsoApplyError
    from nso_adapter.store.models import Device, DeviceCapability

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    await _seed_snmp_and_static_route(device_id)
    async with session() as db:
        dev = await db.get(Device, device_id)
        dev.ned_id, dev.sw_version = "cisco-ios-cli:cisco-ios", "15.7"
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    device_err = "RPC error: something rejected"

    async def _combined(client, device_name, modules, *, dry_run=False, strict=False):
        if not dry_run:
            raise NsoApplyError(
                "nso_patch_failed",
                "rejected",
                detail={"nso_error": {"ietf-restconf:errors": {"error": [{"error-message": device_err}]}}},
            )
        raise ConnectionError("transient blip during localisation")  # transport, not a conclusive reject

    mock_client = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", _combined))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        assert (await db.execute(select(DeviceCapability))).scalars().all() == []  # no false 'unsupported'
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_run_apply_marks_failed_even_when_session_poisoned(adapter_client, monkeypatch):
    """run_apply's failure handler must rollback the poisoned session before committing the
    failed-status, or the status commit itself throws and the job is stuck 'running' (#11)."""
    device_id = await _seed_device(name="rtr-poison")
    job_id = await _seed_apply_job(device_id)

    async def _poison(db, job, job_id, device_id, force):
        # A real DB error (duplicate PK) puts the AsyncSession into a needs-rollback state,
        # exactly like a failed flush mid-apply; the failure handler must rollback first.
        db.add(Job(id=job_id, job_type=JobType.apply, device_id=device_id, status=JobStatus.queued))
        await db.flush()  # IntegrityError → session poisoned; propagates to run_apply's handler

    with patch("nso_adapter.core.apply._execute_apply", _poison):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        assert (await db.get(Job, job_id)).status == JobStatus.failed


@pytest.mark.asyncio
async def test_run_apply_atomic_staging_failure_reverts_deploying(adapter_client, monkeypatch):
    """If a body-builder raises during staging (e.g. a malformed IP address), the attr states
    just marked 'deploying' must be reverted, not left stuck deploying forever (#12)."""
    from nso_adapter.store.models import InterfaceAttrState

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01")
    iface_id, attr_id = await _seed_interface_with_intent(
        device_id, "Gi0/0", "description", "uplink", SyncState.accepted
    )
    await _seed_ip_intent(iface_id, address="10.0.0.1", accepted=True)  # malformed: no /prefix → build raises
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    with patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        assert (await db.get(Job, job_id)).status == JobStatus.failed
        attr_state = await db.get(InterfaceAttrState, attr_id)
        assert attr_state.sync_state != SyncState.deploying  # reverted, not stuck deploying
        assert attr_state.sync_state == SyncState.accepted


def test_capability_scopes_for_interface_config_covers_attribute_and_ip():
    """The merged interface-config module carries BOTH the attribute and IP scopes, so a
    rejection must record capability under both (else a preflight for interface_attribute
    sees a false 'fully supported') (#17)."""
    from nso_adapter.core.apply import _IFACE_CONFIG_ROOT, _capability_scopes_for

    assert _capability_scopes_for(_IFACE_CONFIG_ROOT) == ["interface_attribute", "interface_ip"]
    assert _capability_scopes_for("snmp-reconciler:snmp-config") == ["snmp"]
    assert _capability_scopes_for("no-such-root") == []


@pytest.mark.asyncio
async def test_diff_interface_ips_preview_excludes_unaccepted(adapter_client):
    """The Apply-diff IP preview must gate on accepted_at like the attribute preview and the
    real apply eligibility — an un-accepted IP intent must not appear in the preview (#19)."""
    from types import SimpleNamespace

    from nso_adapter.core.apply import _diff_interface_ips

    device_id = await _seed_device("rtr-diff", 301)
    iface_id = await _seed_iface(device_id, "Gi0/1")
    await _seed_ip_intent(iface_id, address="10.0.0.1/24", accepted=True)
    await _seed_ip_intent(iface_id, address="10.0.0.2/24", accepted=False)

    seen_rows: list = []

    async def _apply_ips(*, client, device_name, interface_name, ip_intent_rows, **kw):
        seen_rows.extend(ip_intent_rows)
        return ""

    nso_apply = SimpleNamespace(apply_interface_ips=_apply_ips)
    async with session() as db:
        iface = await db.get(DbInterface, iface_id)
        await _diff_interface_ips(db, nso_apply, object(), "rtr-diff", {iface_id: iface})

    assert {r.address for r in seen_rows} == {"10.0.0.1/24"}  # un-accepted 10.0.0.2/24 excluded


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
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed


# ── #108: post-apply reader-compare (the #26 silent-drop class, caught in seconds) ──
#
# _verify_native_or_raise re-diffs the committed payload against the CDB SERVICE tree —
# both sides sit behind the same FASTMAP writer, so a writer that silently drops an
# object stays invisible (#26, proven live on rg03). The reader-compare closes that
# hole from the other side: after a scope's batch commit reports success, re-read the
# scope's device-state ACTION section (READSEM 1328) and require every intended key to
# be present. A missing key marks those rows apply_failed (retryable) and fails the JOB,
# so the plugin settles deploying→apply_failed on the immediate post-apply reconcile
# instead of waiting out stuck_deploying_grace_minutes.

# reader-compare reads the device-state-read ACTION now — a test seeds the post-commit
# device view as the action's certified output: {atomic, device-name, <wire>: <section>}.
# A method-level mock bypasses NsoClient certification (exercised end-to-end in
# tests/nso/test_device_state_client.py); the section still carries a terminal status.
_RC_WIRE = {
    "static_route": "static-route",
    "snmp": "snmp-config",
    "route_policy": "route-policy",
    "bgp": "bgp-config",
    "isis": "isis-interface",
}


def _rc_action(device_name: str, scope: str, section: dict) -> dict:
    """A certified device-state-read output carrying one scope's post-commit section."""
    return {"atomic": True, "device-name": device_name, _RC_WIRE[scope]: section}


async def test_run_apply_reader_compare_flags_silent_drop(adapter_client):
    """The #26 scenario: the static-route writer 'succeeds' but the device view never
    gains the route → the job FAILS and the row carries reader_compare_missing."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-drop", 401)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.26.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-drop", "static_route", {"status": "ok", "route": []}
    )  # commit "ok", key never landed
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        assert job.result["reader_compare"]["static_route"] == "missing"
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error["code"] == "reader_compare_missing"
        assert "198.18.26.0/24" in row.last_apply_error["message"]


async def test_run_apply_reader_compare_ok_when_key_lands(adapter_client):
    """A landed key keeps the scope green and records reader_compare=ok."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-ok", 402)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.27.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-ok",
        "static_route",
        {"status": "ok", "route": [{"vrf": "", "prefix": "198.18.27.0/24", "next-hop": "10.0.0.1"}]},
    )
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        assert job.result["reader_compare"]["static_route"] == "ok"
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None
        assert row.last_apply_at is not None


from tests.core.conftest import SNMP_COMMUNITY, SNMP_VAULT_REF, community_export_name  # noqa: E402


def _community_export_name(secret: str) -> str:
    """The export's community key: sha256(community string)[:16] — never the intent label."""
    import hashlib

    return hashlib.sha256(secret.encode()).hexdigest()[:16]


async def test_run_apply_reader_compare_does_not_fail_a_landed_community(adapter_client):
    """A community that DID land must not be re-flagged as a silent writer drop.

    The check keyed SnmpCommunityIntent by `label`, but network-state-export keys a
    community by sha256(community-string)[:16] — and the adapter never sees that string
    (it pushes a Vault triple; NSO resolves the secret). The sets could never intersect, so
    EVERY successful SNMP apply was stamped reader_compare_missing and failed, forever.
    """
    from nso_adapter.store.models import SnmpCommunityIntent

    device_id = await _seed_device("rtr-rc-snmp", 404)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id,
                label="prod-ro",
                vault_ref="network/netbox/snmp/community/prod-ro#community",
                access="ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    # The community IS on the device — under its hashed export identity.
    mock_client.get_device_state_section.return_value = {
        "status": "ok",
        "community": [{"name": _community_export_name("s3cr3t"), "access": "ro"}],
        "v3-user": [],
        "host": [],
    }
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_snmp_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["snmp_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        row = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None


async def test_run_apply_reader_compare_still_catches_a_dropped_snmp_host(adapter_client):
    """Dropping the un-keyable community grain must not blunt the check: the
    address-keyed host and username-keyed v3-user are still verified."""
    from nso_adapter.store.models import SnmpHostIntent

    device_id = await _seed_device("rtr-rc-snmp-host", 405)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="prod-ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-snmp-host", "snmp", {"status": "ok", "community": [], "v3-user": [], "host": []}
    )  # never landed
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_snmp_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["snmp"] == "missing"
        row = (await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))).scalars().one()
        assert row.last_apply_error["code"] == "reader_compare_missing"


async def test_run_apply_reader_compare_absent_reader_surface_is_not_a_drop(adapter_client):
    """A section the NED does not export (status=unsupported) means "unknown", not "the
    writer dropped everything".

    The device-state action declares status=unsupported for a family the NED has no export
    surface for — absence there proves nothing, so the scope stays "unknown" and green. The
    legacy None→{} coercion classified every intended key a silent writer drop and pinned the
    scope permanently apply_failed on a device where NSO had committed the intent; the
    envelope's status closes that blind spot.
    """
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-none", 406)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.29.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-none", "static_route", {"status": "unsupported"}
    )  # no export surface on this NED
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        assert job.result["reader_compare"]["static_route"] == "unknown"
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None


async def test_run_apply_reader_compare_empty_list_payload_is_still_a_drop(adapter_client):
    """A reader that DOES answer, with the scope's list empty, is a real silent drop —
    the export surface exists and reports nothing there. Must still fail."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-empty", 407)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.30.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-empty", "static_route", {"status": "ok", "route": []}
    )  # answered — and the route is NOT there
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["static_route"] == "missing"


async def _seed_nokia_device(name: str, netbox_id: int) -> int:
    """A device whose NED cannot hold every canonical community member (SR OS)."""
    async with session() as db:
        d = Device(
            nso_instance="nso-dev",
            nso_device_name=name,
            netbox_device_id=netbox_id,
            ned_id="timos-nc-23.10",
        )
        db.add(d)
        await db.commit()
        await db.refresh(d)
        return d.id


async def test_run_apply_reader_compare_skips_a_fully_unrepresentable_community_list(adapter_client):
    """An object the writer DELIBERATELY could not render must not be called a silent drop.

    apply_route_policy_config skips community members the NED cannot hold (`bandwidth:` has
    no SR OS policy keyword), so a community-list whose members are ALL unrepresentable
    emits {"name": …, "entry": []} — which has no renderable CLI form, never lands, and so
    never appears in the export. The PUT already reports these to the plugin via
    `unsupported_members` so it can mark them "unsupported on <ned>". Demanding the object
    be present anyway turned a known, deliberately-tolerated codec skip into a hard,
    permanently-recurring apply failure for the whole route_policy scope.
    """
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_nokia_device("rtr-rp-unsup", 408)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="community_list",
                name="CL-COLOR-ONLY",
                entries=[{"community": "bandwidth:64500:100"}],  # unrepresentable on SR OS
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    # The export answers — the object legitimately is not there, because nothing was rendered.
    mock_client.get_device_state_section.return_value = {
        "status": "ok",
        "community-list": [],
        "prefix-list": [],
        "route-map": [],
    }
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_route_policy_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["route_policy_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        row = (
            (await db.execute(select(RoutePolicyObjectIntent).where(RoutePolicyObjectIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None


async def test_run_apply_reader_compare_still_fails_a_representable_community_list(adapter_client):
    """A community-list the NED CAN hold, that did not land, is still a real silent drop."""
    from nso_adapter.store.models import RoutePolicyObjectIntent

    device_id = await _seed_nokia_device("rtr-rp-real", 409)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            RoutePolicyObjectIntent(
                device_id=device_id,
                family="community_list",
                name="CL-STD",
                entries=[{"community": "64500:100"}],  # plain asn:val — SR OS takes it verbatim
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    # reader-compare reads the device-state ACTION; device-name is echoed by the real action but
    # ignored on this method mock (cert is exercised in test_device_state_client.py).
    mock_client.run_device_state_read.return_value = {
        "atomic": True,
        "device-name": None,
        "route-policy": {"status": "ok", "community-list": [], "prefix-list": [], "route-map": []},
    }
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_route_policy_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["route_policy"] == "missing"


async def test_run_apply_reader_compare_reader_error_is_nonfatal(adapter_client):
    """The check must never fail a good apply: a reader exception records 'error' and
    leaves the scope green (transparency without false alarms)."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-err", 403)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id,
                vrf="",
                prefix="198.18.28.0/24",
                next_hop="10.0.0.1",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.side_effect = RuntimeError("reader down")
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["static_route_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}
        assert job.result["reader_compare"]["static_route"] == "error"


async def test_run_apply_reader_compare_bgp_checks_router_and_peers(adapter_client):
    """A BgpRouterIntent expands to its router asn AND every scope peer address — the
    reader nests router→scope→peer like the service. A dropped peer flags the row."""
    from nso_adapter.store.models import BgpPeerIntent, BgpRouterIntent, BgpScopeIntent

    device_id = await _seed_device("rtr-rc-bgp", 404)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        router = BgpRouterIntent(device_id=device_id, asn="65100", accepted_at=datetime.now(UTC))
        db.add(router)
        await db.flush()
        scope = BgpScopeIntent(router_id=router.id, vrf="")
        db.add(scope)
        await db.flush()
        db.add(BgpPeerIntent(scope_id=scope.id, peer_address="10.0.0.7"))
        db.add(BgpPeerIntent(scope_id=scope.id, peer_address="10.0.0.9"))
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-bgp",
        "bgp",
        {"status": "ok", "router": [{"asn": 65100, "scope": [{"vrf": "", "peer": [{"peer-address": "10.0.0.7"}]}]}]},
    )  # 10.0.0.9 silently dropped
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_bgp_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["bgp"] == "missing"
        assert job.result["bgp_count_by_outcome"] == {"in_sync": 0, "apply_failed": 1}
        router = (
            (await db.execute(select(BgpRouterIntent).where(BgpRouterIntent.device_id == device_id))).scalars().one()
        )
        assert router.last_apply_error["code"] == "reader_compare_missing"
        assert "10.0.0.9" in router.last_apply_error["message"]


async def test_run_apply_reader_compare_isis_flags_only_missing_model(adapter_client):
    """isis mixes interface and process rows in one scope — only the model whose key
    is absent (here the process) is flagged; the landed interface row stays green."""
    from nso_adapter.store.models import IsisInterfaceIntent, IsisProcessIntent

    device_id = await _seed_device("rtr-rc-isis", 405)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            IsisInterfaceIntent(
                device_id=device_id, interface_name="ge-0/0/0", af="ipv4", accepted_at=datetime.now(UTC)
            )
        )
        db.add(IsisProcessIntent(device_id=device_id, process_tag="CORE", accepted_at=datetime.now(UTC)))
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-isis",
        "isis",
        {
            "status": "ok",
            "interface": [{"interface-name": "ge-0/0/0", "af": "ipv4"}],
            "process": [],  # process silently dropped
        },
    )
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_isis_interfaces", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.failed
        assert job.result["reader_compare"]["isis"] == "missing"
        assert job.result["isis_count_by_outcome"] == {"in_sync": 1, "apply_failed": 1}
        iface_row = (
            (await db.execute(select(IsisInterfaceIntent).where(IsisInterfaceIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        proc_row = (
            (await db.execute(select(IsisProcessIntent).where(IsisProcessIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert iface_row.last_apply_error is None
        assert proc_row.last_apply_error["code"] == "reader_compare_missing"


# ── CR-A17: a community that never LANDED is a silent drop like any other ─────────────────────
#
# The post-apply reader-compare exists to catch the #26 class: the commit reports success and the
# key never reaches the device. It covered every scope but the SNMP community — whose intent key is
# a label and whose export key is sha256(community-string)[:16], a digest of a secret the adapter
# never sees. So the row was simply left out of the check, and the one scope where a silent drop
# means a MISSING CREDENTIAL (monitoring goes blind, and nobody finds out until it matters) was the
# one scope the drop-detector did not cover.
#
# The adapter holds the vault_ref, so it can compute that digest itself.


async def _seed_community(device_id: int, *, label="prod-ro", vault_ref=SNMP_VAULT_REF) -> None:
    from nso_adapter.store.models import SnmpCommunityIntent

    async with session() as db:
        db.add(
            SnmpCommunityIntent(
                device_id=device_id,
                label=label,
                vault_ref=vault_ref,
                access="ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()


async def _apply_snmp(device_id: int, job_id: int, snmp_view: dict) -> Job:
    mock_client = AsyncMock(spec=NsoClient)
    section = {"status": "ok", **snmp_view}

    async def _read(device_name, families, *, timeout=None):
        # reader-compare reads the device-state ACTION; echo the requested device (as the real
        # action does) so the shape is faithful — cert itself is covered in test_device_state_client.py.
        return {"atomic": True, "device-name": device_name, "snmp-config": section}

    mock_client.run_device_state_read.side_effect = _read
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_snmp_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)
    async with session() as db:
        return await db.get(Job, job_id)


async def test_a_community_the_writer_SILENTLY_DROPPED_is_now_caught(adapter_client, vault):
    """The commit said success. The device has no such community. Monitoring is blind and NetBox
    says in_sync. This is exactly the #26 class the check was built for — it just could not see
    into this grain until it could resolve the secret.
    """
    from nso_adapter.store.models import SnmpCommunityIntent

    vault()
    device_id = await _seed_device("rtr-a17-drop", 431)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)

    job = await _apply_snmp(device_id, job_id, {"community": [], "v3-user": [], "host": []})

    assert job.status == JobStatus.failed
    assert job.result["reader_compare"]["snmp"] == "missing"
    async with session() as db:
        row = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error["code"] == "reader_compare_missing"


async def test_a_community_that_DID_land_is_verified_green_not_merely_skipped(adapter_client, vault):
    """It used to pass this case by not looking. Now it looks, resolves the secret, matches the
    digest the device reports, and says ok — the difference between "we checked" and "we didn't".
    """
    vault()
    device_id = await _seed_device("rtr-a17-ok", 432)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)

    job = await _apply_snmp(
        device_id,
        job_id,
        {"community": [{"name": community_export_name(SNMP_COMMUNITY), "access": "ro"}], "v3-user": [], "host": []},
    )

    assert job.status == JobStatus.succeeded
    assert job.result["reader_compare"]["snmp"] == "ok"
    assert job.result["snmp_count_by_outcome"] == {"in_sync": 1, "apply_failed": 0}


async def test_VAULT_DOWN_must_not_stamp_a_landed_community_apply_failed(adapter_client, vault):
    """Fail OPEN, and this is why it matters here more than on the removal side.

    Stamping `reader_compare_missing` because VAULT was unreachable would fail the apply, flip the
    row to apply_failed and pin the plugin's SNMP scope red — for a community sitting on the device
    exactly as intended. A check that cannot run must abstain, not accuse.
    """
    from nso_adapter.store.models import SnmpCommunityIntent

    vault(fail=True)
    device_id = await _seed_device("rtr-a17-vaultdown", 433)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)

    job = await _apply_snmp(
        device_id,
        job_id,
        {"community": [{"name": community_export_name(SNMP_COMMUNITY)}], "v3-user": [], "host": []},
    )

    assert job.status == JobStatus.succeeded
    async with session() as db:
        row = (
            (await db.execute(select(SnmpCommunityIntent).where(SnmpCommunityIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None, "a Vault outage must never accuse the WRITER of dropping"


async def test_a_dropped_HOST_is_still_caught_when_the_community_grain_goes_dark(adapter_client, vault):
    """One grain being unverifiable must not blunt the others — the address-keyed host still fails."""
    from nso_adapter.store.models import SnmpHostIntent

    vault(fail=True)  # community unverifiable
    device_id = await _seed_device("rtr-a17-mixed", 434)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)
    async with session() as db:
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="prod-ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    job = await _apply_snmp(device_id, job_id, {"community": [], "v3-user": [], "host": []})

    assert job.result["reader_compare"]["snmp"] == "missing"
    async with session() as db:
        host = (await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))).scalars().one()
        assert host.last_apply_error["code"] == "reader_compare_missing"


# ── READSEM 1328 — the reader-compare/action-migration behaviours ─────────────────────────────


async def test_mixed_community_and_host_vault_down_is_PARTIAL_not_ok(adapter_client, vault):
    """r3-M2 (a PRE-EXISTING false-green fixed here): a Vault-unverifiable community alongside a
    host that DID land must NOT report a clean 'ok' after checking only the host. It reports
    'partial' and names the unchecked community — symmetric with the residue path's 'partial'.
    'missing' still beats 'partial', so this only fires when nothing checkable is actually absent.
    """
    from nso_adapter.store.models import SnmpHostIntent

    vault(fail=True)  # the community grain goes dark
    device_id = await _seed_device("rtr-a17-partial", 435)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)
    async with session() as db:
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="prod-ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    # the host IS on the device; the community cannot be re-keyed (Vault down)
    job = await _apply_snmp(device_id, job_id, {"community": [], "v3-user": [], "host": [{"address": "198.18.5.9"}]})

    assert job.status == JobStatus.succeeded  # partial is not a failure
    assert job.result["reader_compare"]["snmp"] == "partial"
    assert job.result["reader_compare_unverifiable"]["snmp"], "the unchecked community must be named"
    async with session() as db:
        host = (await db.execute(select(SnmpHostIntent).where(SnmpHostIntent.device_id == device_id))).scalars().one()
        assert host.last_apply_error is None  # the host landed and was verified


async def test_all_unverifiable_scope_runs_NO_action_and_is_unknown(adapter_client, vault):
    """r2-m3: a scope whose every expected key is Vault-unverifiable (a lone community, Vault down)
    must record 'unknown' WITHOUT ever running the (heavy) device-state action — there is nothing
    to look for on the device."""
    vault(fail=True)
    device_id = await _seed_device("rtr-a17-allunver", 436)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)

    mock_client = AsyncMock(spec=NsoClient)
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_snmp_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    mock_client.run_device_state_read.assert_not_awaited()  # never ran the action
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["reader_compare"]["snmp"] == "unknown"


async def test_reader_compare_budget_exhaustion_yields_unknown(adapter_client, monkeypatch):
    """r4-M1: the default-path verify is HARD-bounded. The FIRST scope's action blocks past the
    wall-clock budget → it is asyncio.wait_for-cut to 'unknown'; a LATER checkable scope then sees
    the budget already spent and SKIPS to 'unknown' without running the (heavy) action at all. The
    apply still SUCCEEDS — a slow or semaphore-contended action can never wedge or fail a good apply.
    Exercises both the timeout branch and the remaining<=0 skip (via _reader_compare_checkable)."""
    from nso_adapter.store.models import StaticRouteIntent, VlanIntent

    # shrink both the per-apply budget and the per-call ceiling to sub-second (generous enough
    # that the first scope reliably reaches the action, tight enough that its cut spends the budget)
    monkeypatch.setattr("nso_adapter.core.removal._VERIFY_TOTAL_BUDGET", 0.3)
    monkeypatch.setattr("nso_adapter.core.removal._VERIFY_PER_CALL_TIMEOUT", 0.3)
    device_id = await _seed_device("rtr-rc-budget", 440)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="198.18.40.0/24", next_hop="10.0.0.1", accepted_at=datetime.now(UTC)
            )
        )
        db.add(VlanIntent(device_id=device_id, vlan_id=444, name="rc-budget", accepted_at=datetime.now(UTC)))
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    reads: list[str] = []

    async def _slow_read(device_name, families, *, timeout=None):
        reads.append(families[0])
        await asyncio.sleep(5)  # far past the 0.05s budget — must be cancelled, not awaited
        return _rc_action(device_name, "static_route", {"status": "ok", "route": []})

    mock_client.run_device_state_read.side_effect = _slow_read
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock),
        patch("nso_adapter.nso.apply.apply_vlan_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    # exactly ONE scope ever reached the action (the first); the budget was spent, so the
    # second scope skipped to unknown without a second action call.
    assert len(reads) == 1, f"a budget-spent scope must NOT run the action, got {reads}"
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # the verify never fails the apply
        assert job.result["reader_compare"]["static_route"] == "unknown"  # budget-cut
        assert job.result["reader_compare"]["vlan"] == "unknown"  # budget-spent skip
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None  # never accused of a silent drop


async def test_atomic_reader_compare_batches_ONE_action_for_all_scopes(adapter_client, monkeypatch):
    """r1-m3: with atomic apply on, every scope commits in ONE transaction, so the presence check
    runs ONE batched device-state action for all checkable wire_names (not one per scope), and
    classifies each section independently — a landed route stays ok while a dropped host fails."""
    from nso_adapter.store.models import SnmpHostIntent, StaticRouteIntent

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01-atomic-rc")
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="10.9.9.0/24", next_hop="1.1.1.1", accepted_at=datetime.now(UTC)
            )
        )
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="x",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    calls: list[list[str]] = []

    async def _read(device_name, families, *, timeout=None):
        calls.append(sorted(families))
        return {
            "atomic": True,
            "device-name": device_name,
            "static-route": {"status": "ok", "route": [{"vrf": "", "prefix": "10.9.9.0/24", "next-hop": "1.1.1.1"}]},
            "snmp-config": {"status": "ok", "community": [], "v3-user": [], "host": []},  # host dropped
        }

    mock_client = AsyncMock()
    mock_client.run_device_state_read.side_effect = _read
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", AsyncMock(return_value=None)))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    assert len(calls) == 1, f"expected exactly one batched action, got {calls}"
    assert set(calls[0]) == {"static-route", "snmp-config"}
    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.result["reader_compare"]["static_route"] == "ok"
        assert job.result["reader_compare"]["snmp"] == "missing"  # the host never landed
        assert job.status == JobStatus.failed


async def test_reader_compare_non_terminal_section_is_error(adapter_client):
    """A section carrying a NON-terminal status (a torn 'not-ready' the action should never emit)
    is classified 'error', never walked — the classifier must not treat it as present data. The
    real transport rejects such a response at certification (test_device_state_client.py); here the
    method mock bypasses cert to prove the classifier's own defence."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-notready", 441)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="198.18.41.0/24", next_hop="10.0.0.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-notready", "static_route", {"status": "not-ready"}
    )
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # error never fails a good apply
        assert job.result["reader_compare"]["static_route"] == "error"


# ── codex review (READSEM 1328): verifier robustness ──────────────────────────────────────────


async def test_reader_compare_malformed_ok_section_is_error_not_job_crash(adapter_client):
    """codex P2: a terminal 'ok' section whose nested data is malformed (route: [1] — an int where
    a keyed dict belongs) makes the walker raise. That raise must be contained to reader_compare=
    'error', never escape and turn a SUCCESSFUL device commit into an internal job failure."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await _seed_device("rtr-rc-malformed", 442)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="198.18.42.0/24", next_hop="10.0.0.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-malformed",
        "static_route",
        {"status": "ok", "route": [1]},  # int, not a keyed dict
    )
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # the commit landed — a read-side glitch must not fail it
        assert job.result["reader_compare"]["static_route"] == "error"
        row = (
            (await db.execute(select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .one()
        )
        assert row.last_apply_error is None  # never accused of a silent drop


async def test_atomic_reader_compare_malformed_section_is_error_not_job_crash(adapter_client, monkeypatch):
    """codex P2 (atomic path): the batched classifier is likewise guarded — a malformed section
    for one scope records 'error' and leaves the successful atomic commit intact."""
    from nso_adapter.store.models import StaticRouteIntent

    monkeypatch.setenv("NSO_ADAPTER_ATOMIC_APPLY", "1")
    device_id = await _seed_device(name="sw01-atomic-malformed")
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="10.9.42.0/24", next_hop="1.1.1.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()
    job_id = await _seed_apply_job(device_id)

    mock_client = AsyncMock()
    mock_client.run_device_state_read.return_value = {
        "atomic": True,
        "device-name": "sw01-atomic-malformed",
        "static-route": {"status": "ok", "route": [1]},  # malformed
    }
    with ExitStack() as stack:
        stack.enter_context(patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client))
        stack.enter_context(patch("nso_adapter.nso.apply.apply_combined", AsyncMock(return_value=None)))
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["reader_compare"]["static_route"] == "error"


async def test_action_failure_preserves_unverifiable_labels(adapter_client, vault):
    """codex P3: when the action RAISES after translation already flagged a Vault-unverifiable
    community (a translatable host kept the scope checkable, so the action did run), the default
    path must still record reader_compare_unverifiable — symmetric with the atomic and residue
    paths — not drop it on the error branch."""
    from nso_adapter.store.models import SnmpHostIntent

    vault(fail=True)  # the community grain is unverifiable
    device_id = await _seed_device("rtr-a17-actfail", 443)
    job_id = await _seed_apply_job(device_id)
    await _seed_community(device_id)
    async with session() as db:
        db.add(
            SnmpHostIntent(
                device_id=device_id,
                address="198.18.5.9",
                version="2c",
                notify_type="traps",
                community_or_user="prod-ro",
                accepted_at=datetime.now(UTC),
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.side_effect = RuntimeError("action exploded")
    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_snmp_config", new_callable=AsyncMock),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded  # a read error never fails a good apply
        assert job.result["reader_compare"]["snmp"] == "error"
        assert job.result["reader_compare_unverifiable"]["snmp"], "the unverifiable community must survive the error"


async def test_verifier_budget_excludes_commit_latency(adapter_client, monkeypatch):
    """codex P1: only VERIFY time counts against _VERIFY_TOTAL_BUDGET — a slow device COMMIT must
    not starve the scope's own silent-drop verification. With a 0.1s budget and a 0.5s commit, the
    scope must STILL be verified ('ok'), not skipped to 'unknown' because the commit ate the clock."""
    from nso_adapter.store.models import StaticRouteIntent

    monkeypatch.setattr("nso_adapter.core.removal._VERIFY_TOTAL_BUDGET", 0.1)
    device_id = await _seed_device("rtr-rc-commitslow", 444)
    job_id = await _seed_apply_job(device_id)
    async with session() as db:
        db.add(
            StaticRouteIntent(
                device_id=device_id, vrf="", prefix="198.18.44.0/24", next_hop="10.0.0.1", accepted_at=datetime.now(UTC)
            )
        )
        await db.commit()

    mock_client = AsyncMock(spec=NsoClient)
    mock_client.run_device_state_read.return_value = _rc_action(
        "rtr-rc-commitslow",
        "static_route",
        {"status": "ok", "route": [{"vrf": "", "prefix": "198.18.44.0/24", "next-hop": "10.0.0.1"}]},
    )

    async def _slow_commit(*_a, **_k):
        await asyncio.sleep(0.5)  # the device commit dwarfs the 0.1s verify budget

    with (
        patch("nso_adapter.core.importer.get_nso_client", return_value=mock_client),
        patch("nso_adapter.nso.apply.apply_static_routes", new_callable=AsyncMock, side_effect=_slow_commit),
    ):
        await run_apply(job_id=job_id, device_id=device_id, force=True)

    async with session() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.succeeded
        assert job.result["reader_compare"]["static_route"] == "ok"  # verified despite the slow commit
