# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for sync_device and detect_drift using NSO package oper-data."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from nso_adapter.bindings.netbox.client import NetboxClient
from nso_adapter.core.importer import _attrs_to_interface_list, sync_device
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import (
    Base,
    DbInterface,
    Device,
    InterfaceAttrState,
    LastSyncStatus,
    ManagedScope,
    MappingStatus,
)


@pytest_asyncio.fixture
async def db_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        yield session
    await engine.dispose()


def _make_nso_client(iface_entry=None, sections=None):
    """Build a fake NsoClient (spec-bound to the real interface).

    interface_attributes is envelope-flipped (READSEM S3 B5): *iface_entry* becomes the
    ``interface-attributes`` section (ok-wrapped; ``None`` = confirmed device absence →
    present-policy keep, the old 404 shape). Other families' sections default to an
    ERROR section (keep + degraded surface) unless supplied via *sections* — matching
    the pre-port behavior where un-stubbed surface reads were degraded.
    """
    client = AsyncMock(spec=NsoClient)
    client.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")
    routed = {"interface-attributes": None if iface_entry is None else {"status": "ok", **iface_entry}}
    routed.update(sections or {})
    client._sections = routed

    async def _get_section(device_name, wire_family):
        return routed.get(wire_family, {"status": "error", "error-reason": "not stubbed in this test"})

    client.get_device_state_section = AsyncMock(side_effect=_get_section)
    return client


# ── _attrs_to_interface_list unit tests ──────────────────────────────────────


def test_attrs_to_interface_list_description_and_enabled():
    entry = {
        "device-name": "sw01",
        "interface": [
            {"interface-name": "GigabitEthernet0/1", "description": "uplink", "enabled": True},
            {"interface-name": "GigabitEthernet0/2", "enabled": False},
        ],
    }
    result = _attrs_to_interface_list(entry)
    assert len(result) == 2
    ge01 = next(i for i in result if i.name == "GigabitEthernet0/1")
    assert ge01.nso.description == "uplink"
    assert ge01.nso.enabled is True
    ge02 = next(i for i in result if i.name == "GigabitEthernet0/2")
    assert ge02.nso.description is None
    assert ge02.nso.enabled is False


def test_attrs_to_interface_list_m27r_logical_fields():
    """M27R: parent-binding/kind/encap-tag/vrf/service pass through; empty → None."""
    entry = {
        "device-name": "ra1",
        "interface": [
            {
                "interface-name": "LAG99:10",
                "enabled": True,
                "kind": "logical",
                "parent-binding": "lag-99",
                "encap-tag": "10",
                "vrf": "",
                "service": "",
            },
            {"interface-name": "1/1/c1", "enabled": True, "kind": "physical"},
        ],
    }
    result = _attrs_to_interface_list(entry)
    logical = next(i for i in result if i.name == "LAG99:10")
    assert logical.kind == "logical"
    assert logical.parent_binding == "lag-99"
    assert logical.encap_tag == "10"
    assert logical.vrf is None  # empty string collapses to None
    port = next(i for i in result if i.name == "1/1/c1")
    assert port.kind == "physical"
    assert port.parent_binding is None
    assert port.encap_tag is None


def test_attrs_to_interface_list_returns_empty_on_none():
    assert _attrs_to_interface_list(None) == []


def test_attrs_to_interface_list_returns_empty_when_no_interface_key():
    assert _attrs_to_interface_list({"device-name": "sw01"}) == []


def test_attrs_to_interface_list_skips_malformed_entry():
    """Entries without interface-name are skipped; valid entries are returned."""
    entry = {
        "interface": [
            {"interface-name": "GigabitEthernet0/1", "enabled": True},
            {"enabled": True},  # malformed — no interface-name
            {"interface-name": "GigabitEthernet0/2", "enabled": False},
        ]
    }
    result = _attrs_to_interface_list(entry)
    assert len(result) == 2
    assert result[0].name == "GigabitEthernet0/1"
    assert result[1].name == "GigabitEthernet0/2"


def test_attrs_to_interface_list_enabled_absent_yields_none():
    """When NSO package omits 'enabled', the domain object carries None — not True/False."""
    entry = {"interface": [{"interface-name": "GigabitEthernet0/1", "description": "uplink"}]}
    result = _attrs_to_interface_list(entry)
    assert len(result) == 1
    assert result[0].nso.enabled is None


async def test_sync_device_creates_interface_rows(db_session: AsyncSession):
    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw01",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=1,
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    db_session.add(ManagedScope(device=device, attribute="enabled"))
    await db_session.commit()

    iface_entry = {
        "device-name": "sw01",
        "interface": [
            {"interface-name": "GigabitEthernet0/1", "description": "link", "enabled": True},
        ],
    }
    nso_client = _make_nso_client(iface_entry)

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        summary = await sync_device(device.id, db_session)

    assert summary["interfaces_created"] == 1
    result = await db_session.execute(select(DbInterface).where(DbInterface.device_id == device.id))
    ifaces = result.scalars().all()
    assert len(ifaces) == 1
    assert ifaces[0].name == "GigabitEthernet0/1"


async def test_sync_device_calls_get_interface_attributes(db_session: AsyncSession):
    """sync_device() uses get_interface_attributes() instead of get_device_config() + normalize."""
    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw04",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=4,
    )
    db_session.add(device)
    await db_session.commit()

    nso_client = _make_nso_client(None)

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        await sync_device(device.id, db_session)

    nso_client.get_device_state_section.assert_any_await("sw04", "interface-attributes")
    # get_device_config must NOT be called
    nso_client.get_device_config.assert_not_called()


async def test_sync_device_marks_unmatched_interfaces_when_empty(db_session: AsyncSession):
    """An AUTHORITATIVE present-empty attrs read (200 with zero interfaces) → unmatched_interfaces.

    A present-empty read is the export saying "this device really has no interfaces we manage";
    that is distinct from a 404/None (unavailable), covered by the keeps-mapping test below.
    """
    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw02",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=2,
    )
    db_session.add(device)
    await db_session.commit()

    nso_client = _make_nso_client({"device-name": "sw02", "interface": []})

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        await sync_device(device.id, db_session)

    await db_session.refresh(device)
    assert device.mapping_status == MappingStatus.unmatched_interfaces


async def test_sync_device_keeps_mapping_when_attrs_unavailable(db_session: AsyncSession):
    """A 404/None attrs read (export down / unsupported NED / not-ready) must NOT demote a
    mapped device to unmatched_interfaces — it keeps the prior mapping and reports the
    interface_attributes surface degraded.

    RED against the old code, which read None as an empty interface list and flipped
    mapping_status to unmatched_interfaces on every transient export blip.
    """
    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw05",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=5,
        mapping_status=MappingStatus.mapped,  # a prior healthy sync mapped it
    )
    db_session.add(device)
    await db_session.commit()

    nso_client = _make_nso_client(None)  # 404 → present-policy not-authoritative

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        await sync_device(device.id, db_session)

    await db_session.refresh(device)
    assert device.mapping_status == MappingStatus.mapped  # kept, NOT demoted
    assert device.last_sync_status == LastSyncStatus.partial
    assert "interface_attributes" in (device.degraded_surfaces or [])


# ── detect_drift integration test ────────────────────────────────────────


async def test_detect_drift_uses_interface_attributes(db_session: AsyncSession):
    from nso_adapter.core.importer import detect_drift
    from nso_adapter.store.models import InterfaceAttrState, SyncState

    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw03",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=3,
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    iface_row = DbInterface(device=device, name="GigabitEthernet0/1")
    db_session.add(iface_row)
    await db_session.commit()

    # Pre-existing attr state so sync_state can compare
    attr_state = InterfaceAttrState(
        interface_id=iface_row.id,
        attribute="description",
        netbox_value="old-desc",
        nso_value="old-desc",
        sync_state=SyncState.imported,
    )
    db_session.add(attr_state)
    await db_session.commit()

    iface_entry = {
        "device-name": "sw03",
        "interface": [
            {"interface-name": "GigabitEthernet0/1", "description": "new-desc", "enabled": True},
        ],
    }
    nso_client = _make_nso_client(iface_entry)
    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = nso_client

    with patch("nso_adapter.core.importer.nso_actions.compare_config", new_callable=AsyncMock):
        summary = await detect_drift(device.id, db_session)

    nso_client.get_device_state_section.assert_any_await("sw03", "interface-attributes")
    assert summary["changes_detected"] == 1


async def test_detect_drift_sees_value_set_directly_in_netbox(db_session: AsyncSession):
    """A description typed straight into NetBox is caught as drift.

    Reproduces device-27 ae2.0: NSO has no description and the cached netbox_value
    matches (both empty), so the OLD cache-only comparison reported no drift. The
    operator set a description directly in NetBox; detect_drift must compare against
    the LIVE NetBox value and report drift — without persisting netbox_value (which
    would let the next sync clobber the operator's edit).
    """
    from nso_adapter.core import importer as imp
    from nso_adapter.core.importer import detect_drift
    from nso_adapter.store.models import InterfaceAttrState, SyncState

    device = Device(
        nso_instance="nso-dev",
        nso_device_name="junos01",
        ned_id="juniper-junos-nc-1.0",
        netbox_device_id=27,
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    iface_row = DbInterface(device=device, name="ae2.0")
    db_session.add(iface_row)
    await db_session.commit()

    # Cache says empty == empty → the old comparison would NOT flag drift.
    attr_state = InterfaceAttrState(
        interface_id=iface_row.id,
        attribute="description",
        netbox_value=None,
        nso_value=None,
        sync_state=SyncState.imported,
    )
    db_session.add(attr_state)
    await db_session.commit()

    # NSO still reports no description for ae2.0 (it's a Junos unit).
    iface_entry = {"device-name": "junos01", "interface": [{"interface-name": "ae2.0", "enabled": True}]}
    imp._nso_clients["nso-dev"] = _make_nso_client(iface_entry)

    # LIVE NetBox carries a description the device lacks.
    nb = AsyncMock(spec=NetboxClient)
    nb.list_interfaces = AsyncMock(
        return_value=[{"id": 1382, "name": "ae2.0", "description": "Core Link", "enabled": True}]
    )
    nb.notify_sync_complete = AsyncMock()
    imp._netbox_client = nb

    try:
        with patch("nso_adapter.core.importer.nso_actions.compare_config", new_callable=AsyncMock):
            summary = await detect_drift(device.id, db_session)
    finally:
        imp._netbox_client = None

    assert summary["changes_detected"] == 1
    await db_session.refresh(attr_state)
    assert attr_state.sync_state == SyncState.changed
    # Clobber-safety: detect_drift must NOT persist the live NetBox value into the cache.
    assert attr_state.netbox_value is None


async def test_sync_change_detection_skips_unchanged_on_resync(db_session: AsyncSession):
    """Second sync with identical NSO values issues ZERO NetBox writes (idempotent)."""
    from unittest.mock import AsyncMock

    from nso_adapter.core import importer as imp

    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw-idem",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=7,
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    db_session.add(ManagedScope(device=device, attribute="enabled"))
    await db_session.commit()

    iface_entry = {
        "device-name": "sw-idem",
        "interface": [
            {"interface-name": "GigabitEthernet0/1", "description": "link", "enabled": True},
        ],
    }
    imp._nso_clients["nso-dev"] = _make_nso_client(iface_entry)

    # Mock NetBox client: bulk_ensure resolves the interface; bulk_patch echoes
    # the patched rows back (confirming the writes), capturing payloads.
    nb = AsyncMock(spec=NetboxClient)
    nb.list_interfaces = AsyncMock(return_value=[{"id": 500, "name": "GigabitEthernet0/1", "parent": None}])
    nb.bulk_create_interfaces = AsyncMock(return_value=[])
    patched_batches = []

    def _patch(p):
        rows = list(p)
        patched_batches.append(rows)
        return [{"id": r["id"]} for r in rows]  # echo = confirmed written

    nb.bulk_patch_interfaces = AsyncMock(side_effect=_patch)
    imp._netbox_client = nb

    try:
        with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
            first = await sync_device(device.id, db_session)
            assert first["interfaces_written"] > 0  # first run writes
            patched_batches.clear()
            second = await sync_device(device.id, db_session)
    finally:
        imp._netbox_client = None

    # Second sync: nothing changed → no writes enqueued.
    assert second["interfaces_written"] == 0
    assert patched_batches == [] or all(len(b) == 0 for b in patched_batches)


async def test_sync_failed_patch_not_marked_synced(db_session: AsyncSession):
    """If the bulk PATCH does NOT confirm an id, its netbox_value stays old and it
    is re-enqueued on the next sync (no false 'synced')."""
    from unittest.mock import AsyncMock

    from nso_adapter.core import importer as imp

    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw-fail",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=8,
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    iface_entry = {
        "device-name": "sw-fail",
        "interface": [{"interface-name": "GigabitEthernet0/1", "description": "newdesc"}],
    }
    imp._nso_clients["nso-dev"] = _make_nso_client(iface_entry)

    nb = AsyncMock(spec=NetboxClient)
    nb.list_interfaces = AsyncMock(return_value=[{"id": 600, "name": "GigabitEthernet0/1", "parent": None}])
    nb.bulk_create_interfaces = AsyncMock(return_value=[])
    # Simulate a failed/timed-out batch: confirm NOTHING.
    nb.bulk_patch_interfaces = AsyncMock(return_value=[])
    imp._netbox_client = nb

    try:
        with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
            first = await sync_device(device.id, db_session)
            assert first["interfaces_written"] == 0  # nothing confirmed
            # state must NOT be marked synced
            row = (
                (await db_session.execute(select(DbInterface).where(DbInterface.device_id == device.id)))
                .scalars()
                .first()
            )
            attr = (
                (await db_session.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == row.id)))
                .scalars()
                .first()
            )
            assert attr.netbox_value != "newdesc"  # not falsely recorded

            # Next sync re-enqueues it (still a delta).
            enqueued = []
            nb.bulk_patch_interfaces = AsyncMock(
                side_effect=lambda p: enqueued.extend(p) or [{"id": r["id"]} for r in p]
            )
            await sync_device(device.id, db_session)
            assert any(r["id"] == 600 for r in enqueued)
    finally:
        imp._netbox_client = None


async def test_sync_device_fans_out_to_routing_surfaces(db_session: AsyncSession):
    """sync_device runs the routing-surface fan-out so 'Sync Now' refreshes IS-IS/BGP/
    OSPF/route-policy/... for the device, not just interface attributes."""
    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw01",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=1,
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    nso_client = _make_nso_client({"device-name": "sw01", "interface": []})

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with (
        patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock),
        patch(
            "nso_adapter.core.importer.refresh_routing_surfaces_for_device",
            new_callable=AsyncMock,
            return_value=[],
        ) as fanout,
    ):
        await sync_device(device.id, db_session)

    fanout.assert_awaited_once()
    assert fanout.await_args.args[1].id == device.id
    assert fanout.await_args.kwargs.get("refresh_source") == "sync"


async def test_refresh_routing_surfaces_isolates_failures(db_session: AsyncSession):
    """READSEM grain b: ONE projected doc feeds the fan-out; a surface whose
    materialization raises must not abort the others."""
    from sqlalchemy import select as _select

    from nso_adapter.core.importer import refresh_routing_surfaces_for_device
    from nso_adapter.store.models import DeviceStaticRoute

    device = Device(nso_instance="nso-dev", nso_device_name="sw01", ned_id="x", netbox_device_id=1)
    db_session.add(device)
    await db_session.commit()

    nso_client = AsyncMock(spec=NsoClient)
    nso_client.get_device_state_doc.return_value = {
        "device-name": "sw01",
        "static-route": {"status": "ok", "route": [{"vrf": "", "prefix": "10.5.0.0/16", "next-hop": "1.1.1.1"}]},
        "isis-interface": {"status": "ok", "process": 42},  # deterministic materializer crash
        "bgp-config": {"status": "error", "error-reason": "extract boom"},  # signalled failure
        "ospf-config": {"status": "ok"},
        "route-policy": {"status": "ok"},
        "snmp-config": {"status": "ok"},
        "logging-config": {"status": "ok"},
        "bfd-config": {"status": "ok"},
        "interface-ip": {"status": "ok"},
    }

    failed = await refresh_routing_surfaces_for_device(db_session, device, nso_client, refresh_source="sync")

    # isis raised inside its materializer, bgp signalled failure; NEITHER aborted the rest:
    # static_route (earlier) materialized its row, and redistribution (later, S3-R3 F2:
    # classified from the SAME snapshot, no wrapper) still recorded a composite outcome.
    assert "isis" in failed
    rows = (
        (await db_session.execute(_select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
        .scalars()
        .all()
    )
    assert [r.prefix for r in rows] == ["10.5.0.0/16"]
    from nso_adapter.store.models import RefreshOutcome as _RO

    redis_outcomes = (
        (await db_session.execute(_select(_RO).where(_RO.device_id == device.id, _RO.family == "redistribution")))
        .scalars()
        .all()
    )
    assert redis_outcomes, "redistribution must classify from the shared snapshot and record"
    nso_client.get_device_state_doc.assert_awaited_once()  # ONE doc GET fed every surface incl. redistribution


async def test_surface_refresher_returns_false_on_read_error(db_session: AsyncSession):
    """A surface refresher must SIGNAL a swallowed NSO read failure (return False), so the
    fan-out can mark the device partial instead of hiding a stale mirror under 'succeeded'."""
    from nso_adapter.core.bgp import refresh_bgp_config_for_device

    device = Device(nso_instance="nso-dev", nso_device_name="sw01", ned_id="x", netbox_device_id=1)
    db_session.add(device)
    await db_session.commit()

    client = AsyncMock(spec=NsoClient)
    client.get_device_state_section = AsyncMock(side_effect=RuntimeError("nso down"))
    ok = await refresh_bgp_config_for_device(db_session, device, client, refresh_source="sync")
    assert ok is False


async def test_surface_refresher_returns_true_on_success(db_session: AsyncSession):
    """A clean read returns True (not degraded), even with no config present."""
    from nso_adapter.core.bgp import refresh_bgp_config_for_device

    device = Device(nso_instance="nso-dev", nso_device_name="sw01", ned_id="x", netbox_device_id=1)
    db_session.add(device)
    await db_session.commit()

    client = AsyncMock(spec=NsoClient)
    client.get_device_state_section = AsyncMock(return_value={"status": "ok", "router": []})
    ok = await refresh_bgp_config_for_device(db_session, device, client, refresh_source="sync")
    assert ok is True


async def test_refresh_routing_surfaces_returns_failed_surfaces(db_session: AsyncSession):
    """The fan-out returns the names of surfaces that failed — whether they RAISED (isis:
    materializer crash) or signalled a read failure (bgp: error-status section)."""
    from nso_adapter.core.importer import refresh_routing_surfaces_for_device

    device = Device(nso_instance="nso-dev", nso_device_name="sw01", ned_id="x", netbox_device_id=1)
    db_session.add(device)
    await db_session.commit()

    nso_client = AsyncMock(spec=NsoClient)
    nso_client.get_device_state_doc.return_value = {
        "device-name": "sw01",
        "static-route": {"status": "ok", "route": [{"vrf": "", "prefix": "10.5.0.0/16", "next-hop": "1.1.1.1"}]},
        "isis-interface": {"status": "ok", "process": 42},  # deterministic materializer crash
        "bgp-config": {"status": "error", "error-reason": "extract boom"},  # signalled failure
        "ospf-config": {"status": "ok"},
        "route-policy": {"status": "ok"},
        "snmp-config": {"status": "ok"},
        "logging-config": {"status": "ok"},
        "bfd-config": {"status": "ok"},
        "interface-ip": {"status": "ok"},
    }

    failed = await refresh_routing_surfaces_for_device(db_session, device, nso_client, refresh_source="sync")

    # bgp's error section also degrades redistribution's bgp COMPONENT (same snapshot,
    # per-component retention) - the composite honestly reports too (S3-R3 F2).
    assert sorted(failed) == ["bgp", "isis", "redistribution"]


async def test_refresh_config_surfaces_isolates_failures(db_session: AsyncSession):
    """The post-apply config-surface fan-out is best-effort under the projection too: one
    surface raising must not abort the others."""
    from sqlalchemy import select as _select

    from nso_adapter.core.importer import refresh_config_surfaces_for_device
    from nso_adapter.store.models import DeviceVlan, RefreshOutcome

    device = Device(nso_instance="nso-dev", nso_device_name="sw-cfg", ned_id="x", netbox_device_id=40)
    db_session.add(device)
    await db_session.commit()

    nso_client = AsyncMock(spec=NsoClient)
    nso_client.get_device_state_doc.return_value = {
        "device-name": "sw-cfg",
        "vlan-database": {"status": "ok", "vlan": [{"vlan-id": 7, "name": "SEVEN"}]},
        "svi": {"status": "ok", "interface": [999]},  # deterministic materializer crash
        "subinterface": {"status": "ok"},
        "interface-mtu": {"status": "ok"},
    }

    failed = await refresh_config_surfaces_for_device(db_session, device, nso_client, refresh_source="apply")

    assert "svi" in failed
    vlans = (await db_session.execute(_select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
    assert [(v.vlan_id, v.name) for v in vlans] == [(7, "SEVEN")]
    # every surface was ATTEMPTED (each records an outcome row), including the one that raised
    fams = {
        o.family
        for o in (await db_session.execute(_select(RefreshOutcome).where(RefreshOutcome.device_id == device.id)))
        .scalars()
        .all()
    }
    assert {"vlan", "svi", "subinterface", "interface_mtu"} <= fams


async def test_refresh_config_surfaces_skips_all_when_disabled(db_session: AsyncSession, monkeypatch):
    """With every config-surface flag off, the fan-out builds no surface list and is a clean no-op."""
    import types

    from nso_adapter.core.importer import refresh_config_surfaces_for_device

    device = Device(nso_instance="nso-dev", nso_device_name="sw-cfg-off", ned_id="x", netbox_device_id=41)
    db_session.add(device)
    await db_session.commit()

    cfg = types.SimpleNamespace(
        scheduler=types.SimpleNamespace(
            enable_vlan_sync=False,
            enable_svi_sync=False,
            enable_subinterface_sync=False,
            enable_interface_mtu_sync=False,
        )
    )
    monkeypatch.setattr("nso_adapter.core.importer.get_config", lambda: cfg)

    # No surfaces enabled → the loop body never runs; must not raise.
    await refresh_config_surfaces_for_device(db_session, device, AsyncMock(), refresh_source="apply")


# ── discover_devices + helpers (paydown of the grandfathered coverage omit) ────

import types  # noqa: E402

from nso_adapter.core.importer import (  # noqa: E402
    _attr_str,
    _load_intent_by_attr,
    discover_devices,
    get_nso_client,
)
from nso_adapter.store.models import InterfaceIntent  # noqa: E402


def _cfg(*instance_names: str):
    return types.SimpleNamespace(nso_instances=[types.SimpleNamespace(name=n) for n in instance_names])


def _discover_client(names_or_exc):
    client = AsyncMock()
    if isinstance(names_or_exc, Exception):
        client.list_devices = AsyncMock(side_effect=names_or_exc)
    else:
        client.list_devices = AsyncMock(return_value=names_or_exc)
    return client


async def test_discover_devices_creates_new_devices(db_session: AsyncSession):
    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = _discover_client([{"name": "sw01"}, {"name": "sw02"}])
    with patch("nso_adapter.core.importer.get_config", return_value=_cfg("nso-dev")):
        await discover_devices(db_session)

    rows = (await db_session.execute(select(Device).where(Device.nso_instance == "nso-dev"))).scalars().all()
    assert sorted(d.nso_device_name for d in rows) == ["sw01", "sw02"]


async def test_discover_devices_skips_nameless_and_does_not_duplicate(db_session: AsyncSession):
    db_session.add(Device(nso_instance="nso-dev", nso_device_name="sw01"))
    await db_session.commit()

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = _discover_client([{"name": "sw01"}, {"name": ""}, {}, {"name": "sw02"}])
    with patch("nso_adapter.core.importer.get_config", return_value=_cfg("nso-dev")):
        await discover_devices(db_session)

    rows = (await db_session.execute(select(Device).where(Device.nso_instance == "nso-dev"))).scalars().all()
    assert sorted(d.nso_device_name for d in rows) == ["sw01", "sw02"]  # no dup, no nameless


async def test_discover_devices_continues_on_list_error(db_session: AsyncSession):
    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = _discover_client(RuntimeError("NSO down"))
    with patch("nso_adapter.core.importer.get_config", return_value=_cfg("nso-dev")):
        await discover_devices(db_session)  # must not raise

    rows = (await db_session.execute(select(Device))).scalars().all()
    assert rows == []


def test_attr_str_normalises_description_and_enabled():
    assert _attr_str("description", "") is None  # blank description collapses to None
    assert _attr_str("description", None) is None
    assert _attr_str("description", "uplink") == "uplink"
    assert _attr_str("enabled", False) == "False"  # enabled keeps its bool string
    assert _attr_str("mtu", None) is None
    assert _attr_str("mtu", 1500) == "1500"


def test_get_nso_client_unregistered_raises():
    with pytest.raises(RuntimeError, match="not registered"):
        get_nso_client("does-not-exist")


def test_register_and_set_client_round_trip():
    """register_nso_client / set_netbox_client store what get_* return."""
    from nso_adapter.core import importer as imp

    sentinel = object()
    imp.register_nso_client("rt-inst", sentinel)
    assert imp.get_nso_client("rt-inst") is sentinel

    nb = object()
    imp.set_netbox_client(nb)
    assert imp.get_netbox_client() is nb
    imp.set_netbox_client(None)  # reset so other tests aren't affected


async def test_load_intent_by_attr_returns_attribute_value_map(db_session: AsyncSession):
    device = Device(nso_instance="nso-dev", nso_device_name="sw09")
    db_session.add(device)
    await db_session.commit()
    iface = DbInterface(device_id=device.id, name="GigabitEthernet0/1")
    db_session.add(iface)
    await db_session.commit()
    db_session.add(InterfaceIntent(interface_id=iface.id, attribute="description", intent_value="uplink"))
    db_session.add(InterfaceIntent(interface_id=iface.id, attribute="enabled", intent_value="True"))
    await db_session.commit()

    intent = await _load_intent_by_attr(db_session, iface.id)
    assert intent == {"description": "uplink", "enabled": "True"}


# ── sync_device branch coverage (404 / NED-fail / netbox-error / intent / notify) ──


async def test_sync_device_unknown_device_raises(db_session: AsyncSession):
    with pytest.raises(ValueError, match="not found"):
        await sync_device(999999, db_session)


async def test_sync_device_unresolved_ned_marks_unmatched(db_session: AsyncSession):
    """A device whose NED can't be resolved is marked unmatched_device + failed, then raises."""
    from nso_adapter.core import importer as imp
    from nso_adapter.store.models import LastSyncStatus

    device = Device(nso_instance="nso-dev", nso_device_name="sw-noned", netbox_device_id=10)  # ned_id None
    db_session.add(device)
    await db_session.commit()

    client = AsyncMock()
    client.get_device_ned_id = AsyncMock(return_value="")  # NSO can't resolve a NED
    imp._nso_clients["nso-dev"] = client
    imp._netbox_client = None

    with pytest.raises(ValueError, match="no NED ID"):
        await sync_device(device.id, db_session)

    await db_session.refresh(device)
    assert device.mapping_status == MappingStatus.unmatched_device
    assert device.last_sync_status == LastSyncStatus.failed


async def test_sync_device_swallows_bulk_ensure_error(db_session: AsyncSession):
    """A NetBox failure during Phase-1 bulk-ensure is logged, not fatal; the sync still lands rows."""
    from nso_adapter.core import importer as imp

    device = Device(
        nso_instance="nso-dev", nso_device_name="sw-nberr", ned_id="cisco-ios-cli-6.95", netbox_device_id=11
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    iface_entry = {"interface": [{"interface-name": "Gi0/0", "description": "x", "enabled": True}]}
    imp._nso_clients["nso-dev"] = _make_nso_client(iface_entry)

    nb = AsyncMock(spec=NetboxClient)
    nb.list_interfaces = AsyncMock(side_effect=RuntimeError("netbox down"))  # bulk_ensure blows up
    nb.notify_sync_complete = AsyncMock()
    imp._netbox_client = nb

    try:
        with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
            summary = await sync_device(device.id, db_session)  # must not raise
    finally:
        imp._netbox_client = None

    assert summary["interfaces_created"] == 1  # DB row still created despite NetBox error
    assert summary["interfaces_written"] == 0  # nothing written (no nb ids resolved)


async def test_sync_device_skips_enabled_write_when_nso_omits_it(db_session: AsyncSession):
    """A pre-synced 'enabled' that NSO stops reporting is not written (the else-continue branch)."""
    from nso_adapter.core import importer as imp
    from nso_adapter.store.models import SyncState

    device = Device(nso_instance="nso-dev", nso_device_name="sw-en", ned_id="cisco-ios-cli-6.95", netbox_device_id=12)
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="enabled"))
    await db_session.commit()
    iface_row = DbInterface(device=device, name="Gi0/0")
    db_session.add(iface_row)
    await db_session.commit()
    db_session.add(
        InterfaceAttrState(
            interface_id=iface_row.id,
            attribute="enabled",
            netbox_value="True",
            nso_value="True",
            sync_state=SyncState.imported,
        )
    )
    await db_session.commit()

    # NSO now reports the interface WITHOUT 'enabled' (None) — prev "True" != None, but no value to write.
    imp._nso_clients["nso-dev"] = _make_nso_client({"interface": [{"interface-name": "Gi0/0", "description": "x"}]})
    nb = AsyncMock(spec=NetboxClient)
    nb.list_interfaces = AsyncMock(return_value=[{"id": 700, "name": "Gi0/0", "parent": None}])
    nb.bulk_create_interfaces = AsyncMock(return_value=[])
    nb.bulk_patch_interfaces = AsyncMock(return_value=[])
    nb.notify_sync_complete = AsyncMock()
    imp._netbox_client = nb

    try:
        with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
            await sync_device(device.id, db_session)
    finally:
        imp._netbox_client = None

    nb.bulk_patch_interfaces.assert_not_called()  # the unreported 'enabled' produced no patch


async def test_sync_device_phase2_uses_intent_state(db_session: AsyncSession):
    """With a deployed intent that differs from the live NSO value, the attr goes to a Phase-2 state."""
    from nso_adapter.core import importer as imp
    from nso_adapter.store.models import InterfaceIntent, SyncState

    device = Device(nso_instance="nso-dev", nso_device_name="sw-int", ned_id="cisco-ios-cli-6.95", netbox_device_id=13)
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()
    iface_row = DbInterface(device=device, name="Gi0/0")
    db_session.add(iface_row)
    await db_session.commit()
    db_session.add(InterfaceIntent(interface_id=iface_row.id, attribute="description", intent_value="deployed"))
    await db_session.commit()

    # NSO reports a description that differs from the deployed intent → drift.
    imp._nso_clients["nso-dev"] = _make_nso_client(
        {"interface": [{"interface-name": "Gi0/0", "description": "live", "enabled": True}]}
    )
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        summary = await sync_device(device.id, db_session)

    # intent != live NSO → Phase-2 'drifted'. changes_detected counts only Phase-1 'changed'.
    assert summary["changes_detected"] == 0
    attr = (
        (await db_session.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == iface_row.id)))
        .scalars()
        .one()
    )
    assert attr.sync_state == SyncState.drifted  # Phase-2 state from compute_sync_state, not "imported"


async def test_sync_device_resolves_ned_when_unset(db_session: AsyncSession):
    """A device with no ned_id has it resolved from NSO, then the sync proceeds normally."""
    from nso_adapter.core import importer as imp

    device = Device(nso_instance="nso-dev", nso_device_name="sw-resolve", netbox_device_id=15)  # ned_id None
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    # _make_nso_client.get_device_ned_id returns a valid NED → resolve succeeds.
    imp._nso_clients["nso-dev"] = _make_nso_client(
        {"interface": [{"interface-name": "Gi0/0", "description": "x", "enabled": True}]}
    )
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        summary = await sync_device(device.id, db_session)

    await db_session.refresh(device)
    assert device.ned_id == "cisco-ios-cli-6.95"  # resolved + persisted
    assert summary["interfaces_created"] == 1


async def test_sync_device_updates_ned_when_changed_in_nso(db_session: AsyncSession):
    """A device whose NED was CHANGED in NSO re-learns the new ned_id on sync.

    ned_id keys the capability matrix, so a stale value silently mis-keys every verdict.
    Regression: _resolve_ned_id used to bail on ``if device.ned_id`` and never refresh, so a
    NED change on the device was never picked up (found via nso-vendor-test on the Arrcus dev 23:
    NSO reported arcos-v8.1.2X-nc-1.0 while the adapter still held arrcus-arcos-nc-8.1.3).
    """
    from nso_adapter.core import importer as imp

    device = Device(
        nso_instance="nso-dev", nso_device_name="sw-rened", ned_id="arrcus-arcos-nc-8.1.3", netbox_device_id=16
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    client = _make_nso_client({"interface": [{"interface-name": "Gi0/0", "description": "x", "enabled": True}]})
    client.get_device_ned_id = AsyncMock(return_value="arcos-v8.1.2X-nc-1.0")  # NED changed in NSO
    imp._nso_clients["nso-dev"] = client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        await sync_device(device.id, db_session)

    await db_session.refresh(device)
    assert device.ned_id == "arcos-v8.1.2X-nc-1.0"  # re-learned, not stuck on the old value


async def test_sync_device_keeps_ned_when_nso_read_returns_nothing(db_session: AsyncSession):
    """A transient NSO read that returns no ned_id must NOT wipe a previously-known ned_id."""
    from nso_adapter.core import importer as imp

    device = Device(
        nso_instance="nso-dev", nso_device_name="sw-keepned", ned_id="cisco-ios-cli-6.95", netbox_device_id=17
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    client = _make_nso_client({"interface": [{"interface-name": "Gi0/0", "description": "x", "enabled": True}]})
    client.get_device_ned_id = AsyncMock(return_value="")  # transient: NSO returned nothing
    imp._nso_clients["nso-dev"] = client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        await sync_device(device.id, db_session)

    await db_session.refresh(device)
    assert device.ned_id == "cisco-ios-cli-6.95"  # kept; a transient empty read must not clobber


async def test_sync_device_survives_a_raising_ned_id_read(db_session: AsyncSession):
    """A RAISING ned-id read must not abort the sync of a device whose NED is already known.

    _resolve_ned_id lost its `if device.ned_id: return` short-circuit and now reads NSO on
    every sync — but only the returns-empty failure was tolerated. get_device_ned_id calls
    raise_for_status(), and that raise came out of the FIRST NSO call in sync_device, before
    sync_from: an NSO restart / load spike returning 502 therefore failed the scheduled sync
    for the ENTIRE fleet, staling every interface/IS-IS/BGP/OSPF/route-policy mirror, where
    previously only a device with an unset ned_id could fail here.

    The sibling test above stubs return_value="" and so never exercised this path.
    """
    import httpx

    from nso_adapter.core import importer as imp

    device = Device(
        nso_instance="nso-dev", nso_device_name="sw-nedboom", ned_id="cisco-ios-cli-6.95", netbox_device_id=18
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    client = _make_nso_client({"interface": [{"interface-name": "Gi0/0", "description": "x", "enabled": True}]})
    client.get_device_ned_id = AsyncMock(
        side_effect=httpx.HTTPStatusError(
            "502 Bad Gateway",
            request=httpx.Request("GET", "http://nso/restconf"),
            response=httpx.Response(502),
        )
    )
    imp._nso_clients["nso-dev"] = client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        await sync_device(device.id, db_session)  # must not raise

    await db_session.refresh(device)
    assert device.ned_id == "cisco-ios-cli-6.95"  # kept — a transient failure must not clobber
    assert device.last_sync_status != LastSyncStatus.failed


async def test_sync_device_raising_ned_id_read_still_fails_an_unknown_ned(db_session: AsyncSession):
    """With NO previously-known ned_id there is nothing to fall back on — the device
    genuinely cannot be synced, so the failure must still surface."""
    import httpx

    from nso_adapter.core import importer as imp

    device = Device(nso_instance="nso-dev", nso_device_name="sw-nedgone", netbox_device_id=19)
    db_session.add(device)
    await db_session.commit()

    client = _make_nso_client({"interface": []})
    client.get_device_ned_id = AsyncMock(
        side_effect=httpx.HTTPStatusError(
            "404 Not Found",
            request=httpx.Request("GET", "http://nso/restconf"),
            response=httpx.Response(404),
        )
    )
    imp._nso_clients["nso-dev"] = client
    imp._netbox_client = None

    with pytest.raises(Exception):  # noqa: B017 — the sync must not silently succeed
        await sync_device(device.id, db_session)


async def test_sync_device_swallows_notify_failure(db_session: AsyncSession):
    """A failing plugin sync-complete callback is best-effort — it must not fail the sync."""
    from nso_adapter.core import importer as imp

    device = Device(
        nso_instance="nso-dev", nso_device_name="sw-notify", ned_id="cisco-ios-cli-6.95", netbox_device_id=14
    )
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    imp._nso_clients["nso-dev"] = _make_nso_client(
        {"interface": [{"interface-name": "Gi0/0", "description": "x", "enabled": True}]}
    )
    nb = AsyncMock(spec=NetboxClient)
    nb.list_interfaces = AsyncMock(return_value=[])
    nb.bulk_create_interfaces = AsyncMock(return_value=[])
    nb.bulk_patch_interfaces = AsyncMock(return_value=[])
    nb.notify_sync_complete = AsyncMock(side_effect=RuntimeError("plugin down"))
    imp._netbox_client = nb

    try:
        with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
            summary = await sync_device(device.id, db_session)  # must not raise
    finally:
        imp._netbox_client = None

    assert summary["interfaces_created"] == 1
    nb.notify_sync_complete.assert_awaited_once()


# ── detect_drift branch coverage ──────────────────────────────────────────────


async def test_detect_drift_unknown_device_raises(db_session: AsyncSession):
    from nso_adapter.core.importer import detect_drift

    with pytest.raises(ValueError, match="not found"):
        await detect_drift(999999, db_session)


async def test_detect_drift_falls_back_to_cache_on_netbox_read_error(db_session: AsyncSession):
    """If reading live NetBox fails, drift compares against the cached value (no crash, no false drift)."""
    from nso_adapter.core import importer as imp
    from nso_adapter.core.importer import detect_drift
    from nso_adapter.store.models import InterfaceAttrState, SyncState

    device = Device(nso_instance="nso-dev", nso_device_name="sw-dr1", ned_id="cisco-ios-cli-6.95", netbox_device_id=20)
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()
    iface = DbInterface(device=device, name="Gi0/0")
    db_session.add(iface)
    await db_session.commit()
    db_session.add(
        InterfaceAttrState(
            interface_id=iface.id,
            attribute="description",
            netbox_value="same",
            nso_value="same",
            sync_state=SyncState.imported,
        )
    )
    await db_session.commit()

    imp._nso_clients["nso-dev"] = _make_nso_client({"interface": [{"interface-name": "Gi0/0", "description": "same"}]})
    nb = AsyncMock(spec=NetboxClient)
    nb.list_interfaces = AsyncMock(side_effect=RuntimeError("netbox down"))  # live read fails → cache fallback
    nb.notify_sync_complete = AsyncMock()
    imp._netbox_client = nb

    try:
        with patch("nso_adapter.core.importer.nso_actions.compare_config", new_callable=AsyncMock):
            summary = await detect_drift(device.id, db_session)
    finally:
        imp._netbox_client = None

    assert summary["changes_detected"] == 0  # cache == nso → imported, not counted


async def test_detect_drift_skips_interface_not_in_db(db_session: AsyncSession):
    """NSO reporting an interface the adapter has never seen is skipped (no DbInterface row)."""
    from nso_adapter.core import importer as imp
    from nso_adapter.core.importer import detect_drift

    device = Device(nso_instance="nso-dev", nso_device_name="sw-dr2", ned_id="cisco-ios-cli-6.95", netbox_device_id=21)
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()

    imp._nso_clients["nso-dev"] = _make_nso_client(
        {"interface": [{"interface-name": "Never-Seen0/0", "description": "x"}]}
    )
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.compare_config", new_callable=AsyncMock):
        summary = await detect_drift(device.id, db_session)

    assert summary["changes_detected"] == 0  # no DbInterface → skipped


async def test_detect_drift_skips_attr_without_state(db_session: AsyncSession):
    """An in-scope attr with no prior attr_state is skipped (nothing to compare against)."""
    from nso_adapter.core import importer as imp
    from nso_adapter.core.importer import detect_drift

    device = Device(nso_instance="nso-dev", nso_device_name="sw-dr3", ned_id="cisco-ios-cli-6.95", netbox_device_id=22)
    db_session.add(device)
    db_session.add(ManagedScope(device=device, attribute="description"))
    await db_session.commit()
    db_session.add(DbInterface(device=device, name="Gi0/0"))  # interface exists, but no attr_state
    await db_session.commit()

    imp._nso_clients["nso-dev"] = _make_nso_client({"interface": [{"interface-name": "Gi0/0", "description": "x"}]})
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.compare_config", new_callable=AsyncMock):
        summary = await detect_drift(device.id, db_session)

    assert summary["changes_detected"] == 0  # no attr_state → skipped


async def test_detect_drift_swallows_notify_failure(db_session: AsyncSession):
    """A failing plugin sync-complete callback must not fail drift detection."""
    from nso_adapter.core import importer as imp
    from nso_adapter.core.importer import detect_drift

    device = Device(nso_instance="nso-dev", nso_device_name="sw-dr4", ned_id="cisco-ios-cli-6.95", netbox_device_id=23)
    db_session.add(device)
    await db_session.commit()

    imp._nso_clients["nso-dev"] = _make_nso_client({"interface": []})
    nb = AsyncMock(spec=NetboxClient)
    nb.list_interfaces = AsyncMock(return_value=[])
    nb.notify_sync_complete = AsyncMock(side_effect=RuntimeError("plugin down"))
    imp._netbox_client = nb

    try:
        with patch("nso_adapter.core.importer.nso_actions.compare_config", new_callable=AsyncMock):
            summary = await detect_drift(device.id, db_session)  # must not raise
    finally:
        imp._netbox_client = None

    assert summary == {"changes_detected": 0}
    nb.notify_sync_complete.assert_awaited_once()


# ── A3b: sync_device validates the sync-from result ──────────────────────────


async def test_sync_device_reports_partial_on_failed_sync_from(db_session: AsyncSession):
    """A3b: a sync-from that returned result:false means the mirror was read from stale CDB —
    the device must report ``partial`` with ``sync_from`` degraded, not a misleading ``succeeded``."""
    device = Device(
        nso_instance="nso-dev", nso_device_name="sw-a3b-fail", ned_id="cisco-ios-cli-6.95", netbox_device_id=88
    )
    db_session.add(device)
    await db_session.commit()

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = _make_nso_client({"interface": []})
    imp._netbox_client = None

    with (
        patch("nso_adapter.core.importer.nso_actions.sync_from", new=AsyncMock(return_value={"result": False})),
        patch("nso_adapter.core.importer.refresh_routing_surfaces_for_device", new=AsyncMock(return_value=[])),
    ):
        await sync_device(device.id, db_session)

    await db_session.refresh(device)
    assert device.last_sync_status == LastSyncStatus.partial
    assert "sync_from" in (device.degraded_surfaces or [])


async def test_sync_device_succeeds_on_ok_sync_from(db_session: AsyncSession):
    """A clean sync-from (result:true) with no degraded surfaces reports ``succeeded`` and does
    not spuriously mark ``sync_from`` degraded."""
    device = Device(
        nso_instance="nso-dev", nso_device_name="sw-a3b-ok", ned_id="cisco-ios-cli-6.95", netbox_device_id=89
    )
    db_session.add(device)
    await db_session.commit()

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = _make_nso_client({"interface": []})
    imp._netbox_client = None

    with (
        patch("nso_adapter.core.importer.nso_actions.sync_from", new=AsyncMock(return_value={"result": True})),
        patch("nso_adapter.core.importer.refresh_routing_surfaces_for_device", new=AsyncMock(return_value=[])),
    ):
        await sync_device(device.id, db_session)

    await db_session.refresh(device)
    assert device.last_sync_status == LastSyncStatus.succeeded
    assert device.degraded_surfaces is None


@pytest.mark.anyio
async def test_sync_device_records_attrs_outcome_rows(db_session: AsyncSession):
    """READSEM S3 B5 (codex R1-F7): the importer-owned attrs read records two-phase
    outcomes (it never did) — family=interface_attributes, replaced on success."""
    from nso_adapter.store.models import RefreshOutcome

    device = Device(
        nso_instance="nso-dev",
        nso_device_name="sw-attrs-rec",
        ned_id="cisco-ios-cli-6.95",
        netbox_device_id=99,
    )
    db_session.add(device)
    await db_session.commit()

    nso_client = _make_nso_client({"device-name": "sw-attrs-rec", "interface": []})

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock):
        await sync_device(device.id, db_session)

    rows = (
        (
            await db_session.execute(
                select(RefreshOutcome).where(
                    RefreshOutcome.device_id == device.id,
                    RefreshOutcome.family == "interface_attributes",
                )
            )
        )
        .scalars()
        .all()
    )
    assert rows, "attrs must record outcomes now"
    assert (rows[-1].read_outcome, rows[-1].succeeded, rows[-1].result) == ("present", True, "replaced")


@pytest.mark.anyio
async def test_projection_supplier_failure_keeps_pop_families(db_session: AsyncSession):
    """Grain b (codex R1-F8): a doc-GET outage fans out export_down via from_outcome —
    pop families KEEP their rows (a fabricated empty/None section would clear them)."""
    from sqlalchemy import select as _select

    from nso_adapter.core.importer import refresh_routing_surfaces_for_device
    from nso_adapter.nso.client import NsoExportUnavailableError
    from nso_adapter.store.models import DeviceStaticRoute

    device = Device(nso_instance="nso-dev", nso_device_name="sw01", ned_id="x", netbox_device_id=1)
    db_session.add(device)
    await db_session.commit()
    db_session.add(DeviceStaticRoute(device_id=device.id, vrf="", prefix="10.6.0.0/16", next_hop="2.2.2.2"))
    await db_session.commit()

    nso_client = AsyncMock(spec=NsoClient)
    nso_client.get_device_state_doc.side_effect = NsoExportUnavailableError("export down")

    with patch("nso_adapter.core.redistribution.refresh_redistribution_for_device", AsyncMock(return_value=False)):
        failed = await refresh_routing_surfaces_for_device(db_session, device, nso_client, refresh_source="sync")

    assert "static_route" in failed  # degraded, honestly
    rows = (
        (await db_session.execute(_select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
        .scalars()
        .all()
    )
    assert [r.prefix for r in rows] == ["10.6.0.0/16"], "an outage must never clear the mirror"


@pytest.mark.anyio
async def test_projection_device_absent_clears_pop_families(db_session: AsyncSession):
    """Grain b: a confirmed device-level absence (doc GET None) clears pop families."""
    from sqlalchemy import select as _select

    from nso_adapter.core.importer import refresh_routing_surfaces_for_device
    from nso_adapter.store.models import DeviceStaticRoute

    device = Device(nso_instance="nso-dev", nso_device_name="ghost", ned_id="x", netbox_device_id=2)
    db_session.add(device)
    await db_session.commit()
    db_session.add(DeviceStaticRoute(device_id=device.id, vrf="", prefix="10.7.0.0/16", next_hop="3.3.3.3"))
    await db_session.commit()

    nso_client = AsyncMock(spec=NsoClient)
    nso_client.get_device_state_doc.return_value = None  # confirmed 404: device unknown to NSO

    with patch("nso_adapter.core.redistribution.refresh_redistribution_for_device", AsyncMock(return_value=True)):
        await refresh_routing_surfaces_for_device(db_session, device, nso_client, refresh_source="sync")

    rows = (
        (await db_session.execute(_select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
        .scalars()
        .all()
    )
    assert rows == []


@pytest.mark.anyio
async def test_projection_heals_not_ready_sections_with_one_action(db_session: AsyncSession):
    """Grain b: the not-ready subset is healed with ONE device-state-read call whose
    output sections feed the same path."""
    from sqlalchemy import select as _select

    from nso_adapter.core.importer import refresh_routing_surfaces_for_device
    from nso_adapter.store.models import DeviceStaticRoute

    device = Device(nso_instance="nso-dev", nso_device_name="sw01", ned_id="x", netbox_device_id=3)
    db_session.add(device)
    await db_session.commit()

    nso_client = AsyncMock(spec=NsoClient)
    ok_empty = {"status": "ok"}
    nso_client.get_device_state_doc.return_value = {
        "device-name": "sw01",
        "static-route": {"status": "not-ready"},
        "isis-interface": {"status": "not-ready"},
        "bgp-config": ok_empty,
        "ospf-config": ok_empty,
        "route-policy": ok_empty,
        "snmp-config": ok_empty,
        "logging-config": ok_empty,
        "bfd-config": ok_empty,
        "interface-ip": ok_empty,
    }
    nso_client.run_device_state_read.return_value = {
        "atomic": True,
        "static-route": {"status": "ok", "route": [{"vrf": "", "prefix": "10.8.0.0/16", "next-hop": "4.4.4.4"}]},
        "isis-interface": {"status": "ok"},
    }

    failed = await refresh_routing_surfaces_for_device(db_session, device, nso_client, refresh_source="sync")

    assert failed == []
    assert nso_client.run_device_state_read.await_count == 1
    heal_args, _ = nso_client.run_device_state_read.await_args
    assert heal_args[0] == "sw01"
    assert sorted(heal_args[1]) == ["isis-interface", "static-route"]  # order follows the sorted wire set
    rows = (
        (await db_session.execute(_select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
        .scalars()
        .all()
    )
    assert [r.prefix for r in rows] == ["10.8.0.0/16"]


@pytest.mark.anyio
async def test_atomic_fanout_uses_one_action_with_the_long_timeout(db_session: AsyncSession):
    """READSEM grain c (B7): refresh_all(atomic=True) issues ONE device-state-read for
    every enabled wire family with the 360s budget (the action may rebuild everything up
    to 3x under commit churn - 3 x rc1 75.6s outruns the 180s default)."""
    from sqlalchemy import select as _select

    from nso_adapter.core.importer import refresh_all_surfaces_for_device
    from nso_adapter.store.models import DeviceStaticRoute

    device = Device(nso_instance="nso-dev", nso_device_name="sw01", ned_id="x", netbox_device_id=4)
    db_session.add(device)
    await db_session.commit()

    nso_client = AsyncMock(spec=NsoClient)
    ok_empty = {"status": "ok"}
    nso_client.run_device_state_read.return_value = {
        "atomic": True,
        "static-route": {"status": "ok", "route": [{"vrf": "", "prefix": "10.9.0.0/16", "next-hop": "5.5.5.5"}]},
        **{
            w: ok_empty
            for w in (
                "isis-interface",
                "bgp-config",
                "ospf-config",
                "route-policy",
                "snmp-config",
                "logging-config",
                "bfd-config",
                "interface-ip",
                "vlan-database",
                "svi",
                "subinterface",
                "interface-mtu",
                "lag-topology",
                "lag-config",
                "l2-service",
                "switchport",
            )
        },
    }

    with patch("nso_adapter.core.redistribution.refresh_redistribution_for_device", AsyncMock(return_value=True)):
        failed = await refresh_all_surfaces_for_device(
            db_session, device, nso_client, refresh_source="onboard", atomic=True
        )

    assert failed == []
    nso_client.get_device_state_doc.assert_not_awaited()  # grain c never reads the record-served doc
    assert nso_client.run_device_state_read.await_count == 1
    args, kwargs = nso_client.run_device_state_read.await_args
    assert args[0] == "sw01"
    assert kwargs.get("timeout") == 360.0
    assert "static-route" in args[1] and "l2-service" in args[1]  # comprehensive family set
    rows = (
        (await db_session.execute(_select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
        .scalars()
        .all()
    )
    assert [r.prefix for r in rows] == ["10.9.0.0/16"]


@pytest.mark.anyio
async def test_atomic_fanout_action_error_keeps_every_family(db_session: AsyncSession):
    """Grain c: an action error (bracket exhaustion, unknown device) keeps ALL rows."""
    from sqlalchemy import select as _select

    from nso_adapter.core.importer import refresh_all_surfaces_for_device
    from nso_adapter.store.models import DeviceStaticRoute

    device = Device(nso_instance="nso-dev", nso_device_name="sw01", ned_id="x", netbox_device_id=5)
    db_session.add(device)
    await db_session.commit()
    db_session.add(DeviceStaticRoute(device_id=device.id, vrf="", prefix="10.10.0.0/16", next_hop="6.6.6.6"))
    await db_session.commit()

    nso_client = AsyncMock(spec=NsoClient)
    nso_client.run_device_state_read.side_effect = RuntimeError("bracket exhausted")

    with patch("nso_adapter.core.redistribution.refresh_redistribution_for_device", AsyncMock(return_value=False)):
        failed = await refresh_all_surfaces_for_device(
            db_session, device, nso_client, refresh_source="onboard", atomic=True
        )

    assert "static_route" in failed
    rows = (
        (await db_session.execute(_select(DeviceStaticRoute).where(DeviceStaticRoute.device_id == device.id)))
        .scalars()
        .all()
    )
    assert [r.prefix for r in rows] == ["10.10.0.0/16"], "torn/failed atomic reads never clear"


@pytest.mark.anyio
async def test_sync_device_atomic_reaches_the_action_not_the_doc(db_session: AsyncSession):
    """Codex S3-R3 F1 (BLOCKER): the atomic flag must survive the WRAPPER hop — sync_device
    -> refresh_routing_surfaces_for_device -> _run_surfaces_projected. A dropped flag
    silently downgrades operator Sync-Now to the record-served doc (grain b)."""
    device = Device(
        nso_instance="nso-dev", nso_device_name="sw-atomic", ned_id="cisco-ios-cli-6.95", netbox_device_id=6
    )
    db_session.add(device)
    await db_session.commit()

    nso_client = _make_nso_client({"device-name": "sw-atomic", "interface": []})
    ok_empty = {"status": "ok"}
    nso_client.run_device_state_read = AsyncMock(
        return_value={
            "atomic": True,
            **{
                w: ok_empty
                for w in (
                    "static-route",
                    "isis-interface",
                    "bgp-config",
                    "ospf-config",
                    "route-policy",
                    "snmp-config",
                    "logging-config",
                    "bfd-config",
                    "interface-ip",
                )
            },
        }
    )

    from nso_adapter.core import importer as imp

    imp._nso_clients["nso-dev"] = nso_client
    imp._netbox_client = None

    with (
        patch("nso_adapter.core.importer.nso_actions.sync_from", new_callable=AsyncMock),
        patch("nso_adapter.core.redistribution.refresh_redistribution_for_device", AsyncMock(return_value=True)),
    ):
        await sync_device(device.id, db_session, atomic=True)

    assert nso_client.run_device_state_read.await_count == 1
    args, kwargs = nso_client.run_device_state_read.await_args
    assert kwargs.get("timeout") == 360.0
    nso_client.get_device_state_doc.assert_not_awaited()


@pytest.mark.anyio
async def test_projected_batch_locks_by_spec_name_and_covers_redistribution(db_session: AsyncSession, monkeypatch):
    """Codex S3-R4: the batch must lock the SAME identities grain-a locks — spec.name
    (lag_topology's spec is named 'lag'; a label-keyed lock excludes no one) — and
    redistribution must be inside the lock regime (its poll job can otherwise interleave
    a newer snapshot between the batch's fetch and apply)."""
    from nso_adapter.core import refresh_engine as engine_mod
    from nso_adapter.core.importer import refresh_all_surfaces_for_device

    device = Device(nso_instance="nso-dev", nso_device_name="sw-locks", ned_id="x", netbox_device_id=7)
    db_session.add(device)
    await db_session.commit()

    acquired: list[str] = []
    real_lock = engine_mod._family_lock

    def _recording_lock(device_id, family):
        acquired.append(family)
        return real_lock(device_id, family)

    monkeypatch.setattr(engine_mod, "_family_lock", _recording_lock)
    # the importer resolves _family_lock at call time from the engine module
    nso_client = AsyncMock(spec=NsoClient)
    nso_client.get_device_state_doc.return_value = {"device-name": "sw-locks"}  # sections coerce to error

    await refresh_all_surfaces_for_device(db_session, device, nso_client, refresh_source="sync")

    assert "lag" in acquired, acquired
    assert "lag_topology" not in acquired, "label-keyed lock would exclude nobody (grain-a locks 'lag')"
    assert "redistribution" in acquired, "redistribution must join the batch lock regime"


@pytest.mark.anyio
async def test_standalone_redistribution_takes_its_family_lock(db_session: AsyncSession, monkeypatch):
    from nso_adapter.core import refresh_engine as engine_mod
    from nso_adapter.core.redistribution import refresh_redistribution_for_device

    device = Device(nso_instance="nso-dev", nso_device_name="sw-rlock", ned_id="x", netbox_device_id=8)
    db_session.add(device)
    await db_session.commit()

    acquired: list[str] = []
    real_lock = engine_mod._family_lock

    def _recording_lock(device_id, family):
        acquired.append(family)
        return real_lock(device_id, family)

    monkeypatch.setattr(engine_mod, "_family_lock", _recording_lock)
    nso_client = AsyncMock(spec=NsoClient)
    nso_client.get_device_state_section.return_value = {"status": "ok"}

    await refresh_redistribution_for_device(db_session, device, nso_client, refresh_source="poll")

    assert "redistribution" in acquired, acquired
