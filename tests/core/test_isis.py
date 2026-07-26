# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/isis.py — refresh upserts IS-IS interface rows, incl. bound_port."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.isis import refresh_isis_interfaces_for_device
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceIsisInterface, DeviceIsisProcess
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


@pytest.mark.anyio
async def test_refresh_normalizes_level_2_alias(adapter_client):
    """s2-12: the reader folds the free-text is-type/circuit-type alias 'level-2' to the
    canonical YANG 'level-2-only', matching the writer so it can't drive phantom drift."""
    device_id = await seed_device(nso_device_name="isis-lvl2", netbox_device_id=968)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [{"process-tag": "0", "is-type": "level-2"}],
            "interface": [{"interface-name": "Gi0/1", "af": "ipv4", "circuit-type": "level-2"}],
        }

        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        proc = (
            (await db.execute(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id)))
            .scalars()
            .one()
        )
        iface = (
            (await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id)))
            .scalars()
            .one()
        )
        assert proc.is_type == "level-2-only"
        assert iface.circuit_type == "level-2-only"


@pytest.mark.anyio
async def test_refresh_persists_bound_port_for_nokia(adapter_client):
    """Nokia IS-IS interfaces carry bound-port; it is stored on the read row.
    Loopback/unbound interfaces (no bound-port) store None."""
    device_id = await seed_device(nso_device_name="isis-nokia01", netbox_device_id=960)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [],
            "interface": [
                {"interface-name": "LAG99:10", "af": "ipv4", "bound-port": "lag-99:10"},
                {"interface-name": "system", "af": "ipv4"},  # loopback, no bound-port
            ],
        }

        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id))
        by_name = {r.interface_name: r for r in result.scalars().all()}
        assert by_name["LAG99:10"].bound_port == "lag-99:10"
        assert by_name["system"].bound_port is None


@pytest.mark.anyio
async def test_refresh_empty_bound_port_string_stored_as_none(adapter_client):
    """An empty-string bound-port (defensive) is normalized to None, not ''."""
    device_id = await seed_device(nso_device_name="isis-nokia02", netbox_device_id=961)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [],
            "interface": [{"interface-name": "LAG99:20", "af": "ipv4", "bound-port": ""}],
        }

        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id))
        row = result.scalars().one()
        assert row.bound_port is None


@pytest.mark.anyio
async def test_refresh_cisco_interface_has_no_bound_port(adapter_client):
    """Cisco/Junos interfaces never emit bound-port → stored None."""
    device_id = await seed_device(nso_device_name="isis-cisco01", netbox_device_id=962)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [],
            "interface": [{"interface-name": "GigabitEthernet0/1", "af": "ipv4"}],
        }

        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id))
        row = result.scalars().one()
        assert row.bound_port is None


@pytest.mark.anyio
async def test_refresh_persists_hello_auth(adapter_client):
    """Per-interface IS-IS hello auth (type + present) is mirrored secret-safe."""
    device_id = await seed_device(nso_device_name="isis-hauth01", netbox_device_id=961)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [],
            "interface": [
                {"interface-name": "ae10.0", "af": "ipv4", "hello-auth-type": "md5", "hello-auth-present": True},
                {"interface-name": "ae11.0", "af": "ipv4"},  # no hello auth
            ],
        }

        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        result = await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id))
        by_name = {r.interface_name: r for r in result.scalars().all()}
        assert by_name["ae10.0"].hello_auth_type == "md5"
        assert by_name["ae10.0"].hello_auth_present is True
        assert by_name["ae11.0"].hello_auth_type is None
        assert by_name["ae11.0"].hello_auth_present is None


async def test_refresh_persists_isis_bfd_enabled(adapter_client):
    """Per-interface IS-IS bfd-enabled is mirrored."""
    device_id = await seed_device(nso_device_name="isis-bfd01", netbox_device_id=963)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [],
            "interface": [
                {"interface-name": "ae10.0", "af": "ipv4", "bfd-enabled": True},
                {"interface-name": "ae11.0", "af": "ipv4"},
            ],
        }
        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")
        by_name = {
            r.interface_name: r
            for r in (await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id)))
            .scalars()
            .all()
        }
        assert by_name["ae10.0"].bfd_enabled is True
        assert by_name["ae11.0"].bfd_enabled is None


@pytest.mark.anyio
async def test_refresh_persists_p1_scalars_and_settings(adapter_client):
    """cross-vendor instance/interface scalars + EAV settings are mirrored."""
    device_id = await seed_device(nso_device_name="isis-p1-01", netbox_device_id=965)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [
                {
                    "process-tag": "0",
                    "lsp-lifetime": 65535,
                    "lsp-refresh-interval": 32767,
                    "lsp-mtu": 1492,
                    "spf-initial-wait": 1000,
                    "spf-max-wait": 10000,
                    "te-enabled": True,
                    "setting": [
                        {"key": "spf_second_wait", "value": "1000"},
                        {"key": "lsp_second_wait", "value": "1000"},
                    ],
                },
            ],
            "interface": [
                {
                    "interface-name": "LAG99:10",
                    "af": "ipv4",
                    "network-type": "point-to-point",
                    "csnp-interval": 10,
                    "mesh-group": "blocked",
                    "setting": [{"key": "hello_padding", "value": "true"}],
                },
            ],
        }

        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        proc = (
            await db.execute(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id))
        ).scalar_one()
        assert proc.lsp_lifetime == 65535
        assert proc.lsp_refresh_interval == 32767
        assert proc.lsp_mtu == 1492
        assert proc.spf_initial_wait == 1000
        assert proc.spf_max_wait == 10000
        assert proc.te_enabled is True
        assert proc.settings == {"spf_second_wait": "1000", "lsp_second_wait": "1000"}

        iface = (
            await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id))
        ).scalar_one()
        assert iface.network_type == "point-to-point"
        assert iface.csnp_interval == 10
        assert iface.mesh_group == "blocked"
        assert iface.settings == {"hello_padding": "true"}


async def test_refresh_persists_p2_levels_and_sr(adapter_client):
    """per-level child lists + segment-routing object are mirrored."""
    device_id = await seed_device(nso_device_name="isis-p2-01", netbox_device_id=967)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [
                {
                    "process-tag": "0",
                    "level": [
                        {"level": 1, "default-metric": 10},
                        {"level": 2, "default-metric": 10, "wide-metrics-only": True},
                    ],
                    "segment-routing": {"enabled": True, "prefix-sid-range": "global"},
                },
            ],
            "interface": [
                {
                    "interface-name": "LAG99:10",
                    "af": "ipv4",
                    "level": [{"level": 2, "metric": 10}],
                },
                {
                    "interface-name": "Loopback0",
                    "af": "ipv4",
                    "passive": True,
                    "prefix-sid": [{"algorithm": 0, "sid-index": 100006, "n-flag": True}],
                },
            ],
        }
        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        proc = (
            await db.execute(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id))
        ).scalar_one()
        assert proc.levels == [
            {"level": 1, "default-metric": 10},
            {"level": 2, "default-metric": 10, "wide-metrics-only": True},
        ]
        assert proc.segment_routing == {"enabled": True, "prefix-sid-range": "global"}

        iface = (
            await db.execute(
                select(DeviceIsisInterface).where(
                    DeviceIsisInterface.device_id == device.id,
                    DeviceIsisInterface.interface_name == "LAG99:10",
                )
            )
        ).scalar_one()
        assert iface.levels == [{"level": 2, "metric": 10}]
        assert iface.prefix_sids is None

        # Per-loopback prefix-SID list is mirrored onto the interface row.
        lo = (
            await db.execute(
                select(DeviceIsisInterface).where(
                    DeviceIsisInterface.device_id == device.id,
                    DeviceIsisInterface.interface_name == "Loopback0",
                )
            )
        ).scalar_one()
        assert lo.prefix_sids == [{"algorithm": 0, "sid-index": 100006, "n-flag": True}]


@pytest.mark.anyio
async def test_refresh_preserves_segment_routing_absence_provenance(adapter_client):
    device_id = await seed_device(nso_device_name="isis-sr-empty", netbox_device_id=975)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [
                {
                    "process-tag": "CORE",
                    "segment-routing-reported": True,
                    "segment-routing-configured": False,
                }
            ],
            "interface": [],
        }

        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        proc = (
            await db.execute(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id))
        ).scalar_one()
        assert proc.segment_routing_reported is True
        assert proc.segment_routing_configured is False
        assert proc.segment_routing is None


@pytest.mark.anyio
async def test_refresh_persists_srv6_locators(adapter_client):
    """The per-process srv6-locator list is mirrored to the srv6_locators column."""
    device_id = await seed_device(nso_device_name="isis-srv6-01", netbox_device_id=971)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [
                {
                    "process-tag": "",
                    "segment-routing": {"srv6-enabled": True},
                    "srv6-locator": [
                        {"name": "LOC1", "prefix": "2001:db8:a1::/64", "algorithm": 128, "enabled": True},
                    ],
                },
            ],
            "interface": [],
        }
        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        proc = (
            await db.execute(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id))
        ).scalar_one()
        assert proc.segment_routing == {"srv6-enabled": True}
        assert proc.srv6_locators == [{"name": "LOC1", "prefix": "2001:db8:a1::/64", "algorithm": 128, "enabled": True}]


@pytest.mark.anyio
async def test_refresh_persists_attached_bit(adapter_client):
    """Per-process suppress/ignore-attached-bit boolean leaves are mirrored to columns."""
    device_id = await seed_device(nso_device_name="isis-att-01", netbox_device_id=972)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [
                {"process-tag": "0", "suppress-attached-bit": True, "ignore-attached-bit": True},
                {"process-tag": "1"},  # neither knob configured -> both None
            ],
            "interface": [],
        }
        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        procs = {
            p.process_tag: p
            for p in (await db.execute(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id)))
            .scalars()
            .all()
        }
        assert procs["0"].suppress_attached_bit is True
        assert procs["0"].ignore_attached_bit is True
        assert procs["1"].suppress_attached_bit is None
        assert procs["1"].ignore_attached_bit is None


@pytest.mark.anyio
async def test_refresh_persists_frr(adapter_client):
    """#83: FRR mirror — process fast-reroute/microloop-avoidance, interface tri-state
    frr-enabled + frr-protection. Absent keys stay None (unconfigured); an explicit
    device-side disable/exclude arrives as frr-enabled=False and must persist as False."""
    device_id = await seed_device(nso_device_name="isis-frr-01", netbox_device_id=973)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [
                {"process-tag": "CORE", "fast-reroute": "ti-lfa", "microloop-avoidance": True},
                {"process-tag": "EDGE"},  # unconfigured -> both None
            ],
            "interface": [
                {"interface-name": "BE1", "af": "ipv4", "frr-enabled": True, "frr-protection": "node"},
                {"interface-name": "BE2", "af": "ipv4", "frr-enabled": False},
                {"interface-name": "BE3", "af": "ipv4"},
            ],
        }
        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        procs = {
            p.process_tag: p
            for p in (await db.execute(select(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id)))
            .scalars()
            .all()
        }
        assert procs["CORE"].fast_reroute == "ti-lfa"
        assert procs["CORE"].microloop_avoidance is True
        assert procs["EDGE"].fast_reroute is None
        assert procs["EDGE"].microloop_avoidance is None

        ifaces = {
            i.interface_name: i
            for i in (await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id)))
            .scalars()
            .all()
        }
        assert ifaces["BE1"].frr_enabled is True
        assert ifaces["BE1"].frr_protection == "node"
        assert ifaces["BE2"].frr_enabled is False
        assert ifaces["BE2"].frr_protection is None
        assert ifaces["BE3"].frr_enabled is None


@pytest.mark.anyio
async def test_refresh_dedups_duplicate_interface(adapter_client):
    """s2-9: a duplicate (interface, af) in the export must not IntegrityError and roll back the
    whole full-replace — deduped (first occurrence wins)."""
    device_id = await seed_device(nso_device_name="isis-dup01", netbox_device_id=969)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_device_state_section.return_value = {
            "status": "ok",
            "process": [],
            "interface": [
                {"interface-name": "Gi0/1", "af": "ipv4", "metric": 10},
                {"interface-name": "Gi0/1", "af": "ipv4", "metric": 20},  # duplicate identity
            ],
        }

        await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="poll")

        rows = (
            (await db.execute(select(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].metric == 10  # first wins
