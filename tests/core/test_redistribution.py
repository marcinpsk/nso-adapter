# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/redistribution.py — refresh_redistribution_for_device."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.redistribution import refresh_redistribution_for_device
from nso_adapter.nso.client import NsoExportUnavailableError
from nso_adapter.store.db import get_session
from nso_adapter.store.models import Device, DeviceRedistribution
from tests.conftest import seed_device


@asynccontextmanager
async def _device_session(device_id: int):
    async for db in get_session():
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return
    raise RuntimeError("no session")


def _nso_client_with_data(ospf=None, isis=None, bgp=None) -> AsyncMock:
    client = AsyncMock()
    client.get_ospf.return_value = ospf or {}
    client.get_isis_interfaces.return_value = isis or {}
    client.get_bgp_config.return_value = bgp or {}
    return client


@pytest.mark.anyio
async def test_refresh_inserts_ospf_rows(adapter_client):
    """OSPF redistribute → DeviceRedistribution rows with dest_protocol='ospf'."""
    device_id = await seed_device(nso_device_name="rd-ospf-sw01", netbox_device_id=7700)
    async with _device_session(device_id) as (db, device):
        nso_client = _nso_client_with_data(
            ospf={
                "instance": [
                    {
                        "process-id": 1,
                        "redistribute": [
                            {"source-protocol": "connected", "source-ref": ""},
                            {"source-protocol": "static", "source-ref": "", "route-map": "RM-STATIC"},
                        ],
                    }
                ]
            }
        )

        await refresh_redistribution_for_device(db, device, nso_client, refresh_source="test")

        result = await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id))
        rows = result.scalars().all()
        assert len(rows) == 2
        protos = {r.source_protocol for r in rows}
        assert protos == {"connected", "static"}
        static_row = next(r for r in rows if r.source_protocol == "static")
        assert static_row.route_map == "RM-STATIC"
        assert static_row.dest_protocol == "ospf"
        assert static_row.dest_ref == "1"


@pytest.mark.anyio
async def test_refresh_inserts_isis_rows(adapter_client):
    """ISIS redistribute → rows with dest_protocol='isis'."""
    device_id = await seed_device(nso_device_name="rd-isis-sw01", netbox_device_id=7701)
    async with _device_session(device_id) as (db, device):
        nso_client = _nso_client_with_data(
            isis={
                "process": [
                    {
                        "process-tag": "CORE",
                        "redistribute": [
                            {"source-protocol": "ospf", "source-ref": "1", "metric": 100},
                        ],
                    }
                ]
            }
        )

        await refresh_redistribution_for_device(db, device, nso_client, refresh_source="test")

        result = await db.execute(
            select(DeviceRedistribution).where(
                DeviceRedistribution.device_id == device_id,
                DeviceRedistribution.dest_protocol == "isis",
            )
        )
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].source_protocol == "ospf"
        assert rows[0].source_ref == "1"
        assert rows[0].metric == 100
        assert rows[0].dest_ref == "CORE"


@pytest.mark.anyio
async def test_refresh_inserts_bgp_rows(adapter_client):
    """BGP address-family redistribute → rows with dest_protocol='bgp', dest_ref='<asn>/<vrf>/<afi>'."""
    device_id = await seed_device(nso_device_name="rd-bgp-sw01", netbox_device_id=7702)
    async with _device_session(device_id) as (db, device):
        nso_client = _nso_client_with_data(
            bgp={
                "router": [
                    {
                        "asn": "65000",
                        "scope": [
                            {
                                "vrf": "",
                                "address-family": [
                                    {
                                        "afi": "ipv4-unicast",
                                        "redistribute": [
                                            {"source-protocol": "connected", "source-ref": ""},
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        )

        await refresh_redistribution_for_device(db, device, nso_client, refresh_source="test")

        result = await db.execute(
            select(DeviceRedistribution).where(
                DeviceRedistribution.device_id == device_id,
                DeviceRedistribution.dest_protocol == "bgp",
            )
        )
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].source_protocol == "connected"
        assert rows[0].dest_ref == "65000//ipv4-unicast"


@pytest.mark.anyio
async def test_refresh_full_replace_semantics(adapter_client):
    """Second refresh fully replaces previous rows (full-replace, not append)."""
    device_id = await seed_device(nso_device_name="rd-replace-sw01", netbox_device_id=7703)
    async with _device_session(device_id) as (db, device):
        nso_client = _nso_client_with_data(
            ospf={
                "instance": [
                    {
                        "process-id": 1,
                        "redistribute": [{"source-protocol": "connected", "source-ref": ""}],
                    }
                ]
            }
        )
        await refresh_redistribution_for_device(db, device, nso_client, refresh_source="test")

        # Second refresh with different data — stale row must disappear
        nso_client2 = _nso_client_with_data(
            ospf={
                "instance": [
                    {
                        "process-id": 1,
                        "redistribute": [{"source-protocol": "static", "source-ref": ""}],
                    }
                ]
            }
        )
        await refresh_redistribution_for_device(db, device, nso_client2, refresh_source="test")

        result = await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id))
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].source_protocol == "static"


@pytest.mark.anyio
async def test_refresh_keeps_rows_when_a_read_is_degraded(adapter_client):
    """A degraded read must NOT full-replace — the last-known rows are kept, refresh returns False.

    redistribution reads three exports (ospf/isis/bgp). When one raises — e.g. a fleet-wide outage
    mid-`packages reload`, where the getter confirms the 404 against the parent container and raises
    NsoExportUnavailableError — full-replacing would wipe this device's redistribution mirror over a
    transient blip. RED against the old unconditional delete, which wiped the rows even though the
    read was degraded (ok=False).
    """
    device_id = await seed_device(nso_device_name="rd-degraded-sw01", netbox_device_id=7715)
    async with _device_session(device_id) as (db, device):
        # Seed rows via a healthy refresh.
        nso_client = _nso_client_with_data(
            ospf={"instance": [{"process-id": 1, "redistribute": [{"source-protocol": "connected", "source-ref": ""}]}]}
        )
        await refresh_redistribution_for_device(db, device, nso_client, refresh_source="test")
        seeded = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert len(seeded) == 1

        # A subsequent refresh hits a fleet-wide export outage: the ospf getter raises.
        degraded = _nso_client_with_data(bgp={"router": []}, isis={"process": []})
        degraded.get_ospf.side_effect = NsoExportUnavailableError(
            "network-state-export:ospf-config is not exported by NSO"
        )
        result = await refresh_redistribution_for_device(db, device, degraded, refresh_source="test")

        assert result is False  # degraded surface
        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1  # kept, NOT wiped over the transient outage
        assert rows[0].source_protocol == "connected"


@pytest.mark.anyio
async def test_refresh_empty_nso_response(adapter_client):
    """Empty NSO responses produce zero rows (no crash)."""
    device_id = await seed_device(nso_device_name="rd-empty-sw01", netbox_device_id=7704)
    async with _device_session(device_id) as (db, device):
        nso_client = _nso_client_with_data()

        await refresh_redistribution_for_device(db, device, nso_client, refresh_source="test")

        result = await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id))
        assert len(result.scalars().all()) == 0


@pytest.mark.anyio
async def test_refresh_ospf_failure_falls_back_to_other_protocols(adapter_client):
    """If OSPF call raises, ISIS/BGP rows are still upserted (graceful partial failure)."""
    device_id = await seed_device(nso_device_name="rd-partial-sw01", netbox_device_id=7705)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()
        nso_client.get_ospf.side_effect = Exception("OSPF NSO error")
        nso_client.get_isis_interfaces.return_value = {}
        nso_client.get_bgp_config.return_value = {
            "router": [
                {
                    "asn": "65001",
                    "scope": [
                        {
                            "vrf": "",
                            "address-family": [
                                {
                                    "afi": "ipv4-unicast",
                                    "redistribute": [{"source-protocol": "static", "source-ref": ""}],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        await refresh_redistribution_for_device(db, device, nso_client, refresh_source="test")

        result = await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id))
        rows = result.scalars().all()
        # BGP row should still exist even though OSPF failed
        assert len(rows) == 1
        assert rows[0].dest_protocol == "bgp"


@pytest.mark.anyio
async def test_refresh_skipped_when_no_nso_device_name(adapter_client):
    """Device without nso_device_name is skipped silently (no DB writes, no NSO calls)."""
    device_id = await seed_device(nso_device_name="", netbox_device_id=7706)
    async with _device_session(device_id) as (db, device):
        nso_client = AsyncMock()

        await refresh_redistribution_for_device(db, device, nso_client, refresh_source="test")

        nso_client.get_ospf.assert_not_awaited()
        result = await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id))
        assert len(result.scalars().all()) == 0


@pytest.mark.anyio
async def test_refresh_dedups_duplicate_identity(adapter_client):
    """s2-9: a duplicate redistribute identity tuple in the export must not IntegrityError and
    roll back the whole full-replace refresh — it is deduped (first occurrence wins)."""
    device_id = await seed_device(nso_device_name="rd-dup-sw01", netbox_device_id=7710)
    async with _device_session(device_id) as (db, device):
        nso_client = _nso_client_with_data(
            ospf={
                "instance": [
                    {
                        "process-id": 1,
                        "redistribute": [
                            {"source-protocol": "connected", "source-ref": "", "metric": 10},
                            {"source-protocol": "connected", "source-ref": "", "metric": 20},  # dup identity
                        ],
                    }
                ]
            }
        )

        await refresh_redistribution_for_device(db, device, nso_client, refresh_source="test")

        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].source_protocol == "connected"
        assert rows[0].metric == 10  # first wins
