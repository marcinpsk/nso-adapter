# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Tests for core/redistribution.py — refresh_redistribution_for_device."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.redistribution import refresh_redistribution_for_device, refresh_redistribution_from_outcomes
from nso_adapter.nso.client import NsoExportUnavailableError
from nso_adapter.nso.read_outcome import Freshness, Present, Unavailable, UnavailableReason
from nso_adapter.store.models import Device, DeviceRedistribution
from tests.conftest import seed_device, session


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


def _nso_client_with_data(ospf=None, isis=None, bgp=None) -> AsyncMock:
    """Envelope-flipped (READSEM S3 B5): serve the three component sections.

    ``client._sections`` is mutable per test: a section dict is served as-is, an
    Exception instance is raised (export-down / read-error shapes).
    """
    client = AsyncMock()
    sections = {
        "ospf-config": {"status": "ok", **(ospf or {})},
        "isis-interface": {"status": "ok", **(isis or {})},
        "bgp-config": {"status": "ok", **(bgp or {})},
    }
    client._sections = sections

    async def _get(device_name, wire_family):
        value = sections[wire_family]
        if isinstance(value, Exception):
            raise value
        return value

    client.get_device_state_section.side_effect = _get
    return client


@pytest.mark.anyio
async def test_superseded_export_outage_does_not_degrade_newer_winner(adapter_client, monkeypatch):
    from nso_adapter.core import redistribution

    device_id = await seed_device(nso_device_name="rd-superseded", netbox_device_id=7699)

    async def superseded_record(*_args, **_kwargs):
        return False

    monkeypatch.setattr(redistribution, "_record_composite", superseded_record)
    async with _device_session(device_id) as (db, device):
        ok = await refresh_redistribution_from_outcomes(
            db,
            device,
            {
                "ospf": Unavailable(UnavailableReason.export_down),
                "isis": Present({}, Freshness.fresh),
                "bgp": Present({}, Freshness.fresh),
            },
            refresh_source="test",
            own_lock=False,
        )

    assert ok is True


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

        # A subsequent refresh hits a fleet-wide export outage: the ospf section read raises.
        degraded = _nso_client_with_data(bgp={"router": []}, isis={"process": []})
        degraded._sections["ospf-config"] = NsoExportUnavailableError(
            "network-state-export:device-state is not exported by NSO"
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
async def test_refresh_keeps_failed_protocol_rows_per_component(adapter_client):
    """Per-component retention (operator decision): a non-outage read error on ONE protocol
    KEEPS that protocol's rows, while the successfully-read protocols still full-replace.

    RED against the old full-replace-from-successes, which deleted the failed protocol's rows
    and re-inserted only from whatever succeeded — silently wiping ospf redistribution here.
    """
    device_id = await seed_device(nso_device_name="rd-percomp-sw01", netbox_device_id=7720)
    async with _device_session(device_id) as (db, device):
        # Healthy seed: one ospf row + one isis row.
        healthy = _nso_client_with_data(
            ospf={
                "instance": [{"process-id": 1, "redistribute": [{"source-protocol": "connected", "source-ref": ""}]}]
            },
            isis={
                "process": [{"process-tag": "CORE", "redistribute": [{"source-protocol": "ospf", "source-ref": "1"}]}]
            },
        )
        await refresh_redistribution_for_device(db, device, healthy, refresh_source="test")
        seeded = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert {(r.dest_protocol, r.source_protocol) for r in seeded} == {("ospf", "connected"), ("isis", "ospf")}

        # Second refresh: ospf read errors (non-outage), isis reports NEW data, bgp is empty.
        degraded = _nso_client_with_data(
            isis={
                "process": [{"process-tag": "CORE", "redistribute": [{"source-protocol": "static", "source-ref": ""}]}]
            },
            bgp={},
        )
        degraded._sections["ospf-config"] = RuntimeError("ospf read timeout")  # read_error, NOT an outage

        ok = await refresh_redistribution_for_device(db, device, degraded, refresh_source="test")

        assert ok is False  # degraded surface — one protocol read failed
        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        by_proto = {r.dest_protocol: r for r in rows}
        # ospf rows KEPT (last-known), because ospf's read failed with a non-outage error.
        assert "ospf" in by_proto
        assert by_proto["ospf"].source_protocol == "connected"
        # isis rows REPLACED from the fresh read (ospf→static), because its read succeeded.
        assert by_proto["isis"].source_protocol == "static"
        assert len(rows) == 2


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
        nso_client = _nso_client_with_data(isis={})
        nso_client._sections["ospf-config"] = Exception("OSPF NSO error")
        nso_client._sections["bgp-config"] = {
            "status": "ok",
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
            ],
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

        nso_client.get_device_state_section.assert_not_awaited()
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


# ── READSEM S3 B5: explicit component aggregation (codex R1-F7) ─────────────────────


@pytest.mark.anyio
async def test_arcos_asymmetry_unsupported_component_is_not_a_failure(adapter_client):
    """The ArcOS shape: OSPF has no reader (unsupported), ISIS/BGP serve fine. The old
    aggregation filed `unsupported` under kept-stale and returned False — the device sat
    permanently `partial`. Declared unsupported must keep the partition AND succeed."""
    device_id = await seed_device(nso_device_name="rd-arcos", netbox_device_id=7707)
    async with _device_session(device_id) as (db, device):
        # SA-1: pre-seed an OSPF partition row — the unsupported component must RETAIN it.
        await _seed_redist_rows(db, device_id, [("ospf", "connected")])
        client = _nso_client_with_data(
            isis={
                "process": [{"process-tag": "CORE", "redistribute": [{"source-protocol": "static", "source-ref": ""}]}]
            },
            bgp={},
        )
        client._sections["ospf-config"] = {"status": "unsupported"}

        ok = await refresh_redistribution_for_device(db, device, client, refresh_source="test")

        assert ok is True, "an unsupported component must not degrade the composite"
        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert {(r.dest_protocol, r.source_protocol) for r in rows} == {
            ("ospf", "connected"),  # retained by the unsupported component
            ("isis", "static"),  # replaced by the authoritative one
        }
    # SA-1: pin the FULL terminal tuple, not just the return value.
    outcome = await _latest_outcome(device_id)
    assert (outcome.read_outcome, outcome.freshness, outcome.result, outcome.succeeded) == (
        "present",
        "fresh",
        "replaced",
        True,
    )


@pytest.mark.anyio
async def test_stale_component_replaces_and_records_degraded(adapter_client):
    """stale = degraded-success: the partition replaces (export's best-known) and the
    composite outcome row carries freshness=stale."""
    from nso_adapter.store.models import RefreshOutcome

    device_id = await seed_device(nso_device_name="rd-stale", netbox_device_id=7708)
    async with _device_session(device_id) as (db, device):
        client = _nso_client_with_data(isis={}, bgp={})
        client._sections["ospf-config"] = {
            "status": "stale",
            "instance": [{"process-id": "1", "redistribute": [{"source-protocol": "connected", "source-ref": ""}]}],
        }

        ok = await refresh_redistribution_for_device(db, device, client, refresh_source="test")

        assert ok is True
        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert {(r.dest_protocol, r.source_protocol) for r in rows} == {("ospf", "connected")}
        outcome_rows = (
            (
                await db.execute(
                    select(RefreshOutcome)
                    .where(RefreshOutcome.device_id == device_id, RefreshOutcome.family == "redistribution")
                    .order_by(RefreshOutcome.id.desc())
                )
            )
            .scalars()
            .all()
        )
        assert outcome_rows, "the composite must record outcomes now"
        assert (outcome_rows[0].freshness, outcome_rows[0].succeeded) == ("stale", True)


@pytest.mark.anyio
async def test_not_ready_component_escalates_a_single_family_action(adapter_client):
    """A not-ready component self-heals via ONE device-state-read for THAT family."""
    device_id = await seed_device(nso_device_name="rd-notready", netbox_device_id=7709)
    async with _device_session(device_id) as (db, device):
        client = _nso_client_with_data(isis={}, bgp={})
        client._sections["ospf-config"] = {"status": "not-ready"}
        client.run_device_state_read.return_value = {
            "atomic": True,
            "ospf-config": {
                "status": "ok",
                "instance": [{"process-id": "1", "redistribute": [{"source-protocol": "static", "source-ref": ""}]}],
            },
        }

        ok = await refresh_redistribution_for_device(db, device, client, refresh_source="test")

        assert ok is True
        client.run_device_state_read.assert_awaited_once_with(device.nso_device_name, ["ospf-config"])
        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert {(r.dest_protocol, r.source_protocol) for r in rows} == {("ospf", "static")}


@pytest.mark.anyio
async def test_kept_component_records_a_failed_composite_outcome(adapter_client):
    from nso_adapter.store.models import RefreshOutcome

    device_id = await seed_device(nso_device_name="rd-keptrec", netbox_device_id=7710)
    async with _device_session(device_id) as (db, device):
        client = _nso_client_with_data(isis={}, bgp={})
        client._sections["ospf-config"] = RuntimeError("boom")

        ok = await refresh_redistribution_for_device(db, device, client, refresh_source="test")

        assert ok is False
        outcome_rows = (
            (
                await db.execute(
                    select(RefreshOutcome)
                    .where(RefreshOutcome.device_id == device_id, RefreshOutcome.family == "redistribution")
                    .order_by(RefreshOutcome.id.desc())
                )
            )
            .scalars()
            .all()
        )
        assert outcome_rows
        # READSEM S4 D7: with isis/bgp authoritative and only ospf error-kept, the
        # composite is degraded-success (present/stale/replaced/True) — the mirror IS
        # serve-worthy including the retained partition — while ok=False above keeps the
        # device partial. (Pre-S4 this recorded unavailable/False, which would make the
        # plugin gate skip a payload whose other partitions genuinely replaced.)
        assert (
            outcome_rows[0].read_outcome,
            outcome_rows[0].freshness,
            outcome_rows[0].result,
            outcome_rows[0].succeeded,
        ) == ("present", "stale", "replaced", True)


# ── READSEM S4 D7: the complete merge terminal contract (codex R2-3/R3-5/R4-3/R5-6) ──


async def _seed_redist_rows(db, device_id: int, entries: list[tuple[str, str]]) -> None:
    """Pre-seed (dest_protocol, source_protocol) partition rows to prove retention."""
    from datetime import UTC, datetime

    from nso_adapter.store.models import DeviceRedistribution

    ts = datetime.now(UTC)
    for dest, source in entries:
        db.add(
            DeviceRedistribution(
                device_id=device_id,
                dest_protocol=dest,
                dest_ref="",
                source_protocol=source,
                source_ref="",
                last_refreshed_at=ts,
                refresh_source="seed",
            )
        )
    await db.commit()


async def _latest_outcome(device_id: int):
    from nso_adapter.store.models import RefreshOutcome

    async with session() as db:
        return (
            await db.execute(
                select(RefreshOutcome)
                .where(RefreshOutcome.device_id == device_id, RefreshOutcome.family == "redistribution")
                .order_by(RefreshOutcome.id.desc())
                .limit(1)
            )
        ).scalar_one()


@pytest.mark.anyio
async def test_mixed_replaced_and_error_retained_is_degraded_present(adapter_client):
    """D7: >=1 component replaced + >=1 retained-by-ERROR -> the composite records
    (present, stale, replaced, succeeded=True) — the payload IS mirror truth including
    the retained partition — while the fn still returns False (device stays partial).

    The old merge recorded (unavailable/read_error, kept, False): under the S4 plugin
    gate that would SKIP a payload whose isis partition genuinely replaced."""
    device_id = await seed_device(nso_device_name="rd-mixed-err", netbox_device_id=7711)
    async with _device_session(device_id) as (db, device):
        await _seed_redist_rows(db, device_id, [("ospf", "static"), ("isis", "connected")])
        client = _nso_client_with_data(
            isis={"process": [{"process-tag": "CORE", "redistribute": [{"source-protocol": "bgp", "source-ref": ""}]}]},
            bgp={},
        )
        client._sections["ospf-config"] = {"status": "error", "reason": "boom"}

        ok = await refresh_redistribution_for_device(db, device, client, refresh_source="test")

        assert ok is False, "a retained-by-error partition keeps the device partial"
        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        got = {(r.dest_protocol, r.source_protocol) for r in rows}
        assert got == {("ospf", "static"), ("isis", "bgp")}, "ospf partition retained (error), isis partition replaced"
    outcome = await _latest_outcome(device_id)
    assert (outcome.read_outcome, outcome.freshness, outcome.result, outcome.succeeded) == (
        "present",
        "stale",
        "replaced",
        True,
    ), "mixed replaced+error-retained is degraded-success on the wire, not unavailable"


@pytest.mark.anyio
async def test_all_unsupported_is_declared_unavailable_not_fresh_present(adapter_client):
    """D7: NO component replaced, all unsupported -> (unavailable/unsupported, kept,
    succeeded=True, return True, rows kept). The old merge computed all_authoritative=True
    and misreported fresh-present/replaced although nothing was read."""
    device_id = await seed_device(nso_device_name="rd-all-unsup", netbox_device_id=7712)
    async with _device_session(device_id) as (db, device):
        await _seed_redist_rows(db, device_id, [("ospf", "static"), ("bgp", "connected")])
        client = _nso_client_with_data()
        for section in ("ospf-config", "isis-interface", "bgp-config"):
            client._sections[section] = {"status": "unsupported"}

        ok = await refresh_redistribution_for_device(db, device, client, refresh_source="test")

        assert ok is True, "declared unsupported is a non-failure (no partial)"
        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert {(r.dest_protocol, r.source_protocol) for r in rows} == {("ospf", "static"), ("bgp", "connected")}
    outcome = await _latest_outcome(device_id)
    assert (outcome.read_outcome, outcome.read_reason, outcome.result, outcome.succeeded) == (
        "unavailable",
        "unsupported",
        "kept",
        True,
    ), "zero authoritative components must never claim fresh-present/replaced"


@pytest.mark.anyio
async def test_stale_replaced_with_unsupported_propagates_stale(adapter_client):
    """D7/R5-6: freshness = worst among the REPLACED authoritative components; an
    unsupported bystander neither hides nor causes staleness."""
    device_id = await seed_device(nso_device_name="rd-stale-unsup", netbox_device_id=7713)
    async with _device_session(device_id) as (db, device):
        client = _nso_client_with_data(bgp={})
        client._sections["ospf-config"] = {"status": "unsupported"}
        client._sections["isis-interface"] = {
            "status": "stale",
            "process": [{"process-tag": "C", "redistribute": [{"source-protocol": "static", "source-ref": ""}]}],
        }

        ok = await refresh_redistribution_for_device(db, device, client, refresh_source="test")

        assert ok is True
    outcome = await _latest_outcome(device_id)
    assert (outcome.read_outcome, outcome.freshness, outcome.result, outcome.succeeded) == (
        "present",
        "stale",
        "replaced",
        True,
    )


@pytest.mark.anyio
async def test_no_authoritative_component_records_worst_reason(adapter_client):
    """D7: no component replaced, mixed error+unsupported -> unavailable with the WORST
    reason (read_error > unsupported), kept, succeeded=False, return False, rows kept."""
    device_id = await seed_device(nso_device_name="rd-none-auth", netbox_device_id=7714)
    async with _device_session(device_id) as (db, device):
        await _seed_redist_rows(db, device_id, [("isis", "static")])
        client = _nso_client_with_data()
        client._sections["ospf-config"] = {"status": "error", "reason": "boom"}
        client._sections["isis-interface"] = {"status": "unsupported"}
        client._sections["bgp-config"] = {"status": "error", "reason": "boom"}

        ok = await refresh_redistribution_for_device(db, device, client, refresh_source="test")

        assert ok is False
        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert {(r.dest_protocol, r.source_protocol) for r in rows} == {("isis", "static")}
    outcome = await _latest_outcome(device_id)
    assert (outcome.read_outcome, outcome.read_reason, outcome.result, outcome.succeeded) == (
        "unavailable",
        "read_error",
        "kept",
        False,
    )


@pytest.mark.anyio
async def test_all_components_device_absent_keeps_and_succeeds(adapter_client):
    """READSEM S5 (1327): device-level absence (every component section None →
    not_authoritative) is a KEPT SUCCESS for redistribution too — consistent with every
    FamilySpec family (`_apply_outcome`), NOT a degraded failure. Before keep-rows, device-absence
    was AbsentAuthoritative (cleared → replaced/succeeded); the bucketing must not now misfile a
    non-failing keep as an error."""
    device_id = await seed_device(nso_device_name="rd-absent", netbox_device_id=7716)
    async with _device_session(device_id) as (db, device):
        await _seed_redist_rows(db, device_id, [("isis", "static")])
        client = _nso_client_with_data()
        client._sections["ospf-config"] = None
        client._sections["isis-interface"] = None
        client._sections["bgp-config"] = None

        ok = await refresh_redistribution_for_device(db, device, client, refresh_source="test")

        assert ok is True  # kept, non-failing — not a degraded surface
        rows = (
            (await db.execute(select(DeviceRedistribution).where(DeviceRedistribution.device_id == device_id)))
            .scalars()
            .all()
        )
        assert {(r.dest_protocol, r.source_protocol) for r in rows} == {("isis", "static")}  # kept
    outcome = await _latest_outcome(device_id)
    assert (outcome.read_outcome, outcome.read_reason, outcome.result, outcome.succeeded) == (
        "unavailable",
        "not_authoritative",
        "kept",
        True,
    )
