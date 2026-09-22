# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""VLAN database + switchport refresh tests (envelope-flipped, READSEM S3 B4)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from nso_adapter.core.vlan import (
    parse_vlan_string,
    refresh_switchport_for_device,
    refresh_vlan_database_for_device,
)
from nso_adapter.nso.client import NsoExportUnavailableError
from nso_adapter.store.models import Device, DeviceSwitchport, DeviceSwitchportTaggedVlan, DeviceVlan
from tests.conftest import seed_device, session


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


def _serve_sections(nso: AsyncMock) -> dict:
    """Route ``get_device_state_section`` per wire family from a mutable dict.

    Values: a section dict (served as-is), ``None`` (confirmed device absence), or an
    Exception instance (raised) — one AsyncMock serves BOTH flipped families, and a test
    mutates the dict between refreshes.
    """
    sections: dict[str, object] = {}

    async def _get(device_name, wire_family):
        value = sections[wire_family]
        if isinstance(value, Exception):
            raise value
        return value

    nso.get_device_state_section.side_effect = _get
    return sections


@pytest.mark.parametrize("raw", [[], ["10", "20"], ("10", "20"), 10])
def test_tagged_vlan_parser_rejects_non_wire_shapes(raw):
    with pytest.raises(ValueError, match=rf"^tagged-vlans must be a string \(type {type(raw).__name__}\)$"):
        parse_vlan_string(raw)


@pytest.mark.parametrize("raw", ["10,,20", "10,20,"])
def test_tagged_vlan_parser_rejects_empty_chunks(raw):
    with pytest.raises(ValueError, match="^tagged-vlans contains an invalid VLAN range$"):
        parse_vlan_string(raw)


@pytest.mark.anyio
async def test_refresh_vlan_database_upserts_and_prunes(adapter_client):
    device_id = await seed_device(nso_device_name="vsw", netbox_device_id=1300)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {
            "status": "ok",
            "vlan": [{"vlan-id": 10, "name": "MGMT"}, {"vlan-id": 20, "name": "DATA"}],
        }
        await refresh_vlan_database_for_device(db, device, nso)
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert {(r.vlan_id, r.name) for r in rows} == {(10, "MGMT"), (20, "DATA")}

        # second refresh drops 20, keeps 10
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "MGMT"}]}
        await refresh_vlan_database_for_device(db, device, nso)
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert {r.vlan_id for r in rows} == {10}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("invalid_entry", "received_type"),
    [({"name": "NO-ID"}, "NoneType"), ({"vlan-id": 10.5, "name": "FRACTIONAL"}, "float")],
)
async def test_refresh_vlan_database_rejects_a_malformed_item_and_keeps_rows(
    adapter_client, invalid_entry, received_type
):
    """An item without a usable vlan id must reject the refresh, never prune the unseen rows."""
    device_id = await seed_device(nso_device_name="vsw-malformed", netbox_device_id=1304)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {
            "status": "ok",
            "vlan": [{"vlan-id": 10, "name": "MGMT"}, {"vlan-id": 20, "name": "DATA"}],
        }
        await refresh_vlan_database_for_device(db, device, nso)

        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "MGMT"}, invalid_entry]}
        with pytest.raises(ValueError, match=f"carries a vlan-id of type {received_type}"):
            await refresh_vlan_database_for_device(db, device, nso)
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert {r.vlan_id for r in rows} == {10, 20}, "a malformed item must never prune its siblings"


@pytest.mark.anyio
async def test_a_malformed_item_is_named_by_its_FIELD_and_never_repeated_verbatim(adapter_client):
    """The refusal interpolated the whole wire item, which is device-derived server data.

    A vlan-database entry carries whatever the export put in it. Repeating it verbatim
    carries that content into every surface that records the refusal. The field name and
    the received type say what is wrong and carry no payload.
    """
    from tests._secret_discipline import assert_chain_free_of, assert_text_free_of

    device_id = await seed_device(nso_device_name="vsw-sink", netbox_device_id=1309)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {
            "status": "ok",
            "vlan": [{"name": "NO-ID", "description": "placeholder-server-text"}],
        }
        with pytest.raises(ValueError) as caught:
            await refresh_vlan_database_for_device(db, device, nso)

    message = str(caught.value)
    assert_text_free_of(message, ["placeholder-server-text", "NO-ID"])
    assert_chain_free_of(caught.value, ["placeholder-server-text"])
    if "vlan-id" not in message:
        raise AssertionError("the diagnostic must still name the field")
    if "NoneType" not in message:
        raise AssertionError("the diagnostic must still name the received type")


#: What a NED can put in a switchport leaf the reader then converts.
_UNTAGGED_TEXT = "placeholder-untagged-secret"
_TAGGED_TEXT = "placeholder-tagged-secret"


async def _switchport_surface_failure(device_id: int, interface: dict) -> tuple[BaseException, list]:
    """Run the REAL switchport surface through the real fan-out; return (raised, records)."""
    from structlog.testing import capture_logs

    from nso_adapter.core.importer import _run_surfaces

    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "MGMT"}]}
        await refresh_vlan_database_for_device(db, device, nso)

        sections["switchport"] = {"status": "ok", "interface": [interface]}
        with pytest.raises(Exception) as caught:  # noqa: B017, the raise IS what is under test
            await refresh_switchport_for_device(db, device, nso)

        with capture_logs() as logs:
            failed = await _run_surfaces(db, device, nso, [("switchport", refresh_switchport_for_device)], "poll")
    assert failed == ["switchport"], "the surface must still be reported as failed"
    return caught.value, logs


@pytest.mark.anyio
async def test_a_malformed_UNTAGGED_VLAN_is_named_by_its_field_and_never_repeated(adapter_client):
    """``int(untagged)`` put the device's own leaf into the ValueError, and the fan-out logs it.

    ``sync.surface_refresh_failed`` records the exception repr, so whatever the NED emitted in
    ``untagged-vlan`` reached the operator log through it.
    """
    from tests._secret_discipline import assert_chain_free_of, assert_records_free_of, assert_text_free_of

    device_id = await seed_device(nso_device_name="vsw-untagged-sink", netbox_device_id=1311)
    raised, logs = await _switchport_surface_failure(
        device_id,
        {"interface-name": "Gi0/1", "mode": "access", "untagged-vlan": _UNTAGGED_TEXT},
    )

    message = str(raised)
    assert_text_free_of(message, [_UNTAGGED_TEXT])
    assert_chain_free_of(raised, [_UNTAGGED_TEXT])
    assert_records_free_of(logs, [_UNTAGGED_TEXT])
    if "untagged-vlan" not in message:
        raise AssertionError("the diagnostic must still name the field")
    if "type str" not in message:
        raise AssertionError("the diagnostic must still name the received type")
    if not [record for record in logs if record["event"] == "sync.surface_refresh_failed"]:
        raise AssertionError("the failed surface was not reported at all")


@pytest.mark.anyio
async def test_a_malformed_TAGGED_VLAN_entry_is_named_by_its_field_and_never_repeated(adapter_client):
    """Reject a non-wire tagged shape without repeating its device-served entries."""
    from tests._secret_discipline import assert_chain_free_of, assert_records_free_of, assert_text_free_of

    device_id = await seed_device(nso_device_name="vsw-tagged-sink", netbox_device_id=1312)
    raised, logs = await _switchport_surface_failure(
        device_id,
        {"interface-name": "Gi0/2", "mode": "trunk", "tagged-vlans": ["10", _TAGGED_TEXT]},
    )

    message = str(raised)
    assert_text_free_of(message, [_TAGGED_TEXT])
    assert_chain_free_of(raised, [_TAGGED_TEXT])
    assert_records_free_of(logs, [_TAGGED_TEXT])
    if "tagged-vlans" not in message:
        raise AssertionError("the diagnostic must still name the field")
    if "type list" not in message:
        raise AssertionError("the diagnostic must still name the received type")
    if not [record for record in logs if record["event"] == "sync.surface_refresh_failed"]:
        raise AssertionError("the failed surface was not reported at all")


@pytest.mark.anyio
async def test_refresh_switchport_links_vlans(adapter_client):
    device_id = await seed_device(nso_device_name="vsw2", netbox_device_id=1301)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {
            "status": "ok",
            "vlan": [{"vlan-id": 10, "name": "A"}, {"vlan-id": 20, "name": "B"}, {"vlan-id": 99, "name": "N"}],
        }
        await refresh_vlan_database_for_device(db, device, nso)
        sections["switchport"] = {
            "status": "ok",
            "interface": [
                {
                    "interface-name": "Gi0/1",
                    "mode": "trunk",
                    "untagged-vlan": 99,
                    "tagged-vlans": "10,10,20",
                }
            ],
        }
        assert await refresh_switchport_for_device(db, device, nso) is True

        sp = (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().one()
        assert sp.mode == "trunk"
        uv = await db.get(DeviceVlan, sp.untagged_vlan_id)
        assert uv.vlan_id == 99
        tagged = (
            (
                await db.execute(
                    select(DeviceVlan.vlan_id)
                    .join(DeviceSwitchportTaggedVlan, DeviceSwitchportTaggedVlan.vlan_id == DeviceVlan.id)
                    .where(DeviceSwitchportTaggedVlan.switchport_id == sp.id)
                )
            )
            .scalars()
            .all()
        )
        assert sorted(tagged) == [10, 20]


@pytest.mark.anyio
async def test_refresh_switchport_rejects_an_empty_tagged_vlan_list_without_clearing_rows(adapter_client):
    """A malformed falsy wire value must fail before replacing the stored VLAN links."""
    device_id = await seed_device(nso_device_name="vsw-empty-tagged", netbox_device_id=1313)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {
            "status": "ok",
            "vlan": [{"vlan-id": 10, "name": "A"}, {"vlan-id": 20, "name": "B"}],
        }
        await refresh_vlan_database_for_device(db, device, nso)
        sections["switchport"] = {
            "status": "ok",
            "interface": [{"interface-name": "Gi0/1", "mode": "trunk", "tagged-vlans": "10,20"}],
        }
        await refresh_switchport_for_device(db, device, nso)

        sections["switchport"] = {
            "status": "ok",
            "interface": [{"interface-name": "Gi0/1", "mode": "trunk", "tagged-vlans": []}],
        }
        with pytest.raises(ValueError, match=r"^tagged-vlans must be a string \(type list\)$"):
            await refresh_switchport_for_device(db, device, nso)

        tagged = (
            (
                await db.execute(
                    select(DeviceVlan.vlan_id)
                    .join(DeviceSwitchportTaggedVlan, DeviceSwitchportTaggedVlan.vlan_id == DeviceVlan.id)
                    .join(DeviceSwitchport, DeviceSwitchport.id == DeviceSwitchportTaggedVlan.switchport_id)
                    .where(DeviceSwitchport.device_id == device.id)
                )
            )
            .scalars()
            .all()
        )
        assert sorted(tagged) == [10, 20], "the rejected refresh must preserve the last valid links"


@pytest.mark.parametrize("wire", [{}, {"tagged-vlans": ""}], ids=["absent", "empty-string"])
@pytest.mark.anyio
async def test_refresh_switchport_treats_an_empty_tagged_vlans_leaf_as_none(adapter_client, wire):
    """Empty is a VALUE, not malformed data, so the mirror clears rather than refusing the read.

    `network-state-export.yang` types the leaf `string` and documents it as
    "(empty = none / trunk-all)", and the producer omits it entirely when the list is empty
    (`switchport.py`: `if tagged: entry["tagged-vlans"] = ...`). Refusing an empty value would
    raise on every access port and roll the whole switchport materializer back.
    """
    device_id = await seed_device(nso_device_name=f"vsw-none-tagged-{len(wire)}", netbox_device_id=1316 + len(wire))
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {
            "status": "ok",
            "vlan": [{"vlan-id": 10, "name": "A"}, {"vlan-id": 20, "name": "B"}],
        }
        await refresh_vlan_database_for_device(db, device, nso)
        sections["switchport"] = {
            "status": "ok",
            "interface": [{"interface-name": "Gi0/1", "mode": "trunk", "tagged-vlans": "10,20"}],
        }
        await refresh_switchport_for_device(db, device, nso)

        sections["switchport"] = {
            "status": "ok",
            "interface": [{"interface-name": "Gi0/1", "mode": "access", **wire}],
        }
        await refresh_switchport_for_device(db, device, nso)

        tagged = (
            (
                await db.execute(
                    select(DeviceVlan.vlan_id)
                    .join(DeviceSwitchportTaggedVlan, DeviceSwitchportTaggedVlan.vlan_id == DeviceVlan.id)
                    .join(DeviceSwitchport, DeviceSwitchport.id == DeviceSwitchportTaggedVlan.switchport_id)
                    .where(DeviceSwitchport.device_id == device.id)
                )
            )
            .scalars()
            .all()
        )
        assert tagged == [], "an empty tagged-vlans is 'none', so the mirror must follow the device"


@pytest.mark.parametrize("tagged_vlans", ["not-a-vlan", "10,not-a-vlan"])
@pytest.mark.anyio
async def test_refresh_switchport_rejects_invalid_tagged_vlan_chunks_without_clearing_rows(
    adapter_client, tagged_vlans
):
    """A malformed provider string must fail before replacing the stored VLAN links."""
    device_id = await seed_device(nso_device_name="vsw-invalid-tagged", netbox_device_id=1314)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {
            "status": "ok",
            "vlan": [{"vlan-id": 10, "name": "A"}, {"vlan-id": 20, "name": "B"}],
        }
        await refresh_vlan_database_for_device(db, device, nso)
        sections["switchport"] = {
            "status": "ok",
            "interface": [{"interface-name": "Gi0/1", "mode": "trunk", "tagged-vlans": "10,20"}],
        }
        await refresh_switchport_for_device(db, device, nso)

        sections["switchport"] = {
            "status": "ok",
            "interface": [{"interface-name": "Gi0/1", "mode": "trunk", "tagged-vlans": tagged_vlans}],
        }
        with pytest.raises(ValueError, match="^tagged-vlans contains an invalid VLAN range$"):
            await refresh_switchport_for_device(db, device, nso)

        tagged = (
            (
                await db.execute(
                    select(DeviceVlan.vlan_id)
                    .join(DeviceSwitchportTaggedVlan, DeviceSwitchportTaggedVlan.vlan_id == DeviceVlan.id)
                    .join(DeviceSwitchport, DeviceSwitchport.id == DeviceSwitchportTaggedVlan.switchport_id)
                    .where(DeviceSwitchport.device_id == device.id)
                )
            )
            .scalars()
            .all()
        )
        assert sorted(tagged) == [10, 20], "the rejected refresh must preserve the last valid links"


@pytest.mark.anyio
async def test_refresh_vlan_database_authoritative_empty_prunes_all(adapter_client):
    """An authoritatively-empty read (status=ok, no vlan list — RESTCONF omits empties) prunes
    every VLAN row for this pop family. (Device-absence, section None, now KEEPS — READSEM S5.)"""
    device_id = await seed_device(nso_device_name="vsw-clr", netbox_device_id=1302)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "MGMT"}]}
        await refresh_vlan_database_for_device(db, device, nso)
        assert (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()

        sections["vlan-database"] = {"status": "ok"}  # authoritative empty → clear
        ok = await refresh_vlan_database_for_device(db, device, nso)
        assert ok is True
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert rows == []


@pytest.mark.anyio
async def test_refresh_vlan_database_keep_on_export_down(adapter_client):
    """A confirmed export outage keeps the last-known VLAN rows and reports degraded (False)."""
    device_id = await seed_device(nso_device_name="vsw-keep", netbox_device_id=1303)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "MGMT"}]}
        await refresh_vlan_database_for_device(db, device, nso)

        sections["vlan-database"] = NsoExportUnavailableError("export down")
        ok = await refresh_vlan_database_for_device(db, device, nso)
        assert ok is False
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device.id))).scalars().all()
        assert {r.vlan_id for r in rows} == {10}  # kept


@pytest.mark.anyio
async def test_refresh_switchport_authoritative_empty_prunes_all(adapter_client):
    """An authoritatively-empty read (status=ok, no interface list) prunes every switchport row.
    (Device-absence, section None, now KEEPS — READSEM S5.)"""
    device_id = await seed_device(nso_device_name="vsw-sp-clr", netbox_device_id=1304)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "A"}]}
        await refresh_vlan_database_for_device(db, device, nso)
        sections["switchport"] = {"status": "ok", "interface": [{"interface-name": "Gi0/1", "mode": "access"}]}
        await refresh_switchport_for_device(db, device, nso)
        assert (
            (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().all()
        )

        sections["switchport"] = {"status": "ok"}  # authoritative empty → clear
        ok = await refresh_switchport_for_device(db, device, nso)
        assert ok is True
        rows = (
            (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().all()
        )
        assert rows == []


@pytest.mark.anyio
async def test_refresh_switchport_keep_on_read_error(adapter_client):
    """A read error keeps the last-known switchport rows and reports degraded (False)."""
    device_id = await seed_device(nso_device_name="vsw-sp-keep", netbox_device_id=1305)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "A"}]}
        await refresh_vlan_database_for_device(db, device, nso)
        sections["switchport"] = {"status": "ok", "interface": [{"interface-name": "Gi0/1", "mode": "access"}]}
        await refresh_switchport_for_device(db, device, nso)

        sections["switchport"] = RuntimeError("timeout")
        ok = await refresh_switchport_for_device(db, device, nso)
        assert ok is False
        rows = (
            (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device.id))).scalars().all()
        )
        assert [r.interface_name for r in rows] == ["Gi0/1"]  # kept


@pytest.mark.anyio
async def test_a_BOOLEAN_vlan_id_is_refused_and_never_bound_to_a_real_vlan(adapter_client):
    """``bool`` is an ``int`` subclass, so ``int(True)`` used to mark VLAN 1 as seen.

    The materializer prunes every VLAN it did not see, so a coerced ``true`` kept a row the
    device never reported and dropped the rows it did.
    """
    device_id = await seed_device(nso_device_name="vsw-bool-vid", netbox_device_id=1315)
    async with _device_session(device_id) as (db, device):
        nso = AsyncMock()
        sections = _serve_sections(nso)
        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": 10, "name": "MGMT"}]}
        await refresh_vlan_database_for_device(db, device, nso)

        sections["vlan-database"] = {"status": "ok", "vlan": [{"vlan-id": True, "name": "COERCED"}]}
        with pytest.raises(ValueError) as caught:
            await refresh_vlan_database_for_device(db, device, nso)

    message = str(caught.value)
    if "vlan-id" not in message or "bool" not in message:
        raise AssertionError("the refusal must name the field and the received type")
    async with _device_session(device_id) as (db, _device):
        rows = (await db.execute(select(DeviceVlan).where(DeviceVlan.device_id == device_id))).scalars().all()
    # The harm the coercion caused: VLAN 1 counted as seen and the real rows were pruned.
    assert [row.vlan_id for row in rows] == [10]


@pytest.mark.anyio
async def test_a_BOOLEAN_untagged_vlan_is_refused_and_never_bound_to_vlan_1(adapter_client):
    """``int(True)`` bound the switchport to VLAN 1, a VLAN the device never named."""
    device_id = await seed_device(nso_device_name="vsw-bool-untagged", netbox_device_id=1316)
    raised, logs = await _switchport_surface_failure(
        device_id,
        {"interface-name": "Gi0/3", "mode": "access", "untagged-vlan": True},
    )

    message = str(raised)
    if "untagged-vlan" not in message or "bool" not in message:
        raise AssertionError("the refusal must name the field and the received type")
    async with _device_session(device_id) as (db, _device):
        rows = (
            (await db.execute(select(DeviceSwitchport).where(DeviceSwitchport.device_id == device_id))).scalars().all()
        )
    assert rows == [], "the refused switchport must leave no row behind"
    if not [record for record in logs if record["event"] == "sync.surface_refresh_failed"]:
        raise AssertionError("the failed surface was not reported at all")
