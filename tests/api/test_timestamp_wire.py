# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The store's one timestamp convention, end to end on PostgreSQL.

Two guards that only mean something on a real PostgreSQL database (sqlite drops
tzinfo on reload, so both pass there for the wrong reason):

* round-trip — an aware instant written to a normalized column reloads as the SAME
  instant, with the process TZ forced off UTC. Guards the writer convention.
* serialization shape — every timestamp on a representative family GET
  (``/interfaces-doc``: read_state + the interface intent's ``last_apply_at``)
  is ``"<iso>Z"``. Guards the ``iso_z`` routing: a raw ``.isoformat() + "Z"`` on a
  tz-aware value emits ``"...+00:00Z"``, which every plugin consumer rejects.
* inbound interpretation — a request-model datetime is read as UTC whether or not the
  plugin sent a zone. A naive value bound to ``timestamptz`` is shifted by the PROCESS
  zone, so the same instant comes back hours off.
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import VALID_TOKEN, seed_device, session

_AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

_ISO_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")

_WRITTEN = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)


@pytest.fixture
def non_utc_process_tz(monkeypatch):
    """Force the PROCESS zone off UTC — a UTC process cannot observe the bug at all."""
    monkeypatch.setenv("TZ", "America/New_York")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


async def test_normalized_columns_round_trip_an_aware_instant(store_engine, non_utc_process_tz):
    """Write aware -> reload -> same instant, on two normalized columns."""
    from nso_adapter.store.models import Device, Job, JobStatus, JobType

    async with session() as db:
        device = Device(nso_instance="nso-ts", nso_device_name="ts-roundtrip", last_sync_at=_WRITTEN)
        db.add(device)
        await db.flush()
        job = Job(job_type=JobType.apply, status=JobStatus.queued, device_id=device.id, started_at=_WRITTEN)
        db.add(job)
        await db.commit()
        device_id, job_id = device.id, job.id

    async with session() as db:
        reloaded_device = await db.get(Device, device_id)
        reloaded_job = await db.get(Job, job_id)

    for label, value in (
        ("devices.last_sync_at", reloaded_device.last_sync_at),
        ("jobs.started_at", reloaded_job.started_at),
    ):
        assert value.tzinfo is not None, f"{label} reloaded naive — the column is still `timestamp`"
        assert value == _WRITTEN, f"{label} reloaded as a different instant: {value!r}"


def _timestamps(node, path=""):
    """Every ``*_at`` / ``*_born`` leaf in the body, by path."""
    if isinstance(node, dict):
        for key, value in node.items():
            child = f"{path}.{key}" if path else key
            if isinstance(value, dict | list):
                yield from _timestamps(value, child)
            elif key.endswith("_at") or key.endswith("_born"):
                yield child, value
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _timestamps(value, f"{path}[{index}]")


async def test_family_get_serializes_every_timestamp_as_iso_z(adapter_client, non_utc_process_tz):
    from nso_adapter.nso.read_outcome import Freshness, Present
    from nso_adapter.store import outcome_store
    from nso_adapter.store.models import DbInterface, InterfaceAttrState, InterfaceIntent, SyncState

    device_id = await seed_device(nso_device_name="ts-wire", netbox_device_id=9701)
    async with session() as db:
        iface = DbInterface(device_id=device_id, name="ge-0/0/0", netbox_interface_id=771)
        db.add(iface)
        await db.flush()
        db.add(
            InterfaceAttrState(
                interface_id=iface.id,
                attribute="description",
                nso_value="core link",
                netbox_value="core link",
                sync_state=SyncState.in_sync,
                last_checked_at=_WRITTEN,
            )
        )
        db.add(
            InterfaceIntent(
                interface_id=iface.id,
                attribute="description",
                intent_value="core link",
                accepted_at=_WRITTEN,
                last_apply_at=_WRITTEN,
            )
        )
        await db.commit()

    async with session() as db:
        attempt_id = await outcome_store.record_read_outcome(
            db, device_id, "interface_attributes", Present({"i": []}, Freshness.fresh), refresh_source="poll"
        )
        await outcome_store.record_result(db, attempt_id, result="replaced", succeeded=True, row_count=1)

    resp = await adapter_client.get(f"/api/v1/devices/{device_id}/interfaces-doc", headers=_AUTH)
    assert resp.status_code == 200, resp.text

    found = dict(_timestamps(resp.json()))
    assert set(found) == {
        "read_state.read_at",
        "read_state.incarnation_born",
        "interfaces[0].attrs.description.last_apply_at",
    }
    for path, value in found.items():
        assert value is not None, f"{path} is null — the test would prove nothing"
        assert _ISO_Z.match(value), f"{path} is not '<iso>Z': {value!r}"


# ── the INBOUND boundary: every request-model datetime is interpreted as UTC ──────────

_RO_COMMUNITY = {"label": "ro1", "vault_ref": "snmp/ro#community", "access": "RO"}


def test_inbound_naive_datetime_is_interpreted_as_utc():
    """A plugin that omits the zone means UTC — never the adapter process's local zone."""
    from nso_adapter.api.snmp import SnmpCommunityEntry

    entry = SnmpCommunityEntry.model_validate({**_RO_COMMUNITY, "accepted_at": "2026-06-01T12:00:00"})
    assert entry.accepted_at == _WRITTEN.replace(hour=12)
    assert entry.accepted_at.utcoffset() == timedelta(0)


def test_inbound_offset_datetime_is_normalized_to_utc():
    """An offset-carrying value keeps its INSTANT and is canonicalized to UTC.

    The instant alone is not enough: aware==aware compares instants, so a stored
    ``14:00+02:00`` would satisfy an equality-only assertion while still handing the
    store a non-UTC clock domain. Pin the offset too."""
    from nso_adapter.api.snmp import SnmpCommunityEntry

    entry = SnmpCommunityEntry.model_validate({**_RO_COMMUNITY, "accepted_at": "2026-06-01T14:00:00+02:00"})
    assert entry.accepted_at == _WRITTEN.replace(hour=12)
    assert entry.accepted_at.utcoffset() == timedelta(0)


async def test_intent_put_with_a_zoneless_accepted_at_round_trips_the_same_instant(adapter_client, non_utc_process_tz):
    """END-TO-END: PUT a zone-less accepted_at, GET the SAME instant back in "<iso>Z".

    Un-normalized, the naive value reaches asyncpg and binds to ``timestamptz`` shifted by
    the process zone (America/New_York here), so the wire value returns hours off."""
    device_id = await seed_device(nso_device_name="ts-inbound", netbox_device_id=9702)

    put = await adapter_client.put(
        f"/api/v1/devices/{device_id}/intent",
        json={
            "attributes": [
                {
                    "interface": "ge-0/0/1",
                    "attribute": "description",
                    "intent_value": "peering",
                    "accepted_at": "2026-06-01T12:00:00",
                }
            ]
        },
        headers=_AUTH,
    )
    assert put.status_code == 200, put.text

    body = (await adapter_client.get(f"/api/v1/devices/{device_id}/intent", headers=_AUTH)).json()
    assert [a["accepted_at"] for a in body["attributes"]] == ["2026-06-01T12:00:00Z"]
