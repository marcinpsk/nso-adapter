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
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from nso_adapter.main import create_app
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


@pytest.fixture
async def pg_store(pg_url):
    """Bind the store globals to a private PostgreSQL database (schema from the template)."""
    from nso_adapter.store import db as store_db

    store_db.init_db(pg_url)
    try:
        yield pg_url
    finally:
        await store_db.get_engine().dispose()
        store_db._engine = None
        store_db._session_factory = None


@pytest.fixture
async def pg_adapter_client(pg_url, tmp_path, monkeypatch):
    """``adapter_client`` on PostgreSQL. Phase 3 makes this the only kind; until then the
    tz-aware wire shape cannot be observed through the sqlite-backed shared fixture."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "secrets:\n"
        "  provider: local\n"
        "nso_instances: []\n"
        "netbox:\n"
        "  base_url: http://netbox.local\n"
        '  api_token_ref: "NETBOX_TOKEN"\n'
        "api:\n"
        '  adapter_token_ref: "ADAPTER_TOKEN"\n'
        f"database_url: {pg_url}\n"
    )
    monkeypatch.setenv("CONFIG_FILE", str(cfg_file))
    monkeypatch.setenv("ADAPTER_TOKEN", VALID_TOKEN)
    monkeypatch.setenv("NETBOX_TOKEN", "nb-test-token")

    from nso_adapter.config import reset_config
    from nso_adapter.store import db as store_db

    reset_config()
    app = create_app()
    try:
        with (
            patch("nso_adapter.main.set_netbox_client"),
            patch("nso_adapter.main.start_scheduler"),
            patch("nso_adapter.main.stop_scheduler"),
            patch("nso_adapter.main.start_workers", new=AsyncMock()),
            patch("nso_adapter.main.stop_workers", new=AsyncMock()),
            patch("nso_adapter.main.persistent_subscriber", new=AsyncMock()),
        ):
            async with app.router.lifespan_context(app):
                async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                    yield client
    finally:
        store_db._engine = None
        store_db._session_factory = None
        reset_config()


async def test_normalized_columns_round_trip_an_aware_instant(pg_store, non_utc_process_tz):
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


async def test_family_get_serializes_every_timestamp_as_iso_z(pg_adapter_client, non_utc_process_tz):
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

    resp = await pg_adapter_client.get(f"/api/v1/devices/{device_id}/interfaces-doc", headers=_AUTH)
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
