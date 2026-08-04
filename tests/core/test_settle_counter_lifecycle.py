# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Appendix S, chunk S2: the counter row's lifecycle, and why it is never created lazily.

The counter row exists so the terminal path never reaches the ``devices`` table. PostgreSQL
validates an inserted FK by locking the referenced row ``FOR KEY SHARE``; offboard holds
``devices FOR UPDATE`` and then reaches for ``jobs``; so a terminal transaction that holds a
job row and then INSERTS a counter closes a real cycle. The row is therefore created with its
device at every insert site, backfilled by the migration for devices that predate it, and
repaired by a sweep — and a missing row is a hard failure in the terminal path, never a lazy
create.

The sweep's PLACEMENT is load-bearing, not decorative: recovery terminalizes, and a
terminalization that finds no counter row raises, so the repair has to run FIRST at both
sites that recover.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import sqlalchemy as sa

from nso_adapter.core import worker as worker_mod
from nso_adapter.store.device_settle import MissingSettleCounter
from nso_adapter.store.models import Device, DeviceSettleCounter, Job, JobStatus, JobType
from tests.conftest import _drop_database, _url_for, seed_device, session

pytestmark = pytest.mark.anyio

_REPO_ROOT = Path(__file__).resolve().parents[2]
# The revision this chunk chains off: the tree one migration BEFORE the counter exists.
_PRE_COUNTER_REVISION = "b2d9f4a71c63"
_BLOCKED_FOR = 0.5


async def _counter(device_id: int) -> int | None:
    async with session() as db:
        return await db.scalar(
            sa.select(DeviceSettleCounter.last_seq).where(DeviceSettleCounter.device_id == device_id)
        )


async def _drop_counter(device_id: int) -> None:
    """Delete the counter out of band — the state a fourth insert site would leave."""
    async with session() as db:
        await db.execute(sa.delete(DeviceSettleCounter).where(DeviceSettleCounter.device_id == device_id))
        await db.commit()


async def _stranded_running_job(device_id: int, job_type: JobType = JobType.apply) -> int:
    """A job stranded ``running`` with a stale heartbeat and no claim: recovery's candidate."""
    async with session() as db:
        job = Job(
            job_type=job_type,
            device_id=device_id,
            status=JobStatus.running,
            run_attempt=1,
            heartbeat_at=datetime.now(UTC) - timedelta(seconds=worker_mod.PROVISION_STALE_AFTER + 600),
        )
        db.add(job)
        await db.commit()
        return job.id


# ── S2.8 (M5): the terminal path never reaches `devices` ─────────────────────


async def test_terminalization_never_locks_the_devices_table(adapter_client, monkeypatch):
    """S2.8 — a terminal write on a counter-less device, against an offboard holding that device.

    The barrier holds the terminal transaction between its job-row CAS and its allocation,
    exactly where a lazy counter INSERT would sit. Offboard then locks ``devices FOR UPDATE``
    and blocks reaching for the same job row.

    Forbidden: the allocation reaching for the ``devices`` key-share lock, which closes the
    cycle and lets PostgreSQL abort one of the two. The allocation is a plain UPDATE of the
    counter row, so the missing row simply raises and the transaction aborts cleanly — no
    deadlock, and no half-written terminal state.
    """
    from nso_adapter.core import claim as claim_mod
    from nso_adapter.core.onboarding import offboard_device

    device_id = await seed_device(nso_device_name="lc-inversion", netbox_device_id=8601)
    job_id = await _stranded_running_job(device_id, JobType.sync)
    await _drop_counter(device_id)

    at_allocation = asyncio.Event()
    may_allocate = asyncio.Event()
    real_allocate = claim_mod.allocate_settle_seq

    async def _barrier(db, dev_id):
        at_allocation.set()
        await may_allocate.wait()
        return await real_allocate(db, dev_id)

    monkeypatch.setattr(claim_mod, "allocate_settle_seq", _barrier)

    async def _terminal_write():
        async with session() as db:
            await claim_mod.terminalize(db, job_id, status=JobStatus.succeeded, expect=JobStatus.running, run_attempt=1)
            await db.commit()

    async def _offboard():
        async with session() as db:
            await offboard_device(db, await db.get(Device, device_id))

    writer = asyncio.create_task(_terminal_write())
    await asyncio.wait_for(at_allocation.wait(), timeout=10)  # the job row is held

    teardown = asyncio.create_task(_offboard())
    await asyncio.sleep(_BLOCKED_FOR)
    assert not teardown.done(), "offboard did not reach the job row this writer holds"

    may_allocate.set()
    with pytest.raises(MissingSettleCounter):
        await asyncio.wait_for(writer, timeout=30)
    await asyncio.wait_for(teardown, timeout=30)  # a deadlock would have aborted one of the two

    async with session() as db:
        assert await db.get(Device, device_id) is None, "offboard did not complete"
        job = await db.get(Job, job_id)
    assert job.settle_seq is None, "a partial terminal state survived the aborted transaction"


# ── S2.9 (M5): every path that creates a device creates its counter ──────────


async def _created_by_onboard_claimed() -> int:
    """The claimed provision path: the Device and its claim in ONE transaction."""
    from unittest.mock import patch

    from nso_adapter.core.onboarding import provision_nso_device
    from tests.core.test_provision import _mock_client

    with patch("nso_adapter.core.importer.get_nso_client", return_value=_mock_client()):
        async with session() as db:
            result = await provision_nso_device(
                db,
                nso_instance="nso-dev",
                device_name="lc-claimed",
                address="10.0.0.5",
                ned_id="cisco-ios-cli-6.114:cisco-ios-cli-6.114",
                authgroup="network",
                netbox_device_id=8611,
            )
    assert result["device_id"] is not None
    return result["device_id"]


async def _created_by_onboard_mapped() -> int:
    """The unclaimed mapping path."""
    from nso_adapter.core.onboarding import onboard_device

    async with session() as db:
        device = await onboard_device(db, "nso-dev", "lc-mapped", 8612)
    return device.id


async def _created_by_discover(fake_nso_client) -> int:
    """The scheduler's NSO discovery upsert."""
    from unittest.mock import patch

    from nso_adapter.core.importer import discover_devices

    with patch("nso_adapter.core.importer.get_nso_client", return_value=fake_nso_client):
        async with session() as db:
            await discover_devices(db)
            return await db.scalar(sa.select(Device.id).where(Device.nso_device_name == "core-rtr-01"))


@pytest.mark.parametrize("path", ["onboard_claimed", "onboard_mapped", "discover"])
async def test_every_device_insert_path_creates_a_counter(adapter_client_with_nso, fake_nso_client, path):
    """S2.9 — every device insert site leaves a counter at ``last_seq = 0``.

    Forbidden: any site minting a device whose first terminal job then raises. The sweep is
    the repair path, not the creation path.
    """
    if path == "onboard_claimed":
        device_id = await _created_by_onboard_claimed()
    elif path == "onboard_mapped":
        device_id = await _created_by_onboard_mapped()
    else:
        device_id = await _created_by_discover(fake_nso_client)

    assert device_id is not None
    assert await _counter(device_id) == 0, f"{path}: the device was created without a settle counter"


def _alembic(db_url: str, *args: str) -> None:
    """Run the real alembic CLI in a subprocess (its ``fileConfig`` owns the root logger)."""
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=_REPO_ROOT,
        capture_output=True,
        env={**os.environ, "DATABASE_URL": db_url},
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"alembic {args} failed:\n{proc.stdout.decode()}\n{proc.stderr.decode()}")


def test_every_device_insert_path_creates_a_counter_migration(pg_admin):
    """S2.9 — the migration backfills a counter for every device that predates it.

    Driven through the real migration chain: a device inserted at the revision BEFORE the
    counter existed must come out of ``upgrade head`` with one. Forbidden: the upgrade
    leaving pre-existing devices counter-less, which strands every one of them at its first
    terminal write.
    """
    dbname = f"settle_backfill_{uuid.uuid4().hex[:10]}"
    with pg_admin.connect() as conn:
        conn.exec_driver_sql(f'CREATE DATABASE "{dbname}"')
    try:
        url = _url_for(dbname, driver="postgresql+psycopg2")
        _alembic(url, "upgrade", _PRE_COUNTER_REVISION)

        engine = sa.create_engine(url)
        with engine.begin() as conn:
            device_id = conn.exec_driver_sql(
                "INSERT INTO devices "
                "(nso_instance, nso_device_name, mapping_status, source_epoch, created_at, updated_at) "
                "VALUES ('nso-dev', 'lc-pre-migration', 'mapped', 1, now(), now()) RETURNING id"
            ).scalar_one()
        engine.dispose()

        _alembic(url, "upgrade", "head")

        engine = sa.create_engine(url)
        with engine.connect() as conn:
            backfilled = conn.exec_driver_sql(
                f"SELECT last_seq FROM device_settle_counter WHERE device_id = {device_id}"
            ).scalar_one_or_none()
        engine.dispose()

        assert backfilled == 0, "the migration left a pre-existing device without a counter"
    finally:
        _drop_database(pg_admin, dbname, expect_clean=False)


# ── S2.9b (r2-M3): the sweep precedes EVERY terminal recovery ────────────────


@pytest.mark.parametrize("site", ["startup", "periodic"])
async def test_the_counter_sweep_precedes_every_terminal_recovery(adapter_client, monkeypatch, site):
    """S2.9b — recovery terminalizes, so the repair must already have run when it does.

    Forbidden: the sweep appended AFTER the reaper. Recovery's terminalization then raises on
    the missing counter — at startup that can abort the lifespan before the repair it needed
    ever runs, and on the periodic tick it aborts the reap batch every time.

    S2.9 does not cover this: it proves the helper, not its placement.
    """
    device_id = await seed_device(
        nso_device_name=f"lc-sweep-{site}", netbox_device_id=8621 if site == "startup" else 8622
    )
    job_id = await _stranded_running_job(device_id)  # an apply: recovery's disposition is TERMINAL
    await _drop_counter(device_id)
    assert await _counter(device_id) is None

    if site == "startup":

        async def _idle_loop(_worker_id, stop):
            await stop.wait()

        monkeypatch.setattr(worker_mod, "_worker_loop", _idle_loop)
        await worker_mod.start_workers(concurrency=1)
        await worker_mod.stop_workers()
    else:
        from nso_adapter.core import scheduler as scheduler_mod

        monkeypatch.setattr(worker_mod, "ensure_workers", lambda: None)
        await scheduler_mod._scheduled_orphan_reap()

    assert await _counter(device_id) == 1, f"{site}: the sweep did not run before the reaper"
    async with session() as db:
        job = await db.get(Job, job_id)
    assert job.status is JobStatus.failed, f"{site}: the stranded job was not recovered in the same pass"
    assert job.settle_seq == 1, f"{site}: recovery's terminal write took no sequence"
