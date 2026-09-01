# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""#1396 R1b chunk 3 / Q10: teardown under the claim, with the intent roots deleted first.

Three defects this pins, all reachable today:

- ``InterfaceIntent``'s FK to ``interfaces`` is restrictive and teardown BULK-deletes
  ``DbInterface``, which bypasses the ORM cascade — so a device with any interface-intent
  row cannot be offboarded at all (M6.21);
- every other intent family is deleted implicitly by ``db.delete(device)``, i.e. AFTER the
  ``Job.device_id`` null-out, which is the ``jobs -> intent`` order that deadlocks against
  an intent endpoint holding an intent row and reaching for the queued apply winner;
- nulling a QUEUED job's ``device_id`` manufactures a non-provision claimless job that the
  worker would dispatch against a device that no longer exists (M6.9m).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa

from nso_adapter.core.claim import ClaimUnavailableError, acquire_claim, release_claim
from tests.conftest import VALID_TOKEN, seed_device, session

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}


def _intent_root_models() -> list:
    """The direct intent roots, derived from the mapper — never hard-coded.

    A family added later joins this list automatically, so it cannot silently
    reintroduce a deferred cascade after the job null-out.
    """
    from nso_adapter.store.models import Device

    return [
        rel.mapper.class_
        for rel in Device.__mapper__.relationships
        if rel.mapper.class_.__tablename__.endswith("_intent")
    ]


def _fill(model, **fixed):
    """Build one row of *model*, filling every NOT NULL column with a typed dummy.

    Structural on purpose: hand-listing columns is how a new NOT NULL column quietly
    stops being exercised.
    """
    values = dict(fixed)
    for col in model.__table__.columns:
        if col.name in values or col.primary_key or col.nullable:
            continue
        if col.default is not None or col.server_default is not None:
            continue
        if col.foreign_keys:
            continue  # supplied by the caller
        python_type = col.type.python_type
        if python_type is bool:
            values[col.name] = False
        elif python_type is int:
            values[col.name] = 1
        elif python_type is float:
            values[col.name] = 1.0
        elif python_type is datetime:
            values[col.name] = datetime.now(UTC)
        elif python_type in (dict, list):
            values[col.name] = python_type()
        else:
            values[col.name] = "x"
    return model(**values)


async def _seed_every_intent_root(device_id: int) -> int:
    """One row per direct intent root, plus the two that hang off an interface."""
    from nso_adapter.store.models import DbInterface, InterfaceIntent, InterfaceIpIntent

    models = _intent_root_models()
    async with session() as db:
        for model in models:
            db.add(_fill(model, device_id=device_id))
        iface = DbInterface(device_id=device_id, name="Ethernet1")
        db.add(iface)
        await db.flush()
        # The restrictive FK: a bulk DELETE of interfaces cannot cascade to it.
        db.add(_fill(InterfaceIntent, interface_id=iface.id, attribute="description", intent_value="x"))
        db.add(_fill(InterfaceIpIntent, interface_id=iface.id, address="192.0.2.1/24", vrf=""))
        await db.commit()
    return len(models)


async def _count(model, device_id: int) -> int:
    async with session() as db:
        return await db.scalar(sa.select(sa.func.count()).select_from(model).where(model.device_id == device_id))


async def _offboard(device_id: int) -> None:
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device

    async with session() as db:
        device = await db.get(Device, device_id)
        await offboard_device(db, device)


async def test_teardown_deletes_every_intent_root_including_interface_intent(adapter_client):
    """M6.21 — 24 direct roots seeded for real, plus the two interface-rooted families."""
    from nso_adapter.store.models import Device, InterfaceIntent

    device_id = await seed_device(nso_device_name="td-roots", netbox_device_id=9500)
    root_count = await _seed_every_intent_root(device_id)
    assert root_count == 24, "the mapper's direct intent-root set moved; update the brief's enumeration"

    await _offboard(device_id)

    async with session() as db:
        assert await db.get(Device, device_id) is None
        assert await db.scalar(sa.select(sa.func.count()).select_from(InterfaceIntent)) == 0
        for model in _intent_root_models():
            remaining = await db.scalar(
                sa.select(sa.func.count()).select_from(model).where(model.device_id == device_id)
            )
            assert remaining == 0, f"{model.__tablename__} survived the teardown"


async def test_teardown_terminalizes_queued_jobs_instead_of_orphaning_them(adapter_client):
    """M6.9m — a nulled QUEUED job is a claimless job the worker would still dispatch."""
    from nso_adapter.store.models import Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="td-queued", netbox_device_id=9501)
    async with session() as db:
        db.add(Job(job_type=JobType.sync, device_id=device_id, status=JobStatus.queued, coalescible=True, context={}))
        db.add(
            Job(
                job_type=JobType.removal,
                device_id=device_id,
                status=JobStatus.queued,
                coalescible=False,
                context={},
            )
        )
        db.add(
            Job(job_type=JobType.apply, device_id=device_id, status=JobStatus.succeeded, coalescible=True, context={})
        )
        await db.commit()

    await _offboard(device_id)

    async with session() as db:
        rows = (await db.execute(sa.select(Job).order_by(Job.id))).scalars().all()
        assert [r.status for r in rows] == [JobStatus.failed, JobStatus.failed, JobStatus.succeeded]
        assert {r.error["code"] for r in rows if r.error} == {"device_offboarded"}
        # History is preserved by nulling device_id — but only once every row is terminal.
        assert all(r.device_id is None for r in rows)
        claimless_and_live = [
            r
            for r in rows
            if r.device_id is None
            and r.job_type != JobType.provision
            and r.status in (JobStatus.queued, JobStatus.running)
        ]
        assert claimless_and_live == [], "a non-provision claimless job survives; the worker would dispatch it"


async def test_a_teardown_failure_after_the_guard_lock_neither_hangs_nor_leaks(adapter_client, monkeypatch):
    """The body dies AFTER ``lock_claim`` took the claim row FOR UPDATE in the caller's
    session. Releasing through a second session then waits on our own uncommitted lock and
    the offboard hangs forever. The guarded transaction must be rolled back first."""
    from nso_adapter.core import onboarding as onboarding_mod
    from nso_adapter.store.models import Device, DeviceClaim

    device_id = await seed_device(nso_device_name="td-claim-lockfail", netbox_device_id=9505)

    def _boom():
        raise RuntimeError("forced post-lock failure")

    monkeypatch.setattr(onboarding_mod, "intent_root_models", _boom)

    async with session() as db:
        device = await db.get(Device, device_id)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(onboarding_mod.offboard_device(db, device), timeout=15)

    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None, "the claim leaked"


async def test_teardown_waits_for_a_held_claim_and_then_refuses(adapter_client, monkeypatch):
    """M6.5 — teardown is a claim holder like any other; it never tears down under a runner."""
    from nso_adapter.config import get_config
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="td-busy", netbox_device_id=9502)
    monkeypatch.setattr(get_config(), "intent_claim_wait_seconds", 0.3)

    holder = await acquire_claim(device_id, "job")
    try:
        with pytest.raises(ClaimUnavailableError):
            await _offboard(device_id)
    finally:
        await release_claim(holder)

    async with session() as db:
        assert await db.get(Device, device_id) is not None


async def test_a_running_job_cannot_be_offboarded_from_under(adapter_client, monkeypatch):
    """The API surface of the same rule: 409, not a half-deleted device."""
    from nso_adapter.config import get_config
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="td-busy-api", netbox_device_id=9503)
    monkeypatch.setattr(get_config(), "intent_claim_wait_seconds", 0.3)

    holder = await acquire_claim(device_id, "job")
    try:
        resp = await adapter_client.delete(f"/api/v1/devices/{device_id}", headers=AUTH)
    finally:
        await release_claim(holder)

    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "conflict"
    async with session() as db:
        assert await db.get(Device, device_id) is not None


async def test_teardown_excludes_a_concurrent_claimant_for_its_whole_run(adapter_client, monkeypatch):
    """The sweeper interlock's mechanism (R3-1): the claim, not the devices row lock."""
    from nso_adapter.config import get_config
    from nso_adapter.store.models import Device

    device_id = await seed_device(nso_device_name="td-excl", netbox_device_id=9504)
    monkeypatch.setattr(get_config(), "intent_claim_wait_seconds", 30.0)

    holder = await acquire_claim(device_id, "job")
    task = asyncio.create_task(_offboard(device_id))
    try:
        await asyncio.sleep(0.4)
        assert not task.done(), "teardown started while another holder had the device"
    finally:
        await release_claim(holder)

    await asyncio.wait_for(task, timeout=30)
    async with session() as db:
        assert await db.get(Device, device_id) is None
        # Cascade-deleted with the device; nothing is left claiming a device that is gone.
        assert (
            await db.scalar(sa.text("SELECT count(*) FROM device_claim WHERE device_id = :d"), {"d": device_id})
        ) == 0
