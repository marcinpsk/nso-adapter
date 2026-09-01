# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The shared device lock orders projection writers, teardown, and job admission."""

from __future__ import annotations

import asyncio
from contextlib import nullcontext
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker

from tests.conftest import VALID_TOKEN, seed_device, session
from tests.core.test_generation_protocol import put_vlans, seed_settings

pytestmark = pytest.mark.anyio

_AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}
_A = ("", "198.18.0.0/24", "192.0.2.1")
_B = ("", "198.18.1.0/24", "192.0.2.2")


async def _backend_pid(db) -> int:
    return await db.scalar(sa.text("SELECT pg_backend_pid()"))


async def _wait_for_blocked_query(
    engine,
    *,
    blocker_pid: int,
    relation: str,
    fragments: tuple[str, ...],
    waiter_pid: int | None = None,
    timeout: float = 5.0,
) -> int:
    """Return the waiter that *blocker_pid* parks on *relation*."""
    pid_clause = "AND activity.pid = :waiter_pid " if waiter_pid is not None else ""
    statement = sa.text(
        "SELECT activity.pid, lower(activity.query) "
        "FROM pg_stat_activity AS activity "
        "WHERE activity.datname = current_database() "
        "AND activity.wait_event_type = 'Lock' "
        "AND CAST(:blocker_pid AS integer) = ANY(pg_blocking_pids(activity.pid)) "
        f"{pid_clause}"
        "AND EXISTS ("
        "SELECT 1 FROM pg_locks AS lock "
        "WHERE lock.pid = activity.pid "
        "AND lock.relation = to_regclass(:relation)"
        ")"
    )
    params = {"blocker_pid": blocker_pid, "relation": relation}
    if waiter_pid is not None:
        params["waiter_pid"] = waiter_pid
    deadline = asyncio.get_running_loop().time() + timeout
    async with engine.connect() as probe:
        while asyncio.get_running_loop().time() < deadline:
            await probe.execute(sa.text("SELECT pg_stat_clear_snapshot()"))
            rows = (await probe.execute(statement, params)).all()
            for pid, query in rows:
                if all(fragment.lower() in query for fragment in fragments):
                    return pid
            await asyncio.sleep(0.02)
    detail = ", ".join(fragments)
    raise AssertionError(f"no query containing [{detail}] waited on {relation!r} within {timeout:g}s")


async def _seed_counter(device_id: int) -> None:
    from nso_adapter.core.generation import lock_projection

    async with session() as db:
        await lock_projection(db, device_id)
        await db.commit()


async def _seed_bfd_intent(device_id: int) -> int:
    from nso_adapter.store.models import BfdIntent

    async with session() as db:
        row = BfdIntent(device_id=device_id, interface_name="Ethernet1", min_tx=300)
        db.add(row)
        await db.commit()
        return row.id


async def _seed_tombstone(device_id: int, *, job_id: int | None = None) -> int:
    from nso_adapter.store.models import StaticRouteTombstone

    async with session() as db:
        row = StaticRouteTombstone(
            device_id=device_id,
            route_id=1,
            vrf=_A[0],
            prefix=_A[1],
            next_hop=_A[2],
            deployed_key=list(_A),
            marking="delete_origin",
            job_id=job_id,
        )
        db.add(row)
        await db.commit()
        return row.id


async def _seed_succeeded_removal(device_id: int) -> int:
    from nso_adapter.store.models import Job, JobStatus, JobType

    async with session() as db:
        job = Job(
            job_type=JobType.removal,
            device_id=device_id,
            status=JobStatus.succeeded,
            coalescible=False,
            context={"scope": "static_route"},
        )
        db.add(job)
        await db.commit()
        return job.id


async def _seed_retry_head(device_id: int) -> None:
    from nso_adapter.core.generation import digest_document
    from nso_adapter.store.models import (
        DeploymentGeneration,
        GenerationMode,
        GenerationStatus,
        Job,
        JobStatus,
        JobType,
    )

    await _seed_counter(device_id)
    document: dict = {}
    async with session() as db:
        failed = Job(
            job_type=JobType.apply,
            device_id=device_id,
            status=JobStatus.failed,
            coalescible=True,
        )
        db.add(failed)
        await db.flush()
        db.add(
            DeploymentGeneration(
                device_id=device_id,
                seq=1,
                mode=GenerationMode.networked,
                status=GenerationStatus.failed,
                document=document,
                digest=digest_document(GenerationMode.networked, document, {}),
                allowed_removal_keys={},
                source_push_seq={},
                stream_revisions={},
                job_id=failed.id,
            )
        )
        await db.commit()


async def _job_ids() -> list[int]:
    from nso_adapter.store.models import Job

    async with session() as db:
        return list((await db.execute(sa.select(Job.id).order_by(Job.id))).scalars().all())


async def test_projection_lock_takes_the_device_before_the_counter(adapter_client, rival_engine):
    from nso_adapter.core.generation import lock_projection
    from nso_adapter.store.db import get_engine
    from nso_adapter.store.models import Device, DeviceGenerationCounter

    device_id = await seed_device(nso_device_name="lock-device-counter", netbox_device_id=9910)
    await _seed_counter(device_id)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with session() as holder, rival() as writer, rival() as counter_probe:
        holder_pid = await _backend_pid(holder)
        writer_pid = await _backend_pid(writer)
        await holder.execute(sa.select(Device.id).where(Device.id == device_id).with_for_update())

        locking = asyncio.create_task(lock_projection(writer, device_id))
        try:
            await _wait_for_blocked_query(
                get_engine(),
                blocker_pid=holder_pid,
                waiter_pid=writer_pid,
                relation="devices",
                fragments=("from devices", "for no key update"),
            )
            assert not locking.done()
            held = await counter_probe.scalar(
                sa.select(DeviceGenerationCounter.device_id)
                .where(DeviceGenerationCounter.device_id == device_id)
                .with_for_update(nowait=True)
            )
            assert held == device_id
            await counter_probe.rollback()
            await holder.commit()
            await asyncio.wait_for(locking, timeout=10)
        finally:
            if not locking.done():
                locking.cancel()
                await asyncio.gather(locking, return_exceptions=True)
            await holder.rollback()
            await writer.rollback()


async def test_projection_writer_commits_before_real_offboard(adapter_client, rival_engine):
    from nso_adapter.core import claim as claim_module
    from nso_adapter.core.generation import lock_projection
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.db import get_engine
    from nso_adapter.store.models import Device, DeviceClaim, Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="lock-writer-wins", netbox_device_id=9911)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with session() as writer, rival() as offboarding:
        writer_pid = await _backend_pid(writer)
        device = await offboarding.get(Device, device_id)
        offboarding_pid = await _backend_pid(offboarding)
        claim_acquired = asyncio.Event()
        continue_offboard = asyncio.Event()
        acquire_claim = claim_module.acquire_claim_or_refuse

        async def acquire_claim_then_wait(*args, **kwargs):
            registration = await acquire_claim(*args, **kwargs)
            claim_acquired.set()
            await continue_offboard.wait()
            return registration

        with patch.object(claim_module, "acquire_claim_or_refuse", acquire_claim_then_wait):
            teardown = asyncio.create_task(offboard_device(offboarding, device))
            try:
                await asyncio.wait_for(claim_acquired.wait(), timeout=10)
                async with session() as check:
                    assert await check.get(DeviceClaim, device_id) is not None

                await lock_projection(writer, device_id)
                continue_offboard.set()
                await _wait_for_blocked_query(
                    get_engine(),
                    blocker_pid=writer_pid,
                    waiter_pid=offboarding_pid,
                    relation="devices",
                    fragments=("from devices", "for update"),
                )
                job = Job(
                    job_type=JobType.apply,
                    device_id=device_id,
                    status=JobStatus.queued,
                    coalescible=True,
                )
                writer.add(job)
                await writer.commit()
                job_id = job.id
                await asyncio.wait_for(teardown, timeout=10)
            finally:
                continue_offboard.set()
                if not teardown.done():
                    teardown.cancel()
                    await asyncio.gather(teardown, return_exceptions=True)

    async with session() as db:
        stored = await db.get(Job, job_id)
        assert stored.status is JobStatus.failed
        assert stored.device_id is None
        assert stored.error["code"] == "device_offboarded"


async def test_switching_writer_commits_before_real_offboard(adapter_client, rival_engine):
    from nso_adapter.core import claim as claim_module
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.core.switching_intent import LagBundleSnapshot, replace_lag_snapshot
    from nso_adapter.store.db import get_engine
    from nso_adapter.store.models import Device, LagBundleIntent

    device_id = await seed_device(nso_device_name="lock-switching-writer-wins", netbox_device_id=9912)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with session() as writer, rival() as offboarding:
        writer_pid = await _backend_pid(writer)
        device = await offboarding.get(Device, device_id)
        offboarding_pid = await _backend_pid(offboarding)
        claim_acquired = asyncio.Event()
        continue_offboard = asyncio.Event()
        acquire_claim = claim_module.acquire_claim_or_refuse

        async def acquire_claim_then_wait(*args, **kwargs):
            registration = await acquire_claim(*args, **kwargs)
            claim_acquired.set()
            await continue_offboard.wait()
            return registration

        with patch.object(claim_module, "acquire_claim_or_refuse", acquire_claim_then_wait):
            teardown = asyncio.create_task(offboard_device(offboarding, device))
            try:
                await asyncio.wait_for(claim_acquired.wait(), timeout=10)
                await replace_lag_snapshot(
                    writer,
                    device_id,
                    (LagBundleSnapshot(name="Port-channel1", lag_id=1),),
                )
                continue_offboard.set()
                await _wait_for_blocked_query(
                    get_engine(),
                    blocker_pid=writer_pid,
                    waiter_pid=offboarding_pid,
                    relation="devices",
                    fragments=("from devices", "for update"),
                )
                await writer.commit()
                await asyncio.wait_for(teardown, timeout=10)
            finally:
                continue_offboard.set()
                if not teardown.done():
                    teardown.cancel()
                    await asyncio.gather(teardown, return_exceptions=True)

    async with session() as db:
        assert await db.get(Device, device_id) is None
        assert await db.scalar(sa.select(sa.func.count()).select_from(LagBundleIntent)) == 0


async def test_document_snapshot_waits_for_a_switching_replacement(adapter_client, rival_engine):
    from nso_adapter.core.generation import lock_device_document
    from nso_adapter.core.switching_intent import (
        LagBundleSnapshot,
        LagMemberSnapshot,
        render_switching_sections,
        replace_lag_snapshot,
    )
    from nso_adapter.store.db import get_engine

    device_id = await seed_device(nso_device_name="lock-switching-snapshot", netbox_device_id=9913)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with session() as writer, rival() as reader:
        writer_pid = await _backend_pid(writer)
        reader_pid = await _backend_pid(reader)
        await replace_lag_snapshot(
            writer,
            device_id,
            (
                LagBundleSnapshot(
                    name="Port-channel1",
                    lag_id=1,
                    members=(LagMemberSnapshot(interface_name="Gi0/1", mode="active"),),
                ),
            ),
        )

        async def read_document():
            await lock_device_document(reader, device_id)
            document = await render_switching_sections(reader, device_id)
            await reader.rollback()
            return document

        reading = asyncio.create_task(read_document())
        try:
            await _wait_for_blocked_query(
                get_engine(),
                blocker_pid=writer_pid,
                waiter_pid=reader_pid,
                relation="devices",
                fragments=("from devices", "for no key update"),
            )
            assert not reading.done()
            await writer.commit()
            document = await asyncio.wait_for(reading, timeout=10)
        finally:
            if not reading.done():
                reading.cancel()
                await asyncio.gather(reading, return_exceptions=True)

    assert document == {
        "lag": {
            "bundle": [
                {
                    "name": "Port-channel1",
                    "lag-id": 1,
                    "member": [{"interface-name": "Gi0/1", "mode": "active"}],
                }
            ]
        }
    }


async def _prepare_offboard_loser(kind: str, adapter_client, device_id: int) -> None:
    if kind == "retry_carrier":
        await _seed_retry_head(device_id)
    elif kind == "auto_apply_put":
        await seed_settings(device_id, auto_apply=True)
    elif kind == "removal_put":
        await seed_settings(device_id, auto_apply=False)
        response = await put_vlans(adapter_client, device_id, [10], seq=1, query="?store_only=true")
        assert response.status_code == 200


async def _run_offboard_loser(kind: str, adapter_client, device_id: int):
    if kind == "retry_carrier":
        from nso_adapter.store.models import DeploymentGeneration

        async with session() as db:
            generation_id = await db.scalar(
                sa.select(DeploymentGeneration.id).where(DeploymentGeneration.device_id == device_id)
            )
        return await adapter_client.post(
            f"/api/v1/devices/{device_id}/actions/retry-generation",
            json={"generation_id": generation_id},
            headers=_AUTH,
        )
    if kind == "auto_apply_put":
        return await put_vlans(adapter_client, device_id, [10], seq=1)
    if kind == "removal_put":
        return await put_vlans(adapter_client, device_id, [], seq=2, query="?delete_origin=true")
    if kind == "lag_store":
        return await adapter_client.post(
            f"/api/v1/devices/{device_id}/lag-config/apply",
            json={"bundles": [{"name": "Port-channel1", "lag_id": 1}]},
            headers=_AUTH,
        )
    if kind == "switchport_store":
        return await adapter_client.post(
            f"/api/v1/devices/{device_id}/switchport/apply",
            json={"interfaces": [{"interface_name": "Gi0/1", "untagged_vlan": 10}]},
            headers=_AUTH,
        )
    return await adapter_client.post(
        f"/api/v1/devices/{device_id}/actions/force-removal",
        json={"scope": "vlan"},
        headers=_AUTH,
    )


@pytest.mark.parametrize(
    "writer_kind",
    ("retry_carrier", "auto_apply_put", "removal_put", "force_removal", "lag_store", "switchport_store"),
)
async def test_real_offboard_commits_before_a_waiting_writer(adapter_client, rival_engine, writer_kind):
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.db import get_engine
    from nso_adapter.store.models import BfdIntent, Device

    device_id = await seed_device(nso_device_name=f"lock-offboard-{writer_kind}", netbox_device_id=9920)
    await _prepare_offboard_loser(writer_kind, adapter_client, device_id)
    bfd_id = await _seed_bfd_intent(device_id)
    jobs_before = await _job_ids()
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with rival() as gate, rival() as offboarding:
        gate_pid = await _backend_pid(gate)
        await gate.execute(sa.select(BfdIntent.id).where(BfdIntent.id == bfd_id).with_for_update())
        device = await offboarding.get(Device, device_id)
        offboarding_pid = await _backend_pid(offboarding)
        teardown = asyncio.create_task(offboard_device(offboarding, device))
        writer = None
        try:
            await _wait_for_blocked_query(
                get_engine(),
                blocker_pid=gate_pid,
                waiter_pid=offboarding_pid,
                relation="bfd_intent",
                fragments=("delete from bfd_intent",),
            )
            writer = asyncio.create_task(_run_offboard_loser(writer_kind, adapter_client, device_id))
            await _wait_for_blocked_query(
                get_engine(),
                blocker_pid=offboarding_pid,
                relation="devices",
                fragments=("from devices", "for no key update"),
            )
            assert not writer.done()
            await gate.rollback()
            await asyncio.wait_for(teardown, timeout=10)
            response = await asyncio.wait_for(writer, timeout=10)
        finally:
            await gate.rollback()
            tasks = [task for task in (writer, teardown) if task is not None and not task.done()]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"
    assert await _job_ids() == jobs_before


@pytest.mark.parametrize("producer", ("sweep", "reclaim"))
async def test_claimed_producers_lock_projection_before_tombstones(adapter_client, rival_engine, producer):
    from nso_adapter.store.db import get_engine
    from nso_adapter.store.models import Device, DeviceGenerationCounter, StaticRouteTombstone

    device_id = await seed_device(nso_device_name=f"lock-{producer}", netbox_device_id=9930)
    await _seed_counter(device_id)
    owner = await _seed_succeeded_removal(device_id) if producer == "reclaim" else None
    tombstone_id = await _seed_tombstone(device_id, job_id=owner)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    if producer == "reclaim":
        from nso_adapter.core.static_route_reclaim import reclaim_one_device
        from tests.core.test_static_route_removal import SrFake, sr_client, wire

        fake = SrFake("lock-reclaim", service=[wire(_B)], device=[wire(_B)])
        patcher = patch("nso_adapter.core.importer.get_nso_client", return_value=sr_client(fake))
        run = reclaim_one_device
    else:
        from nso_adapter.core.tombstone_sweep import sweep_one_device

        patcher = nullcontext()
        run = sweep_one_device

    async with rival() as gate, rival() as worker, rival() as device_waiter, rival() as counter_waiter:
        gate_pid = await _backend_pid(gate)
        device_waiter_pid = await _backend_pid(device_waiter)
        counter_waiter_pid = await _backend_pid(counter_waiter)
        await gate.execute(
            sa.select(StaticRouteTombstone.id).where(StaticRouteTombstone.id == tombstone_id).with_for_update()
        )
        with patcher:
            producing = asyncio.create_task(run(device_id, db=worker))
            device_lock = counter_lock = None
            try:
                producer_pid = await _wait_for_blocked_query(
                    get_engine(),
                    blocker_pid=gate_pid,
                    relation="static_route_tombstone",
                    fragments=("from static_route_tombstone", "for update"),
                )
                device_lock = asyncio.create_task(
                    device_waiter.execute(sa.select(Device.id).where(Device.id == device_id).with_for_update())
                )
                await _wait_for_blocked_query(
                    get_engine(),
                    blocker_pid=producer_pid,
                    waiter_pid=device_waiter_pid,
                    relation="devices",
                    fragments=("from devices", "for update"),
                )
                counter_lock = asyncio.create_task(
                    counter_waiter.execute(
                        sa.select(DeviceGenerationCounter.device_id)
                        .where(DeviceGenerationCounter.device_id == device_id)
                        .with_for_update()
                    )
                )
                await _wait_for_blocked_query(
                    get_engine(),
                    blocker_pid=producer_pid,
                    waiter_pid=counter_waiter_pid,
                    relation="device_generation_counter",
                    fragments=("from device_generation_counter", "for update"),
                )
                await gate.rollback()
                await asyncio.wait_for(producing, timeout=10)
            finally:
                await gate.rollback()
                tasks = [
                    task for task in (device_lock, counter_lock, producing) if task is not None and not task.done()
                ]
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                for waiter in (device_waiter, counter_waiter):
                    await waiter.rollback()


async def test_ordinary_admission_takes_device_key_share_before_insert(adapter_client, rival_engine):
    from nso_adapter.core.jobs import admit_coalescible_job
    from nso_adapter.store.db import get_engine
    from nso_adapter.store.models import Device, JobType

    device_id = await seed_device(nso_device_name="lock-job-admission", netbox_device_id=9940)
    rival = async_sessionmaker(rival_engine, expire_on_commit=False)

    async with session() as holder, rival() as writer:
        holder_pid = await _backend_pid(holder)
        writer_pid = await _backend_pid(writer)
        await holder.execute(sa.select(Device.id).where(Device.id == device_id).with_for_update())
        admitting = asyncio.create_task(admit_coalescible_job(writer, device_id, JobType.sync))
        try:
            await _wait_for_blocked_query(
                get_engine(),
                blocker_pid=holder_pid,
                waiter_pid=writer_pid,
                relation="devices",
                fragments=("from devices", "for key share"),
            )
            assert not admitting.done()
            await holder.commit()
            created, winner = await asyncio.wait_for(admitting, timeout=10)
            assert created is not None
            assert winner is None
            await writer.commit()
        finally:
            if not admitting.done():
                admitting.cancel()
                await asyncio.gather(admitting, return_exceptions=True)
            await holder.rollback()
            await writer.rollback()
