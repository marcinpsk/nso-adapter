# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The cutover window's reset and read-job discard run as runner commands (#1522 W4, W6)."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import structlog
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from nso_adapter.core.claim import acquire_claim, lock_claim, release_claim
from nso_adapter.core.cutover import CutoverFollowupBlocked, prepare_followup_recovery
from nso_adapter.core.cutover_runner import main
from nso_adapter.core.generation import mark_job_generations_running
from nso_adapter.core.importer import get_nso_client
from nso_adapter.core.jobs import enqueue_job, enqueue_provision_job
from nso_adapter.core.worker import FollowupSyncFailed, run_inspected_generation
from nso_adapter.store.models import (
    DeploymentGeneration,
    DeviceClaim,
    DeviceProjectionStream,
    GenerationMode,
    GenerationStatus,
    Job,
    JobStatus,
    JobType,
)
from tests._secret_discipline import assert_text_free_of
from tests.conftest import AUTH, _write_config, seed_device, session, start_job
from tests.core.test_cutover_runner import _admit, _admit_vlan_removal
from tests.core.test_generation_protocol import put_vlans

pytestmark = pytest.mark.anyio

_DISCARDED = {
    "code": "cutover_discarded",
    "message": "Queued read job discarded before the cutover authority reset",
    "detail": {},
}


@pytest.mark.parametrize("command", ("discard-read-jobs", "reset"))
def test_runner_refuses_a_synchronous_postgresql_url_before_a_transaction(tmp_path, monkeypatch, command):
    config = _write_config(
        tmp_path,
        monkeypatch,
        database_url="postgresql://placeholder:placeholder@127.0.0.1:1/placeholder",
    )
    repo_root = Path(__file__).resolve().parents[2]
    proc = subprocess.run(
        [sys.executable, "-m", "nso_adapter.core.cutover_runner", command],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=60,
        env={"PATH": "/usr/bin:/bin", "CONFIG_FILE": str(config)},
    )

    assert proc.returncode == 1
    assert proc.stdout == ""
    assert proc.stderr.startswith(f"{command.split('-')[0]} refused: database_url must use ")
    assert "postgresql+asyncpg" in proc.stderr
    assert "got 'postgresql'" in proc.stderr
    if "Traceback" in proc.stderr:
        raise AssertionError("the runner printed a traceback for an unsupported driver")


async def _run(capsys, *argv: str) -> tuple[int, str, str]:
    """Run the CLI in its own thread and event loop, as a separate process would."""

    def call() -> int:
        try:
            return main(list(argv))
        except SystemExit as exc:
            return int(exc.code)

    capsys.readouterr()
    was_configured = structlog.is_configured()
    previous = structlog.get_config().copy()
    try:
        code = await asyncio.to_thread(call)
    finally:
        if was_configured:
            structlog.configure(**previous)
        else:
            structlog.reset_defaults()
    captured = capsys.readouterr()
    return code, captured.out, captured.err


async def _release(maintenance_client, name: str, sequence: int) -> tuple[int, int]:
    """Admit and release one vlan Apply through the maintenance runner; return device and job."""
    http, recorder = maintenance_client
    device_id, generation_id, job_id, digest = await _admit(http, name, sequence)
    original = recorder._handle

    async def applied_state(method, url, content=None, headers=None):
        response = await original(method, url, content, headers)
        if "dry-run=" not in url:
            recorder.dry_run_delta = ""
        return response

    recorder._handle = applied_state
    assert await run_inspected_generation(device_id, generation_id, digest) is JobStatus.succeeded
    recorder._handle = original
    return device_id, job_id


async def _authorized(device_id: int) -> dict[str, object]:
    async with session() as db:
        rows = await db.execute(
            select(DeviceProjectionStream.stream, DeviceProjectionStream.authorized_document).where(
                DeviceProjectionStream.device_id == device_id,
                DeviceProjectionStream.authorized_document.is_not(None),
            )
        )
        return dict(rows.all())


async def _jobs(*job_ids: int) -> list[Job]:
    async with session() as db:
        return [await db.get(Job, job_id) for job_id in job_ids]


async def _run_with_live_app(capsys, store_engine, client, *argv: str) -> tuple[int, str, str]:
    """Give the CLI thread its own PostgreSQL pool, as a separate process has."""
    from nso_adapter.store import db as store_db

    async def dispose_cli_engine():
        await store_db.get_engine().dispose()

    original_factory = store_db._session_factory
    await store_engine.dispose()
    try:
        with (
            patch("nso_adapter.main.init_db", store_db.init_db),
            patch("nso_adapter.main._dispose_engine", dispose_cli_engine),
            patch("nso_adapter.main._build_nso_clients", return_value={"nso-dev": client}),
        ):
            return await _run(capsys, *argv)
    finally:
        store_db._engine = store_engine
        store_db._session_factory = original_factory


async def test_window_discards_read_jobs_then_resets_authority(maintenance_client, capsys):
    device_id, apply_id = await _release(maintenance_client, "cutover-window", 16131)
    other_device = await seed_device(nso_device_name="cutover-window-other", netbox_device_id=16132)
    read_types = (JobType.sync, JobType.sync_now, JobType.sync_from_nso, JobType.detect_drift, JobType.connect)
    async with session() as db:
        read_jobs = [(await enqueue_job(device_id, job_type, db))[0] for job_type in read_types]
        read_jobs.append((await enqueue_job(other_device, JobType.sync, db))[0])
    read_ids = [job.id for job in read_jobs]
    assert "vlan" in await _authorized(device_id)

    code, _out, err = await _run(capsys, "reset")
    assert code == 1
    assert err.startswith("reset refused: cutover blocked by ")
    assert f"device {device_id} job {read_ids[0]} (queued)" in err
    assert "vlan" in await _authorized(device_id)

    code, out, _err = await _run(capsys, "discard-read-jobs")
    assert code == 0
    assert json.loads(out) == {
        "discarded": [
            {"device_id": job.device_id, "job_id": job.id, "job_type": job.job_type.value}
            for job in sorted(read_jobs, key=lambda job: (job.device_id, job.id))
        ]
    }
    apply_job, *discarded = await _jobs(apply_id, *read_ids)
    assert apply_job.status is JobStatus.succeeded
    for job in discarded:
        assert job.status is JobStatus.failed
        assert job.error == _DISCARDED
        assert job.run_attempt == 0 and job.started_at is None
    own = sorted(job.settle_seq for job in discarded if job.device_id == device_id)
    assert own == list(range(apply_job.settle_seq + 1, apply_job.settle_seq + 1 + len(read_types)))
    assert [job.settle_seq for job in discarded if job.device_id == other_device] == [1]

    code, out, _err = await _run(capsys, "discard-read-jobs")
    assert (code, json.loads(out)) == (0, {"discarded": []})

    code, out, _err = await _run(capsys, "reset")
    assert code == 0
    assert json.loads(out) == {
        "devices": [
            {
                "device_id": device_id,
                "streams": 1,
                "deployed_keys": 0,
                "pending_clears": 0,
                "tombstones": 0,
                "stream_pending_clears": 0,
            }
        ]
    }
    assert await _authorized(device_id) == {}

    code, out, _err = await _run(capsys, "reset")
    assert (code, json.loads(out)) == (0, {"devices": []})


@pytest.mark.parametrize("blocker", ("admitted_apply", "failed_generation"))
async def test_reset_refusal_exits_one_and_commits_nothing(maintenance_client, capsys, blocker):
    http, _recorder = maintenance_client
    if blocker == "admitted_apply":
        device_id, generation_id, job_id, _digest = await _admit(http, "cutover-reset-refused", 16133)
        expected = (
            f"reset refused: cutover blocked by device {device_id} generation {generation_id} (pending), "
            f"device {device_id} job {job_id} (queued)\n"
        )
    else:
        device_id = await seed_device(nso_device_name="cutover-reset-failed", netbox_device_id=16134)
        async with session() as db:
            db.add(DeviceProjectionStream(device_id=device_id, stream="vlan", authorized_document={"vlan_intent": []}))
            generation = DeploymentGeneration(
                device_id=device_id,
                seq=1,
                mode=GenerationMode.networked,
                status=GenerationStatus.failed,
                document={},
                digest="0" * 64,
                allowed_removal_keys={},
                source_push_seq={},
                stream_revisions={},
            )
            db.add(generation)
            await db.commit()
            generation_id = generation.id
        expected = f"reset refused: cutover blocked by device {device_id} generation {generation_id} (failed)\n"
    before = await _authorized(device_id)
    assert "vlan" in before

    code, out, err = await _run(capsys, "reset")

    assert (code, out, err) == (1, "", expected)
    assert await _authorized(device_id) == before
    async with session() as db:
        assert (await db.get(DeploymentGeneration, generation_id)).status is not GenerationStatus.settled


@pytest.mark.parametrize("offender", ("queued_apply", "running_read", "queued_provision"))
async def test_discard_refuses_and_changes_nothing(maintenance_client, capsys, offender):
    http, _recorder = maintenance_client
    if offender == "queued_apply":
        device_id, _generation_id, offender_id, _digest = await _admit(http, "cutover-discard-apply", 16135)
        described = "queued apply carrying a generation"
    else:
        device_id = await seed_device(nso_device_name=f"cutover-discard-{offender}", netbox_device_id=16136)
        async with session() as db:
            if offender == "running_read":
                offender_id = (await enqueue_job(device_id, JobType.connect, db))[0].id
                await start_job(offender_id)
                described = "running connect"
            else:
                params = {"nso_instance": "nso-dev", "device_name": "cutover-discard-onboard"}
                offender_id = (await enqueue_provision_job(uuid.uuid4(), params, db))[0].id
                described = "queued provision"
    async with session() as db:
        sync_id = (await enqueue_job(device_id, JobType.sync, db))[0].id
    before = {job.id: (job.status, job.error, job.settle_seq) for job in await _jobs(offender_id, sync_id)}

    code, out, err = await _run(capsys, "discard-read-jobs")

    assert (code, out) == (1, "")
    assert err.startswith("discard refused: ")
    assert f"job {offender_id} ({described})" in err
    assert f"job {sync_id} " not in err
    after = {job.id: (job.status, job.error, job.settle_seq) for job in await _jobs(offender_id, sync_id)}
    assert after == before
    assert after[sync_id] == (JobStatus.queued, None, None)


@pytest.mark.parametrize("scoped", (False, True))
async def test_discard_refuses_read_job_carrying_a_generation(maintenance_client, capsys, scoped):
    device_id = await seed_device(nso_device_name="cutover-discard-carrier", netbox_device_id=16137)
    async with session() as db:
        sync_id = (await enqueue_job(device_id, JobType.sync, db))[0].id
        db.add(
            DeploymentGeneration(
                device_id=device_id,
                seq=1,
                mode=GenerationMode.networked,
                status=GenerationStatus.pending,
                document={},
                digest="0" * 64,
                allowed_removal_keys={},
                source_push_seq={},
                stream_revisions={},
                job_id=sync_id,
            )
        )
        await db.commit()

    scope = ("--device", str(device_id)) if scoped else ()
    code, _out, err = await _run(capsys, "discard-read-jobs", *scope)

    assert code == 1
    assert f"device {device_id} job {sync_id} (queued sync carrying a generation)" in err
    async with session() as db:
        assert (await db.get(Job, sync_id)).status is JobStatus.queued
        generation = await db.scalar(select(DeploymentGeneration).where(DeploymentGeneration.job_id == sync_id))
        assert generation.status is GenerationStatus.pending


async def test_device_discard_leaves_other_devices_and_allows_apply(maintenance_client, capsys):
    http, _recorder = maintenance_client
    device_id = await seed_device(nso_device_name="cutover-device-drain", netbox_device_id=16138)
    other_id, _generation_id, other_job_id, _digest = await _admit(http, "cutover-other-apply", 16139)
    async with session() as db:
        own_id = (await enqueue_job(device_id, JobType.sync, db))[0].id
        other_read_id = (await enqueue_job(other_id, JobType.sync, db))[0].id

    code, out, err = await _run(capsys, "discard-read-jobs", "--device", str(device_id))

    assert code == 0
    assert json.loads(out) == {"discarded": [{"device_id": device_id, "job_id": own_id, "job_type": "sync"}]}
    own, other_read, other_apply = await _jobs(own_id, other_read_id, other_job_id)
    assert own.status is JobStatus.failed and own.settle_seq == 1
    assert other_read.status is JobStatus.queued and other_apply.status is JobStatus.queued


async def test_recover_failed_followup_runs_only_sync(maintenance_client, store_engine, capsys):
    device_id, removal = await _admit_vlan_removal(maintenance_client, 16142)
    _http, recorder = maintenance_client
    client = get_nso_client("nso-dev")
    client.get_device_ned_id = AsyncMock(return_value="")
    with pytest.raises(FollowupSyncFailed):
        await run_inspected_generation(device_id, removal.id, removal.digest)
    removal_writes = [commit for commit in recorder.commits if commit["method"] == "put"]
    client.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")

    code, out, err = await _run_with_live_app(capsys, store_engine, client, "recover-followup", str(device_id))

    assert code == 0
    result = json.loads(out)
    assert result["generation_id"] == removal.id
    assert result["status"] == "succeeded"
    assert [commit for commit in recorder.commits if commit["method"] == "put"] == removal_writes
    async with session() as db:
        assert (await db.get(DeploymentGeneration, removal.id)).status is GenerationStatus.settled


async def _admit_mixed_vlan_chain(maintenance_client):
    http, recorder = maintenance_client
    device_id, initial_id, _job_id, digest = await _admit(http, "cutover-companion", 16152)
    original = recorder._handle

    async def applied_state(method, url, content=None, headers=None):
        response = await original(method, url, content, headers)
        if "dry-run=" not in url:
            recorder.dry_run_delta = ""
        return response

    recorder._handle = applied_state
    assert await run_inspected_generation(device_id, initial_id, digest) is JobStatus.succeeded
    recorder._handle = original
    assert (
        await put_vlans(http, device_id, [10, 20], seq=16153, query="?store_only=true", names={10: "", 20: "new"})
    ).status_code == 200
    admitted = await http.post(
        f"/api/v1/devices/{device_id}/actions/apply",
        json={"apply_attempt_id": str(uuid.uuid4()), "selected": {"vlan": 16153}},
        headers=AUTH,
    )
    assert admitted.status_code == 202, admitted.text
    async with session() as db:
        chain = (
            await db.scalars(
                select(DeploymentGeneration)
                .where(DeploymentGeneration.device_id == device_id, DeploymentGeneration.id != initial_id)
                .order_by(DeploymentGeneration.seq)
            )
        ).all()
        assert len(chain) == 2
        removal, companion = chain
        assert removal.removal_context and companion.removal_context is None
        assert removal.apply_attempt_id == companion.apply_attempt_id
        assert companion.status is GenerationStatus.pending and companion.attempts == 0
    return device_id, recorder, removal, companion


async def test_recover_followup_with_pending_companion_apply(maintenance_client, store_engine, capsys):
    device_id, recorder, removal, companion = await _admit_mixed_vlan_chain(maintenance_client)
    client = get_nso_client("nso-dev")
    client.get_device_ned_id = AsyncMock(return_value="")
    with pytest.raises(FollowupSyncFailed):
        await run_inspected_generation(device_id, removal.id, removal.digest)
    removal_writes = [commit for commit in recorder.commits if commit["method"] == "put"]
    client.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")

    code, out, err = await _run_with_live_app(capsys, store_engine, client, "recover-followup", str(device_id))

    assert code == 0, err
    assert json.loads(out)["generation_id"] == removal.id
    assert [commit for commit in recorder.commits if commit["method"] == "put"] == removal_writes
    async with session() as db:
        assert (await db.get(DeploymentGeneration, removal.id)).status is GenerationStatus.settled
        pending = await db.get(DeploymentGeneration, companion.id)
        assert pending.status is GenerationStatus.pending and pending.attempts == 0
        assert (await db.get(Job, pending.job_id)).status is JobStatus.queued


async def test_recover_followup_refuses_a_companion_start_without_deadlock(
    maintenance_client, store_engine, rival_engine
):
    device_id, _recorder, removal, companion = await _admit_mixed_vlan_chain(maintenance_client)
    client = get_nso_client("nso-dev")
    client.get_device_ned_id = AsyncMock(return_value="")
    with pytest.raises(FollowupSyncFailed):
        await run_inspected_generation(device_id, removal.id, removal.digest)

    generations_locked = asyncio.Event()
    worker_has_job = asyncio.Event()

    def observe_generation_lock(_conn, _cursor, statement, *_rest):
        if "FROM deployment_generation" in statement and "FOR UPDATE" in statement:
            generations_locked.set()

    async def start_companion():
        reg = await acquire_claim(device_id, "job")
        assert reg is not None
        try:
            maker = async_sessionmaker(rival_engine, expire_on_commit=False)
            async with maker() as db:
                await lock_claim(db, reg)
                job = await db.get(Job, companion.job_id, with_for_update=True)
                assert job is not None and job.status is JobStatus.queued
                worker_has_job.set()
                await asyncio.wait_for(generations_locked.wait(), timeout=10)
                await mark_job_generations_running(db, job.id)
                await db.commit()
        finally:
            await release_claim(reg)

    async def recover():
        async with session() as db:
            return await prepare_followup_recovery(db, device_id)

    event.listen(store_engine.sync_engine, "after_cursor_execute", observe_generation_lock)
    try:
        worker = asyncio.create_task(start_companion())
        await asyncio.wait_for(worker_has_job.wait(), timeout=10)
        results = await asyncio.wait_for(
            asyncio.gather(asyncio.create_task(recover()), worker, return_exceptions=True), timeout=15
        )
    finally:
        event.remove(store_engine.sync_engine, "after_cursor_execute", observe_generation_lock)

    assert isinstance(results[0], CutoverFollowupBlocked), results
    assert f"device {device_id} is busy" in str(results[0])
    assert results[1] is None, results
    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None
        assert (await db.get(DeploymentGeneration, companion.id)).status is GenerationStatus.running


async def test_recover_followup_refuses_executed_companion(maintenance_client, store_engine, capsys):
    device_id, _recorder, removal, companion = await _admit_mixed_vlan_chain(maintenance_client)
    client = get_nso_client("nso-dev")
    client.get_device_ned_id = AsyncMock(return_value="")
    with pytest.raises(FollowupSyncFailed):
        await run_inspected_generation(device_id, removal.id, removal.digest)
    client.get_device_ned_id = AsyncMock(return_value="cisco-ios-cli-6.95")
    assert await run_inspected_generation(device_id, companion.id, companion.digest) is JobStatus.succeeded

    code, out, err = await _run_with_live_app(capsys, store_engine, client, "recover-followup", str(device_id))

    assert (code, out) == (1, "")
    assert f"generation {companion.id} follows the settled removal" in err
    async with session() as db:
        assert (await db.get(DeploymentGeneration, removal.id)).status is GenerationStatus.settled
        assert (await db.get(DeploymentGeneration, companion.id)).attempts == 1


async def test_recover_missing_followup_creates_and_runs_only_sync(maintenance_client, store_engine, capsys):
    device_id, removal = await _admit_vlan_removal(maintenance_client, 16150)
    _http, recorder = maintenance_client
    followup_context = json.dumps({"followup_of_job_id": removal.job_id})

    def fail_followup_insert(_conn, _cursor, statement, parameters, _context, _executemany):
        values = parameters.values() if isinstance(parameters, dict) else parameters
        if statement.startswith("INSERT INTO jobs ") and followup_context in values:
            raise RuntimeError("follow-up insert failed")

    event.listen(store_engine.sync_engine, "before_cursor_execute", fail_followup_insert)
    try:
        with pytest.raises(FollowupSyncFailed, match="queued no follow-up sync"):
            await run_inspected_generation(device_id, removal.id, removal.digest)
    finally:
        event.remove(store_engine.sync_engine, "before_cursor_execute", fail_followup_insert)
    removal_writes = [commit for commit in recorder.commits if commit["method"] == "put"]
    client = get_nso_client("nso-dev")

    code, out, err = await _run_with_live_app(capsys, store_engine, client, "recover-followup", str(device_id))

    assert code == 0
    assert json.loads(out)["status"] == "succeeded"
    assert [commit for commit in recorder.commits if commit["method"] == "put"] == removal_writes
    async with session() as db:
        sync = await db.get(Job, json.loads(out)["job_id"])
        assert sync.status is JobStatus.succeeded
        assert sync.context == {"followup_of_job_id": removal.job_id}


@pytest.mark.parametrize("offender", ("queued_apply", "running_read"))
async def test_device_discard_refuses_its_own_live_blocker(maintenance_client, capsys, offender):
    http, _recorder = maintenance_client
    if offender == "queued_apply":
        device_id, _generation_id, blocker_id, _digest = await _admit(http, "cutover-scoped-apply", 16143)
    else:
        device_id = await seed_device(nso_device_name="cutover-scoped-running", netbox_device_id=16144)
        async with session() as db:
            blocker_id = (await enqueue_job(device_id, JobType.connect, db))[0].id
        await start_job(blocker_id)
    async with session() as db:
        read_id = (await enqueue_job(device_id, JobType.sync, db))[0].id

    code, out, err = await _run(capsys, "discard-read-jobs", "--device", str(device_id))

    assert (code, out) == (1, "")
    assert err.startswith("discard refused: ") and f"job {blocker_id}" in err
    assert (await _jobs(read_id))[0].status is JobStatus.queued


@pytest.mark.parametrize("state", ("no_generation", "pending_apply", "succeeded_followup"))
async def test_recover_followup_refuses_without_failed_settled_removal(maintenance_client, store_engine, capsys, state):
    http, _recorder = maintenance_client
    if state == "no_generation":
        device_id = await seed_device(nso_device_name="cutover-no-followup", netbox_device_id=16147)
    elif state == "pending_apply":
        device_id, _generation_id, _job_id, _digest = await _admit(http, "cutover-pending-followup", 16148)
    else:
        device_id, removal = await _admit_vlan_removal(maintenance_client, 16149)
        assert await run_inspected_generation(device_id, removal.id, removal.digest) is JobStatus.succeeded
    client = get_nso_client("nso-dev")

    code, out, err = await _run_with_live_app(capsys, store_engine, client, "recover-followup", str(device_id))

    assert (code, out) == (1, "")
    assert "recover-followup refused: " in err


async def test_recover_followup_refuses_missing_device(maintenance_client, store_engine, capsys):
    device_id = 999999
    client = get_nso_client("nso-dev")

    code, out, err = await _run_with_live_app(capsys, store_engine, client, "recover-followup", str(device_id))

    assert (code, out) == (1, "")
    assert err.endswith(f"recover-followup refused: device {device_id} no longer exists\n")
    assert_text_free_of(err, ["Traceback"])
