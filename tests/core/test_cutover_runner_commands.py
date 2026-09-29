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

import pytest
import structlog
from sqlalchemy import select

from nso_adapter.core.cutover_runner import main
from nso_adapter.core.jobs import enqueue_job, enqueue_provision_job
from nso_adapter.core.worker import run_inspected_generation
from nso_adapter.store.models import (
    DeploymentGeneration,
    DeviceProjectionStream,
    GenerationMode,
    GenerationStatus,
    Job,
    JobStatus,
    JobType,
)
from tests.conftest import _write_config, seed_device, session, start_job
from tests.core.test_cutover_runner import _admit
from tests.store.test_db_init import _RETIRED_SCHEME, _RETIRED_URL

pytestmark = pytest.mark.anyio

_DISCARDED = {
    "code": "cutover_discarded",
    "message": "Queued read job discarded before the cutover authority reset",
    "detail": {},
}


@pytest.mark.parametrize("command", ("discard-read-jobs", "reset"))
@pytest.mark.parametrize(
    ("database_url", "expected"),
    (
        ("postgresql://placeholder:placeholder@127.0.0.1:1/placeholder", ("must use ", "got 'postgresql'")),
        (_RETIRED_URL, ("must be a PostgreSQL URL", f"got {_RETIRED_SCHEME!r}")),
    ),
)
def test_runner_refuses_an_unsupported_url_before_a_transaction(tmp_path, monkeypatch, command, database_url, expected):
    config = _write_config(tmp_path, monkeypatch, database_url=database_url)
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
    assert proc.stderr.startswith(f"{command.split('-')[0]} refused: database_url {expected[0]}")
    assert "postgresql+asyncpg" in proc.stderr
    assert expected[1] in proc.stderr
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


async def test_discard_refuses_read_job_carrying_a_generation(maintenance_client, capsys):
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

    code, _out, err = await _run(capsys, "discard-read-jobs")

    assert code == 1
    assert f"device {device_id} job {sync_id} (queued sync carrying a generation)" in err
    async with session() as db:
        assert (await db.get(Job, sync_id)).status is JobStatus.queued
        generation = await db.scalar(select(DeploymentGeneration).where(DeploymentGeneration.job_id == sync_id))
        assert generation.status is GenerationStatus.pending
