# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The test guard checks real persistence and cannot be swallowed by a runner."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
import sqlalchemy as sa


def test_terminal_execution_guard_allows_schema_inspection_without_jobs(pg_provisioner):
    with pg_provisioner.connect() as conn:
        assert conn.execute(sa.text("SELECT to_regclass('jobs')")).scalar_one() is None


@pytest.mark.parametrize("writer", ["orm", "core", "raw", "cte", "comment", "only"])
def test_terminal_execution_guard_rejects_invalid_persisted_results(tmp_path, writer):
    repo = Path(__file__).resolve().parents[1]
    (tmp_path / "conftest.py").write_text("from tests.conftest import *\n")
    (tmp_path / "test_persistence.py").write_text(
        """
import pytest
import sqlalchemy as sa
from nso_adapter.store.models import Device, Job, JobStatus, JobType

@pytest.mark.anyio
async def test_invalid_execution(db_session):
    device = Device(nso_instance="test", nso_device_name="placeholder-device")
    db_session.add(device)
    await db_session.flush()
    job = Job(device_id=device.id, job_type=JobType.apply, status=JobStatus.running, coalescible=False)
    db_session.add(job)
    await db_session.commit()
    try:
        if WRITER == "orm":
            job.status = JobStatus.failed
            job.result = {}
        elif WRITER == "core":
            await db_session.execute(sa.update(Job).where(Job.id == job.id).values(status=JobStatus.failed, result={}))
        elif WRITER == "raw":
            await db_session.execute(sa.text("UPDATE jobs SET status = 'failed', result = '{}' WHERE id = :id"), {"id": job.id})
        elif WRITER == "comment":
            await db_session.execute(sa.text("UPDATE /* finalize */ jobs SET status = 'failed', result = '{}' WHERE id = :id"), {"id": job.id})
        elif WRITER == "only":
            await db_session.execute(sa.text("UPDATE ONLY jobs SET status = 'failed', result = '{}' WHERE id = :id"), {"id": job.id})
        else:
            await db_session.execute(sa.text("WITH terminal AS (UPDATE jobs SET status = 'failed', result = '{}' WHERE id = :id RETURNING id) SELECT id FROM terminal"), {"id": job.id})
        await db_session.commit()
    except Exception:
        await db_session.rollback()
""".replace("WRITER", repr(writer))
    )
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "anyio", "-o", "addopts=", str(tmp_path)],
        cwd=repo,
        env={**os.environ, "PYTHONPATH": str(repo)},
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    output = result.stdout + result.stderr
    assert result.returncode == pytest.ExitCode.TESTS_FAILED, output
    assert "terminal execution result violates contract" in output, output
    assert "missing structured execution evidence" in output, output
