# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The database freezes a job's queue class while its lifecycle remains writable."""

from __future__ import annotations

import json
import os

import pytest
import sqlalchemy as sa

from tests.store.migration_harness import alembic, engine_on, load_migration, private_database

_MIGRATION = "b9e3d7a1c5f2_job_queue_classes.py"
_FUNCTION = "job_reject_coalescible_rewrite"
_TRIGGER = "job_coalescible_immutable"
_FROZEN_FUNCTION_SOURCE = """
BEGIN
    IF NEW.coalescible IS DISTINCT FROM OLD.coalescible THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'job ' || OLD.id || ' is immutable: coalescible may not be updated';
    END IF;
    RETURN NEW;
END;
"""

_FUNCTION_SOURCE_SQL = sa.text(
    """
    SELECT p.prosrc
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'public'
       AND p.proname = :name
       AND pg_get_function_identity_arguments(p.oid) = ''
    """
)
_TRIGGER_FUNCTION_SQL = sa.text(
    """
    SELECT p.proname
      FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_proc p ON p.oid = t.tgfoid
     WHERE n.nspname = 'public'
       AND c.relname = 'jobs'
       AND t.tgname = :name
       AND NOT t.tgisinternal
    """
)


def _installed_function_source(engine) -> str | None:
    with engine.connect() as conn:
        return conn.execute(_FUNCTION_SOURCE_SQL, {"name": _FUNCTION}).scalar_one_or_none()


def _installed_trigger_function(engine) -> str | None:
    with engine.connect() as conn:
        return conn.execute(_TRIGGER_FUNCTION_SQL, {"name": _TRIGGER}).scalar_one_or_none()


def _function_source(statement: str) -> str:
    return statement.partition(" AS $$")[2].partition("$$ LANGUAGE plpgsql")[0]


def _seed_classified_job(session, job_type: str, status: str) -> tuple[int, bool]:
    from nso_adapter.store.models import Device, Job, JobStatus, JobType

    typed_job = JobType(job_type)
    typed_status = JobStatus(status)
    coalescible = typed_job not in (JobType.removal, JobType.provision)
    device = Device(
        nso_instance="nso-dev",
        nso_device_name=f"immutable-{job_type}-{status}",
        netbox_device_id=None,
    )
    session.add(device)
    session.flush()
    active_provision = typed_job is JobType.provision and typed_status in (JobStatus.queued, JobStatus.running)
    job = Job(
        job_type=typed_job,
        status=typed_status,
        coalescible=coalescible,
        device_id=None if active_provision else device.id,
        context={"nso_instance": "nso-dev", "device_name": device.nso_device_name},
        run_attempt=1 if typed_status is JobStatus.running else 0,
    )
    session.add(job)
    session.flush()
    job_id = job.id
    session.commit()
    return job_id, coalescible


@pytest.mark.parametrize("job_type", ("removal", "provision", "apply"))
@pytest.mark.parametrize(
    "status",
    (
        pytest.param("queued", id="queued"),
        pytest.param("running", id="running"),
        pytest.param("succeeded", id="terminal"),
    ),
)
def test_raw_sql_cannot_reclassify_a_job(pg_sync_session, job_type: str, status: str):
    job_id, original = _seed_classified_job(pg_sync_session, job_type, status)

    with pytest.raises(sa.exc.IntegrityError) as excinfo:
        pg_sync_session.execute(
            sa.text("UPDATE jobs SET coalescible = NOT coalescible WHERE id = :job_id"),
            {"job_id": job_id},
        )
    assert excinfo.value.orig.pgcode == "23000"
    assert "coalescible may not be updated" in str(excinfo.value)
    pg_sync_session.rollback()

    stored = pg_sync_session.scalar(sa.text("SELECT coalescible FROM jobs WHERE id = :job_id"), {"job_id": job_id})
    assert stored is original


def test_job_lifecycle_columns_remain_writable(pg_sync_session):
    from nso_adapter.store.models import Device, Job, JobStatus, JobType

    device = Device(
        nso_instance="nso-dev",
        nso_device_name="immutable-lifecycle",
        netbox_device_id=None,
    )
    pg_sync_session.add(device)
    pg_sync_session.flush()
    succeeded = Job(
        job_type=JobType.sync,
        status=JobStatus.queued,
        coalescible=True,
        device_id=device.id,
        context={},
    )
    failed = Job(
        job_type=JobType.removal,
        status=JobStatus.queued,
        coalescible=False,
        device_id=device.id,
        context={},
    )
    pg_sync_session.add_all([succeeded, failed])
    pg_sync_session.flush()
    succeeded_id, failed_id = succeeded.id, failed.id
    pg_sync_session.commit()

    pg_sync_session.execute(
        sa.text(
            "UPDATE jobs SET status = 'running', context = CAST(:context AS json), "
            "heartbeat_at = now(), run_attempt = run_attempt + 1 WHERE id IN (:succeeded_id, :failed_id)"
        ),
        {
            "context": json.dumps({"phase": "running"}),
            "succeeded_id": succeeded_id,
            "failed_id": failed_id,
        },
    )
    pg_sync_session.execute(
        sa.text(
            "UPDATE jobs SET status = 'succeeded', result = CAST(:result AS json), settle_seq = 1 WHERE id = :job_id"
        ),
        {"result": json.dumps({"outcome": "ok"}), "job_id": succeeded_id},
    )
    pg_sync_session.execute(
        sa.text("UPDATE jobs SET status = 'failed', error = CAST(:error AS json), settle_seq = 2 WHERE id = :job_id"),
        {"error": json.dumps({"code": "test_failure"}), "job_id": failed_id},
    )
    pg_sync_session.commit()

    rows = {
        row.id: row
        for row in pg_sync_session.execute(
            sa.text(
                "SELECT id, status, coalescible, result, error, context, heartbeat_at, run_attempt, settle_seq "
                "FROM jobs WHERE id IN (:succeeded_id, :failed_id)"
            ),
            {"succeeded_id": succeeded_id, "failed_id": failed_id},
        )
    }
    succeeded_row = rows[succeeded_id]
    assert succeeded_row.status == "succeeded"
    assert succeeded_row.coalescible is True
    assert succeeded_row.result == {"outcome": "ok"}
    assert succeeded_row.error is None
    assert succeeded_row.context == {"phase": "running"}
    assert succeeded_row.heartbeat_at is not None
    assert succeeded_row.run_attempt == 1
    assert succeeded_row.settle_seq == 1

    failed_row = rows[failed_id]
    assert failed_row.status == "failed"
    assert failed_row.coalescible is False
    assert failed_row.result is None
    assert failed_row.error == {"code": "test_failure"}
    assert failed_row.context == {"phase": "running"}
    assert failed_row.heartbeat_at is not None
    assert failed_row.run_attempt == 1
    assert failed_row.settle_seq == 2


@pytest.mark.anyio
async def test_offboard_detaches_jobs_without_reclassifying_them(adapter_client):
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device, Job, JobStatus, JobType
    from tests.conftest import seed_device, session

    device_id = await seed_device(nso_device_name="immutable-offboard", netbox_device_id=8890)
    async with session() as db:
        history = Job(
            job_type=JobType.sync,
            status=JobStatus.succeeded,
            coalescible=True,
            device_id=device_id,
        )
        pending = Job(
            job_type=JobType.removal,
            status=JobStatus.queued,
            coalescible=False,
            device_id=device_id,
        )
        db.add_all([history, pending])
        await db.commit()
        history_id, pending_id = history.id, pending.id

    async with session() as db:
        await offboard_device(db, await db.get(Device, device_id))

    async with session() as db:
        history = await db.get(Job, history_id)
        pending = await db.get(Job, pending_id)
    assert history.status is JobStatus.succeeded
    assert history.coalescible is True
    assert history.device_id is None
    assert pending.status is JobStatus.failed
    assert pending.coalescible is False
    assert pending.device_id is None
    assert pending.error["code"] == "device_offboarded"


def test_alembic_installs_the_frozen_job_function(pg_provisioner):
    module = load_migration(_MIGRATION)
    migration_source = _function_source(module._JOB_COALESCIBLE_IMMUTABILITY_DDL[0])
    assert migration_source == _FROZEN_FUNCTION_SOURCE

    with private_database(pg_provisioner, "job_immutable_frozen") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            installed_source = _installed_function_source(engine)
    assert installed_source == migration_source


def test_create_all_installs_the_live_job_trigger_in_a_fresh_database(pg_provisioner):
    from nso_adapter.store.ddl import job_coalescible_immutability_ddl
    from nso_adapter.store.models import Base

    with private_database(pg_provisioner, "job_immutable_create_all") as sync_url:
        with engine_on(sync_url) as engine:
            assert not sa.inspect(engine).has_table("jobs")
            assert _installed_function_source(engine) is None
            assert _installed_trigger_function(engine) is None

            Base.metadata.create_all(engine)

            assert sa.inspect(engine).has_table("jobs")
            installed_source = _installed_function_source(engine)
            installed_trigger_function = _installed_trigger_function(engine)
    assert installed_source == _function_source(job_coalescible_immutability_ddl()[0])
    assert installed_trigger_function == _FUNCTION


def test_historical_job_trigger_does_not_track_the_live_helper(pg_provisioner, tmp_path, monkeypatch):
    module = load_migration(_MIGRATION)
    marker = tmp_path / "job-sitecustomize-loaded"
    sitecustomize = tmp_path / "sitecustomize.py"
    sitecustomize.write_text(
        "from pathlib import Path\n"
        "from nso_adapter.store import ddl\n"
        f"Path({str(marker)!r}).touch()\n"
        "_original = ddl.job_coalescible_immutability_ddl\n"
        "def _drifted():\n"
        "    statements = _original()\n"
        "    return (statements[0].replace('coalescible may not be updated', 'drifted live helper'), *statements[1:])\n"
        "ddl.job_coalescible_immutability_ddl = _drifted\n"
    )

    with private_database(pg_provisioner, "job_immutable_drift") as sync_url:
        old_pythonpath = os.environ.get("PYTHONPATH")
        pythonpath = str(tmp_path) if old_pythonpath is None else os.pathsep.join((str(tmp_path), old_pythonpath))
        monkeypatch.setenv("PYTHONPATH", pythonpath)
        alembic(sync_url, "upgrade", module.revision)
        assert marker.exists(), "the Alembic subprocess did not load the injected sitecustomize"

        with engine_on(sync_url) as engine:
            historical_source = _installed_function_source(engine)
        assert historical_source == _FROZEN_FUNCTION_SOURCE
        assert "drifted live helper" not in historical_source

        monkeypatch.delenv("PYTHONPATH", raising=False)
        if old_pythonpath is not None:
            monkeypatch.setenv("PYTHONPATH", old_pythonpath)
