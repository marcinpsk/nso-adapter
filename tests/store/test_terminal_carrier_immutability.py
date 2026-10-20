# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Terminal carrier evidence is write-once at the PostgreSQL boundary."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from nso_adapter.store.models import (
    DeploymentGeneration,
    Device,
    GenerationMode,
    GenerationStatus,
    Job,
    JobStatus,
    JobType,
)


def _seed(
    db, *, job_status=JobStatus.failed, generation_status=GenerationStatus.failed, snapshot=True, empty_payload=False
):
    device = Device(nso_instance="nso-test", nso_device_name="terminal-carrier", netbox_device_id=16116)
    db.add(device)
    db.flush()
    job = Job(
        job_type=JobType.apply,
        status=job_status,
        device_id=device.id,
        coalescible=False,
        result=None if empty_payload else {"sections": {"vlan": {"outcome": "failed"}}},
        error=None if empty_payload else {"code": "device_refused"},
    )
    db.add(job)
    db.flush()
    generation = DeploymentGeneration(
        device_id=device.id,
        seq=1,
        mode=GenerationMode.networked,
        status=generation_status,
        document={},
        digest="a" * 64,
        job_id=job.id,
        carrier_job_id=job.id if snapshot else None,
        carrier_job_status=job.status.value if snapshot else None,
        carrier_job_result=job.result if snapshot else None,
        carrier_job_error=job.error if snapshot else None,
    )
    db.add(generation)
    db.commit()
    return job.id, generation.id


@pytest.mark.parametrize("status", [JobStatus.succeeded, JobStatus.failed])
@pytest.mark.parametrize(
    "rewrite",
    [
        "result = '{\"changed\": true}'::json",
        'error = \'{"code": "changed"}\'::json',
        "result = NULL",
        "error = NULL",
        "status = 'queued'",
        "status = 'running'",
        "status = CASE WHEN status = 'failed' THEN 'succeeded'::jobstatus ELSE 'failed'::jobstatus END",
    ],
)
def test_terminal_job_evidence_rewrites_are_refused(pg_sync_session, status, rewrite):
    job_id, _ = _seed(pg_sync_session, job_status=status)

    with pytest.raises(sa.exc.IntegrityError) as excinfo:
        pg_sync_session.execute(sa.text(f"UPDATE jobs SET {rewrite} WHERE id = :id"), {"id": job_id})
    assert excinfo.value.orig.pgcode == "23000"
    assert "terminal status, result and error may not be updated" in str(excinfo.value.orig)
    pg_sync_session.rollback()


@pytest.mark.parametrize("status", [JobStatus.succeeded, JobStatus.failed])
def test_terminal_job_settlement_and_offboarding_fields_stay_writable(pg_sync_session, status):
    job_id, _ = _seed(pg_sync_session, job_status=status)

    pg_sync_session.execute(
        sa.text(
            "UPDATE jobs SET settle_seq = 7, device_id = NULL, status = status, "
            "result = result, error = error WHERE id = :id"
        ),
        {"id": job_id},
    )
    pg_sync_session.commit()
    row = pg_sync_session.execute(
        sa.text("SELECT settle_seq, device_id, status FROM jobs WHERE id = :id"), {"id": job_id}
    ).one()
    assert tuple(row) == (7, None, status.value)


@pytest.mark.parametrize("status", ["failed", "outcome_unknown", "settled", "abandoned"])
@pytest.mark.parametrize(
    "rewrite",
    [
        "carrier_job_id = carrier_job_id + 1",
        "carrier_job_status = 'succeeded'",
        "carrier_job_result = '{\"changed\": true}'::jsonb",
        "carrier_job_error = NULL",
        "carrier_job_id = NULL, carrier_job_status = NULL, carrier_job_result = NULL, carrier_job_error = NULL",
    ],
)
def test_established_generation_snapshot_rewrites_are_refused(pg_sync_session, status, rewrite):
    _, generation_id = _seed(pg_sync_session, generation_status=GenerationStatus(status))
    target = "abandoned" if status == "settled" else "settled"

    with pytest.raises(sa.exc.IntegrityError) as excinfo:
        pg_sync_session.execute(
            sa.text(f"UPDATE deployment_generation SET status = :status, {rewrite} WHERE id = :id"),
            {"id": generation_id, "status": target},
        )
    assert excinfo.value.orig.pgcode == "23000"
    assert "terminal carrier evidence may not be updated" in str(excinfo.value.orig)
    pg_sync_session.rollback()


@pytest.mark.parametrize(
    "rewrite",
    [
        "carrier_job_id = job_id",
        "carrier_job_status = 'failed'",
        "carrier_job_result = '{}'::jsonb",
        "carrier_job_id = job_id + 1, carrier_job_status = 'failed'",
        "carrier_job_id = job_id, carrier_job_status = 'queued'",
        "carrier_job_id = job_id, carrier_job_status = 'failed'",
    ],
)
def test_terminal_generation_refuses_an_incomplete_snapshot_fill(pg_sync_session, rewrite):
    _, generation_id = _seed(pg_sync_session, snapshot=False)

    with pytest.raises(sa.exc.IntegrityError) as excinfo:
        pg_sync_session.execute(
            sa.text(f"UPDATE deployment_generation SET status = 'settled', {rewrite} WHERE id = :id"),
            {"id": generation_id},
        )
    assert excinfo.value.orig.pgcode == "23000"
    assert "terminal carrier evidence may not be updated" in str(excinfo.value.orig)
    pg_sync_session.rollback()


@pytest.mark.parametrize("empty_payload", [False, True])
def test_terminal_generation_snapshot_can_be_filled_once(pg_sync_session, empty_payload):
    job_id, generation_id = _seed(pg_sync_session, snapshot=False, empty_payload=empty_payload)

    pg_sync_session.execute(
        sa.text(
            "UPDATE deployment_generation AS generation SET status = 'settled', carrier_job_id = job.id, "
            "carrier_job_status = job.status::text, carrier_job_result = NULLIF(job.result::jsonb, 'null'::jsonb), "
            "carrier_job_error = NULLIF(job.error::jsonb, 'null'::jsonb) FROM jobs AS job "
            "WHERE generation.id = :id AND job.id = generation.job_id"
        ),
        {"id": generation_id},
    )
    pg_sync_session.commit()
    snapshot = pg_sync_session.execute(
        sa.text(
            "SELECT carrier_job_id, carrier_job_status, carrier_job_result, carrier_job_error "
            "FROM deployment_generation WHERE id = :id"
        ),
        {"id": generation_id},
    ).one()
    assert tuple(snapshot) == (
        job_id,
        "failed",
        None if empty_payload else {"sections": {"vlan": {"outcome": "failed"}}},
        None if empty_payload else {"code": "device_refused"},
    )
    pg_sync_session.execute(
        sa.text("UPDATE deployment_generation SET status = 'abandoned' WHERE id = :id"),
        {"id": generation_id},
    )
    pg_sync_session.commit()
    with pytest.raises(sa.exc.IntegrityError):
        pg_sync_session.execute(
            sa.text("UPDATE deployment_generation SET carrier_job_id = carrier_job_id + 1 WHERE id = :id"),
            {"id": generation_id},
        )
    pg_sync_session.rollback()
