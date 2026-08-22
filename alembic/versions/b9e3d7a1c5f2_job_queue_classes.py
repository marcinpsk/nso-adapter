# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Add coalescible and dedicated job queue classes.

Revision ID: b9e3d7a1c5f2
Revises: e4a8c2d1f6b9
Create Date: 2026-08-22
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b9e3d7a1c5f2"
down_revision: str | None = "e4a8c2d1f6b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_QUEUED_INDEX = "uq_job_queued_per_device_type"
_OLD_QUEUED_WHERE = "status = 'queued' AND job_type <> 'removal'"
_COALESCIBLE_QUEUED_WHERE = "status = 'queued' AND coalescible"
_CHECKS = (
    ("ck_job_removal_not_coalescible", "job_type <> 'removal' OR NOT coalescible"),
    ("ck_job_provision_not_coalescible", "job_type <> 'provision' OR NOT coalescible"),
    (
        "ck_job_active_provision_without_device",
        "job_type <> 'provision' OR status NOT IN ('queued', 'running') OR device_id IS NULL",
    ),
    (
        "ck_job_detached_non_provision_terminal",
        "job_type = 'provision' OR device_id IS NOT NULL OR status IN ('succeeded', 'failed')",
    ),
)
_VALIDATE_REMOVAL_CARDINALITY = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM jobs AS job
          JOIN deployment_generation AS generation ON generation.job_id = job.id
         WHERE job.job_type = 'removal'
         GROUP BY job.id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'check_violation',
            MESSAGE = 'removal job carries more than one generation';
    END IF;
END;
$$
"""
_VALIDATE_APPLY_QUIESCENCE = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM jobs
         WHERE job_type = 'apply'
           AND device_id IS NOT NULL
           AND status IN ('queued', 'running')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'check_violation',
            MESSAGE = 'device-bound Apply job is queued or running';
    END IF;
END;
$$
"""
_VALIDATE_DOWNGRADE_QUEUE_CARDINALITY = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM jobs
         WHERE status = 'queued'
           AND job_type <> 'removal'
           AND device_id IS NOT NULL
         GROUP BY device_id, job_type
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'check_violation',
            MESSAGE = 'cannot downgrade job queue classes: multiple queued non-removal jobs '
                      'exist for one device and type; resolve the queue before downgrading';
    END IF;
END;
$$
"""
_JOB_COALESCIBLE_IMMUTABILITY_DDL = (
    """CREATE OR REPLACE FUNCTION job_reject_coalescible_rewrite() RETURNS trigger AS $$
BEGIN
    IF NEW.coalescible IS DISTINCT FROM OLD.coalescible THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'job ' || OLD.id || ' is immutable: coalescible may not be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;""",
    "DROP TRIGGER IF EXISTS job_coalescible_immutable ON jobs",
    "CREATE TRIGGER job_coalescible_immutable BEFORE UPDATE ON jobs "
    "FOR EACH ROW EXECUTE FUNCTION job_reject_coalescible_rewrite()",
)
_JOB_COALESCIBLE_IMMUTABILITY_DROP_DDL = (
    "DROP TRIGGER IF EXISTS job_coalescible_immutable ON jobs",
    "DROP FUNCTION IF EXISTS job_reject_coalescible_rewrite()",
)


def upgrade() -> None:
    op.add_column("jobs", sa.Column("coalescible", sa.Boolean(), nullable=True))
    op.execute(_VALIDATE_REMOVAL_CARDINALITY)
    op.execute(_VALIDATE_APPLY_QUIESCENCE)
    op.execute("UPDATE jobs SET coalescible = CASE WHEN job_type IN ('removal', 'provision') THEN false ELSE true END")
    op.alter_column("jobs", "coalescible", nullable=False)
    for name, condition in _CHECKS:
        op.create_check_constraint(name, "jobs", condition)
    op.drop_index(_QUEUED_INDEX, table_name="jobs")
    op.create_index(
        _QUEUED_INDEX,
        "jobs",
        ["device_id", "job_type"],
        unique=True,
        postgresql_where=sa.text(_COALESCIBLE_QUEUED_WHERE),
    )
    for statement in _JOB_COALESCIBLE_IMMUTABILITY_DDL:
        op.execute(statement)


def downgrade() -> None:
    op.execute(_VALIDATE_DOWNGRADE_QUEUE_CARDINALITY)
    for statement in _JOB_COALESCIBLE_IMMUTABILITY_DROP_DDL:
        op.execute(statement)
    op.drop_index(_QUEUED_INDEX, table_name="jobs")
    op.create_index(
        _QUEUED_INDEX,
        "jobs",
        ["device_id", "job_type"],
        unique=True,
        postgresql_where=sa.text(_OLD_QUEUED_WHERE),
    )
    for name, _condition in reversed(_CHECKS):
        op.drop_constraint(name, "jobs", type_="check")
    op.drop_column("jobs", "coalescible")
