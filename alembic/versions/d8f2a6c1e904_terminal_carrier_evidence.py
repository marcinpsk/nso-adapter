# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Freeze terminal carrier evidence.

Revision ID: d8f2a6c1e904
Revises: c39b7d2e4a61
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d8f2a6c1e904"
down_revision: str | Sequence[str] | None = "c39b7d2e4a61"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen copy of the store.ddl output at this revision: a later helper change needs its own revision.
_GENERATION_CARRIER_IMMUTABILITY_DDL = (
    """CREATE OR REPLACE FUNCTION deployment_generation_reject_carrier_rewrite() RETURNS trigger AS $$
BEGIN
    IF OLD.status IN ('settled', 'failed', 'outcome_unknown', 'abandoned')
        AND NEW.status IN ('settled', 'failed', 'outcome_unknown', 'abandoned')
        AND ROW(NEW.carrier_job_id, NEW.carrier_job_status, NEW.carrier_job_result, NEW.carrier_job_error)
            IS DISTINCT FROM ROW(OLD.carrier_job_id, OLD.carrier_job_status, OLD.carrier_job_result, OLD.carrier_job_error)
        AND NOT (
            OLD.carrier_job_id IS NULL AND OLD.carrier_job_status IS NULL
            AND OLD.carrier_job_result IS NULL AND OLD.carrier_job_error IS NULL
            AND EXISTS (
                SELECT 1 FROM jobs AS job
                WHERE job.id = NEW.job_id AND job.id = NEW.carrier_job_id
                    AND job.status IN ('succeeded', 'failed')
                    AND NEW.carrier_job_status = job.status::text
                    AND NEW.carrier_job_result IS NOT DISTINCT FROM NULLIF(job.result::jsonb, 'null'::jsonb)
                    AND NEW.carrier_job_error IS NOT DISTINCT FROM NULLIF(job.error::jsonb, 'null'::jsonb)
            )
        ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'deployment_generation ' || OLD.id || ' is immutable: terminal carrier evidence may not be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;""",
    "DROP TRIGGER IF EXISTS deployment_generation_carrier_immutable ON deployment_generation",
    "CREATE TRIGGER deployment_generation_carrier_immutable BEFORE UPDATE ON deployment_generation "
    "FOR EACH ROW EXECUTE FUNCTION deployment_generation_reject_carrier_rewrite()",
)

_GENERATION_CARRIER_IMMUTABILITY_DROP_DDL = (
    "DROP TRIGGER IF EXISTS deployment_generation_carrier_immutable ON deployment_generation",
    "DROP FUNCTION IF EXISTS deployment_generation_reject_carrier_rewrite()",
)

_JOB_TERMINAL_EVIDENCE_IMMUTABILITY_DDL = (
    """CREATE OR REPLACE FUNCTION job_reject_terminal_evidence_rewrite() RETURNS trigger AS $$
BEGIN
    IF OLD.status IN ('succeeded', 'failed') AND (
        NEW.status IS DISTINCT FROM OLD.status
        OR NEW.result::jsonb IS DISTINCT FROM OLD.result::jsonb
        OR NEW.error::jsonb IS DISTINCT FROM OLD.error::jsonb
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'job ' || OLD.id || ' is immutable: terminal status, result and error may not be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;""",
    "DROP TRIGGER IF EXISTS job_terminal_evidence_immutable ON jobs",
    "CREATE TRIGGER job_terminal_evidence_immutable BEFORE UPDATE ON jobs "
    "FOR EACH ROW EXECUTE FUNCTION job_reject_terminal_evidence_rewrite()",
)

_JOB_TERMINAL_EVIDENCE_IMMUTABILITY_DROP_DDL = (
    "DROP TRIGGER IF EXISTS job_terminal_evidence_immutable ON jobs",
    "DROP FUNCTION IF EXISTS job_reject_terminal_evidence_rewrite()",
)


def upgrade() -> None:
    """Upgrade schema."""
    for statement in _JOB_TERMINAL_EVIDENCE_IMMUTABILITY_DDL:
        op.execute(statement)
    for statement in _GENERATION_CARRIER_IMMUTABILITY_DDL:
        op.execute(statement)


def downgrade() -> None:
    """Downgrade schema."""
    for statement in _JOB_TERMINAL_EVIDENCE_IMMUTABILITY_DROP_DDL:
        op.execute(statement)
    for statement in _GENERATION_CARRIER_IMMUTABILITY_DROP_DDL:
        op.execute(statement)
