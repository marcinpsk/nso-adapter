# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Database objects the ORM metadata cannot express, defined ONCE (#1522 §G1).

Each trigger has to exist on both schema paths: ``create_all`` (the startup convenience)
renders it from here, and each alembic revision installs a frozen literal copy of this output
at its own schema. A later change here needs a new revision; the schema-parity test compares
the head triggers of both paths, and ruff (TID251) refuses a revision that imports this module.
"""

from __future__ import annotations

#: A deployment generation's identity: what a retry re-sends and what settlement stamps.
#: Everything else on the row (status, job_id, attempts, last_error, updated_at, settled_at)
#: is lifecycle and stays writable.
#: Terminal carrier evidence has its own guard below.
GENERATION_IMMUTABLE_COLUMNS: tuple[str, ...] = (
    "device_id",
    "seq",
    "mode",
    "document",
    "digest",
    "allowed_removal_keys",
    "source_push_seq",
    "stream_revisions",
    "removal_context",
    "settlement_cohort",
    "apply_attempt_id",
    "created_at",
)

#: Immutable columns with a native equality operator. Every other column is compared as
#: text. PostgreSQL defines no equality operator for ``json``, so this safe default prevents
#: a newly guarded JSON column from breaking every lifecycle update at runtime.
_COMPARABLE_COLUMNS = frozenset({"device_id", "seq", "mode", "digest", "apply_attempt_id", "created_at"})

GENERATION_IMMUTABLE_TRIGGER = "deployment_generation_immutable"
_FUNCTION = "deployment_generation_reject_rewrite"
JOB_COALESCIBLE_IMMUTABLE_TRIGGER = "job_coalescible_immutable"
_JOB_COALESCIBLE_FUNCTION = "job_reject_coalescible_rewrite"


def _compare(col: str) -> str:
    cast = "" if col in _COMPARABLE_COLUMNS else "::text"
    return f"NEW.{col}{cast} IS DISTINCT FROM OLD.{col}{cast}"


def generation_immutability_ddl(
    columns: tuple[str, ...] = GENERATION_IMMUTABLE_COLUMNS,
) -> tuple[str, ...]:
    """Return the statements that install the trigger, in order."""
    predicate = "\n        OR ".join(_compare(col) for col in columns)
    column_names = ", ".join(columns)
    return (
        # No printf placeholders in the message: SQLAlchemy's DDL construct percent-formats
        # its statement, so a literal % here would break the create_all path.
        f"""
CREATE OR REPLACE FUNCTION {_FUNCTION}() RETURNS trigger AS $$
BEGIN
    IF {predicate} THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'deployment_generation ' || OLD.id || ' is immutable: {column_names} may not be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
""".strip(),
        f"DROP TRIGGER IF EXISTS {GENERATION_IMMUTABLE_TRIGGER} ON deployment_generation",
        f"CREATE TRIGGER {GENERATION_IMMUTABLE_TRIGGER} BEFORE UPDATE ON deployment_generation "
        f"FOR EACH ROW EXECUTE FUNCTION {_FUNCTION}()",
    )


def generation_immutability_drop_ddl() -> tuple[str, ...]:
    """Return the statements that remove the trigger and its function."""
    return (
        f"DROP TRIGGER IF EXISTS {GENERATION_IMMUTABLE_TRIGGER} ON deployment_generation",
        f"DROP FUNCTION IF EXISTS {_FUNCTION}()",
    )


def generation_carrier_immutability_ddl() -> tuple[str, ...]:
    """Freeze terminal snapshots and permit one complete fill from the current carrier."""
    return (
        """
CREATE OR REPLACE FUNCTION deployment_generation_reject_carrier_rewrite() RETURNS trigger AS $$
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
$$ LANGUAGE plpgsql;
""".strip(),
        "DROP TRIGGER IF EXISTS deployment_generation_carrier_immutable ON deployment_generation",
        "CREATE TRIGGER deployment_generation_carrier_immutable BEFORE UPDATE ON deployment_generation "
        "FOR EACH ROW EXECUTE FUNCTION deployment_generation_reject_carrier_rewrite()",
    )


def generation_carrier_immutability_drop_ddl() -> tuple[str, ...]:
    """Return the statements that remove the generation carrier guard."""
    return (
        "DROP TRIGGER IF EXISTS deployment_generation_carrier_immutable ON deployment_generation",
        "DROP FUNCTION IF EXISTS deployment_generation_reject_carrier_rewrite()",
    )


def job_terminal_evidence_immutability_ddl() -> tuple[str, ...]:
    """Freeze terminal job status and payload while retaining lifecycle updates."""
    return (
        """
CREATE OR REPLACE FUNCTION job_reject_terminal_evidence_rewrite() RETURNS trigger AS $$
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
$$ LANGUAGE plpgsql;
""".strip(),
        "DROP TRIGGER IF EXISTS job_terminal_evidence_immutable ON jobs",
        "CREATE TRIGGER job_terminal_evidence_immutable BEFORE UPDATE ON jobs "
        "FOR EACH ROW EXECUTE FUNCTION job_reject_terminal_evidence_rewrite()",
    )


def job_terminal_evidence_immutability_drop_ddl() -> tuple[str, ...]:
    """Return the statements that remove the terminal job guard."""
    return (
        "DROP TRIGGER IF EXISTS job_terminal_evidence_immutable ON jobs",
        "DROP FUNCTION IF EXISTS job_reject_terminal_evidence_rewrite()",
    )


def job_reject_coalescible_rewrite() -> str:
    """Return the function that rejects a job queue-class rewrite."""
    return f"""
CREATE OR REPLACE FUNCTION {_JOB_COALESCIBLE_FUNCTION}() RETURNS trigger AS $$
BEGIN
    IF NEW.coalescible IS DISTINCT FROM OLD.coalescible THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'job ' || OLD.id || ' is immutable: coalescible may not be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
""".strip()


def job_coalescible_immutability_ddl() -> tuple[str, ...]:
    """Return the statements that install the job queue-class trigger."""
    return (
        job_reject_coalescible_rewrite(),
        f"DROP TRIGGER IF EXISTS {JOB_COALESCIBLE_IMMUTABLE_TRIGGER} ON jobs",
        f"CREATE TRIGGER {JOB_COALESCIBLE_IMMUTABLE_TRIGGER} BEFORE UPDATE OF coalescible ON jobs "
        f"FOR EACH ROW WHEN (NEW.coalescible IS DISTINCT FROM OLD.coalescible) "
        f"EXECUTE FUNCTION {_JOB_COALESCIBLE_FUNCTION}()",
    )


def job_coalescible_immutability_drop_ddl() -> tuple[str, ...]:
    """Return the statements that remove the job queue-class trigger."""
    return (
        f"DROP TRIGGER IF EXISTS {JOB_COALESCIBLE_IMMUTABLE_TRIGGER} ON jobs",
        f"DROP FUNCTION IF EXISTS {_JOB_COALESCIBLE_FUNCTION}()",
    )


def observation_immutability_ddl() -> tuple[str, ...]:
    return (
        """
CREATE OR REPLACE FUNCTION read_observation_reject_update() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = 'integrity_constraint_violation',
        MESSAGE = 'read_observation is immutable: updates are refused';
END;
$$ LANGUAGE plpgsql;
""".strip(),
        "DROP TRIGGER IF EXISTS read_observation_immutable ON read_observation",
        "CREATE TRIGGER read_observation_immutable BEFORE UPDATE ON read_observation "
        "FOR EACH ROW EXECUTE FUNCTION read_observation_reject_update()",
    )


def observation_immutability_drop_ddl() -> tuple[str, ...]:
    return (
        "DROP TRIGGER IF EXISTS read_observation_immutable ON read_observation",
        "DROP FUNCTION IF EXISTS read_observation_reject_update()",
    )


EXECUTION_ATTEMPT_IMMUTABLE_COLUMNS = (
    "device_id",
    "generation_id",
    "job_correlation_id",
    "run_attempt",
    "operation_kind",
    "document_digest",
    "claim_token",
    "stream_revisions",
    "generation_authorities",
    "manual_attempt_id",
    "removal_tombstone_ids",
    "logical_membership",
    "attempted_carriers",
    "safe_origins",
    "prior_bookkeeping",
    "created_at",
)


def execution_evidence_immutability_ddl() -> tuple[str, ...]:
    """Freeze preparation, recorded responses, and logical outcome evidence."""
    predicate = "\n        OR ".join(
        f"NEW.{column} IS DISTINCT FROM OLD.{column}" for column in EXECUTION_ATTEMPT_IMMUTABLE_COLUMNS
    )
    return (
        f"""
CREATE OR REPLACE FUNCTION execution_attempt_reject_rewrite() RETURNS trigger AS $$
BEGIN
    IF {predicate}
        OR (NEW.job_id IS DISTINCT FROM OLD.job_id AND NEW.job_id IS NOT NULL)
        OR (OLD.phase = 'terminal' AND
            (NEW.phase IS DISTINCT FROM OLD.phase OR NEW.response_phase IS DISTINCT FROM OLD.response_phase
             OR NEW.response_classification IS DISTINCT FROM OLD.response_classification
             OR NEW.commit_acknowledged IS DISTINCT FROM OLD.commit_acknowledged))
        OR (OLD.phase = 'response_recorded' AND NEW.phase = 'prepared')
        OR (OLD.commit_acknowledged AND NOT NEW.commit_acknowledged)
        OR EXISTS (SELECT 1 FROM jsonb_each(OLD.response_classification) checkpoint
                   WHERE NEW.response_classification -> checkpoint.key IS DISTINCT FROM checkpoint.value) THEN
        RAISE EXCEPTION USING ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'execution attempt evidence is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
""".strip(),
        "DROP TRIGGER IF EXISTS execution_attempt_immutable ON apply_execution_attempt",
        "CREATE TRIGGER execution_attempt_immutable BEFORE UPDATE ON apply_execution_attempt "
        "FOR EACH ROW EXECUTE FUNCTION execution_attempt_reject_rewrite()",
        """
CREATE OR REPLACE FUNCTION row_outcome_reject_update() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE = 'integrity_constraint_violation',
        MESSAGE = 'row outcome evidence is immutable';
END;
$$ LANGUAGE plpgsql;
""".strip(),
        "DROP TRIGGER IF EXISTS row_outcome_immutable ON row_outcome",
        "CREATE TRIGGER row_outcome_immutable BEFORE UPDATE ON row_outcome "
        "FOR EACH ROW EXECUTE FUNCTION row_outcome_reject_update()",
    )


def execution_evidence_immutability_drop_ddl() -> tuple[str, ...]:
    return (
        "DROP TRIGGER IF EXISTS row_outcome_immutable ON row_outcome",
        "DROP FUNCTION IF EXISTS row_outcome_reject_update()",
        "DROP TRIGGER IF EXISTS execution_attempt_immutable ON apply_execution_attempt",
        "DROP FUNCTION IF EXISTS execution_attempt_reject_rewrite()",
    )
