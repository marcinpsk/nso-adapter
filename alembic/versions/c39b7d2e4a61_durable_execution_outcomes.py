# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Store durable execution attempts and logical outcomes.

Revision ID: c39b7d2e4a61
Revises: 8a86079e0dfe
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "c39b7d2e4a61"
down_revision: str | Sequence[str] | None = "8a86079e0dfe"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen copy of the store.ddl output at this revision: a later helper change needs its own revision.
_EXECUTION_EVIDENCE_IMMUTABILITY_DDL = (
    """CREATE OR REPLACE FUNCTION execution_attempt_reject_rewrite() RETURNS trigger AS $$
BEGIN
    IF NEW.device_id IS DISTINCT FROM OLD.device_id
        OR NEW.generation_id IS DISTINCT FROM OLD.generation_id
        OR NEW.job_correlation_id IS DISTINCT FROM OLD.job_correlation_id
        OR NEW.run_attempt IS DISTINCT FROM OLD.run_attempt
        OR NEW.operation_kind IS DISTINCT FROM OLD.operation_kind
        OR NEW.document_digest IS DISTINCT FROM OLD.document_digest
        OR NEW.claim_token IS DISTINCT FROM OLD.claim_token
        OR NEW.stream_revisions IS DISTINCT FROM OLD.stream_revisions
        OR NEW.generation_authorities IS DISTINCT FROM OLD.generation_authorities
        OR NEW.manual_attempt_id IS DISTINCT FROM OLD.manual_attempt_id
        OR NEW.removal_tombstone_ids IS DISTINCT FROM OLD.removal_tombstone_ids
        OR NEW.logical_membership IS DISTINCT FROM OLD.logical_membership
        OR NEW.attempted_carriers IS DISTINCT FROM OLD.attempted_carriers
        OR NEW.safe_origins IS DISTINCT FROM OLD.safe_origins
        OR NEW.prior_bookkeeping IS DISTINCT FROM OLD.prior_bookkeeping
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
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
$$ LANGUAGE plpgsql;""",
    "DROP TRIGGER IF EXISTS execution_attempt_immutable ON apply_execution_attempt",
    "CREATE TRIGGER execution_attempt_immutable BEFORE UPDATE ON apply_execution_attempt "
    "FOR EACH ROW EXECUTE FUNCTION execution_attempt_reject_rewrite()",
    """CREATE OR REPLACE FUNCTION row_outcome_reject_update() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE = 'integrity_constraint_violation',
        MESSAGE = 'row outcome evidence is immutable';
END;
$$ LANGUAGE plpgsql;""",
    "DROP TRIGGER IF EXISTS row_outcome_immutable ON row_outcome",
    "CREATE TRIGGER row_outcome_immutable BEFORE UPDATE ON row_outcome "
    "FOR EACH ROW EXECUTE FUNCTION row_outcome_reject_update()",
)

_EXECUTION_EVIDENCE_IMMUTABILITY_DROP_DDL = (
    "DROP TRIGGER IF EXISTS row_outcome_immutable ON row_outcome",
    "DROP FUNCTION IF EXISTS row_outcome_reject_update()",
    "DROP TRIGGER IF EXISTS execution_attempt_immutable ON apply_execution_attempt",
    "DROP FUNCTION IF EXISTS execution_attempt_reject_rewrite()",
)


def upgrade() -> None:
    op.create_table(
        "apply_execution_attempt",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("generation_id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("job_correlation_id", sa.Integer(), nullable=False),
        sa.Column("run_attempt", sa.Integer(), nullable=False),
        sa.Column("operation_kind", sa.String(16), nullable=False),
        sa.Column("document_digest", sa.String(64), nullable=False),
        sa.Column("claim_token", sa.String(64), nullable=False),
        sa.Column("stream_revisions", postgresql.JSONB(), nullable=False),
        sa.Column("generation_authorities", postgresql.JSONB(), nullable=False),
        sa.Column("manual_attempt_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("removal_tombstone_ids", postgresql.JSONB(), nullable=False),
        sa.Column("logical_membership", postgresql.JSONB(), nullable=False),
        sa.Column("attempted_carriers", postgresql.JSONB(), nullable=False),
        sa.Column("safe_origins", postgresql.JSONB(), nullable=False),
        sa.Column("prior_bookkeeping", postgresql.JSONB(), nullable=False),
        sa.Column("phase", sa.String(32), server_default=sa.text("'prepared'"), nullable=False),
        sa.Column("response_phase", sa.String(32), nullable=True),
        sa.Column("response_classification", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("commit_acknowledged", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["generation_id"], ["deployment_generation.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("job_id", "run_attempt", name="uq_execution_attempt_job_run"),
        sa.UniqueConstraint("job_correlation_id", "run_attempt", name="uq_execution_attempt_correlation_run"),
        sa.CheckConstraint("run_attempt > 0", name="ck_execution_attempt_run"),
        sa.CheckConstraint("operation_kind IN ('apply', 'removal')", name="ck_execution_attempt_operation"),
        sa.CheckConstraint("phase IN ('prepared', 'response_recorded', 'terminal')", name="ck_execution_attempt_phase"),
        sa.CheckConstraint(
            "response_phase IS NULL OR response_phase IN ('pre_commit_dry_run', 'commit', 'post_commit_verification')",
            name="ck_execution_attempt_response_phase",
        ),
    )
    op.create_index("ix_execution_attempt_device_generation", "apply_execution_attempt", ["device_id", "generation_id"])
    op.create_index(
        "ix_execution_attempt_removal_authority",
        "apply_execution_attempt",
        ["removal_tombstone_ids"],
        postgresql_using="gin",
    )
    op.create_table(
        "row_outcome",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("execution_attempt_id", sa.Integer(), nullable=False),
        sa.Column("stream", sa.String(64), nullable=False),
        sa.Column("row_subject", sa.String(128), nullable=False),
        sa.Column("row_key", sa.String(64), nullable=False),
        sa.Column("settlement_cohort", sa.BigInteger(), nullable=True),
        sa.Column("source_identity", postgresql.JSONB(), nullable=False),
        sa.Column("logical_identity", postgresql.JSONB(), nullable=False),
        sa.Column("carrier_identity", postgresql.JSONB(), nullable=False),
        sa.Column("disposition", sa.String(16), nullable=False),
        sa.Column("refusals", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["execution_attempt_id"], ["apply_execution_attempt.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "execution_attempt_id",
            "stream",
            "row_subject",
            "row_key",
            "settlement_cohort",
            name="uq_row_outcome_logical_identity",
            postgresql_nulls_not_distinct=True,
        ),
        sa.CheckConstraint(
            "disposition IN ('in_sync', 'refused', 'not_applied', 'unavailable')",
            name="ck_row_outcome_disposition",
        ),
        sa.CheckConstraint(
            "(disposition = 'refused' AND jsonb_array_length(refusals) > 0) OR "
            "(disposition <> 'refused' AND refusals = '[]'::jsonb)",
            name="ck_row_outcome_refusals",
        ),
    )
    op.create_index("ix_row_outcome_attempt", "row_outcome", ["execution_attempt_id"])
    for statement in _EXECUTION_EVIDENCE_IMMUTABILITY_DDL:
        op.execute(statement)


def downgrade() -> None:
    for statement in _EXECUTION_EVIDENCE_IMMUTABILITY_DROP_DDL:
        op.execute(statement)
    op.drop_table("row_outcome")
    op.drop_table("apply_execution_attempt")
