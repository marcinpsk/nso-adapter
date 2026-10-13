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
from nso_adapter.store.ddl import execution_evidence_immutability_ddl, execution_evidence_immutability_drop_ddl

revision: str = "c39b7d2e4a61"
down_revision: str | Sequence[str] | None = "8a86079e0dfe"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


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
    for statement in execution_evidence_immutability_ddl():
        op.execute(statement)


def downgrade() -> None:
    for statement in execution_evidence_immutability_drop_ddl():
        op.execute(statement)
    op.drop_table("row_outcome")
    op.drop_table("apply_execution_attempt")
