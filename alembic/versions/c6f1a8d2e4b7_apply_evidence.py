# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Add durable Apply attempts and generation carrier evidence.

Revision ID: c6f1a8d2e4b7
Revises: b9e3d7a1c5f2
Create Date: 2026-08-25
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "c6f1a8d2e4b7"
down_revision: str | None = "b9e3d7a1c5f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX = "ix_generation_device_apply_attempt"
_IMMUTABLE_COLUMNS = (
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
_PREVIOUS_IMMUTABLE_COLUMNS = (
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
    "created_at",
)
_GENERATION_IMMUTABILITY_DDL = (
    """CREATE OR REPLACE FUNCTION deployment_generation_reject_rewrite() RETURNS trigger AS $$
BEGIN
    IF NEW.device_id IS DISTINCT FROM OLD.device_id
        OR NEW.seq IS DISTINCT FROM OLD.seq
        OR NEW.mode IS DISTINCT FROM OLD.mode
        OR NEW.document::text IS DISTINCT FROM OLD.document::text
        OR NEW.digest IS DISTINCT FROM OLD.digest
        OR NEW.allowed_removal_keys::text IS DISTINCT FROM OLD.allowed_removal_keys::text
        OR NEW.source_push_seq::text IS DISTINCT FROM OLD.source_push_seq::text
        OR NEW.stream_revisions::text IS DISTINCT FROM OLD.stream_revisions::text
        OR NEW.removal_context::text IS DISTINCT FROM OLD.removal_context::text
        OR NEW.settlement_cohort::text IS DISTINCT FROM OLD.settlement_cohort::text
        OR NEW.apply_attempt_id IS DISTINCT FROM OLD.apply_attempt_id
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'deployment_generation ' || OLD.id || ' is immutable: device_id, seq, mode, document, digest, """
    """allowed_removal_keys, source_push_seq, stream_revisions, removal_context, settlement_cohort, apply_attempt_id, created_at may not be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;""",
    "DROP TRIGGER IF EXISTS deployment_generation_immutable ON deployment_generation",
    "CREATE TRIGGER deployment_generation_immutable BEFORE UPDATE ON deployment_generation "
    "FOR EACH ROW EXECUTE FUNCTION deployment_generation_reject_rewrite()",
)
_PREVIOUS_GENERATION_IMMUTABILITY_DDL = (
    """CREATE OR REPLACE FUNCTION deployment_generation_reject_rewrite() RETURNS trigger AS $$
BEGIN
    IF NEW.device_id IS DISTINCT FROM OLD.device_id
        OR NEW.seq IS DISTINCT FROM OLD.seq
        OR NEW.mode IS DISTINCT FROM OLD.mode
        OR NEW.document::text IS DISTINCT FROM OLD.document::text
        OR NEW.digest IS DISTINCT FROM OLD.digest
        OR NEW.allowed_removal_keys::text IS DISTINCT FROM OLD.allowed_removal_keys::text
        OR NEW.source_push_seq::text IS DISTINCT FROM OLD.source_push_seq::text
        OR NEW.stream_revisions::text IS DISTINCT FROM OLD.stream_revisions::text
        OR NEW.removal_context::text IS DISTINCT FROM OLD.removal_context::text
        OR NEW.settlement_cohort::text IS DISTINCT FROM OLD.settlement_cohort::text
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'deployment_generation ' || OLD.id || ' is immutable: device_id, seq, mode, document, digest, """
    """allowed_removal_keys, source_push_seq, stream_revisions, removal_context, settlement_cohort, created_at may not be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;""",
    "DROP TRIGGER IF EXISTS deployment_generation_immutable ON deployment_generation",
    "CREATE TRIGGER deployment_generation_immutable BEFORE UPDATE ON deployment_generation "
    "FOR EACH ROW EXECUTE FUNCTION deployment_generation_reject_rewrite()",
)


def upgrade() -> None:
    op.create_table(
        "deployment_apply_attempt",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("selected", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("admission_state", sa.Text(), nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=False),
        sa.Column("response", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id", "device_id", name="uq_apply_attempt_id_device"),
    )
    op.add_column(
        "deployment_generation",
        sa.Column("apply_attempt_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column("deployment_generation", sa.Column("carrier_job_id", sa.Integer(), nullable=True))
    op.add_column("deployment_generation", sa.Column("carrier_job_status", sa.Text(), nullable=True))
    op.add_column(
        "deployment_generation",
        sa.Column("carrier_job_result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "deployment_generation",
        sa.Column("carrier_job_error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_foreign_key(
        "fk_generation_apply_attempt",
        "deployment_generation",
        "deployment_apply_attempt",
        ["apply_attempt_id", "device_id"],
        ["id", "device_id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        _INDEX,
        "deployment_generation",
        ["device_id", "apply_attempt_id"],
        unique=False,
    )
    for statement in _GENERATION_IMMUTABILITY_DDL:
        op.execute(statement)


def downgrade() -> None:
    for statement in _PREVIOUS_GENERATION_IMMUTABILITY_DDL:
        op.execute(statement)
    op.drop_index(_INDEX, table_name="deployment_generation")
    op.drop_constraint("fk_generation_apply_attempt", "deployment_generation", type_="foreignkey")
    op.drop_column("deployment_generation", "carrier_job_error")
    op.drop_column("deployment_generation", "carrier_job_result")
    op.drop_column("deployment_generation", "carrier_job_status")
    op.drop_column("deployment_generation", "carrier_job_id")
    op.drop_column("deployment_generation", "apply_attempt_id")
    op.drop_table("deployment_apply_attempt")
