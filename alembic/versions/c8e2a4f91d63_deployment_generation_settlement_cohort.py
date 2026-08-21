# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Give marking-split deployment generations an explicit settlement cohort.

Only the generations created by one static-route marking split share a cohort. Ordinary
generations keep NULL, so a historical generation with the same device, stream and revision
cannot block a later deployment from certifying that revision.

Revision ID: c8e2a4f91d63
Revises: b7d5e2f18c40
Create Date: 2026-08-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c8e2a4f91d63"
down_revision: str | None = "b7d5e2f18c40"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SEQUENCE = sa.Sequence("deployment_generation_settlement_cohort_seq")
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
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'deployment_generation ' || OLD.id || ' is immutable: device_id, seq, mode, document, digest, """
    """allowed_removal_keys, source_push_seq, stream_revisions, removal_context, created_at may not be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;""",
    "DROP TRIGGER IF EXISTS deployment_generation_immutable ON deployment_generation",
    "CREATE TRIGGER deployment_generation_immutable BEFORE UPDATE ON deployment_generation "
    "FOR EACH ROW EXECUTE FUNCTION deployment_generation_reject_rewrite()",
)


def upgrade() -> None:
    op.execute(sa.schema.CreateSequence(_SEQUENCE))
    op.add_column("deployment_generation", sa.Column("settlement_cohort", sa.BigInteger(), nullable=True))
    for statement in _GENERATION_IMMUTABILITY_DDL:
        op.execute(statement)


def downgrade() -> None:
    for statement in _PREVIOUS_GENERATION_IMMUTABILITY_DDL:
        op.execute(statement)
    op.drop_column("deployment_generation", "settlement_cohort")
    op.execute(sa.schema.DropSequence(_SEQUENCE))
