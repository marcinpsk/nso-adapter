# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S3 B7: the sync_now job type (grain-c atomic Sync-Now).

Job.job_type is a NATIVE PostgreSQL enum (baseline f556ce117498 creates ``jobtype``) -
a new member needs ALTER TYPE, or production INSERTs fail (codex S3-R1 F9).

Revision ID: c9d4e2f1a3b5
Revises: b8e3f0a1c2d4
"""

from alembic import op

revision = "c9d4e2f1a3b5"
down_revision = "b8e3f0a1c2d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.execute("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'sync_now'")


def downgrade() -> None:
    # PostgreSQL enums cannot drop a value; the member is additive and harmless to leave.
    pass
