# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM S5a B: the sync_from_nso job type (comprehensive CDB-only mirror read).

Job.job_type is a NATIVE PostgreSQL enum (baseline f556ce117498 creates ``jobtype``) -
a new member needs ALTER TYPE, or production INSERTs fail. The member is declared LAST
in the Python enum to match ADD VALUE's append order (schema-parity gate).

Revision ID: d4f6a8b0c2e1
Revises: b6c8d0e2f4a1
"""

from alembic import op

revision = "d4f6a8b0c2e1"
down_revision = "b6c8d0e2f4a1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.execute("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'sync_from_nso'")


def downgrade() -> None:
    # PostgreSQL enums cannot drop a value; the member is additive and harmless to leave.
    pass
