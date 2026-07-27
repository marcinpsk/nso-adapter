# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""device partial sync status + degraded_surfaces

Adds the ``partial`` value to the ``lastsyncstatus`` enum and a ``degraded_surfaces``
JSON column on ``devices``. A sync whose interface reconcile succeeded but whose routing
surfaces failed to read is now recorded as ``partial`` with the offending surface names,
instead of a misleading ``succeeded`` over a stale mirror.

Revision ID: 3f9c1e7b2a04
Revises: f556ce117498
Create Date: 2026-07-02 10:05:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3f9c1e7b2a04"
down_revision: str | Sequence[str] | None = "f556ce117498"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("devices", sa.Column("degraded_surfaces", sa.JSON(), nullable=True))

    # PostgreSQL: extend the existing enum type. ALTER TYPE ... ADD VALUE cannot run
    # inside the migration's transaction, so use alembic's autocommit_block. Place
    # 'partial' before 'failed' to mirror the Python enum's declared order. SQLite
    # stores the enum as VARCHAR, so no type alteration is needed there.
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute("ALTER TYPE lastsyncstatus ADD VALUE IF NOT EXISTS 'partial' BEFORE 'failed'")


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("devices", "degraded_surfaces")
    # NB: PostgreSQL cannot drop a value from an enum type without recreating it; the
    # 'partial' value is left in place on downgrade (harmless — nothing references it once
    # the column is gone and the code no longer writes it).
