# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Record source revision and snapshot digest with switching preparations.

Revision ID: c6e8a1f42d90
Revises: f3b8d1e7a9c2
Create Date: 2026-09-25
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c6e8a1f42d90"
down_revision: str | Sequence[str] | None = "f3b8d1e7a9c2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "device_projection_stream"
_SLOT_CHECK = (
    "(prepared_revision IS NULL AND prepared_tables IS NULL AND prepared_deletions IS NULL AND "
    "prepared_source_revision IS NULL AND prepared_source_digest IS NULL) OR "
    "(prepared_revision IS NOT NULL AND prepared_tables IS NOT NULL AND prepared_deletions IS NOT NULL AND "
    "prepared_source_revision IS NOT NULL AND prepared_source_digest IS NOT NULL)"
)


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("prepared_source_revision", sa.BigInteger(), nullable=True))
    op.add_column(_TABLE, sa.Column("prepared_source_digest", sa.String(length=64), nullable=True))
    # Old slots have no source identity. A new sender must prepare them again.
    op.execute(
        sa.text(
            "UPDATE device_projection_stream SET prepared_revision = NULL, prepared_tables = NULL, "
            "prepared_deletions = NULL WHERE prepared_revision IS NOT NULL"
        )
    )
    op.drop_constraint("ck_projection_stream_prepared_slot", _TABLE, type_="check")
    op.create_check_constraint("ck_projection_stream_prepared_slot", _TABLE, _SLOT_CHECK)


def downgrade() -> None:
    op.drop_constraint("ck_projection_stream_prepared_slot", _TABLE, type_="check")
    op.create_check_constraint(
        "ck_projection_stream_prepared_slot",
        _TABLE,
        "(prepared_revision IS NULL AND prepared_tables IS NULL AND prepared_deletions IS NULL) OR "
        "(prepared_revision IS NOT NULL AND prepared_tables IS NOT NULL AND prepared_deletions IS NOT NULL)",
    )
    op.drop_column(_TABLE, "prepared_source_digest")
    op.drop_column(_TABLE, "prepared_source_revision")
