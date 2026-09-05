# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Add the prepared projection slot for the out-of-protocol switching streams.

Revision ID: b3d9f2a6c410
Revises: a5c7e9b1d3f6
Create Date: 2026-09-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b3d9f2a6c410"
down_revision: str | None = "a5c7e9b1d3f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "device_projection_stream"
_COLUMNS = ("prepared_revision", "prepared_tables", "prepared_deletions")


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("prepared_revision", sa.BigInteger(), nullable=True))
    op.add_column(_TABLE, sa.Column("prepared_tables", sa.JSON(), nullable=True))
    op.add_column(_TABLE, sa.Column("prepared_deletions", sa.JSON(), nullable=True))
    op.create_check_constraint(
        "ck_projection_stream_prepared_slot",
        _TABLE,
        "(prepared_revision IS NULL AND prepared_tables IS NULL AND prepared_deletions IS NULL) OR "
        "(prepared_revision IS NOT NULL AND prepared_tables IS NOT NULL AND prepared_deletions IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_projection_stream_prepared_revision",
        _TABLE,
        "prepared_revision IS NULL OR (prepared_revision > 0 AND prepared_revision <= desired_revision)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_projection_stream_prepared_revision", _TABLE, type_="check")
    op.drop_constraint("ck_projection_stream_prepared_slot", _TABLE, type_="check")
    for column in _COLUMNS:
        op.drop_column(_TABLE, column)
