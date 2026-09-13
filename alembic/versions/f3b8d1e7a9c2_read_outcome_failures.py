# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Persist authored read-failure classifications on refresh outcomes.

Revision ID: f3b8d1e7a9c2
Revises: c7b4e1d90f28
Create Date: 2026-09-13 16:32:57.581804

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "f3b8d1e7a9c2"
down_revision: str | Sequence[str] | None = "c7b4e1d90f28"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "refresh_outcome",
        sa.Column(
            "read_failures",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("refresh_outcome", "read_failures")
