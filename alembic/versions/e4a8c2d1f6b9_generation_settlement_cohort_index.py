# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Index deployment generations by their global settlement cohort.

Revision ID: e4a8c2d1f6b9
Revises: c8e2a4f91d63
Create Date: 2026-08-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e4a8c2d1f6b9"
down_revision: str | None = "c8e2a4f91d63"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX = "ix_generation_settlement_cohort"


def upgrade() -> None:
    op.create_index(
        _INDEX,
        "deployment_generation",
        ["settlement_cohort"],
        unique=False,
        postgresql_where=sa.text("settlement_cohort IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name="deployment_generation")
