# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM 1332: source epochs and payload publication revisions.

Revision ID: e8c1a4f7b2d9
Revises: d4f6a8b0c2e1
Create Date: 2026-07-26
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e8c1a4f7b2d9"
down_revision: str | Sequence[str] | None = "d4f6a8b0c2e1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("devices", sa.Column("source_epoch", sa.BigInteger(), server_default="1", nullable=False))
    op.add_column("refresh_outcome", sa.Column("source_epoch", sa.BigInteger(), server_default="1", nullable=False))
    # Existing two-commit pointers are intentionally unproven; never manufacture a
    # revision by backfilling attempt_id.
    op.add_column("refresh_outcome_pointer", sa.Column("payload_revision", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("refresh_outcome_pointer", "payload_revision")
    op.drop_column("refresh_outcome", "source_epoch")
    op.drop_column("devices", "source_epoch")
