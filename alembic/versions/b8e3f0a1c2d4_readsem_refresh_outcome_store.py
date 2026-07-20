# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""READSEM two-phase refresh-outcome store.

Additive: two new tables recording every read-mirror refresh attempt's outcome (READSEM §2.4).

* ``refresh_outcome`` — one row per attempt, updated across two phases (read outcome, then
  materialization result). ``id`` is the immutable attempt id + start-order key.
* ``refresh_outcome_pointer`` — per-(device, family) pointer to the newest TERMINAL attempt by
  start order.

Revision ID: b8e3f0a1c2d4
Revises: f7c2a9e10b34
Create Date: 2026-07-20
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b8e3f0a1c2d4"
down_revision: str | Sequence[str] | None = "f7c2a9e10b34"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "refresh_outcome",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("family", sa.String(length=32), nullable=False),
        sa.Column("refresh_source", sa.String(length=32), nullable=False),
        sa.Column("read_outcome", sa.String(length=32), nullable=False),
        sa.Column("read_reason", sa.String(length=32), nullable=True),
        sa.Column("freshness", sa.String(length=16), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("result", sa.String(length=16), nullable=True),
        sa.Column("succeeded", sa.Boolean(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_refresh_outcome_device_id"), "refresh_outcome", ["device_id"], unique=False)
    op.create_index(op.f("ix_refresh_outcome_family"), "refresh_outcome", ["family"], unique=False)
    op.create_table(
        "refresh_outcome_pointer",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("family", sa.String(length=32), nullable=False),
        sa.Column("attempt_id", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["attempt_id"], ["refresh_outcome.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "family", name="uq_refresh_outcome_pointer"),
    )
    op.create_index(
        op.f("ix_refresh_outcome_pointer_device_id"), "refresh_outcome_pointer", ["device_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_refresh_outcome_pointer_device_id"), table_name="refresh_outcome_pointer")
    op.drop_table("refresh_outcome_pointer")
    op.drop_index(op.f("ix_refresh_outcome_family"), table_name="refresh_outcome")
    op.drop_index(op.f("ix_refresh_outcome_device_id"), table_name="refresh_outcome")
    op.drop_table("refresh_outcome")
