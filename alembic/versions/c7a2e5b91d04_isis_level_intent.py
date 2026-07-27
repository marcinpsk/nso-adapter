# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""isis per-level process intent

Per-level (L1/L2) IS-IS process tuning accepted by the NetBox operator —
wide-metrics-only, labeled-preference, disabled — keyed (device, process_tag,
level) independently of isis_process_intent (the flex-algo pattern) and nested
into the isis-reconciler process-config payload at body-build.

Revision ID: c7a2e5b91d04
Revises: a1d7f3c9e582
Create Date: 2026-07-07 19:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c7a2e5b91d04"
down_revision: str | None = "a1d7f3c9e582"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "isis_level_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("process_tag", sa.String(length=128), nullable=False),
        sa.Column("level", sa.Integer(), nullable=False),
        sa.Column("wide_metrics_only", sa.Boolean(), nullable=True),
        sa.Column("labeled_preference", sa.Integer(), nullable=True),
        sa.Column("disabled", sa.Boolean(), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "process_tag", "level", name="uq_isislevelintent_identity"),
    )
    op.create_index(op.f("ix_isis_level_intent_device_id"), "isis_level_intent", ["device_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_isis_level_intent_device_id"), table_name="isis_level_intent")
    op.drop_table("isis_level_intent")
