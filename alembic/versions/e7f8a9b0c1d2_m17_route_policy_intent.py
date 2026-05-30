# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M17 route-policy intent table.

Revision ID: e7f8a9b0c1d2
Revises: d6e7f8a9b0c1
Create Date: 2026-07-10
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "e7f8a9b0c1d2"
down_revision = "d6e7f8a9b0c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "route_policy_object_intent",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("family", sa.String(32), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("entries", sa.JSON, nullable=False),
        sa.Column("accepted_at", sa.DateTime, nullable=True),
        sa.Column("last_apply_at", sa.DateTime, nullable=True),
        sa.Column("last_apply_error", sa.JSON, nullable=True),
        sa.UniqueConstraint("device_id", "family", "name", name="uq_rpoi_identity"),
    )
    op.create_index("ix_rpoi_device_id", "route_policy_object_intent", ["device_id"])


def downgrade() -> None:
    op.drop_index("ix_rpoi_device_id", "route_policy_object_intent")
    op.drop_table("route_policy_object_intent")
