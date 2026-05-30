# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M18 B2 — isis_process_intent table.

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-07-15
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "isis_process_intent",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False),
        sa.Column("process_tag", sa.String(128), nullable=False, server_default=""),
        sa.Column("net", sa.String(100), nullable=True),
        sa.Column("is_type", sa.String(50), nullable=True),
        sa.Column("metric_style", sa.String(20), nullable=True),
        sa.Column("overload_bit", sa.Boolean, nullable=True),
        sa.Column("area_auth_type", sa.String(10), nullable=True),
        sa.Column("area_auth_key", sa.String(128), nullable=True),
        sa.Column("domain_auth_type", sa.String(10), nullable=True),
        sa.Column("domain_auth_key", sa.String(128), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON, nullable=True),
    )
    op.create_index("ix_isis_process_intent_device_id", "isis_process_intent", ["device_id"])
    op.create_unique_constraint(
        "uq_isisprocessintent_identity", "isis_process_intent", ["device_id", "process_tag"]
    )


def downgrade() -> None:
    op.drop_table("isis_process_intent")
