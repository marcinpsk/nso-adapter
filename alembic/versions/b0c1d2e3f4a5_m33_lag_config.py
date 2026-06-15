# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M33: add lag_bundle_config and lag_member_config tables.

Revision ID: b0c1d2e3f4a5
Revises: 5a6b7c8d9e0f
Create Date: 2026-06-06 20:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b0c1d2e3f4a5"
down_revision: str | Sequence[str] | None = "5a6b7c8d9e0f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "lag_bundle_config",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("lag_id", sa.Integer(), nullable=False),
        sa.Column("min_links", sa.Integer(), nullable=True),
        sa.Column("system_priority", sa.Integer(), nullable=True),
        sa.Column("system_id", sa.String(length=17), nullable=True),
        sa.Column("timer", sa.String(length=8), nullable=True),
        sa.Column("admin_key", sa.Integer(), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(length=64), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "lag_id", name="uq_lag_bundle_config_device_lag"),
    )
    op.create_index(op.f("ix_lag_bundle_config_device_id"), "lag_bundle_config", ["device_id"], unique=False)
    op.create_table(
        "lag_member_config",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("lag_bundle_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=True),
        sa.Column("port_priority", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["lag_bundle_id"], ["lag_bundle_config.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lag_bundle_id", "interface_name", name="uq_lag_member_config_bundle_iface"),
    )
    op.create_index(op.f("ix_lag_member_config_lag_bundle_id"), "lag_member_config", ["lag_bundle_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_lag_member_config_lag_bundle_id"), table_name="lag_member_config")
    op.drop_table("lag_member_config")
    op.drop_index(op.f("ix_lag_bundle_config_device_id"), table_name="lag_bundle_config")
    op.drop_table("lag_bundle_config")
