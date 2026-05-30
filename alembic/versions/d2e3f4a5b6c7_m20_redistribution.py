# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M20 A2: redistribution read-mirror + intent tables.

Merges the two leaf heads from M19 OSPF (c3d4e5f6a7b8) and
M17 BGP peer AF policy refs (f8a9b0c1d2e3) before adding new tables.

Revision ID: d2e3f4a5b6c7
Revises: c3d4e5f6a7b8, f8a9b0c1d2e3
Create Date: 2026-07-25
"""

from alembic import op
import sqlalchemy as sa

revision = "d2e3f4a5b6c7"
down_revision = ("c3d4e5f6a7b8", "f8a9b0c1d2e3")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "device_redistribution",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("dest_protocol", sa.String(length=16), nullable=False),
        sa.Column("dest_ref", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("source_protocol", sa.String(length=16), nullable=False),
        sa.Column("source_ref", sa.String(length=64), nullable=False, server_default=""),
        sa.Column("route_map", sa.String(length=128), nullable=True),
        sa.Column("metric", sa.Integer(), nullable=True),
        sa.Column("metric_type", sa.String(length=16), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(length=32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "device_id", "dest_protocol", "dest_ref", "source_protocol", "source_ref",
            name="uq_deviceredistribution_identity",
        ),
    )
    op.create_index("ix_device_redistribution_device_id", "device_redistribution", ["device_id"])

    op.create_table(
        "redistribution_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("dest_protocol", sa.String(length=16), nullable=False),
        sa.Column("dest_ref", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("source_protocol", sa.String(length=16), nullable=False),
        sa.Column("source_ref", sa.String(length=64), nullable=False, server_default=""),
        sa.Column("route_map", sa.String(length=128), nullable=True),
        sa.Column("metric", sa.Integer(), nullable=True),
        sa.Column("metric_type", sa.String(length=16), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "device_id", "dest_protocol", "dest_ref", "source_protocol", "source_ref",
            name="uq_redistributionintent_identity",
        ),
    )
    op.create_index("ix_redistribution_intent_device_id", "redistribution_intent", ["device_id"])


def downgrade() -> None:
    op.drop_index("ix_redistribution_intent_device_id", table_name="redistribution_intent")
    op.drop_table("redistribution_intent")
    op.drop_index("ix_device_redistribution_device_id", table_name="device_redistribution")
    op.drop_table("device_redistribution")
