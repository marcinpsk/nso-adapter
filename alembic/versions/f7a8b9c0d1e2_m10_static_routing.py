# SPDX-License-Identifier: Apache-2.0
"""m10_static_routing

Revision ID: f7a8b9c0d1e2
Revises: e6f7a8b9c0d1
Create Date: 2026-05-29 09:00:00.000000

Adds static routing tables (M10 A2):
  - device_static_route  (read mirror from network-state-export)
  - static_route_intent  (write-path intent accepted by NetBox operator)
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f7a8b9c0d1e2"
down_revision: str | None = "e6f7a8b9c0d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "device_static_route",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("vrf", sa.String(128), nullable=False, server_default=""),
        sa.Column("prefix", sa.String(64), nullable=False),
        sa.Column("next_hop", sa.String(64), nullable=False, server_default=""),
        sa.Column("interface_next_hop", sa.String(128), nullable=True),
        sa.Column("metric", sa.Integer(), nullable=True),
        sa.Column("permanent", sa.Boolean(), nullable=True),
        sa.Column("tag", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "vrf", "prefix", "next_hop", name="uq_devicestaticroute_identity"),
    )
    op.create_index("ix_device_static_route_device_id", "device_static_route", ["device_id"])

    op.create_table(
        "static_route_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("vrf", sa.String(128), nullable=False, server_default=""),
        sa.Column("prefix", sa.String(64), nullable=False),
        sa.Column("next_hop", sa.String(64), nullable=False, server_default=""),
        sa.Column("interface_next_hop", sa.String(128), nullable=True),
        sa.Column("metric", sa.Integer(), nullable=True),
        sa.Column("permanent", sa.Boolean(), nullable=True),
        sa.Column("tag", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "vrf", "prefix", "next_hop", name="uq_staticrouteintent_identity"),
    )
    op.create_index("ix_static_route_intent_device_id", "static_route_intent", ["device_id"])


def downgrade() -> None:
    op.drop_index("ix_static_route_intent_device_id", table_name="static_route_intent")
    op.drop_table("static_route_intent")
    op.drop_index("ix_device_static_route_device_id", table_name="device_static_route")
    op.drop_table("device_static_route")
