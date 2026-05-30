# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M17 route-policy read-mirror tables.

Revision ID: d6e7f8a9b0c1
Revises: c5d6e7f8a9b0
Create Date: 2026-07-10
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d6e7f8a9b0c1"
down_revision = "c5d6e7f8a9b0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── prefix-list ──────────────────────────────────────────────────────────
    op.create_table(
        "device_route_policy_prefix_list",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(256), nullable=False),
        sa.Column("family", sa.Integer, nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime, nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="poll"),
        sa.UniqueConstraint("device_id", "name", name="uq_drppl_identity"),
    )
    op.create_index("ix_drppl_device_id", "device_route_policy_prefix_list", ["device_id"])

    op.create_table(
        "device_route_policy_prefix_list_entry",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "prefix_list_id",
            sa.Integer,
            sa.ForeignKey("device_route_policy_prefix_list.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.Integer, nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("prefix", sa.String(64), nullable=False),
        sa.Column("ge", sa.Integer, nullable=True),
        sa.Column("le", sa.Integer, nullable=True),
        sa.UniqueConstraint("prefix_list_id", "sequence", name="uq_drpple_seq"),
    )
    op.create_index(
        "ix_drpple_prefix_list_id", "device_route_policy_prefix_list_entry", ["prefix_list_id"]
    )

    # ── community-list ────────────────────────────────────────────────────────
    op.create_table(
        "device_route_policy_community_list",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(256), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime, nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="poll"),
        sa.UniqueConstraint("device_id", "name", name="uq_drpcl_identity"),
    )
    op.create_index("ix_drpcl_device_id", "device_route_policy_community_list", ["device_id"])

    op.create_table(
        "device_route_policy_community_list_entry",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "community_list_id",
            sa.Integer,
            sa.ForeignKey("device_route_policy_community_list.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.Integer, nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("community", sa.String(128), nullable=False),
        sa.UniqueConstraint("community_list_id", "sequence", name="uq_drpcle_seq"),
    )
    op.create_index(
        "ix_drpcle_community_list_id",
        "device_route_policy_community_list_entry",
        ["community_list_id"],
    )

    # ── as-path ───────────────────────────────────────────────────────────────
    op.create_table(
        "device_route_policy_as_path",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(256), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime, nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="poll"),
        sa.UniqueConstraint("device_id", "name", name="uq_drpap_identity"),
    )
    op.create_index("ix_drpap_device_id", "device_route_policy_as_path", ["device_id"])

    op.create_table(
        "device_route_policy_as_path_entry",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "as_path_id",
            sa.Integer,
            sa.ForeignKey("device_route_policy_as_path.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.Integer, nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("pattern", sa.Text, nullable=False),
        sa.UniqueConstraint("as_path_id", "sequence", name="uq_drpape_seq"),
    )
    op.create_index("ix_drpape_as_path_id", "device_route_policy_as_path_entry", ["as_path_id"])

    # ── route-map ─────────────────────────────────────────────────────────────
    op.create_table(
        "device_route_policy_route_map",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(256), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime, nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="poll"),
        sa.UniqueConstraint("device_id", "name", name="uq_drprm_identity"),
    )
    op.create_index("ix_drprm_device_id", "device_route_policy_route_map", ["device_id"])

    op.create_table(
        "device_route_policy_route_map_entry",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "route_map_id",
            sa.Integer,
            sa.ForeignKey("device_route_policy_route_map.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.Integer, nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("match_prefix_lists", sa.JSON, nullable=True),
        sa.Column("match_community_lists", sa.JSON, nullable=True),
        sa.Column("match_as_paths", sa.JSON, nullable=True),
        sa.Column("match_json", sa.Text, nullable=True),
        sa.Column("set_json", sa.Text, nullable=True),
        sa.UniqueConstraint("route_map_id", "sequence", name="uq_drprme_seq"),
    )
    op.create_index(
        "ix_drprme_route_map_id", "device_route_policy_route_map_entry", ["route_map_id"]
    )


def downgrade() -> None:
    op.drop_table("device_route_policy_route_map_entry")
    op.drop_table("device_route_policy_route_map")
    op.drop_table("device_route_policy_as_path_entry")
    op.drop_table("device_route_policy_as_path")
    op.drop_table("device_route_policy_community_list_entry")
    op.drop_table("device_route_policy_community_list")
    op.drop_table("device_route_policy_prefix_list_entry")
    op.drop_table("device_route_policy_prefix_list")
