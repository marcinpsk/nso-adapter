# SPDX-License-Identifier: Apache-2.0
"""add device_bgp_peer_group + device_bgp_peer_group_af (peer-group/template objects)

network-state-export now exports BGP peer-group / template objects with their
OWN per-AF route-map / prefix-list policies (M15 B1). Mirror them so the GET
endpoint hands them to the plugin, which models each as a netbox_routing
BGPPeerTemplate carrying its own BGPPeerAddressFamily policy rows.

Revision ID: c9d0e1f2a3b4
Revises: b8c9d0e1f2a3
Create Date: 2026-06-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c9d0e1f2a3b4"
down_revision: str | Sequence[str] | None = "b8c9d0e1f2a3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "device_bgp_peer_group",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scope_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("remote_as", sa.String(length=32), nullable=True),
        sa.Column("source", sa.String(length=256), nullable=True),
        sa.ForeignKeyConstraint(["scope_id"], ["device_bgp_scope.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("scope_id", "name", name="uq_devicebgppeergroup_identity"),
    )
    op.create_index(op.f("ix_device_bgp_peer_group_scope_id"), "device_bgp_peer_group", ["scope_id"], unique=False)

    op.create_table(
        "device_bgp_peer_group_af",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("peer_group_id", sa.Integer(), nullable=False),
        sa.Column("af", sa.String(length=32), nullable=False),
        sa.Column("routemap_in", sa.String(length=255), nullable=True),
        sa.Column("routemap_out", sa.String(length=255), nullable=True),
        sa.Column("prefixlist_in", sa.String(length=255), nullable=True),
        sa.Column("prefixlist_out", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(["peer_group_id"], ["device_bgp_peer_group.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("peer_group_id", "af", name="uq_devicebgppeergroupaf_identity"),
    )
    op.create_index(
        op.f("ix_device_bgp_peer_group_af_peer_group_id"),
        "device_bgp_peer_group_af",
        ["peer_group_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_device_bgp_peer_group_af_peer_group_id"), table_name="device_bgp_peer_group_af")
    op.drop_table("device_bgp_peer_group_af")
    op.drop_index(op.f("ix_device_bgp_peer_group_scope_id"), table_name="device_bgp_peer_group")
    op.drop_table("device_bgp_peer_group")
