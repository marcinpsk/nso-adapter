# SPDX-License-Identifier: Apache-2.0
"""m15_bgp_tables

Revision ID: b4c5d6e7f8a9
Revises: a8b9c0d1e2f3
Create Date: 2026-07-01 09:00:00.000000

Adds BGP config read-mirror tables (M15 A2):
  - device_bgp_router  (per-device BGP router/ASN from network-state-export)
  - device_bgp_scope   (per-VRF BGP context)
  - device_bgp_address_family (per-scope AF activation)
  - device_bgp_peer    (per-scope BGP neighbor)
  - device_bgp_peer_af (per-peer AF activation)
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b4c5d6e7f8a9"
down_revision: str | None = "a8b9c0d1e2f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "device_bgp_router",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("asn", sa.String(32), nullable=False),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "asn", name="uq_devicebgprouter_identity"),
    )
    op.create_index("ix_device_bgp_router_device_id", "device_bgp_router", ["device_id"])

    op.create_table(
        "device_bgp_scope",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("router_id", sa.Integer(), nullable=False),
        sa.Column("vrf", sa.String(128), nullable=False, server_default=""),
        sa.ForeignKeyConstraint(["router_id"], ["device_bgp_router.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("router_id", "vrf", name="uq_devicebgpscope_identity"),
    )
    op.create_index("ix_device_bgp_scope_router_id", "device_bgp_scope", ["router_id"])

    op.create_table(
        "device_bgp_address_family",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scope_id", sa.Integer(), nullable=False),
        sa.Column("af", sa.String(32), nullable=False),
        sa.ForeignKeyConstraint(["scope_id"], ["device_bgp_scope.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("scope_id", "af", name="uq_devicebgpaf_identity"),
    )
    op.create_index("ix_device_bgp_address_family_scope_id", "device_bgp_address_family", ["scope_id"])

    op.create_table(
        "device_bgp_peer",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scope_id", sa.Integer(), nullable=False),
        sa.Column("peer_address", sa.String(64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("peer_group", sa.String(128), nullable=True),
        sa.Column("remote_as", sa.String(32), nullable=True),
        sa.Column("local_as", sa.String(32), nullable=True),
        sa.Column("ttl", sa.Integer(), nullable=True),
        sa.Column("password", sa.String(256), nullable=True),
        sa.ForeignKeyConstraint(["scope_id"], ["device_bgp_scope.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("scope_id", "peer_address", name="uq_devicebgppeer_identity"),
    )
    op.create_index("ix_device_bgp_peer_scope_id", "device_bgp_peer", ["scope_id"])

    op.create_table(
        "device_bgp_peer_af",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("peer_id", sa.Integer(), nullable=False),
        sa.Column("af", sa.String(32), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="1"),
        sa.ForeignKeyConstraint(["peer_id"], ["device_bgp_peer.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("peer_id", "af", name="uq_devicebgppeeraf_identity"),
    )
    op.create_index("ix_device_bgp_peer_af_peer_id", "device_bgp_peer_af", ["peer_id"])


def downgrade() -> None:
    op.drop_index("ix_device_bgp_peer_af_peer_id", table_name="device_bgp_peer_af")
    op.drop_table("device_bgp_peer_af")
    op.drop_index("ix_device_bgp_peer_scope_id", table_name="device_bgp_peer")
    op.drop_table("device_bgp_peer")
    op.drop_index("ix_device_bgp_address_family_scope_id", table_name="device_bgp_address_family")
    op.drop_table("device_bgp_address_family")
    op.drop_index("ix_device_bgp_scope_router_id", table_name="device_bgp_scope")
    op.drop_table("device_bgp_scope")
    op.drop_index("ix_device_bgp_router_device_id", table_name="device_bgp_router")
    op.drop_table("device_bgp_router")
