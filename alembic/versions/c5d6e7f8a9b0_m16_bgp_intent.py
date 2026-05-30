# SPDX-License-Identifier: Apache-2.0
"""m16_bgp_intent

Revision ID: c5d6e7f8a9b0
Revises: b4c5d6e7f8a9
Create Date: 2026-07-07 10:00:00.000000

Adds BGP write-path intent tables (M16 B2):
  - bgp_router_intent  (per-device, per-ASN accepted intent)
  - bgp_scope_intent   (per-router-intent, per-VRF context)
  - bgp_af_intent      (per-scope-intent, per-AF activation)
  - bgp_peer_intent    (per-scope-intent, per-neighbor)
  - bgp_peer_af_intent (per-peer-intent, per-AF activation)
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c5d6e7f8a9b0"
down_revision: str | None = "b4c5d6e7f8a9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "bgp_router_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("asn", sa.String(32), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "asn", name="uq_bgprouterintent_identity"),
    )
    op.create_index("ix_bgp_router_intent_device_id", "bgp_router_intent", ["device_id"])

    op.create_table(
        "bgp_scope_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("router_id", sa.Integer(), nullable=False),
        sa.Column("vrf", sa.String(128), nullable=False, server_default=""),
        sa.ForeignKeyConstraint(["router_id"], ["bgp_router_intent.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("router_id", "vrf", name="uq_bgpscopeintent_identity"),
    )
    op.create_index("ix_bgp_scope_intent_router_id", "bgp_scope_intent", ["router_id"])

    op.create_table(
        "bgp_af_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scope_id", sa.Integer(), nullable=False),
        sa.Column("af", sa.String(32), nullable=False),
        sa.ForeignKeyConstraint(["scope_id"], ["bgp_scope_intent.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("scope_id", "af", name="uq_bgpafintent_identity"),
    )
    op.create_index("ix_bgp_af_intent_scope_id", "bgp_af_intent", ["scope_id"])

    op.create_table(
        "bgp_peer_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("scope_id", sa.Integer(), nullable=False),
        sa.Column("peer_address", sa.String(64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("peer_group", sa.String(128), nullable=True),
        sa.Column("remote_as", sa.String(32), nullable=True),
        sa.Column("local_as", sa.String(32), nullable=True),
        sa.Column("ttl", sa.Integer(), nullable=True),
        sa.Column("password", sa.String(256), nullable=True),
        sa.ForeignKeyConstraint(["scope_id"], ["bgp_scope_intent.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("scope_id", "peer_address", name="uq_bgppeerintent_identity"),
    )
    op.create_index("ix_bgp_peer_intent_scope_id", "bgp_peer_intent", ["scope_id"])

    op.create_table(
        "bgp_peer_af_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("peer_id", sa.Integer(), nullable=False),
        sa.Column("af", sa.String(32), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.ForeignKeyConstraint(["peer_id"], ["bgp_peer_intent.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("peer_id", "af", name="uq_bgppeerafintent_identity"),
    )
    op.create_index("ix_bgp_peer_af_intent_peer_id", "bgp_peer_af_intent", ["peer_id"])


def downgrade() -> None:
    op.drop_index("ix_bgp_peer_af_intent_peer_id", table_name="bgp_peer_af_intent")
    op.drop_table("bgp_peer_af_intent")
    op.drop_index("ix_bgp_peer_intent_scope_id", table_name="bgp_peer_intent")
    op.drop_table("bgp_peer_intent")
    op.drop_index("ix_bgp_af_intent_scope_id", table_name="bgp_af_intent")
    op.drop_table("bgp_af_intent")
    op.drop_index("ix_bgp_scope_intent_router_id", table_name="bgp_scope_intent")
    op.drop_table("bgp_scope_intent")
    op.drop_index("ix_bgp_router_intent_device_id", table_name="bgp_router_intent")
    op.drop_table("bgp_router_intent")
