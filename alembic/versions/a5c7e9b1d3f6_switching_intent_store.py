# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Add durable LAG and switchport intent snapshots.

Revision ID: a5c7e9b1d3f6
Revises: c6f1a8d2e4b7
Create Date: 2026-08-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a5c7e9b1d3f6"
down_revision: str | None = "c6f1a8d2e4b7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "lag_bundle_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("lag_id", sa.BigInteger(), nullable=True),
        sa.Column("min_links", sa.Integer(), nullable=True),
        sa.Column("system_priority", sa.Integer(), nullable=True),
        sa.Column("system_id", sa.String(length=17), nullable=True),
        sa.Column("timer", sa.String(length=8), nullable=True),
        sa.Column("admin_key", sa.Integer(), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(none_as_null=True), nullable=True),
        sa.CheckConstraint(
            "admin_key IS NULL OR admin_key BETWEEN 0 AND 65535",
            name="ck_lag_bundle_intent_admin_key_uint16",
        ),
        sa.CheckConstraint(
            "lag_id IS NULL OR lag_id BETWEEN 0 AND 4294967295",
            name="ck_lag_bundle_intent_lag_id_uint32",
        ),
        sa.CheckConstraint(
            "min_links IS NULL OR min_links BETWEEN 0 AND 65535",
            name="ck_lag_bundle_intent_min_links_uint16",
        ),
        sa.CheckConstraint(
            "system_priority IS NULL OR system_priority BETWEEN 0 AND 65535",
            name="ck_lag_bundle_intent_system_priority_uint16",
        ),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "name", name="uq_lag_bundle_intent_identity"),
    )
    op.create_index("ix_lag_bundle_intent_device_id", "lag_bundle_intent", ["device_id"], unique=False)
    op.create_table(
        "lag_member_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("lag_bundle_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=True),
        sa.Column("port_priority", sa.Integer(), nullable=True),
        sa.CheckConstraint(
            "port_priority IS NULL OR port_priority BETWEEN 0 AND 65535",
            name="ck_lag_member_intent_port_priority_uint16",
        ),
        sa.ForeignKeyConstraint(["lag_bundle_id"], ["lag_bundle_intent.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lag_bundle_id", "interface_name", name="uq_lag_member_intent_identity"),
    )
    op.create_index("ix_lag_member_intent_lag_bundle_id", "lag_member_intent", ["lag_bundle_id"], unique=False)
    op.create_table(
        "switchport_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=True),
        sa.Column("untagged_vlan", sa.Integer(), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(none_as_null=True), nullable=True),
        sa.CheckConstraint(
            "untagged_vlan IS NULL OR untagged_vlan BETWEEN 0 AND 65535",
            name="ck_switchport_intent_untagged_vlan_uint16",
        ),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_switchport_intent_identity"),
    )
    op.create_index("ix_switchport_intent_device_id", "switchport_intent", ["device_id"], unique=False)
    op.create_table(
        "switchport_tagged_vlan_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("switchport_id", sa.Integer(), nullable=False),
        sa.Column("vlan_id", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "vlan_id BETWEEN 0 AND 65535",
            name="ck_switchport_tagged_vlan_intent_vlan_id_uint16",
        ),
        sa.ForeignKeyConstraint(["switchport_id"], ["switchport_intent.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "switchport_id",
            "vlan_id",
            name="uq_switchport_tagged_vlan_intent_identity",
        ),
    )
    op.create_index(
        "ix_switchport_tagged_vlan_intent_switchport_id",
        "switchport_tagged_vlan_intent",
        ["switchport_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_switchport_tagged_vlan_intent_switchport_id",
        table_name="switchport_tagged_vlan_intent",
    )
    op.drop_table("switchport_tagged_vlan_intent")
    op.drop_index("ix_switchport_intent_device_id", table_name="switchport_intent")
    op.drop_table("switchport_intent")
    op.drop_index("ix_lag_member_intent_lag_bundle_id", table_name="lag_member_intent")
    op.drop_table("lag_member_intent")
    op.drop_index("ix_lag_bundle_intent_device_id", table_name="lag_bundle_intent")
    op.drop_table("lag_bundle_intent")
