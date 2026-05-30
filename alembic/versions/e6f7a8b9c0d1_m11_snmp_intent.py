# SPDX-License-Identifier: Apache-2.0
"""m11_snmp_intent

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
Create Date: 2026-06-10 01:00:00.000000

Adds SNMP write-path intent tables (M11 B2):
  - snmp_community_intent  (label + vault_ref; no community string ever stored)
  - snmp_v3_user_intent    (username + auth/priv vault refs)
  - snmp_host_intent       (address + version/notify_type/community_or_user)
  - snmp_system_info_intent (location + contact)
"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e6f7a8b9c0d1"
down_revision: str | Sequence[str] | None = "d5e6f7a8b9c0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "snmp_community_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("label", sa.String(128), nullable=False),
        sa.Column("vault_ref", sa.String(512), nullable=False),
        sa.Column("access", sa.String(4), nullable=False),
        sa.Column("acl", sa.String(256), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "label", name="uq_snmpcommintent_device_label"),
    )
    op.create_index("ix_snmp_community_intent_device_id", "snmp_community_intent", ["device_id"])

    op.create_table(
        "snmp_v3_user_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("username", sa.String(128), nullable=False),
        sa.Column("auth_vault_ref", sa.String(512), nullable=True),
        sa.Column("priv_vault_ref", sa.String(512), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "username", name="uq_snmpv3userintent_device_user"),
    )
    op.create_index("ix_snmp_v3_user_intent_device_id", "snmp_v3_user_intent", ["device_id"])

    op.create_table(
        "snmp_host_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("address", sa.String(64), nullable=False),
        sa.Column("version", sa.String(4), nullable=False),
        sa.Column("notify_type", sa.String(8), nullable=False),
        sa.Column("community_or_user", sa.String(128), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "address", name="uq_snmphostintent_device_addr"),
    )
    op.create_index("ix_snmp_host_intent_device_id", "snmp_host_intent", ["device_id"])

    op.create_table(
        "snmp_system_info_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False, unique=True),
        sa.Column("location", sa.String(256), nullable=True),
        sa.Column("contact", sa.String(256), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_snmp_system_info_intent_device_id", "snmp_system_info_intent", ["device_id"])


def downgrade() -> None:
    op.drop_index("ix_snmp_system_info_intent_device_id", table_name="snmp_system_info_intent")
    op.drop_table("snmp_system_info_intent")
    op.drop_index("ix_snmp_host_intent_device_id", table_name="snmp_host_intent")
    op.drop_table("snmp_host_intent")
    op.drop_index("ix_snmp_v3_user_intent_device_id", table_name="snmp_v3_user_intent")
    op.drop_table("snmp_v3_user_intent")
    op.drop_index("ix_snmp_community_intent_device_id", table_name="snmp_community_intent")
    op.drop_table("snmp_community_intent")
