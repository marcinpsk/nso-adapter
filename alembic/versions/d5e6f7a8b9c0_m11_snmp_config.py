# SPDX-License-Identifier: Apache-2.0
"""m11_snmp_config

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-06-10 00:00:00.000000

Adds SNMP read-mirror tables:
  - snmp_community  (community_hash, access, acl — no secret values)
  - snmp_v3_user    (username, has_auth_secret, has_priv_secret — no passwords)
  - snmp_host       (address, version, notify_type, port — no community strings)
  - snmp_system_info (location, contact)
"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d5e6f7a8b9c0"
down_revision: str | Sequence[str] | None = "c4d5e6f7a8b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "snmp_community",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("community_hash", sa.String(64), nullable=False),
        sa.Column("access", sa.String(4), nullable=False),
        sa.Column("acl", sa.String(256), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_source", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "community_hash", name="uq_snmpcommunity_device_hash"),
    )
    op.create_index("ix_snmp_community_device_id", "snmp_community", ["device_id"])

    op.create_table(
        "snmp_v3_user",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("username", sa.String(256), nullable=False),
        sa.Column("has_auth_secret", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("has_priv_secret", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_source", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "username", name="uq_snmpv3user_device_username"),
    )
    op.create_index("ix_snmp_v3_user_device_id", "snmp_v3_user", ["device_id"])

    op.create_table(
        "snmp_host",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("address", sa.String(64), nullable=False),
        sa.Column("version", sa.String(8), nullable=True),
        sa.Column("notify_type", sa.String(16), nullable=True),
        sa.Column("port", sa.Integer(), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_source", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "address", name="uq_snmphost_device_address"),
    )
    op.create_index("ix_snmp_host_device_id", "snmp_host", ["device_id"])

    op.create_table(
        "snmp_system_info",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("location", sa.String(256), nullable=True),
        sa.Column("contact", sa.String(256), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_source", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", name="uq_snmpsysteminfo_device"),
    )
    op.create_index("ix_snmp_system_info_device_id", "snmp_system_info", ["device_id"])


def downgrade() -> None:
    op.drop_index("ix_snmp_system_info_device_id", table_name="snmp_system_info")
    op.drop_table("snmp_system_info")
    op.drop_index("ix_snmp_host_device_id", table_name="snmp_host")
    op.drop_table("snmp_host")
    op.drop_index("ix_snmp_v3_user_device_id", table_name="snmp_v3_user")
    op.drop_table("snmp_v3_user")
    op.drop_index("ix_snmp_community_device_id", table_name="snmp_community")
    op.drop_table("snmp_community")
