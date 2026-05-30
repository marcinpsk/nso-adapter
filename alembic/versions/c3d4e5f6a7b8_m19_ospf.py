# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M19 A2: OSPF read-mirror + intent tables.

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-07-20
"""

from alembic import op
import sqlalchemy as sa

revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "device_ospf_instance",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("process_id", sa.Integer, nullable=False),
        sa.Column("router_id", sa.String(64), nullable=True),
        sa.Column("vrf", sa.String(64), nullable=False, server_default=""),
        sa.Column("areas", sa.JSON, nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="never"),
        sa.UniqueConstraint("device_id", "process_id", name="uq_deviceospfinstance_identity"),
    )

    op.create_table(
        "device_ospf_interface",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("interface_name", sa.String(128), nullable=False),
        sa.Column("process_id", sa.Integer, nullable=True),
        sa.Column("area_id", sa.String(64), nullable=True),
        sa.Column("passive", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("priority", sa.Integer, nullable=True),
        sa.Column("cost", sa.Integer, nullable=True),
        sa.Column("network_type", sa.String(32), nullable=True),
        sa.Column("auth_type", sa.String(32), nullable=True),
        sa.Column("auth_present", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(32), nullable=False, server_default="never"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_deviceospfinterface_identity"),
    )

    op.create_table(
        "ospf_instance_intent",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("process_id", sa.Integer, nullable=False),
        sa.Column("router_id", sa.String(64), nullable=True),
        sa.Column("vrf", sa.String(64), nullable=False, server_default=""),
        sa.Column("areas", sa.JSON, nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON, nullable=True),
        sa.UniqueConstraint("device_id", "process_id", name="uq_ospfinstanceintent_identity"),
    )

    op.create_table(
        "ospf_interface_intent",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("device_id", sa.Integer, sa.ForeignKey("devices.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("interface_name", sa.String(128), nullable=False),
        sa.Column("process_id", sa.Integer, nullable=True),
        sa.Column("area_id", sa.String(64), nullable=True),
        sa.Column("passive", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("priority", sa.Integer, nullable=True),
        sa.Column("cost", sa.Integer, nullable=True),
        sa.Column("network_type", sa.String(32), nullable=True),
        sa.Column("auth_type", sa.String(32), nullable=True),
        sa.Column("auth_key", sa.String(128), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON, nullable=True),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_ospfinterfaceintent_identity"),
    )


def downgrade() -> None:
    op.drop_table("ospf_interface_intent")
    op.drop_table("ospf_instance_intent")
    op.drop_table("device_ospf_interface")
    op.drop_table("device_ospf_instance")
