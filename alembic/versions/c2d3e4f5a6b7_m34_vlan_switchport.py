# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M34: add device_vlan, device_switchport, device_switchport_tagged_vlan tables.

Revision ID: c2d3e4f5a6b7
Revises: b0c1d2e3f4a5
Create Date: 2026-06-07 08:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c2d3e4f5a6b7"
down_revision: Union[str, Sequence[str], None] = "b0c1d2e3f4a5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "device_vlan",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("vlan_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(length=32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "vlan_id", name="uq_devicevlan_identity"),
    )
    op.create_index(op.f("ix_device_vlan_device_id"), "device_vlan", ["device_id"], unique=False)

    op.create_table(
        "device_switchport",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=True),
        sa.Column("untagged_vlan_id", sa.Integer(), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(length=32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["untagged_vlan_id"], ["device_vlan.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_deviceswitchport_identity"),
    )
    op.create_index(
        op.f("ix_device_switchport_device_id"), "device_switchport", ["device_id"], unique=False
    )

    op.create_table(
        "device_switchport_tagged_vlan",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("switchport_id", sa.Integer(), nullable=False),
        sa.Column("vlan_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["switchport_id"], ["device_switchport.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["vlan_id"], ["device_vlan.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("switchport_id", "vlan_id", name="uq_swtaggedvlan_identity"),
    )


def downgrade() -> None:
    op.drop_table("device_switchport_tagged_vlan")
    op.drop_index(op.f("ix_device_switchport_device_id"), table_name="device_switchport")
    op.drop_table("device_switchport")
    op.drop_index(op.f("ix_device_vlan_device_id"), table_name="device_vlan")
    op.drop_table("device_vlan")
