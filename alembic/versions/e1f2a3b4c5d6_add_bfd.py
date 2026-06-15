# SPDX-License-Identifier: Apache-2.0
"""add device_bfd_interface + device_bgp_peer.bfd_enabled

network-state-export now exports per-interface BFD (timers + micro-BFD) and a
per-peer bfd-enabled flag. Mirror them: a device_bfd_interface table (the plugin
dedupes a shared BFDProfile + creates BFDInterface from these) and a bfd_enabled
column on device_bgp_peer.

Revision ID: e1f2a3b4c5d6
Revises: d0e1f2a3b4c5
Create Date: 2026-06-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e1f2a3b4c5d6"
down_revision: str | Sequence[str] | None = "d0e1f2a3b4c5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_bgp_peer", sa.Column("bfd_enabled", sa.Boolean(), nullable=True))
    op.create_table(
        "device_bfd_interface",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("bound_port", sa.String(length=256), nullable=True),
        sa.Column("min_tx", sa.Integer(), nullable=True),
        sa.Column("min_rx", sa.Integer(), nullable=True),
        sa.Column("multiplier", sa.Integer(), nullable=True),
        sa.Column("micro_bfd", sa.Boolean(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_devicebfdinterface_identity"),
    )
    op.create_index(op.f("ix_device_bfd_interface_device_id"), "device_bfd_interface", ["device_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_device_bfd_interface_device_id"), table_name="device_bfd_interface")
    op.drop_table("device_bfd_interface")
    op.drop_column("device_bgp_peer", "bfd_enabled")
