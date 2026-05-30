# SPDX-License-Identifier: Apache-2.0
"""m12_interface_ip

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-06-01 00:00:00.000000

Adds:
  - interface_ip_address table (read mirror of per-interface IPs from NSO)
  - interface_ip_intent table (write-path IP intent, structured, separate from InterfaceIntent)
"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c4d5e6f7a8b9"
down_revision: str | Sequence[str] | None = "b3c4d5e6f7a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "interface_ip_address",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(256), nullable=False),
        sa.Column("address", sa.String(64), nullable=False),
        sa.Column("vrf", sa.String(256), nullable=False, server_default=""),
        sa.Column("family", sa.String(8), nullable=False),
        sa.Column("secondary", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_source", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "device_id", "interface_name", "address", "vrf",
            name="uq_ifipaddr_device_iface_addr_vrf",
        ),
    )
    op.create_index("ix_interface_ip_address_device_id", "interface_ip_address", ["device_id"])

    op.create_table(
        "interface_ip_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("interface_id", sa.Integer(), nullable=False),
        sa.Column("address", sa.String(64), nullable=False),
        sa.Column("vrf", sa.String(256), nullable=False, server_default=""),
        sa.Column("family", sa.String(8), nullable=False),
        sa.Column("secondary", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["interface_id"], ["interfaces.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "interface_id", "address", "vrf",
            name="uq_ifipintent_iface_addr_vrf",
        ),
    )
    op.create_index("ix_interface_ip_intent_interface_id", "interface_ip_intent", ["interface_id"])


def downgrade() -> None:
    op.drop_index("ix_interface_ip_intent_interface_id", table_name="interface_ip_intent")
    op.drop_table("interface_ip_intent")
    op.drop_index("ix_interface_ip_address_device_id", table_name="interface_ip_address")
    op.drop_table("interface_ip_address")
