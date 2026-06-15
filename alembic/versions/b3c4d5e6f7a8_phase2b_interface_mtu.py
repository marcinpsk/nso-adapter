"""add device_interface_mtu (Phase 2b interface MTU read mirror)

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7
Create Date: 2026-06-12

Read mirror for per-interface MTU (mtu / ip-mtu / mpls-mtu + Nokia bound-port)
exported from NSO under FLAG_NO_DEFAULTS. Read-only first — no intent table yet.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "b3c4d5e6f7a8"
down_revision = "a2b3c4d5e6f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "device_interface_mtu",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("mtu", sa.Integer(), nullable=True),
        sa.Column("ip_mtu", sa.Integer(), nullable=True),
        sa.Column("mpls_mtu", sa.Integer(), nullable=True),
        sa.Column("bound_port", sa.String(length=128), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(length=32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_deviceifmtu_identity"),
    )
    op.create_index(op.f("ix_device_interface_mtu_device_id"), "device_interface_mtu", ["device_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_device_interface_mtu_device_id"), table_name="device_interface_mtu")
    op.drop_table("device_interface_mtu")
