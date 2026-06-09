"""add device_svi (M35 SVI/IRB read mirror)

Revision ID: a4b5c6d7e8f9
Revises: f3a4b5c6d7e8
Create Date: 2026-06-09

Read mirror of L3 VLAN interfaces (SVIs / IRBs) exported from NSO. No IPs — those
ride the interface-ip path. One row per (device, interface-name).
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "a4b5c6d7e8f9"
down_revision = "f3a4b5c6d7e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "device_svi",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("vlan_id", sa.Integer(), nullable=False),
        sa.Column("svi_type", sa.String(length=8), nullable=False, server_default="svi"),
        sa.Column("vrf", sa.String(length=128), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(length=32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_devicesvi_identity"),
    )
    op.create_index(op.f("ix_device_svi_device_id"), "device_svi", ["device_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_device_svi_device_id"), table_name="device_svi")
    op.drop_table("device_svi")
