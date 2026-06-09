"""add device_subinterface + subinterface_intent (M36 dot1q subinterfaces)

Revision ID: c6d7e8f9a0b1
Revises: b5c6d7e8f9a0
Create Date: 2026-06-09

Read mirror + write-path intent for dot1q L3 subinterfaces (IOS Gi0/1.100,
Junos ge-0/0/0.100) exported from NSO. No IPs — those ride interface-ip. The
dot1q tag is interface-local encapsulation, deliberately not a foreign key.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c6d7e8f9a0b1"
down_revision = "b5c6d7e8f9a0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "device_subinterface",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("parent_interface", sa.String(length=128), nullable=True),
        sa.Column("dot1q_vlan", sa.Integer(), nullable=True),
        sa.Column("sub_type", sa.String(length=16), nullable=False, server_default="subinterface"),
        sa.Column("vrf", sa.String(length=128), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(length=32), nullable=False, server_default="never"),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_devicesubif_identity"),
    )
    op.create_index(
        op.f("ix_device_subinterface_device_id"), "device_subinterface", ["device_id"], unique=False
    )

    op.create_table(
        "subinterface_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("parent_interface", sa.String(length=128), nullable=True),
        sa.Column("dot1q_vlan", sa.Integer(), nullable=True),
        sa.Column("sub_type", sa.String(length=16), nullable=False, server_default="subinterface"),
        sa.Column("vrf", sa.String(length=128), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_subifintent_identity"),
    )
    op.create_index(
        op.f("ix_subinterface_intent_device_id"), "subinterface_intent", ["device_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_subinterface_intent_device_id"), table_name="subinterface_intent")
    op.drop_table("subinterface_intent")
    op.drop_index(op.f("ix_device_subinterface_device_id"), table_name="device_subinterface")
    op.drop_table("device_subinterface")
