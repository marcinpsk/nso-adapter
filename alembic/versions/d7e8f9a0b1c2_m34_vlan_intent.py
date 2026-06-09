"""add vlan_intent (M34 VLAN-database write path)

Revision ID: d7e8f9a0b1c2
Revises: c6d7e8f9a0b1
Create Date: 2026-06-09

Write-path intent mirror for VLAN-database entries (vid + name) the operator
accepted. The single device Apply commits these via the vlan-reconciler service.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "d7e8f9a0b1c2"
down_revision = "c6d7e8f9a0b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "vlan_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("vlan_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "vlan_id", name="uq_vlanintent_identity"),
    )
    op.create_index(op.f("ix_vlan_intent_device_id"), "vlan_intent", ["device_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_vlan_intent_device_id"), table_name="vlan_intent")
    op.drop_table("vlan_intent")
