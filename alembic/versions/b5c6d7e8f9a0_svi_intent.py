"""add svi_intent (SVI/IRB write path)

Revision ID: b5c6d7e8f9a0
Revises: a4b5c6d7e8f9
Create Date: 2026-06-09

Write-path intent mirror for L3 VLAN interfaces the operator accepted. The single
device Apply commits these via the svi-reconciler NSO service.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "b5c6d7e8f9a0"
down_revision = "a4b5c6d7e8f9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "svi_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("vlan_id", sa.Integer(), nullable=False),
        sa.Column("svi_type", sa.String(length=8), nullable=False, server_default="svi"),
        sa.Column("vrf", sa.String(length=128), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_sviintent_identity"),
    )
    op.create_index(op.f("ix_svi_intent_device_id"), "svi_intent", ["device_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_svi_intent_device_id"), table_name="svi_intent")
    op.drop_table("svi_intent")
