"""add interface_mtu_intent (Phase 2b MTU write path)

Revision ID: c5d6e7f8a9b0
Revises: b3c4d5e6f7a8
Create Date: 2026-06-12

Write-path intent for per-interface MTU (mtu / ip-mtu / mpls-mtu) accepted by the
operator. Applied to the device via the mtu-reconciler NSO service.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c5d6e7f8a9b0"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "interface_mtu_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("mtu", sa.Integer(), nullable=True),
        sa.Column("ip_mtu", sa.Integer(), nullable=True),
        sa.Column("mpls_mtu", sa.Integer(), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_ifmtuintent_identity"),
    )
    op.create_index(
        op.f("ix_interface_mtu_intent_device_id"), "interface_mtu_intent", ["device_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_interface_mtu_intent_device_id"), table_name="interface_mtu_intent")
    op.drop_table("interface_mtu_intent")
