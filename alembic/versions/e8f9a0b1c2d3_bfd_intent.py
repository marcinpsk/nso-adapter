"""add bfd_intent (per-interface BFD write path)

Revision ID: e8f9a0b1c2d3
Revises: d7e8f9a0b1c2
Create Date: 2026-06-09

Write-path intent mirror for per-interface BFD timers the operator accepted. The
single device Apply commits these via the bfd-reconciler service.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "e8f9a0b1c2d3"
down_revision = "d7e8f9a0b1c2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bfd_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("min_tx", sa.Integer(), nullable=True),
        sa.Column("min_rx", sa.Integer(), nullable=True),
        sa.Column("multiplier", sa.Integer(), nullable=True),
        sa.Column("micro_bfd", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "interface_name", name="uq_bfdintent_identity"),
    )
    op.create_index(op.f("ix_bfd_intent_device_id"), "bfd_intent", ["device_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_bfd_intent_device_id"), table_name="bfd_intent")
    op.drop_table("bfd_intent")
