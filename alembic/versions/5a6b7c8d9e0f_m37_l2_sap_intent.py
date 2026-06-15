"""m37 l2 sap intent (P2b write path)

Revision ID: 5a6b7c8d9e0f
Revises: 4625760d7784
Create Date: 2026-06-06 13:45:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5a6b7c8d9e0f"
down_revision: str | Sequence[str] | None = "4625760d7784"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "l2_sap_intent",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("service_name", sa.String(length=64), nullable=False),
        sa.Column("service_type", sa.String(length=16), nullable=False),
        sa.Column("sap_id", sa.String(length=64), nullable=False),
        sa.Column("port", sa.String(length=64), nullable=False),
        sa.Column("outer_tag", sa.Integer(), nullable=True),
        sa.Column("inner_tag", sa.Integer(), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_apply_error", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "service_name", "sap_id", name="uq_l2sapintent_identity"),
    )
    op.create_index(op.f("ix_l2_sap_intent_device_id"), "l2_sap_intent", ["device_id"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_l2_sap_intent_device_id"), table_name="l2_sap_intent")
    op.drop_table("l2_sap_intent")
