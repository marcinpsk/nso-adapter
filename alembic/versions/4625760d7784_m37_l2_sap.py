"""m37 l2 sap

Revision ID: 4625760d7784
Revises: d5e6f7a8b9c0
Create Date: 2026-06-06 06:37:26.179387

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4625760d7784'
down_revision: Union[str, Sequence[str], None] = 'd5e6f7a8b9c0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "device_l2_sap",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("service_name", sa.String(length=64), nullable=False),
        sa.Column("service_type", sa.String(length=16), nullable=False),
        sa.Column("service_id", sa.Integer(), nullable=True),
        sa.Column("sap_id", sa.String(length=64), nullable=False),
        sa.Column("port", sa.String(length=64), nullable=False),
        sa.Column("outer_tag", sa.Integer(), nullable=True),
        sa.Column("inner_tag", sa.Integer(), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_source", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "service_name", "sap_id", name="uq_devicel2sap_identity"),
    )
    op.create_index(op.f("ix_device_l2_sap_device_id"), "device_l2_sap", ["device_id"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_device_l2_sap_device_id"), table_name="device_l2_sap")
    op.drop_table("device_l2_sap")
