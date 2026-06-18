"""device_failover (mgmt-IP failover state)

Revision ID: f1a2b3c4d5e6
Revises: 1d3b747848f7
Create Date: 2026-06-18 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f1a2b3c4d5e6"
down_revision: str | Sequence[str] | None = "1d3b747848f7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "device_failover",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("primary_ip", sa.String(length=64), nullable=True),
        sa.Column("oob_ip", sa.String(length=64), nullable=True),
        sa.Column("active_address", sa.String(length=16), server_default=sa.text("'primary'"), nullable=False),
        sa.Column("consecutive_failures", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("consecutive_successes", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("manual_override", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("oob_healthy", sa.Boolean(), nullable=True),
        sa.Column("oob_health_checked_at", sa.DateTime(), nullable=True),
        sa.Column("last_probe_at", sa.DateTime(), nullable=True),
        sa.Column("last_probe_result", sa.String(length=16), nullable=True),
        sa.Column("last_switch_at", sa.DateTime(), nullable=True),
        sa.Column("next_primary_probe_at", sa.DateTime(), nullable=True),
        sa.Column("next_oob_probe_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    # device_id is the FK + the one-row-per-device guarantee, realized as a unique index
    # (mirrors device_settings — keeps create_all ≡ alembic for the parity test).
    op.create_index(op.f("ix_device_failover_device_id"), "device_failover", ["device_id"], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_device_failover_device_id"), table_name="device_failover")
    op.drop_table("device_failover")
