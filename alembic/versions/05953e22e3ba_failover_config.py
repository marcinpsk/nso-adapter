"""failover_config (global mgmt-IP failover tuning singleton)

Revision ID: 05953e22e3ba
Revises: f1a2b3c4d5e6
Create Date: 2026-06-18 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "05953e22e3ba"
down_revision: str | Sequence[str] | None = "f1a2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "failover_config",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("primary_probe_interval", sa.Integer(), server_default=sa.text("15"), nullable=False),
        sa.Column("oob_probe_interval", sa.Integer(), server_default=sa.text("360"), nullable=False),
        sa.Column("failure_threshold", sa.Integer(), server_default=sa.text("3"), nullable=False),
        sa.Column("success_threshold", sa.Integer(), server_default=sa.text("5"), nullable=False),
        sa.Column("probe_timeout", sa.Float(), server_default=sa.text("10.0"), nullable=False),
        sa.Column("probe_concurrency", sa.Integer(), server_default=sa.text("8"), nullable=False),
        sa.Column("max_flips_per_tick", sa.Integer(), server_default=sa.text("8"), nullable=False),
        sa.Column("sync_from_after_switch", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("failover_config")
