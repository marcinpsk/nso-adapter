"""device_capability matrix

Revision ID: 9a4b853376a2
Revises: f8a9b0c1d2e3
Create Date: 2026-06-14

Per-(ned_id, sw_version) route-policy capability cache (the compatibility matrix),
persisted so it survives an adapter restart. Rows carry source='probe' (representable
half, from the NSO capability-probe action) or source='apply' (accepted half, from a
real device-parser rejection). Keyed by (ned_id, sw_version, scope, name).
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "9a4b853376a2"
down_revision = "f8a9b0c1d2e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "device_capability",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ned_id", sa.String(length=256), nullable=False),
        sa.Column("sw_version", sa.String(length=128), nullable=False),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("detail", sa.String(length=256), nullable=False),
        sa.Column("source", sa.String(length=16), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ned_id", "sw_version", "scope", "name", name="uq_device_capability"),
    )
    op.create_index("ix_device_capability_ned_id", "device_capability", ["ned_id"])
    op.create_index("ix_device_capability_sw_version", "device_capability", ["sw_version"])


def downgrade() -> None:
    op.drop_index("ix_device_capability_sw_version", table_name="device_capability")
    op.drop_index("ix_device_capability_ned_id", table_name="device_capability")
    op.drop_table("device_capability")
