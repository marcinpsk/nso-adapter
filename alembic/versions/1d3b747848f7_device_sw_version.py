"""device.sw_version

Revision ID: 1d3b747848f7
Revises: 9a4b853376a2
Create Date: 2026-06-14

Persist the last platform version learned from a capability probe on each device,
so the capability cache can resolve a device's (ned_id, sw_version) key WITHOUT a
live probe — letting the plugin panel read verdicts cheaply (refresh=false).
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "1d3b747848f7"
down_revision = "9a4b853376a2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("devices", sa.Column("sw_version", sa.String(length=128), nullable=True))


def downgrade() -> None:
    op.drop_column("devices", "sw_version")
