"""add enabled (OSPF process admin-state) to ospf instance store + intent

Revision ID: a1b2c3d4e5f6
Revises: f9a0b1c2d3e4
Create Date: 2026-06-12

Captures the OSPF process admin-state (Nokia SR OS 'admin-state enable') on both
the read mirror (device_ospf_instance) and the write-path intent
(ospf_instance_intent), so the reconcile can detect and re-assert the enable.
Nullable: None when the NED has no explicit admin-state (process enabled by
config presence).
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "f9a0b1c2d3e4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("device_ospf_instance", sa.Column("enabled", sa.Boolean(), nullable=True))
    op.add_column("ospf_instance_intent", sa.Column("enabled", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("ospf_instance_intent", "enabled")
    op.drop_column("device_ospf_instance", "enabled")
