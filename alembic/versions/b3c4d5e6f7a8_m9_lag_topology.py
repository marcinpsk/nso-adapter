# SPDX-License-Identifier: Apache-2.0
"""m9_lag_topology

Revision ID: b3c4d5e6f7a8
Revises: a1b2c3d4e5f6
Create Date: 2026-05-28 12:00:00.000000

Adds:
  - lag_interface table (per-device LAG parent interfaces from NSO)
  - lag_member table (per-LAG member interface + LACP mode)
"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = 'b3c4d5e6f7a8'
down_revision: str | Sequence[str] | None = 'a1b2c3d4e5f6'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'lag_interface',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('device_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(256), nullable=False),
        sa.Column('lag_id', sa.Integer(), nullable=False),
        sa.Column('last_refreshed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('refresh_source', sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(['device_id'], ['devices.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('device_id', 'name', name='uq_laginterface_device_name'),
    )
    op.create_index('ix_lag_interface_device_id', 'lag_interface', ['device_id'])

    op.create_table(
        'lag_member',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('lag_interface_id', sa.Integer(), nullable=False),
        sa.Column('interface_name', sa.String(256), nullable=False),
        sa.Column('mode', sa.String(32), nullable=False),
        sa.ForeignKeyConstraint(['lag_interface_id'], ['lag_interface.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('lag_interface_id', 'interface_name', name='uq_lagmember_lag_iface'),
    )
    op.create_index('ix_lag_member_lag_interface_id', 'lag_member', ['lag_interface_id'])


def downgrade() -> None:
    op.drop_index('ix_lag_member_lag_interface_id', table_name='lag_member')
    op.drop_table('lag_member')
    op.drop_index('ix_lag_interface_device_id', table_name='lag_interface')
    op.drop_table('lag_interface')
