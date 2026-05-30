# SPDX-License-Identifier: Apache-2.0
"""phase2_m5_intent_and_settings

Revision ID: a1b2c3d4e5f6
Revises: 30321bde3fc8
Create Date: 2026-05-23 18:00:00.000000

Adds:
  - device_settings table (per-device auto_apply flag)
  - interface_intent table (intent mirror for apply worker)
  - jobs.context column (apply job snapshot)
  - accepted / deploying / apply_failed compliance statuses (enum extension)
"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = 'a1b2c3d4e5f6'
down_revision: str | Sequence[str] | None = '30321bde3fc8'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'device_settings',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('device_id', sa.Integer(), nullable=False),
        sa.Column('auto_apply', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['device_id'], ['devices.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('device_id'),
    )

    op.create_table(
        'interface_intent',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('interface_id', sa.Integer(), nullable=False),
        sa.Column('attribute', sa.String(64), nullable=False),
        sa.Column('intent_value', sa.Text(), nullable=True),
        sa.Column('accepted_at', sa.DateTime(), nullable=True),
        sa.Column('last_apply_at', sa.DateTime(), nullable=True),
        sa.Column('last_apply_error', sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(['interface_id'], ['interfaces.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('interface_id', 'attribute'),
    )
    op.create_index('ix_interface_intent_interface_id', 'interface_intent', ['interface_id'])

    op.add_column('jobs', sa.Column('context', sa.JSON(), nullable=True))

    # SQLite does not support ALTER COLUMN to change enum values.
    # The ORM uses String under the hood for SQLite, so existing rows are fine.
    # The new enum values (accepted, deploying, apply_failed) are recognised
    # automatically once the Python Enum is updated.


def downgrade() -> None:
    op.drop_column('jobs', 'context')
    op.drop_index('ix_interface_intent_interface_id', table_name='interface_intent')
    op.drop_table('interface_intent')
    op.drop_table('device_settings')
