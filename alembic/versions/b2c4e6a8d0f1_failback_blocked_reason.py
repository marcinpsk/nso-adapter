# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Surface a stuck failback on the failover row (#1630).

Revision ID: b2c4e6a8d0f1
Revises: a6d4f2c8e1b3
Create Date: 2026-08-28
"""

import sqlalchemy as sa

from alembic import op

revision = "b2c4e6a8d0f1"
down_revision = "a6d4f2c8e1b3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("device_failover", sa.Column("failback_blocked_reason", sa.String(length=32), nullable=True))


def downgrade() -> None:
    op.drop_column("device_failover", "failback_blocked_reason")
