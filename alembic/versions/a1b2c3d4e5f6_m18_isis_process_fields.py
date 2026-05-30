# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""M18 A2: add metric_style, overload_bit, area/domain auth fields to device_isis_process.

Revision ID: a1b2c3d4e5f6
Revises: f8a9b0c1d2e3
Create Date: 2026-07-15 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "a1b2c3d4e5f6"
down_revision = "f8a9b0c1d2e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("device_isis_process", sa.Column("metric_style", sa.String(32), nullable=True))
    op.add_column("device_isis_process", sa.Column("overload_bit", sa.Boolean, nullable=True))
    op.add_column("device_isis_process", sa.Column("area_auth_type", sa.String(32), nullable=True))
    op.add_column("device_isis_process", sa.Column("area_auth_present", sa.Boolean, nullable=True))
    op.add_column("device_isis_process", sa.Column("domain_auth_type", sa.String(32), nullable=True))
    op.add_column("device_isis_process", sa.Column("domain_auth_present", sa.Boolean, nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_process", "domain_auth_present")
    op.drop_column("device_isis_process", "domain_auth_type")
    op.drop_column("device_isis_process", "area_auth_present")
    op.drop_column("device_isis_process", "area_auth_type")
    op.drop_column("device_isis_process", "overload_bit")
    op.drop_column("device_isis_process", "metric_style")
