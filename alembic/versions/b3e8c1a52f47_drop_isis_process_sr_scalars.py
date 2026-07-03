# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""drop stale device_isis_process sr_enabled/sr_node_msd scalars

Segment-routing state moved to the ``segment_routing`` JSON bag (the plugin
reconciles it into the ISISSegmentRouting 1:1 child). The top-level sr_enabled/
sr_node_msd read-surface scalars were left behind, still written by the reader but
consumed by nothing — stale columns that drift from the plugin overlay. Drop them.

Revision ID: b3e8c1a52f47
Revises: 9d2f4e6a1c3b
Create Date: 2026-07-02 14:20:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b3e8c1a52f47"
down_revision: str | Sequence[str] | None = "9d2f4e6a1c3b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column("device_isis_process", "sr_enabled")
    op.drop_column("device_isis_process", "sr_node_msd")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column("device_isis_process", sa.Column("sr_node_msd", sa.Integer(), nullable=True))
    op.add_column("device_isis_process", sa.Column("sr_enabled", sa.Boolean(), nullable=True))
