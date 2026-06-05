# SPDX-License-Identifier: Apache-2.0
"""add IS-IS P2 per-level + segment-routing mirror columns (M33 P2)

network-state-export now exports per-level child lists (process/level and
interface/level) and a segment-routing object. Mirror them as JSON bags on the
read tables so the plugin can reconcile them into the netbox_routing
ISISLevel / ISISInterfaceLevel / ISISSegmentRouting child tables.

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-06-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c4d5e6f7a8b9"
down_revision: str | Sequence[str] | None = "b2c3d4e5f6a7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_process", sa.Column("levels", sa.JSON(), nullable=True))
    op.add_column("device_isis_process", sa.Column("segment_routing", sa.JSON(), nullable=True))
    op.add_column("device_isis_interface", sa.Column("levels", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_interface", "levels")
    op.drop_column("device_isis_process", "segment_routing")
    op.drop_column("device_isis_process", "levels")
