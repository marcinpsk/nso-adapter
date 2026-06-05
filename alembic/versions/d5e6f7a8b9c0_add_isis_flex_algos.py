# SPDX-License-Identifier: Apache-2.0
"""add IS-IS flex-algo mirror column (M33 P2b)

network-state-export now exports a flex-algo list per IS-IS process. Mirror it
as a JSON bag so the plugin can reconcile ISISFlexAlgo child rows.

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-06-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d5e6f7a8b9c0"
down_revision: str | Sequence[str] | None = "c4d5e6f7a8b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_process", sa.Column("flex_algos", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_process", "flex_algos")
