# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""IS-IS segment-routing read provenance

Revision ID: a1c4e7f9b2d5
Revises: e8c1a4f7b2d9
Create Date: 2026-07-26 18:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a1c4e7f9b2d5"
down_revision: str | None = "e8c1a4f7b2d9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "device_isis_process",
        sa.Column("segment_routing_reported", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "device_isis_process",
        sa.Column("segment_routing_configured", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("device_isis_process", "segment_routing_configured")
    op.drop_column("device_isis_process", "segment_routing_reported")
