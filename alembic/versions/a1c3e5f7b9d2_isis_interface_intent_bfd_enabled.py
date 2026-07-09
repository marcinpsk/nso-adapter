# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""isis interface intent bfd-enabled

Write-side extension (#77): bfd_enabled joins the IS-IS interface write intent so the
operator can enable/disable IS-IS BFD per interface. Nullable boolean (tri-state:
None = no opinion / NED default, True = enable, False = disable), mirroring the other
optional interface intent leaves (metric / circuit_type / network_type).

Revision ID: a1c3e5f7b9d2
Revises: f2a9c4e1b8d7
Create Date: 2026-07-09 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a1c3e5f7b9d2"
down_revision: str | None = "f2a9c4e1b8d7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("isis_interface_intent", sa.Column("bfd_enabled", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("isis_interface_intent", "bfd_enabled")
