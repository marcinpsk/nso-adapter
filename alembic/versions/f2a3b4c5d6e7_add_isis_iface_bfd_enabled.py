# SPDX-License-Identifier: Apache-2.0
"""add device_isis_interface.bfd_enabled

network-state-export now exports whether BFD is enabled for IS-IS on each
interface (IOS `isis bfd`, Junos `bfd-liveness-detection`, Nokia `bfd-liveness`).

Revision ID: f2a3b4c5d6e7
Revises: e1f2a3b4c5d6
Create Date: 2026-06-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f2a3b4c5d6e7"
down_revision: str | Sequence[str] | None = "e1f2a3b4c5d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_interface", sa.Column("bfd_enabled", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_interface", "bfd_enabled")
