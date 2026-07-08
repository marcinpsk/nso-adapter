# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""isis process attached-bit knobs

ATT-bit instance knobs read from network-state-export's per-process
``suppress-attached-bit`` / ``ignore-attached-bit`` leaves -> netbox_routing
ISISInstance.suppress_attached_bit / ignore_attached_bit. Stored as nullable
booleans on device_isis_process, mirroring te_enabled.

Revision ID: f2a9c4e1b8d7
Revises: e1f4a7c2b9d3
Create Date: 2026-07-09 09:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f2a9c4e1b8d7"
down_revision: str | None = "e1f4a7c2b9d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_process", sa.Column("suppress_attached_bit", sa.Boolean(), nullable=True))
    op.add_column("device_isis_process", sa.Column("ignore_attached_bit", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_process", "ignore_attached_bit")
    op.drop_column("device_isis_process", "suppress_attached_bit")
