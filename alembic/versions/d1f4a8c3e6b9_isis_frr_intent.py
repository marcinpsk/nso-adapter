# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""isis FRR intent columns (#83 write path)

Write-path intent for the FRR/TI-LFA pipeline: per-interface tri-state
``frr_enabled`` + ``frr_protection`` (link | node) on isis_interface_intent,
per-process ``fast_reroute`` (lfa | remote-lfa | ti-lfa) + tri-state
``microloop_avoidance`` on isis_process_intent — the write mirror of the
read columns added in c9e2f5a7b1d3.

Revision ID: d1f4a8c3e6b9
Revises: c9e2f5a7b1d3
Create Date: 2026-07-11 00:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d1f4a8c3e6b9"
down_revision: str | None = "c9e2f5a7b1d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("isis_interface_intent", sa.Column("frr_enabled", sa.Boolean(), nullable=True))
    op.add_column("isis_interface_intent", sa.Column("frr_protection", sa.String(length=8), nullable=True))
    op.add_column("isis_process_intent", sa.Column("fast_reroute", sa.String(length=16), nullable=True))
    op.add_column("isis_process_intent", sa.Column("microloop_avoidance", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("isis_process_intent", "microloop_avoidance")
    op.drop_column("isis_process_intent", "fast_reroute")
    op.drop_column("isis_interface_intent", "frr_protection")
    op.drop_column("isis_interface_intent", "frr_enabled")
