# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""isis FRR mirror columns (#83)

FRR/TI-LFA read mirror from network-state-export: per-process ``fast-reroute``
(lfa | remote-lfa | ti-lfa) + ``microloop-avoidance`` -> netbox_routing
ISISInstance.fast_reroute / microloop_avoidance, and per-interface tri-state
``frr-enabled`` + ``frr-protection`` (link | node) -> ISISInterface.frr_enabled /
frr_protection. Nullable, mirroring the attached-bit knobs.

Revision ID: c9e2f5a7b1d3
Revises: b7d9e2f4a6c8
Create Date: 2026-07-10 22:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c9e2f5a7b1d3"
down_revision: str | None = "b7d9e2f4a6c8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_isis_process", sa.Column("fast_reroute", sa.String(length=16), nullable=True))
    op.add_column("device_isis_process", sa.Column("microloop_avoidance", sa.Boolean(), nullable=True))
    op.add_column("device_isis_interface", sa.Column("frr_enabled", sa.Boolean(), nullable=True))
    op.add_column("device_isis_interface", sa.Column("frr_protection", sa.String(length=8), nullable=True))


def downgrade() -> None:
    op.drop_column("device_isis_interface", "frr_protection")
    op.drop_column("device_isis_interface", "frr_enabled")
    op.drop_column("device_isis_process", "microloop_avoidance")
    op.drop_column("device_isis_process", "fast_reroute")
