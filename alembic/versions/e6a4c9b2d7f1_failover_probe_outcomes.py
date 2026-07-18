# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Structured failover probe outcomes and active-address timeout.

Revision ID: e6a4c9b2d7f1
Revises: d4b1e9c3a705
Create Date: 2026-07-18
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e6a4c9b2d7f1"
down_revision: str | Sequence[str] | None = "d4b1e9c3a705"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("device_failover", sa.Column("oob_health_result", sa.String(length=16), nullable=True))
    op.add_column("device_failover", sa.Column("oob_health_detail", sa.Text(), nullable=True))
    op.add_column("device_failover", sa.Column("last_probe_target", sa.String(length=16), nullable=True))
    op.add_column("device_failover", sa.Column("last_probe_detail", sa.Text(), nullable=True))
    op.add_column(
        "failover_config",
        sa.Column("active_probe_timeout", sa.Float(), server_default=sa.text("45.0"), nullable=False),
    )

    # The old boolean/ok-fail fields cannot distinguish a timeout from genuine
    # unreachability. Preserve successes, but label historical failures unknown rather
    # than manufacturing certainty the old schema never recorded.
    op.execute("UPDATE device_failover SET last_probe_result = 'unknown' WHERE last_probe_result = 'fail'")
    op.execute(
        "UPDATE device_failover SET oob_health_result = CASE "
        "WHEN oob_healthy = true THEN 'ok' "
        "WHEN oob_healthy = false THEN 'unknown' "
        "ELSE NULL END"
    )


def downgrade() -> None:
    op.execute("UPDATE device_failover SET last_probe_result = 'fail' WHERE last_probe_result != 'ok'")
    op.drop_column("failover_config", "active_probe_timeout")
    op.drop_column("device_failover", "last_probe_detail")
    op.drop_column("device_failover", "last_probe_target")
    op.drop_column("device_failover", "oob_health_detail")
    op.drop_column("device_failover", "oob_health_result")
