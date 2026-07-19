# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Add lag_bundle_config.vpc_sensitive (NX-P2 vPC preserve/REFUSE).

Carries the network-state-export reader's per-bundle vPC flag through the adapter so the
plugin can gate/badge a vPC-protected LAG (the lag-reconciler refuses it zero-write, so it
must never be offered for accept). Absent/False = ordinary, onboardable bundle.

Revision ID: f7c2a9e10b34
Revises: e6a4c9b2d7f1
Create Date: 2026-07-19
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f7c2a9e10b34"
down_revision: str | Sequence[str] | None = "e6a4c9b2d7f1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "lag_bundle_config",
        sa.Column("vpc_sensitive", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )


def downgrade() -> None:
    op.drop_column("lag_bundle_config", "vpc_sensitive")
