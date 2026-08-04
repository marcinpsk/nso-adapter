# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The durable carrier for a static-route clear no job could deliver (#1396 R2 §4.11).

A cleared leaf leaves the device only via a networked PUT. Two paths cannot issue one — a
clear riding with an unmarked shrink becomes a ``no-networking`` detach, and a failed or
sweeper-re-issued removal rebuilds its context from the tombstone, which carries no clear —
so without a store-side carrier the next apply reports the row in sync while the old value
is still live.

JSONB, two lists keyed by whether the clearing push may authorize a device write:
``{"authorized": [...], "store_only": [...]}``. Nullable with no backfill: R1-era deferred
clears are a separate subject (the brief's Appendix D) and cannot be reconstructed from the
device-wide boolean R1 kept.

Revision ID: a4e7c2b90f31
Revises: b8d4f1c2e7a3
Create Date: 2026-08-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "a4e7c2b90f31"
down_revision: str | Sequence[str] | None = "b8d4f1c2e7a3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "static_route_intent",
        sa.Column("pending_clear", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("static_route_intent", "pending_clear")
