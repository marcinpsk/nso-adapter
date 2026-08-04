# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The pusher's per-route intent generation (#1396 R3 §4.5).

BIGINT because the plugin allocates from a database-global sequence that is never reset,
so the value space has to outlive every overlay and management row recreation.

Nullable with no backfill and no server default: NULL means "this row predates the
generation contract", which is what keeps an uncorrelated result from settling. ``0`` is
the plugin's unallocated sentinel and must never appear here by accident.

Revision ID: c1b6e93a4d27
Revises: a4e7c2b90f31
Create Date: 2026-08-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c1b6e93a4d27"
down_revision: str | Sequence[str] | None = "a4e7c2b90f31"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("static_route_intent", sa.Column("intent_generation", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("static_route_intent", "intent_generation")
