# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The backfill-only request mode joins the push-receipt identity (#1503 §4.4, O2b).

``intent_push_receipt`` already compares the two request-mode flags alongside the body
digest, because the body does not say what a request DOES with it. ``?backfill_only=true``
is a third such flag and the most divergent of them: the pass adopts ``route_id`` onto the
rows the payload still names, prunes the uncorrelated ``route_id IS NULL`` rows that hold a
device's replacement fence shut, and writes no content, no tombstone and no job at all. One
sequence carrying one body under that mode and under an ordinary push is two completely
different sets of rows, so without the column a mis-numbered backfill would REPLAY the
ordinary push's stored response and apply nothing.

One column, ``backfill_only BOOLEAN NOT NULL DEFAULT false``. Every existing receipt was
admitted before the mode existed, so the server default states their truth exactly and no
backfill is needed.

Revision ID: b7d5e2f18c40
Revises: a4e1c7b09f52
Create Date: 2026-08-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b7d5e2f18c40"
down_revision: str | None = "a4e1c7b09f52"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "intent_push_receipt",
        sa.Column("backfill_only", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )


def downgrade() -> None:
    op.drop_column("intent_push_receipt", "backfill_only")
