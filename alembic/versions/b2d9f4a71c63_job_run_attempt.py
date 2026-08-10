# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The per-execution run-attempt token on ``jobs`` (Appendix S §3.1).

A status-only predicate cannot identify the execution that owns a terminal write:
recovery requeues ``running -> queued`` and a successor re-enters ``running``, so
``queued|running`` suppresses the mandated rerun and ``running``-only clobbers the
successor. The token is bumped in the same UPDATE that starts a run and compared by
every terminal write.

INTEGER, not a uuid: the value only has to differ between successive executions of one
job row, and the row itself is the scope. NOT NULL DEFAULT 0 so existing rows carry a
value no live registration can name — an in-flight run at upgrade time is at attempt 0
and its terminal write is refused, which recovery then re-dispositions.

Revision ID: b2d9f4a71c63
Revises: c1b6e93a4d27
Create Date: 2026-08-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b2d9f4a71c63"
down_revision: str | Sequence[str] | None = "c1b6e93a4d27"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("run_attempt", sa.Integer(), nullable=False, server_default=sa.text("0")),
    )


def downgrade() -> None:
    op.drop_column("jobs", "run_attempt")
